#!/usr/bin/env python3
"""
IMAP Mail Migration Tool
========================
Переносит почту между любыми IMAP-серверами (Яндекс, Gmail, Mail.ru, свой сервер и др.).

Возможности:
  - Докачка (resume): пропускает уже перенесённые письма по Message-ID
  - Прогресс-бар с ETA, скоростью и объёмом (tqdm)
  - Верификация: подсчёт писем на источнике и приёмнике после переноса
  - Graceful shutdown по Ctrl+C с сохранением состояния; пауза по SIGUSR1 или файлу
  - Подсчёт и отображение размера перенесённых данных
  - Автомаппинг папок (Яндекс, Gmail, Mail.ru и др.) на стандартные IMAP-имена
  - Сохранение флагов и INTERNALDATE
  - Автопереподключение при обрывах (с экспоненциальным backoff)
  - Подробный итоговый отчёт с разбивкой по папкам
  - Dry-run режим

Использование:
  python imap_migrate.py --config config.yaml --list-folders
  python imap_migrate.py --config config.yaml --dry-run
  python imap_migrate.py --config config.yaml
  python imap_migrate.py --config config.yaml --verify
  python imap_migrate.py --config config.yaml --folders INBOX,Sent
"""

import imaplib
import json
import logging
import argparse
import os
import signal
import ssl
import sys
import time
import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional, TypedDict

try:
    import yaml
    HAS_YAML = True
except ImportError:
    HAS_YAML = False

try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False

# ---------------------------------------------------------------------------
# IMAP: увеличиваем лимит строки (по умолчанию 10 000 -- мало для больших писем)
# ---------------------------------------------------------------------------
if hasattr(imaplib, "_MAXLINE"):
    imaplib._MAXLINE = 10_000_000

# ---------------------------------------------------------------------------
# ANSI-цвета для консоли
# ---------------------------------------------------------------------------
class Colors:
    """ANSI color codes. Автоотключение если stdout не терминал."""
    _enabled = hasattr(sys.stdout, "isatty") and sys.stdout.isatty()

    RESET  = "\033[0m"  if _enabled else ""
    BOLD   = "\033[1m"  if _enabled else ""
    DIM    = "\033[2m"  if _enabled else ""
    GREEN  = "\033[32m" if _enabled else ""
    YELLOW = "\033[33m" if _enabled else ""
    RED    = "\033[31m" if _enabled else ""
    CYAN   = "\033[36m" if _enabled else ""
    BLUE   = "\033[34m" if _enabled else ""
    MAG    = "\033[35m" if _enabled else ""


def human_size(nbytes: int | float) -> str:
    """Человекочитаемый размер: 1234567 -> '1.18 MB'."""
    for unit in ("B", "KB", "MB", "GB"):
        if abs(nbytes) < 1024:
            return f"{nbytes:.1f} {unit}" if unit != "B" else f"{int(nbytes)} B"
        nbytes /= 1024
    return f"{nbytes:.2f} TB"


def human_duration(seconds: float) -> str:
    """Человекочитаемая длительность."""
    if seconds < 60:
        return f"{seconds:.0f} сек"
    elif seconds < 3600:
        m, s = divmod(int(seconds), 60)
        return f"{m} мин {s} сек"
    else:
        h, rem = divmod(int(seconds), 3600)
        m, s = divmod(rem, 60)
        return f"{h} ч {m} мин"


def friendly_error(e: Exception) -> str:
    """Returns a user-friendly message for known connection/IMAP errors."""
    msg = str(e).strip()
    if "Broken pipe" in msg or "Errno 32" in msg:
        return "Соединение разорвано (Broken pipe)"
    if "nodename nor servname" in msg or "Errno 8" in msg:
        return "DNS-ошибка: сервер недоступен"
    if "EOF" in msg or "Connection reset" in msg:
        return "Сервер закрыл соединение"
    if "timed out" in msg or "timeout" in msg.lower():
        return "Таймаут соединения"
    if "Connection refused" in msg:
        return "Подключение отклонено (порт закрыт или сервер недоступен)"
    return msg


# ---------------------------------------------------------------------------
# Logging -- файловый + консольный (через tqdm.write чтобы не ломать прогресс)
# ---------------------------------------------------------------------------
class TqdmLoggingHandler(logging.StreamHandler):
    """Handler, который пишет через tqdm.write() чтобы не ломать прогресс-бар."""
    def emit(self, record):
        try:
            msg = self.format(record)
            if HAS_TQDM:
                tqdm.write(msg, file=self.stream)
            else:
                self.stream.write(msg + self.terminator)
            self.flush()
        except Exception:
            self.handleError(record)


LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def setup_logging(log_file: Optional[str] = None, verbose: bool = False):
    level = logging.DEBUG if verbose else logging.INFO
    root = logging.getLogger()
    root.setLevel(level)
    root.handlers.clear()

    console = TqdmLoggingHandler(sys.stdout)
    console.setFormatter(logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT))
    root.addHandler(console)

    if log_file:
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setFormatter(logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT))
        root.addHandler(fh)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------
@dataclass
class ServerConfig:
    host: str = ""
    port: int = 993
    user: str = ""
    password: str = ""
    ssl: bool = True
    starttls: bool = False

    def __repr__(self) -> str:
        pwd = "***" if self.password else ""
        return (f"ServerConfig(host={self.host!r}, port={self.port}, "
                f"user={self.user!r}, password={pwd!r}, "
                f"ssl={self.ssl}, starttls={self.starttls})")


@dataclass
class MigrationConfig:
    source: ServerConfig = field(default_factory=ServerConfig)
    destination: ServerConfig = field(default_factory=ServerConfig)
    batch_limit: int = 0
    state_file: str = "migration_state.json"
    log_file: str = "migration.log"
    folder_map: dict[str, str] = field(default_factory=dict)
    exclude_folders: list[str] = field(default_factory=list)
    only_folders: list[str] = field(default_factory=list)
    timeout: int = 120
    throttle: float = 0.05
    max_retries: int = 3
    verbose: bool = False
    verify: bool = False
    scan_batch_size: int = 100
    folder_retries: int = 3
    noop_interval: int = 30
    reconnect_max_wait: int = 300
    status_interval: int = 600
    use_builtin_map: bool = True
    exclude_flags: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Built-in folder name mapping (Yandex, Mail.ru, Gmail, etc.) -> standard IMAP
# ---------------------------------------------------------------------------
BUILTIN_FOLDER_MAP = {
    "Входящие": "INBOX",
    "Отправленные": "Sent",
    "Черновики": "Drafts",
    "Спам": "Junk",
    "Удалённые": "Trash",
    "Удаленные": "Trash",
    "Sent": "Sent",
    "Drafts": "Drafts",
    "Spam": "Junk",
    "Trash": "Trash",
    "Outbox": "Outbox",
    "INBOX": "INBOX",
    "[Gmail]/Sent Mail": "Sent",
    "[Gmail]/Drafts": "Drafts",
    "[Gmail]/Trash": "Trash",
    "[Gmail]/Spam": "Junk",
    "Корзина": "Trash",
}


# ---------------------------------------------------------------------------
# State management -- докачка
# ---------------------------------------------------------------------------
class MigrationState:
    """Хранит Message-ID уже перенесённых писем + статистику по папкам + UID-кэш для быстрого resume."""

    def __init__(self, state_file: str):
        self.state_file = Path(state_file)
        self.migrated: dict[str, set[str]] = {}
        self.folder_stats: dict[str, dict] = {}
        self.uid_cache: dict[str, set[str]] = {}
        self.uidvalidity: dict[str, str] = {}
        self._load()

    def _load(self):
        if self.state_file.exists():
            try:
                data = json.loads(self.state_file.read_text(encoding="utf-8"))
                for folder, ids in data.get("migrated", data).items():
                    if isinstance(ids, list):
                        self.migrated[folder] = set(ids)
                self.folder_stats = data.get("folder_stats", {})
                for folder, uids in data.get("uid_cache", {}).items():
                    if isinstance(uids, list):
                        self.uid_cache[folder] = set(uids)
                self.uidvalidity = data.get("uidvalidity", {})
                total = sum(len(v) for v in self.migrated.values())
                logging.info(
                    f"Загружено состояние: {Colors.CYAN}{total}{Colors.RESET} писем "
                    f"в {len(self.migrated)} папках"
                )
            except Exception as e:
                logging.error(f"Не удалось загрузить state-файл: {e}")

    def save(self):
        data = {
            "migrated": {f: sorted(ids) for f, ids in self.migrated.items()},
            "folder_stats": self.folder_stats,
            "uid_cache": {f: sorted(uids) for f, uids in self.uid_cache.items()},
            "uidvalidity": self.uidvalidity,
            "saved_at": datetime.now().isoformat(),
        }
        tmp = self.state_file.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=1), encoding="utf-8")
        tmp.replace(self.state_file)

    def is_migrated(self, folder: str, message_id: str) -> bool:
        return message_id in self.migrated.get(folder, set())

    def mark_migrated(
        self, folder: str, message_id: str, msg_size: int = 0, src_uid: Optional[str] = None
    ):
        self.migrated.setdefault(folder, set()).add(message_id)
        fs = self.folder_stats.setdefault(folder, {"count": 0, "bytes": 0})
        fs["count"] += 1
        fs["bytes"] += msg_size
        if src_uid:
            self.uid_cache.setdefault(folder, set()).add(src_uid)

    def get_cached_uids(self, dst_folder: str) -> set[str]:
        return self.uid_cache.get(dst_folder, set()).copy()

    def set_uidvalidity(self, src_folder: str, value: str):
        self.uidvalidity[src_folder] = value

    def get_uidvalidity(self, src_folder: str) -> Optional[str]:
        return self.uidvalidity.get(src_folder)

    def invalidate_uid_cache(self, dst_folder: str, src_folder: str):
        self.uid_cache.pop(dst_folder, None)
        self.uidvalidity.pop(src_folder, None)

    def count(self, folder: str) -> int:
        return len(self.migrated.get(folder, set()))

    def total_bytes(self) -> int:
        return sum(s.get("bytes", 0) for s in self.folder_stats.values())


# ---------------------------------------------------------------------------
# IMAP helpers
# ---------------------------------------------------------------------------
def connect_imap(cfg: ServerConfig, timeout: int = 120) -> imaplib.IMAP4_SSL | imaplib.IMAP4:
    """Подключение и логин к IMAP-серверу."""
    logging.debug(f"Подключение к {cfg.host}:{cfg.port} (user={cfg.user})")
    ctx = ssl.create_default_context() if (cfg.ssl or cfg.starttls) else None

    if cfg.ssl:
        conn = imaplib.IMAP4_SSL(cfg.host, cfg.port, ssl_context=ctx, timeout=timeout)
    else:
        conn = imaplib.IMAP4(cfg.host, cfg.port, timeout=timeout)
        if cfg.starttls and ctx:
            conn.starttls(ssl_context=ctx)

    conn.login(cfg.user, cfg.password)
    logging.debug(f"Логин OK: {cfg.user}@{cfg.host}")
    return conn


def list_folders(conn: imaplib.IMAP4) -> list[str]:
    """Получить список папок (с разбором LIST-ответа)."""
    status, data = conn.list()
    if status != "OK":
        raise RuntimeError(f"Не удалось получить список папок: {status}")

    folders = []
    # Яндекс LIST формат варьируется:
    #   (\Flags) "." "FolderName"
    #   (\Flags) "|" FolderName
    #   (\Flags) "." INBOX
    # Используем надёжный пошаговый парсинг вместо одного regex.
    for item in data:
        if item is None:
            continue
        raw = item if isinstance(item, bytes) else item[0] if isinstance(item, tuple) else item
        if not isinstance(raw, bytes):
            continue

        try:
            line = raw.decode("utf-8")
        except UnicodeDecodeError:
            line = raw.decode("latin-1")

        logging.debug(f"  LIST raw: {line!r}")

        # Шаг 1: извлекаем флаги в скобках
        flags_match = re.match(r'\(([^)]*)\)', line)
        if not flags_match:
            continue
        flags_str = flags_match.group(1)
        rest = line[flags_match.end():].strip()

        # Пропускаем \Noselect
        if "\\Noselect" in flags_str or "\\NoSelect" in flags_str:
            continue

        # Шаг 2: извлекаем разделитель (в кавычках) и имя папки
        # Варианты rest:
        #   "." "FolderName"
        #   "." FolderName
        #   "|" "Folder Name"
        #   NIL "INBOX"
        delim_match = re.match(r'(?:"[^"]*"|NIL)\s+(.*)', rest)
        if delim_match:
            name_part = delim_match.group(1).strip()
        else:
            # fallback: берём всё после первого пробела
            parts = rest.split(None, 1)
            name_part = parts[1].strip() if len(parts) > 1 else rest.strip()

        # Снимаем кавычки если есть
        if name_part.startswith('"') and name_part.endswith('"'):
            name_part = name_part[1:-1]

        if name_part:
            folders.append(name_part)

    return folders


def get_folder_uidvalidity(conn: imaplib.IMAP4, folder: str) -> Optional[str]:
    """Returns UIDVALIDITY for the folder (for cache validation)."""
    try:
        status, data = conn.status(f'"{folder}"', "(UIDVALIDITY)")
        if status == "OK" and data and data[0]:
            raw = data[0] if isinstance(data[0], bytes) else data[0].encode()
            m = re.search(rb"UIDVALIDITY\s+(\d+)", raw)
            if m:
                return m.group(1).decode()
    except Exception:
        pass
    return None


def folder_message_count(conn: imaplib.IMAP4, folder: str) -> int:
    """Количество писем в папке (быстрый SELECT)."""
    status, data = conn.select(f'"{folder}"', readonly=True)
    if status != "OK":
        return -1
    try:
        return int(data[0])
    except (ValueError, IndexError, TypeError):
        return -1


def decode_folder_name(name: str) -> str:
    """Декодирует IMAP modified UTF-7."""
    if "&" in name:
        try:
            return name.encode("ascii").decode("imap4-utf-7")
        except (UnicodeDecodeError, LookupError):
            pass
    return name


def fetch_message_ids_batch(
    conn: imaplib.IMAP4, uid_list: list[bytes]
) -> list[tuple[bytes, str | None]]:
    """
    Returns [(uid, message_id_header), ...] for the given UIDs.
    BODY.PEEK does not mark messages as read.
    """
    if not uid_list:
        return []
    uid_range = b",".join(uid_list)
    status, fetch_data = conn.uid("FETCH", uid_range.decode(), "(BODY.PEEK[HEADER.FIELDS (MESSAGE-ID)])")
    if status != "OK":
        return []

    results = []
    current_uid = None
    for item in fetch_data:
        if isinstance(item, tuple) and len(item) >= 2:
            header_line = item[0]
            if isinstance(header_line, bytes):
                uid_match = re.search(rb"UID (\d+)", header_line)
                if uid_match:
                    current_uid = uid_match.group(1)

            header_body = item[1]
            if isinstance(header_body, bytes):
                mid_match = re.search(rb"Message-ID:\s*(<[^>]+>)", header_body, re.IGNORECASE)
                mid = mid_match.group(1).decode("utf-8", errors="replace") if mid_match else None
                if current_uid:
                    results.append((current_uid, mid))
                current_uid = None
    return results


def fetch_full_message(conn: imaplib.IMAP4, uid: bytes) -> tuple[bytes | None, list[str], datetime | None]:
    """Скачивает полное письмо. Возвращает (raw_message, flags, internal_date)."""
    status, data = conn.uid("FETCH", uid.decode(), "(FLAGS INTERNALDATE RFC822)")
    if status != "OK" or not data or data[0] is None:
        return None, [], None

    raw_message = None
    flags = []
    internal_date = None

    for item in data:
        if isinstance(item, tuple) and len(item) >= 2:
            meta = item[0]
            if isinstance(meta, bytes):
                flags_match = re.search(rb"FLAGS \(([^)]*)\)", meta)
                if flags_match:
                    flags_raw = flags_match.group(1).decode("utf-8", errors="replace")
                    flags = [f.strip() for f in flags_raw.split() if f.strip()]

                date_match = re.search(rb'INTERNALDATE "([^"]+)"', meta)
                if date_match:
                    try:
                        parsed = imaplib.Internaldate2tuple(b'"' + date_match.group(1) + b'"')
                        if parsed:
                            internal_date = datetime(*parsed[:6])
                    except Exception as e:
                        logging.debug(f"Date parse error: {e}")

            raw_message = item[1] if isinstance(item[1], bytes) else None

    return raw_message, flags, internal_date


STANDARD_IMAP_FLAGS = frozenset({"\\Seen", "\\Answered", "\\Flagged", "\\Deleted", "\\Draft"})


def upload_message(
    conn: imaplib.IMAP4,
    folder: str,
    raw_message: bytes,
    flags: list[str],
    internal_date: datetime | None,
    exclude_flags: Optional[set] = None,
) -> bool:
    """Uploads message to destination folder. Keeps only standard IMAP flags; drops \\Recent and non-standard keywords (e.g. $NotJunk)."""
    exclude = exclude_flags or set()
    filtered = [
        f for f in flags
        if f not in exclude
        and not f.startswith("\\Recent")
        and (f in STANDARD_IMAP_FLAGS or not f.startswith("$"))
    ]
    flag_str = " ".join(filtered)
    flag_str = f"({flag_str})" if flag_str else "()"

    date_str = None
    if internal_date:
        date_str = imaplib.Time2Internaldate(time.mktime(internal_date.timetuple()))

    try:
        status, _ = conn.append(f'"{folder}"', flag_str, date_str, raw_message)
        return status == "OK"
    except Exception as e:
        logging.debug(f"APPEND exception: {e}", exc_info=True)
        logging.error(f"Ошибка APPEND в '{folder}': {friendly_error(e)}")
        return False


def ensure_folder_exists(conn: imaplib.IMAP4, folder: str):
    """Создаёт папку на целевом сервере, если её нет."""
    try:
        status, _ = conn.status(f'"{folder}"', "(MESSAGES)")
        if status == "OK":
            return
    except imaplib.IMAP4.error:
        pass
    logging.info(f"  Создаю папку: {Colors.CYAN}{folder}{Colors.RESET}")
    try:
        conn.create(f'"{folder}"')
        conn.subscribe(f'"{folder}"')
    except Exception as e:
        logging.warning(f"  Не удалось создать папку '{folder}': {friendly_error(e)}")


# ---------------------------------------------------------------------------
# TypedDict definitions for stats and per-folder reports
# ---------------------------------------------------------------------------
class _StatsDict(TypedDict):
    total_scanned: int
    skipped_existing: int
    migrated_ok: int
    errors: int
    skipped_no_msgid: int
    bytes_transferred: int


class _FolderReport(TypedDict):
    src: str
    dst: str
    total: int
    skipped: int
    migrated: int
    errors: int
    bytes: int
    elapsed: float


# ---------------------------------------------------------------------------
# Основной мигратор
# ---------------------------------------------------------------------------
class IMAPMigrator:
    def __init__(self, config: MigrationConfig, dry_run: bool = False):
        self.config = config
        self.dry_run = dry_run
        self.state = MigrationState(config.state_file)
        self._interrupted = False

        self.stats: _StatsDict = {
            "total_scanned": 0,
            "skipped_existing": 0,
            "migrated_ok": 0,
            "errors": 0,
            "skipped_no_msgid": 0,
            "bytes_transferred": 0,
        }
        self.folder_reports: list[_FolderReport] = []
        self._paused = False
        self._pause_logged = False
        self.pause_file = Path(config.state_file).resolve().parent / ".migration.pause"

        # Graceful shutdown
        signal.signal(signal.SIGINT, self._handle_interrupt)
        signal.signal(signal.SIGTERM, self._handle_interrupt)
        sigusr1 = getattr(signal, "SIGUSR1", None)
        if sigusr1 is not None:
            signal.signal(sigusr1, self._handle_pause)

    def _is_paused(self) -> bool:
        """True if pause was requested via SIGUSR1 or pause file (Windows)."""
        return self._paused or self.pause_file.exists()

    def _handle_pause(self, signum, frame):
        self._paused = not self._paused
        if self._paused:
            self.state.save()
            logging.warning(
                f"\n{Colors.YELLOW}Пауза. Отправьте SIGUSR1 снова или удалите {self.pause_file} для продолжения.{Colors.RESET}"
            )
        else:
            logging.info(f"  {Colors.GREEN}Возобновление{Colors.RESET}")

    def _handle_interrupt(self, signum, frame):
        if self._interrupted:
            logging.critical("Повторный сигнал -- принудительный выход")
            sys.exit(1)
        self._interrupted = True
        logging.warning(
            f"\n{Colors.YELLOW}Получен сигнал прерывания. "
            f"Завершаю текущее письмо и сохраняю состояние...{Colors.RESET}"
        )

    def resolve_dest_folder(self, src_folder: str) -> str:
        if src_folder in self.config.folder_map:
            return self.config.folder_map[src_folder]
        decoded = decode_folder_name(src_folder)
        if self.config.use_builtin_map:
            if decoded in BUILTIN_FOLDER_MAP:
                return BUILTIN_FOLDER_MAP[decoded]
            if src_folder in BUILTIN_FOLDER_MAP:
                return BUILTIN_FOLDER_MAP[src_folder]
        return decoded

    def should_process_folder(self, folder: str) -> bool:
        decoded = decode_folder_name(folder)
        if self.config.only_folders:
            return folder in self.config.only_folders or decoded in self.config.only_folders
        if folder in self.config.exclude_folders or decoded in self.config.exclude_folders:
            return False
        return True

    def _reconnect(self, label: str = "") -> tuple[imaplib.IMAP4, imaplib.IMAP4]:
        prefix = f"[{label}] " if label else ""
        max_wait = max(2, self.config.reconnect_max_wait)
        wait = 2
        reconnect_start: Optional[float] = None
        while not self._interrupted:
            try:
                logging.info(f"  {prefix}Переподключение к серверам...")
                src = connect_imap(self.config.source, self.config.timeout)
                dst = connect_imap(self.config.destination, self.config.timeout)
                if reconnect_start is not None:
                    lost_sec = time.time() - reconnect_start
                    logging.info(
                        f"  {prefix}{Colors.GREEN}Соединение восстановлено{Colors.RESET} "
                        f"(потеряно: {human_duration(lost_sec)})"
                    )
                return src, dst
            except Exception as e:
                if reconnect_start is None:
                    reconnect_start = time.time()
                logging.debug(f"Reconnect failed: {e}", exc_info=True)
                logging.warning(
                    f"  {prefix}{friendly_error(e)}. Повтор через {wait} сек..."
                )
                time.sleep(wait)
                wait = min(wait * 2, max_wait)
        raise ConnectionError("Прервано пользователем")

    def _noop(self, conn: imaplib.IMAP4) -> bool:
        """Sends NOOP to keep connection alive. Returns True if connection is healthy."""
        try:
            conn.noop()
            return True
        except Exception:
            return False

    def _ensure_connection_healthy(
        self, src_conn: imaplib.IMAP4, dst_conn: imaplib.IMAP4, label: str
    ) -> tuple[imaplib.IMAP4, imaplib.IMAP4]:
        """Checks both connections with NOOP; reconnects if either is dead."""
        src_ok = self._noop(src_conn)
        dst_ok = self._noop(dst_conn)
        if src_ok and dst_ok:
            return src_conn, dst_conn
        logging.info(f"  [{label}] Проверка соединения: переподключение...")
        return self._reconnect(label)

    def _get_all_uids(
        self, src_conn: imaplib.IMAP4, dst_conn: imaplib.IMAP4, folder: str
    ) -> tuple[list[bytes], imaplib.IMAP4, imaplib.IMAP4]:
        """
        Returns (list of UIDs, src_conn, dst_conn). Reconnects and retries on failure.
        For very large folders, uses UID range pagination to avoid timeouts.
        """
        max_retries = max(1, self.config.folder_retries)
        for attempt in range(1, max_retries + 1):
            try:
                status = src_conn.select(f'"{folder}"', readonly=True)
                if status[0] != "OK":
                    return [], src_conn, dst_conn

                status, uid_data = src_conn.uid("SEARCH", None, "ALL")
                if status != "OK" or not uid_data or not uid_data[0]:
                    return [], src_conn, dst_conn

                uids = uid_data[0].split()
                return uids, src_conn, dst_conn
            except (imaplib.IMAP4.abort, imaplib.IMAP4.error, OSError, BrokenPipeError) as e:
                if attempt < max_retries:
                    wait = min(2 ** attempt, 30)
                    logging.warning(
                        f"  Получение UID: {friendly_error(e)}. Повтор через {wait} сек..."
                    )
                    time.sleep(wait)
                    src_conn, dst_conn = self._reconnect(folder)
                else:
                    raise
        return [], src_conn, dst_conn

    def migrate_folder(
        self,
        src_conn: imaplib.IMAP4,
        dst_conn: imaplib.IMAP4,
        src_folder: str,
        dst_folder: str,
    ) -> tuple[imaplib.IMAP4, imaplib.IMAP4]:
        """Migrates one folder using batch scan+transfer. Returns (src_conn, dst_conn)."""
        mode_tag = f"{Colors.YELLOW}DRY-RUN{Colors.RESET} " if self.dry_run else ""
        logging.info(
            f"\n{Colors.BOLD}{'=' * 50}{Colors.RESET}\n"
            f"  {mode_tag}{Colors.BOLD}{src_folder}{Colors.RESET} -> {Colors.CYAN}{dst_folder}{Colors.RESET}"
        )

        src_conn, dst_conn = self._ensure_connection_healthy(src_conn, dst_conn, src_folder)

        logging.info("  Получение списка UID...")
        uids, src_conn, dst_conn = self._get_all_uids(src_conn, dst_conn, src_folder)
        total = len(uids)

        if total == 0:
            logging.info(f"  {Colors.GREEN}Папка пуста{Colors.RESET}")
            self.folder_reports.append({
                "src": src_folder, "dst": dst_folder,
                "total": 0, "skipped": 0, "migrated": 0, "errors": 0,
                "bytes": 0, "elapsed": 0.0,
            })
            return src_conn, dst_conn

        current_uv = get_folder_uidvalidity(src_conn, src_folder)
        cached_uids: set[str] = set()
        if current_uv and current_uv == self.state.get_uidvalidity(src_folder):
            cached_uids = self.state.get_cached_uids(dst_folder)
        elif current_uv:
            self.state.invalidate_uid_cache(dst_folder, src_folder)
        if current_uv:
            self.state.set_uidvalidity(src_folder, current_uv)

        uids_decoded = [(uid, uid.decode()) for uid in uids]
        skipped_by_cache = {s for _, s in uids_decoded if s in cached_uids}
        uids_to_scan = [uid for uid, s in uids_decoded if s not in cached_uids]

        if skipped_by_cache:
            logging.info(
                f"  Кэш UID: пропущено {len(skipped_by_cache):,} писем "
                f"(без FETCH). К сканированию: {len(uids_to_scan):,}"
            )

        if HAS_TQDM and not self.dry_run and total > 500:
            logging.info(f"  Всего: {Colors.BOLD}{total}{Colors.RESET} писем (батчи по {self.config.scan_batch_size})")

        folder_report: _FolderReport = {
            "src": src_folder, "dst": dst_folder,
            "total": total, "skipped": len(skipped_by_cache),
            "migrated": 0, "errors": 0,
            "bytes": 0, "elapsed": 0.0,
        }
        self.stats["total_scanned"] += total
        self.stats["skipped_existing"] += len(skipped_by_cache)

        if not self.dry_run:
            ensure_folder_exists(dst_conn, dst_folder)

        batch_size = max(1, self.config.scan_batch_size)

        pbar = None
        if HAS_TQDM and not self.dry_run:
            pbar = tqdm(
                total=total,
                desc=f"  {dst_folder}",
                unit="msg",
                bar_format=(
                    "  {l_bar}{bar}| {n_fmt}/{total_fmt} "
                    "[{elapsed}<{remaining}, {rate_fmt}] {postfix}"
                ),
                dynamic_ncols=True,
            )
            if skipped_by_cache:
                pbar.update(len(skipped_by_cache))

        folder_start = time.time()
        last_noop = time.time()
        last_status_time = folder_start
        total_migrated_this_run = 0

        for batch_start in range(0, len(uids_to_scan), batch_size):
            if self._interrupted:
                logging.warning("  Прерывание -- сохраняю состояние...")
                break

            batch_uids = uids_to_scan[batch_start:batch_start + batch_size]

            if self.config.noop_interval > 0 and (time.time() - last_noop) >= self.config.noop_interval:
                self._noop(src_conn)
                self._noop(dst_conn)
                last_noop = time.time()

            try:
                batch_messages = fetch_message_ids_batch(src_conn, batch_uids)
            except (imaplib.IMAP4.abort, imaplib.IMAP4.error, OSError, BrokenPipeError):
                src_conn, dst_conn = self._reconnect(src_folder)
                try:
                    src_conn.select(f'"{src_folder}"', readonly=True)
                except Exception as e:
                    logging.debug(f"Re-select after reconnect failed: {e}")
                batch_messages = fetch_message_ids_batch(src_conn, batch_uids)

            to_migrate_batch = [
                (uid, mid) for uid, mid in batch_messages
                if not self.state.is_migrated(
                    dst_folder,
                    mid if mid else f"__uid_{uid.decode()}_{src_folder}",
                )
            ]
            folder_report["skipped"] += len(batch_messages) - len(to_migrate_batch)
            self.stats["skipped_existing"] += len(batch_messages) - len(to_migrate_batch)

            for uid, msg_id in to_migrate_batch:
                if self._interrupted:
                    break
                if self._is_paused() and not self._interrupted:
                    logging.warning(
                        f"  Пауза (SIGUSR1 или удалите {self.pause_file} для продолжения)"
                    )
                    while self._is_paused() and not self._interrupted:
                        time.sleep(1)
                if self._interrupted:
                    break
                unique_key = msg_id if msg_id else f"__uid_{uid.decode()}_{src_folder}"
                if not msg_id:
                    self.stats["skipped_no_msgid"] += 1

                if self.dry_run:
                    self.stats["migrated_ok"] += 1
                    folder_report["migrated"] += 1
                    continue

                success = False
                msg_size = 0
                for attempt in range(1, self.config.max_retries + 1):
                    try:
                        raw, flags, idate = fetch_full_message(src_conn, uid)
                        if raw is None:
                            break
                        msg_size = len(raw)
                        ok = upload_message(
                            dst_conn, dst_folder, raw, flags, idate,
                            exclude_flags=set(self.config.exclude_flags),
                        )
                        if ok:
                            self.state.mark_migrated(
                                dst_folder, unique_key, msg_size, src_uid=uid.decode()
                            )
                            self.stats["migrated_ok"] += 1
                            self.stats["bytes_transferred"] += msg_size
                            folder_report["migrated"] += 1
                            folder_report["bytes"] += msg_size
                            total_migrated_this_run += 1
                            success = True
                            break
                        logging.warning(
                            f"  Ошибка записи uid={uid.decode()}: сервер отклонил APPEND "
                            f"(попытка {attempt}/{self.config.max_retries})"
                        )
                    except (imaplib.IMAP4.abort, imaplib.IMAP4.error, OSError, BrokenPipeError) as e:
                        logging.warning(
                            f"  Ошибка uid={uid.decode()}: {friendly_error(e)} "
                            f"(попытка {attempt}/{self.config.max_retries})"
                        )
                        if attempt < self.config.max_retries:
                            wait = min(2 ** attempt, 30)
                            logging.info(f"  Жду {wait} сек перед переподключением...")
                            time.sleep(wait)
                            try:
                                src_conn, dst_conn = self._reconnect(src_folder)
                                src_conn.select(f'"{src_folder}"', readonly=True)
                            except Exception as re_err:
                                logging.error(f"  Реконнект не удался: {re_err}")

                if not success:
                    self.stats["errors"] += 1
                    folder_report["errors"] += 1

                if pbar:
                    pbar.set_postfix_str(
                        f"{human_size(folder_report['bytes'])} | err={folder_report['errors']}",
                        refresh=False,
                    )
                    pbar.update(1)

            if pbar:
                pbar.update(len(batch_messages) - len(to_migrate_batch))

            processed = len(skipped_by_cache) + batch_start + len(batch_messages)
            if (
                self.config.status_interval > 0
                and (time.time() - last_status_time) >= self.config.status_interval
            ):
                last_status_time = time.time()
                pct = 100.0 * processed / total if total else 0
                elapsed = last_status_time - folder_start
                rate = processed / elapsed if elapsed > 0 else 0
                remaining = total - processed
                eta_sec = remaining / rate if rate > 0 else 0
                logging.info(
                    f"  [{src_folder}] Прогресс: {processed:,} / {total:,} ({pct:.1f}%) — "
                    f"{human_size(folder_report['bytes'])} — ETA ~{human_duration(eta_sec)}"
                )

            if self.config.throttle > 0:
                time.sleep(self.config.throttle)

            self.state.save()
            if 0 < self.config.batch_limit <= total_migrated_this_run:
                logging.info(f"  Достигнут batch_limit={self.config.batch_limit}")
                break

        if pbar:
            pbar.close()

        folder_report["elapsed"] = time.time() - folder_start
        self.state.save()

        fr = folder_report
        skip_count = fr["skipped"]
        logging.info(
            f"  Всего: {Colors.BOLD}{total}{Colors.RESET} | "
            f"Уже перенесено: {Colors.GREEN}{skip_count}{Colors.RESET} | "
            f"К переносу: {Colors.CYAN}{fr['migrated']}{Colors.RESET}"
        )
        speed = fr["bytes"] / fr["elapsed"] if fr["elapsed"] > 0 else 0
        err_str = f" | {Colors.RED}Ошибки: {fr['errors']}{Colors.RESET}" if fr["errors"] else ""
        logging.info(
            f"  {Colors.GREEN}Готово:{Colors.RESET} +{fr['migrated']} писем, "
            f"{human_size(fr['bytes'])}, "
            f"{human_duration(fr['elapsed'])} "
            f"({human_size(speed)}/s){err_str}"
        )
        self.folder_reports.append(folder_report)

        return src_conn, dst_conn

    def verify_counts(self, src_conn: imaplib.IMAP4, dst_conn: imaplib.IMAP4, folders: list[tuple[str, str]]) -> None:
        """Верификация: сравнение количества писем на источнике и приёмнике."""
        logging.info(f"\n{Colors.BOLD}{'=' * 50}{Colors.RESET}")
        logging.info(f"{Colors.BOLD}  ВЕРИФИКАЦИЯ{Colors.RESET}")
        logging.info(f"{'=' * 50}")

        all_ok = True
        for src_f, dst_f in folders:
            src_count = folder_message_count(src_conn, src_f)
            dst_count = folder_message_count(dst_conn, dst_f)

            if src_count < 0:
                logging.warning(f"  {src_f}: не удалось прочитать источник")
                continue
            if dst_count < 0:
                logging.warning(f"  {dst_f}: не удалось прочитать приёмник")
                continue

            match = src_count == dst_count
            icon = f"{Colors.GREEN}OK{Colors.RESET}" if match else f"{Colors.RED}MISMATCH{Colors.RESET}"
            logging.info(
                f"  {src_f} -> {dst_f}: "
                f"src={src_count} dst={dst_count} [{icon}]"
            )
            if not match:
                all_ok = False

        if all_ok:
            logging.info(f"\n  {Colors.GREEN}Все папки совпадают!{Colors.RESET}")
        else:
            logging.warning(
                f"\n  {Colors.YELLOW}Есть расхождения. Возможные причины: "
                f"batch_limit, ошибки, или письма без Message-ID. "
                f"Попробуйте запустить повторно.{Colors.RESET}"
            )

    def print_final_report(self, elapsed: float):
        """Подробный итоговый отчёт."""
        s = self.stats
        total_bytes = s["bytes_transferred"]

        logging.info(f"\n{Colors.BOLD}{'=' * 60}{Colors.RESET}")
        logging.info(f"{Colors.BOLD}  ИТОГОВЫЙ ОТЧЁТ{Colors.RESET}")
        logging.info(f"{'=' * 60}")

        if self.folder_reports:
            logging.info(
                f"\n  {'Папка':<25} {'Всего':>7} {'Новых':>7} "
                f"{'Ошибок':>7} {'Размер':>10} {'Время':>10}"
            )
            logging.info(f"  {'-' * 70}")
            for fr in self.folder_reports:
                err_col = Colors.RED if fr["errors"] else ""
                err_end = Colors.RESET if fr["errors"] else ""
                logging.info(
                    f"  {fr['dst']:<25} {fr['total']:>7} "
                    f"{Colors.GREEN}{fr['migrated']:>7}{Colors.RESET} "
                    f"{err_col}{fr['errors']:>7}{err_end} "
                    f"{human_size(fr['bytes']):>10} "
                    f"{human_duration(fr['elapsed']):>10}"
                )

        logging.info(f"\n  {'-' * 40}")
        logging.info(f"  Просканировано писем:   {Colors.BOLD}{s['total_scanned']}{Colors.RESET}")
        logging.info(f"  Перенесено:             {Colors.GREEN}{s['migrated_ok']}{Colors.RESET}")
        logging.info(f"  Пропущено (уже есть):   {s['skipped_existing']}")
        logging.info(f"  Без Message-ID:         {s['skipped_no_msgid']}")
        if s["errors"]:
            logging.info(f"  Ошибки:                 {Colors.RED}{s['errors']}{Colors.RESET}")
        else:
            logging.info(f"  Ошибки:                 {Colors.GREEN}0{Colors.RESET}")
        logging.info(f"  Объём данных:           {Colors.CYAN}{human_size(total_bytes)}{Colors.RESET}")

        speed = total_bytes / elapsed if elapsed > 0 else 0
        logging.info(f"  Средняя скорость:       {human_size(speed)}/s")
        logging.info(f"  Общее время:            {Colors.BOLD}{human_duration(elapsed)}{Colors.RESET}")

        if self._interrupted:
            logging.info(
                f"\n  {Colors.YELLOW}Миграция прервана. "
                f"Состояние сохранено -- запустите снова для продолжения.{Colors.RESET}"
            )

        logging.info(f"{'=' * 60}\n")

    def run(self) -> bool:
        """Основной цикл миграции."""
        mode = f"{Colors.YELLOW}DRY-RUN{Colors.RESET}" if self.dry_run else f"{Colors.GREEN}ПЕРЕНОС{Colors.RESET}"
        start_ts = datetime.now().strftime(DATE_FORMAT)
        pid = os.getpid()
        logging.info(f"\n{Colors.BOLD}{'=' * 60}{Colors.RESET}")
        logging.info("  IMAP Migration")
        logging.info(f"  {self.config.source.user}@{self.config.source.host}")
        logging.info(f"      -> {self.config.destination.user}@{self.config.destination.host}")
        logging.info(f"  Режим: {mode}")
        logging.info(f"  Время старта: {start_ts}")
        if hasattr(signal, "SIGUSR1"):
            logging.info(f"  PID: {pid} (kill -USR1 {pid} для паузы)")
        else:
            logging.info(f"  PID: {pid} (создайте .migration.pause для паузы)")
        logging.info(f"{'=' * 60}")

        max_wait = max(2, self.config.reconnect_max_wait)
        wait = 2
        while not self._interrupted:
            try:
                src_conn = connect_imap(self.config.source, self.config.timeout)
                dst_conn = connect_imap(self.config.destination, self.config.timeout)
                break
            except Exception as e:
                logging.warning(
                    f"  Не удалось подключиться: {friendly_error(e)}. "
                    f"Повтор через {wait} сек..."
                )
                time.sleep(wait)
                wait = min(wait * 2, max_wait)
        else:
            logging.critical("Прервано пользователем")
            return False

        logging.info(f"  {Colors.GREEN}Подключение установлено{Colors.RESET}")

        src_folders = list_folders(src_conn)
        logging.info(f"\n  Папки на источнике ({len(src_folders)}):")
        for f in src_folders:
            dst = self.resolve_dest_folder(f)
            skip = "" if self.should_process_folder(f) else f" {Colors.DIM}[пропуск]{Colors.RESET}"
            count = folder_message_count(src_conn, f)
            count_str = f" ({count})" if count >= 0 else ""
            logging.info(f"    {f}{count_str} -> {Colors.CYAN}{dst}{Colors.RESET}{skip}")

        folders_to_process = [f for f in src_folders if self.should_process_folder(f)]
        folder_pairs = [(f, self.resolve_dest_folder(f)) for f in folders_to_process]
        logging.info(f"\n  К обработке: {Colors.BOLD}{len(folders_to_process)}{Colors.RESET} папок")

        start_time = time.time()

        max_folder_retries = max(1, self.config.folder_retries)
        for src_f, dst_f in folder_pairs:
            if self._interrupted:
                break
            for folder_attempt in range(1, max_folder_retries + 1):
                try:
                    src_conn, dst_conn = self.migrate_folder(src_conn, dst_conn, src_f, dst_f)
                    break
                except Exception as e:
                    logging.error(
                        f"Критическая ошибка '{src_f}' (попытка {folder_attempt}/{max_folder_retries}): {e}"
                    )
                    if folder_attempt < max_folder_retries:
                        try:
                            src_conn, dst_conn = self._reconnect(src_f)
                        except Exception:
                            logging.critical("Реконнект не удался -- останавливаю")
                            break
                    else:
                        logging.warning(f"  Папка '{src_f}' пропущена после {max_folder_retries} попыток")
                        try:
                            src_conn, dst_conn = self._reconnect(src_f)
                        except Exception as e:
                            logging.debug(f"Post-folder reconnect failed: {e}")
                        break

        elapsed = time.time() - start_time

        # Верификация
        if self.config.verify and not self.dry_run and not self._interrupted:
            try:
                src_conn = connect_imap(self.config.source, self.config.timeout)
                dst_conn = connect_imap(self.config.destination, self.config.timeout)
                self.verify_counts(src_conn, dst_conn, folder_pairs)
            except Exception as e:
                logging.warning(f"Ошибка верификации: {e}")

        self.print_final_report(elapsed)

        for conn in [src_conn, dst_conn]:
            try:
                conn.logout()
            except Exception:
                pass

        self.state.save()
        return self.stats["errors"] == 0


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------
def load_config(path: str) -> MigrationConfig:
    cfg = MigrationConfig()
    try:
        raw = Path(path).read_text(encoding="utf-8")
    except OSError as e:
        raise RuntimeError(f"Cannot read config file '{path}': {e}") from e
    try:
        data = yaml.safe_load(raw) if HAS_YAML else json.loads(raw)
    except Exception as e:
        raise RuntimeError(f"Invalid config format in '{path}': {e}") from e
    if not isinstance(data, dict):
        raise ValueError(f"Config must be a YAML/JSON mapping, got: {type(data).__name__}")

    src = data.get("source", {})
    cfg.source = ServerConfig(
        host=src.get("host", ""),
        port=src.get("port", 993),
        user=src.get("user", ""),
        password=src.get("password", ""),
        ssl=src.get("ssl", True),
        starttls=src.get("starttls", False),
    )

    dst = data.get("destination", {})
    cfg.destination = ServerConfig(
        host=dst.get("host", ""),
        port=dst.get("port", 993),
        user=dst.get("user", ""),
        password=dst.get("password", ""),
        ssl=dst.get("ssl", True),
        starttls=dst.get("starttls", False),
    )

    opts = data.get("options", {})
    cfg.batch_limit = opts.get("batch_limit", 0)
    cfg.state_file = opts.get("state_file", "migration_state.json")
    cfg.log_file = opts.get("log_file", "migration.log")
    cfg.timeout = opts.get("timeout", 120)
    cfg.throttle = opts.get("throttle", 0.05)
    cfg.max_retries = opts.get("max_retries", 3)
    cfg.verbose = opts.get("verbose", False)
    cfg.scan_batch_size = opts.get("scan_batch_size", 100)
    cfg.folder_retries = opts.get("folder_retries", 3)
    cfg.noop_interval = opts.get("noop_interval", 30)
    cfg.reconnect_max_wait = opts.get("reconnect_max_wait", 300)
    cfg.status_interval = opts.get("status_interval", 600)
    cfg.use_builtin_map = opts.get("use_builtin_map", True)
    cfg.exclude_flags = opts.get("exclude_flags", [])

    cfg.folder_map = data.get("folder_map", {})
    cfg.exclude_folders = data.get("exclude_folders", [])
    cfg.only_folders = data.get("only_folders", [])

    errors = []
    for label, srv in [("source", cfg.source), ("destination", cfg.destination)]:
        if not srv.host:
            errors.append(f"{label}.host is required")
        if not srv.user:
            errors.append(f"{label}.user is required")
        if not srv.password:
            errors.append(f"{label}.password is required")
        if not isinstance(srv.port, int) or not (1 <= srv.port <= 65535):
            errors.append(f"{label}.port must be integer 1-65535")
    if errors:
        raise ValueError("Configuration errors:\n  " + "\n  ".join(errors))

    return cfg


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(
        description="IMAP Mail Migration Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры:
  %(prog)s -c config.yaml --list-folders     Показать папки и кол-во писем
  %(prog)s -c config.yaml --dry-run          Пробный прогон без записи
  %(prog)s -c config.yaml                    Перенос
  %(prog)s -c config.yaml --verify           Перенос + верификация
  %(prog)s -c config.yaml -f INBOX,Sent      Только указанные папки
        """,
    )
    parser.add_argument("--config", "-c", required=True, help="Путь к config.yaml")
    parser.add_argument("--dry-run", "-n", action="store_true", help="Проверка без записи")
    parser.add_argument("--folders", "-f", help="Только указанные папки (через запятую)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Подробный вывод (DEBUG)")
    parser.add_argument("--verify", action="store_true", help="Верификация после переноса")
    parser.add_argument("--list-folders", action="store_true", help="Показать папки и выйти")

    args = parser.parse_args()
    config = load_config(args.config)

    if args.verbose:
        config.verbose = True
    if args.verify:
        config.verify = True
    if args.folders:
        config.only_folders = [f.strip() for f in args.folders.split(",")]

    setup_logging(config.log_file, config.verbose)

    # Режим: показать папки
    if args.list_folders:
        conn = connect_imap(config.source, config.timeout)
        folders = list_folders(conn)
        print(f"\n{Colors.BOLD}Папки на {config.source.host} ({config.source.user}):{Colors.RESET}\n")
        for f in folders:
            decoded = decode_folder_name(f)
            if f in config.folder_map:
                mapped = config.folder_map[f]
            elif config.use_builtin_map:
                mapped = BUILTIN_FOLDER_MAP.get(decoded, BUILTIN_FOLDER_MAP.get(f, decoded))
            else:
                mapped = config.folder_map.get(decoded, config.folder_map.get(f, decoded)) or decoded
            arrow = f" -> {Colors.CYAN}{mapped}{Colors.RESET}" if mapped != decoded else ""
            count = folder_message_count(conn, f)
            count_str = f"  {Colors.DIM}({count} писем){Colors.RESET}" if count >= 0 else ""
            print(f"  {f}{arrow}{count_str}")
        conn.logout()
        return

    if not HAS_YAML:
        logging.warning(
            f"{Colors.YELLOW}PyYAML не установлен -- конфиг должен быть JSON. "
            f"Для YAML: pip install pyyaml{Colors.RESET}"
        )
    if not HAS_TQDM:
        logging.warning(
            f"{Colors.YELLOW}tqdm не установлен -- прогресс-бар недоступен. "
            f"Установите: pip install tqdm{Colors.RESET}"
        )

    migrator = IMAPMigrator(config, dry_run=args.dry_run)
    success = migrator.run()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()