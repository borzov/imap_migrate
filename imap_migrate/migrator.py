"""IMAP migration orchestration."""

from __future__ import annotations

import imaplib
import logging
import os
import signal
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional, TypedDict

try:
    from tqdm import tqdm
except ImportError:
    tqdm = None  # type: ignore[misc, assignment]

from imap_migrate.colors import Colors
from imap_migrate.config import MigrationConfig
from imap_migrate.exceptions import MessageIdBatchFetchError
from imap_migrate.folders import decode_folder_name, resolve_destination_folder
from imap_migrate.imap_ops import (
    connect_imap,
    ensure_folder_exists,
    fetch_folder_total_bytes,
    fetch_full_message,
    fetch_message_ids_batch,
    folder_message_count,
    get_folder_uidvalidity,
    list_folders,
    upload_message,
)
from imap_migrate.logging_setup import DATE_FORMAT, HAS_TQDM
from imap_migrate.state import MigrationState
from imap_migrate.util import friendly_error, human_duration, human_size


class _StatsDict(TypedDict):
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
        self.state._acquire_lock()
        self._interrupted = False

        self.stats: _StatsDict = {
            "migrated_ok": 0,
            "errors": 0,
            "skipped_no_msgid": 0,
            "bytes_transferred": 0,
        }
        self.folder_reports: list[_FolderReport] = []
        self._paused = False
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
        return resolve_destination_folder(
            src_folder,
            self.config.folder_map,
            self.config.use_builtin_map,
        )

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

        Uses ``UID SEARCH ALL`` for the folder. Very large folders may need a higher
        server/client timeout; UID range pagination is not implemented yet.
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

        if HAS_TQDM and tqdm is not None and not self.dry_run and total > 500:
            logging.info(f"  Всего: {Colors.BOLD}{total}{Colors.RESET} писем (батчи по {self.config.scan_batch_size})")

        folder_report: _FolderReport = {
            "src": src_folder, "dst": dst_folder,
            "total": total, "skipped": len(skipped_by_cache),
            "migrated": 0, "errors": 0,
            "bytes": 0, "elapsed": 0.0,
        }
        if not self.dry_run:
            if not ensure_folder_exists(dst_conn, dst_folder):
                logging.error(f"  Папка '{dst_folder}' недоступна — пропускаю '{src_folder}'")
                self.stats["errors"] += 1
                folder_report["errors"] += 1
                self.folder_reports.append(folder_report)
                return src_conn, dst_conn

        batch_size = max(1, self.config.scan_batch_size)

        pbar = None
        if HAS_TQDM and tqdm is not None and not self.dry_run:
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

        exclude_flags_set: frozenset[str] = frozenset(self.config.exclude_flags)
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

            max_batch_attempts = max(2, self.config.folder_retries)
            batch_messages: list[tuple[bytes, str | None]] = []
            for fetch_try in range(max_batch_attempts):
                try:
                    batch_messages = fetch_message_ids_batch(src_conn, batch_uids)
                    break
                except MessageIdBatchFetchError as err:
                    logging.warning(
                        "  BATCH FETCH Message-ID: %s (попытка %s/%s)",
                        err,
                        fetch_try + 1,
                        max_batch_attempts,
                    )
                    if fetch_try + 1 >= max_batch_attempts:
                        raise
                    src_conn, dst_conn = self._reconnect(src_folder)
                    try:
                        src_conn.select(f'"{src_folder}"', readonly=True)
                    except Exception as exc:
                        logging.debug("Re-select after batch fetch failure: %s", exc)
                except (imaplib.IMAP4.abort, imaplib.IMAP4.error, OSError, BrokenPipeError) as _fe:
                    if fetch_try + 1 >= max_batch_attempts:
                        raise
                    logging.warning(
                        "  BATCH FETCH IMAP: %s (попытка %s/%s)",
                        friendly_error(_fe),
                        fetch_try + 1,
                        max_batch_attempts,
                    )
                    src_conn, dst_conn = self._reconnect(src_folder)
                    try:
                        src_conn.select(f'"{src_folder}"', readonly=True)
                    except Exception as exc:
                        logging.debug("Re-select after reconnect failed: %s", exc)

            to_migrate_batch = [
                (uid, mid) for uid, mid in batch_messages
                if not self.state.is_migrated(
                    dst_folder,
                    mid if mid else f"__uid_{uid.decode()}_{src_folder}",
                )
            ]
            folder_report["skipped"] += len(batch_messages) - len(to_migrate_batch)

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
                            exclude_flags=exclude_flags_set,
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

            self.state.save_if_needed()
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

        total_scanned = sum(fr["total"] for fr in self.folder_reports)
        total_skipped = sum(fr["skipped"] for fr in self.folder_reports)
        logging.info(f"\n  {'-' * 40}")
        logging.info(f"  Просканировано писем:   {Colors.BOLD}{total_scanned:,}{Colors.RESET}")
        logging.info(f"  Перенесено:             {Colors.GREEN}{s['migrated_ok']:,}{Colors.RESET}")
        logging.info(f"  Пропущено (уже есть):   {total_skipped:,}")
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

    def _print_startup_stats(
        self,
        src_conn: imaplib.IMAP4,
        dst_conn: imaplib.IMAP4,
        folder_pairs: list[tuple[str, str]],
    ) -> None:
        """Comprehensive per-folder overview at startup/resume."""
        if self.config.skip_detailed_startup_stats:
            logging.info(
                f"\n{Colors.BOLD}  Обзор папок:{Colors.RESET} {Colors.DIM}пропущен "
                f"(options.skip_detailed_startup_stats){Colors.RESET}"
            )
            return
        logging.info(f"\n{Colors.BOLD}  Обзор папок:{Colors.RESET}")

        W_PAIR = 38
        W_N = 9
        W_S = 9

        hdr = (
            f"  {'Папка':<{W_PAIR}}"
            f"{'Src':>{W_N}}{'Src MB':>{W_S}}"
            f"{'Dst':>{W_N}}"
            f"{'Сост':>{W_N}}{'Передано':>{W_S}}"
            f"{'Осталось':>{W_N}}{'Данных':>{W_S}}"
        )
        sep = "─" * (W_PAIR + W_N * 4 + W_S * 3)
        logging.info(f"{Colors.DIM}{hdr}{Colors.RESET}")
        logging.info(f"  {Colors.DIM}{sep}{Colors.RESET}")

        tot_src_n = tot_src_b = tot_dst_n = tot_state_n = tot_state_b = 0
        tot_rem_n = tot_rem_b = 0

        for src_f, dst_f in folder_pairs:
            src_n = folder_message_count(src_conn, src_f)
            src_b = fetch_folder_total_bytes(src_conn, src_f)
            dst_n = folder_message_count(dst_conn, dst_f)
            state_n = self.state.count(dst_f)
            state_b = self.state.folder_stats.get(dst_f, {}).get("bytes", 0)
            rem_n = max(0, src_n - state_n) if src_n >= 0 else -1
            rem_b = max(0, src_b - state_b) if src_b >= 0 else -1

            pair = f"{src_f} → {dst_f}"

            def fmt_n(v: int) -> str:
                return f"{v:,}" if v >= 0 else "?"

            def fmt_b(v: int) -> str:
                return human_size(v) if v >= 0 else "?"

            rem_col = Colors.GREEN if rem_n == 0 else (Colors.YELLOW if rem_n > 0 else "")
            rem_end = Colors.RESET if rem_col else ""

            line = (
                f"  {pair:<{W_PAIR}}"
                f"{fmt_n(src_n):>{W_N}}{fmt_b(src_b):>{W_S}}"
                f"{fmt_n(dst_n):>{W_N}}"
                f"{fmt_n(state_n):>{W_N}}{fmt_b(state_b):>{W_S}}"
                f"{rem_col}{fmt_n(rem_n):>{W_N}}{fmt_b(rem_b):>{W_S}}{rem_end}"
            )
            logging.info(line)

            if src_n >= 0:
                tot_src_n += src_n
            if src_b >= 0:
                tot_src_b += src_b
            if dst_n >= 0:
                tot_dst_n += dst_n
            tot_state_n += state_n
            tot_state_b += state_b
            if rem_n >= 0:
                tot_rem_n += rem_n
            if rem_b >= 0:
                tot_rem_b += rem_b

        logging.info(f"  {Colors.DIM}{sep}{Colors.RESET}")
        tot_rem_col = Colors.GREEN if tot_rem_n == 0 else Colors.YELLOW
        total_line = (
            f"  {'Итого':<{W_PAIR}}"
            f"{tot_src_n:>{W_N},}{human_size(tot_src_b):>{W_S}}"
            f"{tot_dst_n:>{W_N},}"
            f"{tot_state_n:>{W_N},}{human_size(tot_state_b):>{W_S}}"
            f"{tot_rem_col}{tot_rem_n:>{W_N},}{human_size(tot_rem_b):>{W_S}}{Colors.RESET}"
        )
        logging.info(total_line)
        logging.info("")

    def run(self) -> bool:
        """Основной цикл миграции."""
        try:
            return self._run()
        finally:
            self.state._release_lock()

    def _run(self) -> bool:
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

        self._print_startup_stats(src_conn, dst_conn, folder_pairs)

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
            v_src = v_dst = None
            try:
                v_src = connect_imap(self.config.source, self.config.timeout)
                v_dst = connect_imap(self.config.destination, self.config.timeout)
                self.verify_counts(v_src, v_dst, folder_pairs)
            except Exception as e:
                logging.warning(f"Ошибка верификации: {e}")
            finally:
                for _vc in (v_src, v_dst):
                    if _vc is not None:
                        try:
                            _vc.logout()
                        except Exception:
                            pass

        self.print_final_report(elapsed)

        for conn in [src_conn, dst_conn]:
            try:
                conn.logout()
            except Exception:
                pass

        self.state.save()
        return self.stats["errors"] == 0


