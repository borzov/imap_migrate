"""IMAP connection helpers and message fetch/upload."""

from __future__ import annotations

import calendar
import imaplib
import logging
import re
import ssl
from datetime import datetime
from typing import Optional

from imap_migrate.colors import Colors
from imap_migrate.config import ServerConfig
from imap_migrate.exceptions import MessageIdBatchFetchError
from imap_migrate.util import friendly_error

if hasattr(imaplib, "_MAXLINE"):
    imaplib._MAXLINE = 10_000_000


def connect_imap(cfg: ServerConfig, timeout: int = 120) -> imaplib.IMAP4_SSL | imaplib.IMAP4:
    """Connect and log in to the IMAP server."""
    logging.debug("Подключение к %s:%s (user=%s)", cfg.host, cfg.port, cfg.user)
    ctx = ssl.create_default_context() if (cfg.ssl or cfg.starttls) else None

    if cfg.ssl:
        conn = imaplib.IMAP4_SSL(cfg.host, cfg.port, ssl_context=ctx, timeout=timeout)
    else:
        conn = imaplib.IMAP4(cfg.host, cfg.port, timeout=timeout)
        if cfg.starttls and ctx:
            conn.starttls(ssl_context=ctx)

    conn.login(cfg.user, cfg.password)
    logging.debug("Логин OK: %s@%s", cfg.user, cfg.host)
    return conn


def list_folders(conn: imaplib.IMAP4) -> list[str]:
    """Return mailbox names from LIST response."""
    status, data = conn.list()
    if status != "OK":
        raise RuntimeError(f"Не удалось получить список папок: {status}")

    folders: list[str] = []
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

        logging.debug("  LIST raw: %r", line)

        flags_match = re.match(r"\(([^)]*)\)", line)
        if not flags_match:
            continue
        flags_str = flags_match.group(1)
        rest = line[flags_match.end() :].strip()

        if "\\Noselect" in flags_str or "\\NoSelect" in flags_str:
            continue

        delim_match = re.match(r'(?:"[^"]*"|NIL)\s+(.*)', rest)
        if delim_match:
            name_part = delim_match.group(1).strip()
        else:
            parts = rest.split(None, 1)
            name_part = parts[1].strip() if len(parts) > 1 else rest.strip()

        if name_part.startswith('"') and name_part.endswith('"'):
            name_part = name_part[1:-1]

        if name_part:
            folders.append(name_part)

    return folders


def get_folder_uidvalidity(conn: imaplib.IMAP4, folder: str) -> Optional[str]:
    """Return UIDVALIDITY for the folder (for cache validation)."""
    try:
        status, data = conn.status(f'"{folder}"', "(UIDVALIDITY)")
        if status == "OK" and data and data[0]:
            raw = data[0] if isinstance(data[0], bytes) else data[0].encode()
            match = re.search(rb"UIDVALIDITY\s+(\d+)", raw)
            if match:
                return match.group(1).decode()
    except Exception:
        pass
    return None


def folder_message_count(conn: imaplib.IMAP4, folder: str) -> int:
    """Return message count in folder (SELECT read-only)."""
    status, data = conn.select(f'"{folder}"', readonly=True)
    if status != "OK":
        return -1
    try:
        return int(data[0])
    except (ValueError, IndexError, TypeError):
        return -1


def fetch_folder_total_bytes(conn: imaplib.IMAP4, folder: str) -> int:
    """Total size of all messages via RFC822.SIZE (metadata only)."""
    try:
        status, _ = conn.select(f'"{folder}"', readonly=True)
        if status != "OK":
            return -1
        status, data = conn.uid("FETCH", "1:*", "(RFC822.SIZE)")
        if status != "OK" or not data:
            return 0
        total = 0
        for item in data:
            if isinstance(item, bytes):
                match = re.search(rb"RFC822\.SIZE (\d+)", item)
                if match:
                    total += int(match.group(1))
        return total
    except Exception as exc:
        logging.debug("fetch_folder_total_bytes(%s): %s", folder, exc)
        return -1


def fetch_message_ids_batch(
    conn: imaplib.IMAP4, uid_list: list[bytes]
) -> list[tuple[bytes, str | None]]:
    """
    Return [(uid, message_id_header), ...] for the given UIDs.
    BODY.PEEK does not mark messages as read.

    Raises:
        MessageIdBatchFetchError: if IMAP status is not OK or UID coverage is incomplete.
    """
    if not uid_list:
        return []
    uid_range = b",".join(uid_list)
    status, fetch_data = conn.uid(
        "FETCH", uid_range.decode(), "(BODY.PEEK[HEADER.FIELDS (MESSAGE-ID)])"
    )
    if status != "OK":
        raise MessageIdBatchFetchError(f"IMAP UID FETCH status={status!r}")

    results: list[tuple[bytes, str | None]] = []
    current_uid: bytes | None = None
    for item in fetch_data:
        if isinstance(item, tuple) and len(item) >= 2:
            header_line = item[0]
            if isinstance(header_line, bytes):
                uid_match = re.search(rb"UID (\d+)", header_line)
                if uid_match:
                    current_uid = uid_match.group(1)

            header_body = item[1]
            if isinstance(header_body, bytes):
                mid_match = re.search(
                    rb"Message-ID:\s*(<[^>]+>)", header_body, re.IGNORECASE
                )
                mid = (
                    mid_match.group(1).decode("utf-8", errors="replace") if mid_match else None
                )
                if current_uid:
                    results.append((current_uid, mid))
                current_uid = None

    expected = frozenset(uid_list)
    parsed_uids = frozenset(uid for uid, _ in results)
    if parsed_uids != expected:
        raise MessageIdBatchFetchError(
            f"UID FETCH incomplete: expected {len(expected)} UIDs, parsed {len(parsed_uids)} "
            f"(batch size {len(uid_list)})"
        )
    return results


def fetch_full_message(
    conn: imaplib.IMAP4, uid: bytes
) -> tuple[bytes | None, list[str], datetime | None]:
    """Download full message. Returns (raw_message, flags, internal_date)."""
    status, data = conn.uid("FETCH", uid.decode(), "(FLAGS INTERNALDATE RFC822)")
    if status != "OK" or not data or data[0] is None:
        return None, [], None

    raw_message: bytes | None = None
    flags: list[str] = []
    internal_date: datetime | None = None

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
                    except Exception as exc:
                        logging.debug("Date parse error: %s", exc)

            raw_message = item[1] if isinstance(item[1], bytes) else None

    return raw_message, flags, internal_date


STANDARD_IMAP_FLAGS = frozenset({"\\Seen", "\\Answered", "\\Flagged", "\\Deleted", "\\Draft"})


def upload_message(
    conn: imaplib.IMAP4,
    folder: str,
    raw_message: bytes,
    flags: list[str],
    internal_date: datetime | None,
    exclude_flags: frozenset[str] = frozenset(),
) -> bool:
    """Upload message to destination folder. Keeps standard IMAP flags only."""
    filtered = [
        f
        for f in flags
        if f not in exclude_flags
        and not f.startswith("\\Recent")
        and (f in STANDARD_IMAP_FLAGS or not f.startswith("$"))
    ]
    flag_str = " ".join(filtered)
    flag_str = f"({flag_str})" if flag_str else "()"

    date_str = None
    if internal_date:
        date_str = imaplib.Time2Internaldate(calendar.timegm(internal_date.timetuple()))

    try:
        status, _ = conn.append(f'"{folder}"', flag_str, date_str, raw_message)
        return status == "OK"
    except Exception as exc:
        logging.debug("APPEND exception: %s", exc, exc_info=True)
        logging.error("Ошибка APPEND в '%s': %s", folder, friendly_error(exc))
        return False


def ensure_folder_exists(conn: imaplib.IMAP4, folder: str) -> bool:
    """Create folder on destination if missing."""
    try:
        status, _ = conn.status(f'"{folder}"', "(MESSAGES)")
        if status == "OK":
            return True
    except imaplib.IMAP4.error:
        pass
    logging.info("  Создаю папку: %s%s%s", Colors.CYAN, folder, Colors.RESET)
    try:
        conn.create(f'"{folder}"')
        conn.subscribe(f'"{folder}"')
        return True
    except Exception as exc:
        logging.error("  Не удалось создать папку '%s': %s", folder, friendly_error(exc))
        return False
