"""Persistent migration state (resume, deduplication, UID cache)."""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import orjson

from imap_migrate.colors import Colors


def _loads_state_json(raw: bytes) -> dict:
    """Parse state JSON from disk (orjson first, then stdlib json)."""
    try:
        data = orjson.loads(raw)
    except Exception:
        data = json.loads(raw.decode("utf-8"))
    if not isinstance(data, dict):
        raise ValueError("state root must be a JSON object")
    return data


class MigrationState:
    """Message-IDs migrated per folder, stats, UID cache for fast resume."""

    def __init__(self, state_file: str) -> None:
        self.state_file = Path(state_file)
        self.migrated: dict[str, set[str]] = {}
        self.folder_stats: dict[str, dict] = {}
        self.uid_cache: dict[str, set[str]] = {}
        self.uidvalidity: dict[str, str] = {}
        self._dirty: int = 0
        self._last_save: float = 0.0
        self._lock_file: Optional[Path] = None
        self._load()

    def _load(self) -> None:
        tmp = self.state_file.with_suffix(".tmp")
        candidates: list[Path] = []
        if self.state_file.exists():
            candidates.append(self.state_file)
        if tmp.exists():
            candidates.append(tmp)

        for candidate in candidates:
            try:
                data = _loads_state_json(candidate.read_bytes())
                for folder, ids in data.get("migrated", data).items():
                    if isinstance(ids, list):
                        self.migrated[folder] = set(ids)
                self.folder_stats = data.get("folder_stats", {})
                for folder, uids in data.get("uid_cache", {}).items():
                    if isinstance(uids, list):
                        self.uid_cache[folder] = set(uids)
                self.uidvalidity = data.get("uidvalidity", {})
                total = sum(len(v) for v in self.migrated.values())
                if candidate == tmp:
                    logging.warning("Состояние восстановлено из резервного файла %s", tmp)
                logging.info(
                    "Загружено состояние: %s%s%s писем в %s папках",
                    Colors.CYAN,
                    total,
                    Colors.RESET,
                    len(self.migrated),
                )
                return
            except Exception as exc:
                logging.warning("Не удалось загрузить %s: %s", candidate, exc)

        if candidates:
            logging.error("State-файл повреждён. Миграция начнётся заново.")

    def save(self) -> None:
        try:
            payload = {
                "migrated": {f: list(ids) for f, ids in self.migrated.items()},
                "folder_stats": self.folder_stats,
                "uid_cache": {f: list(uids) for f, uids in self.uid_cache.items()},
                "uidvalidity": self.uidvalidity,
                "saved_at": datetime.now().isoformat(),
            }
            raw = orjson.dumps(payload)
            tmp = self.state_file.with_suffix(".tmp")
            tmp.write_bytes(raw)
            tmp.replace(self.state_file)
            self._dirty = 0
            self._last_save = time.time()
        except Exception as exc:
            logging.critical("Не удалось сохранить state-файл: %s", exc)

    def save_if_needed(self, force: bool = False) -> None:
        """Save state if enough messages migrated or time elapsed since last save."""
        now = time.time()
        if force or self._dirty >= 500 or (now - self._last_save) >= 30.0:
            self.save()

    def is_migrated(self, folder: str, message_id: str) -> bool:
        return message_id in self.migrated.get(folder, set())

    def mark_migrated(
        self,
        folder: str,
        message_id: str,
        msg_size: int = 0,
        src_uid: Optional[str] = None,
    ) -> None:
        self.migrated.setdefault(folder, set()).add(message_id)
        fs = self.folder_stats.setdefault(folder, {"count": 0, "bytes": 0})
        fs["count"] += 1
        fs["bytes"] += msg_size
        if src_uid:
            self.uid_cache.setdefault(folder, set()).add(src_uid)
        self._dirty += 1

    def _acquire_lock(self) -> None:
        lock_path = self.state_file.with_suffix(".lock")
        try:
            fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            os.write(fd, str(os.getpid()).encode())
            os.close(fd)
            self._lock_file = lock_path
        except FileExistsError:
            try:
                existing_pid = int(lock_path.read_text().strip())
                os.kill(existing_pid, 0)
                raise RuntimeError(
                    f"Другой процесс уже выполняет миграцию с этим конфигом "
                    f"(PID {existing_pid}).\n"
                    f"  Если процесс завершился некорректно, удалите: {lock_path}"
                ) from None
            except (ProcessLookupError, PermissionError):
                logging.warning("Удаляю устаревший lock-файл (PID не запущен)")
                lock_path.unlink(missing_ok=True)
                self._acquire_lock()
            except (ValueError, OSError):
                lock_path.unlink(missing_ok=True)
                self._acquire_lock()

    def _release_lock(self) -> None:
        if self._lock_file and self._lock_file.exists():
            try:
                self._lock_file.unlink()
            except OSError:
                pass

    def get_cached_uids(self, dst_folder: str) -> set[str]:
        return self.uid_cache.get(dst_folder, set()).copy()

    def set_uidvalidity(self, src_folder: str, value: str) -> None:
        self.uidvalidity[src_folder] = value

    def get_uidvalidity(self, src_folder: str) -> Optional[str]:
        return self.uidvalidity.get(src_folder)

    def invalidate_uid_cache(self, dst_folder: str, src_folder: str) -> None:
        self.uid_cache.pop(dst_folder, None)
        self.uidvalidity.pop(src_folder, None)

    def count(self, folder: str) -> int:
        return len(self.migrated.get(folder, set()))

    def total_bytes(self) -> int:
        return sum(s.get("bytes", 0) for s in self.folder_stats.values())
