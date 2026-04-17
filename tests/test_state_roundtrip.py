"""Tests for migration state persistence."""

import json
from pathlib import Path

from imap_migrate.state import MigrationState


def test_migration_state_loads_stdlib_json_and_saves_orjson(tmp_path: Path) -> None:
    legacy = {
        "migrated": {"INBOX": ["<a@b>", "<c@d>"]},
        "folder_stats": {"INBOX": {"count": 2, "bytes": 100}},
        "uid_cache": {"INBOX": ["1", "2"]},
        "uidvalidity": {"INBOX": "123"},
    }
    state_path = tmp_path / "state.json"
    state_path.write_text(json.dumps(legacy, ensure_ascii=False), encoding="utf-8")

    ms = MigrationState(str(state_path))
    assert ms.migrated["INBOX"] == {"<a@b>", "<c@d>"}
    assert ms.uid_cache["INBOX"] == {"1", "2"}

    ms.mark_migrated("INBOX", "<new@x>", 10, src_uid="99")
    ms.save()

    raw = state_path.read_bytes()
    assert b"migrated" in raw
    ms2 = MigrationState(str(state_path))
    assert "<new@x>" in ms2.migrated["INBOX"]
    assert "99" in ms2.uid_cache["INBOX"]
