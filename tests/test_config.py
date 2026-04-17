"""Tests for configuration loading."""

import json
from pathlib import Path

from imap_migrate.config import load_config


def test_load_config_minimal_json(tmp_path: Path) -> None:
    cfg_path = tmp_path / "cfg.json"
    cfg_path.write_text(
        json.dumps(
            {
                "source": {
                    "host": "imap.example.com",
                    "user": "a@example.com",
                    "password": "secret",
                },
                "destination": {
                    "host": "imap.other.com",
                    "user": "b@other.com",
                    "password": "secret2",
                },
                "options": {
                    "verify": True,
                    "skip_detailed_startup_stats": True,
                },
            }
        ),
        encoding="utf-8",
    )
    cfg = load_config(str(cfg_path))
    assert cfg.source.host == "imap.example.com"
    assert cfg.verify is True
    assert cfg.skip_detailed_startup_stats is True
