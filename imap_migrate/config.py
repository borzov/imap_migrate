"""Configuration dataclasses and YAML/JSON loading."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path

from imap_migrate.logging_setup import HAS_YAML

try:
    import yaml
except ImportError:
    yaml = None  # type: ignore[assignment]


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
        return (
            f"ServerConfig(host={self.host!r}, port={self.port}, "
            f"user={self.user!r}, password={pwd!r}, "
            f"ssl={self.ssl}, starttls={self.starttls})"
        )


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
    skip_detailed_startup_stats: bool = False


def load_config(path: str) -> MigrationConfig:
    """Load and validate migration config from YAML or JSON."""
    cfg = MigrationConfig()
    try:
        raw = Path(path).read_text(encoding="utf-8")
    except OSError as exc:
        raise RuntimeError(f"Cannot read config file '{path}': {exc}") from exc
    try:
        if HAS_YAML and yaml is not None:
            data = yaml.safe_load(raw)
        else:
            data = json.loads(raw)
    except Exception as exc:
        raise RuntimeError(f"Invalid config format in '{path}': {exc}") from exc
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
    config_stem = Path(path).stem
    cfg.state_file = opts.get("state_file") or f"migration_state_{config_stem}.json"
    cfg.log_file = opts.get("log_file") or f"migration_{config_stem}.log"
    old_default = Path("migration_state.json")
    if (
        not Path(cfg.state_file).exists()
        and old_default.exists()
        and cfg.state_file != "migration_state.json"
    ):
        logging.warning(
            f"Обнаружен legacy state-файл migration_state.json. "
            f"Для продолжения прерванной миграции переименуйте его:\n"
            f"  mv migration_state.json {cfg.state_file}"
        )
    cfg.timeout = opts.get("timeout", 120)
    cfg.throttle = opts.get("throttle", 0.05)
    cfg.max_retries = opts.get("max_retries", 3)
    cfg.verbose = opts.get("verbose", False)
    cfg.verify = bool(opts.get("verify", False))
    cfg.scan_batch_size = opts.get("scan_batch_size", 100)
    cfg.folder_retries = opts.get("folder_retries", 3)
    cfg.noop_interval = opts.get("noop_interval", 30)
    cfg.reconnect_max_wait = opts.get("reconnect_max_wait", 300)
    cfg.status_interval = opts.get("status_interval", 600)
    cfg.use_builtin_map = opts.get("use_builtin_map", True)
    cfg.exclude_flags = opts.get("exclude_flags", [])
    cfg.skip_detailed_startup_stats = bool(opts.get("skip_detailed_startup_stats", False))

    cfg.folder_map = data.get("folder_map", {})
    cfg.exclude_folders = data.get("exclude_folders", [])
    cfg.only_folders = data.get("only_folders", [])

    errors: list[str] = []
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
