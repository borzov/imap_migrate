"""Command-line entry point."""

from __future__ import annotations

import argparse
import logging
import sys

from imap_migrate import __version__
from imap_migrate.colors import Colors
from imap_migrate.config import load_config
from imap_migrate.folders import decode_folder_name, resolve_destination_folder
from imap_migrate.imap_ops import connect_imap, folder_message_count, list_folders
from imap_migrate.logging_setup import HAS_TQDM, HAS_YAML, setup_logging
from imap_migrate.migrator import IMAPMigrator


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
    parser.add_argument("--config", "-c", help="Путь к config.yaml")
    parser.add_argument("--dry-run", "-n", action="store_true", help="Проверка без записи")
    parser.add_argument("--folders", "-f", help="Только указанные папки (через запятую)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Подробный вывод (DEBUG)")
    parser.add_argument("--verify", action="store_true", help="Верификация после переноса")
    parser.add_argument("--list-folders", action="store_true", help="Показать папки и выйти")
    parser.add_argument(
        "--version",
        action="store_true",
        help="Показать версию и выйти",
    )

    args = parser.parse_args()
    if args.version:
        print(__version__)
        return

    if not args.config:
        parser.error("the following arguments are required: --config/-c")

    config = load_config(args.config)

    if args.verbose:
        config.verbose = True
    if args.verify:
        config.verify = True
    if args.folders:
        config.only_folders = [f.strip() for f in args.folders.split(",")]

    setup_logging(config.log_file, config.verbose)

    if args.list_folders:
        conn = connect_imap(config.source, config.timeout)
        folders = list_folders(conn)
        print(f"\n{Colors.BOLD}Папки на {config.source.host} ({config.source.user}):{Colors.RESET}\n")
        for folder in folders:
            decoded = decode_folder_name(folder)
            mapped = resolve_destination_folder(
                folder,
                config.folder_map,
                config.use_builtin_map,
            )
            arrow = f" -> {Colors.CYAN}{mapped}{Colors.RESET}" if mapped != decoded else ""
            count = folder_message_count(conn, folder)
            count_str = f"  {Colors.DIM}({count} писем){Colors.RESET}" if count >= 0 else ""
            print(f"  {folder}{arrow}{count_str}")
        conn.logout()
        return

    if not HAS_YAML:
        logging.warning(
            "%sPyYAML не установлен -- конфиг должен быть JSON. "
            "Для YAML: pip install pyyaml%s",
            Colors.YELLOW,
            Colors.RESET,
        )
    if not HAS_TQDM:
        logging.warning(
            "%stqdm не установлен -- прогресс-бар недоступен. "
            "Установите: pip install tqdm%s",
            Colors.YELLOW,
            Colors.RESET,
        )

    migrator = IMAPMigrator(config, dry_run=args.dry_run)
    success = migrator.run()
    sys.exit(0 if success else 1)
