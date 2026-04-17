"""Tests for folder decoding and destination resolution."""

from imap_migrate.constants import BUILTIN_FOLDER_MAP
from imap_migrate.folders import decode_folder_name, resolve_destination_folder


def test_decode_folder_name_plain_ascii_unchanged() -> None:
    assert decode_folder_name("INBOX") == "INBOX"


def test_resolve_destination_folder_builtin_inbox() -> None:
    assert (
        resolve_destination_folder("Входящие", {}, True)
        == BUILTIN_FOLDER_MAP["Входящие"]
    )


def test_resolve_destination_folder_explicit_map_wins() -> None:
    assert resolve_destination_folder("Custom", {"Custom": "INBOX"}, True) == "INBOX"


def test_resolve_destination_folder_no_builtin_returns_decoded() -> None:
    assert resolve_destination_folder("RareBox", {}, False) == "RareBox"
