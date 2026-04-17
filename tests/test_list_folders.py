"""Tests for LIST response parsing."""

from imap_migrate.imap_ops import list_folders


class _ListConn:
    def __init__(self, lines: list[bytes]) -> None:
        self._lines = lines

    def list(self) -> tuple[str, list]:
        return "OK", self._lines


def test_list_folders_yandex_style() -> None:
    data = [
        rb'(\HasNoChildren) "." INBOX',
        rb'(\HasNoChildren) "." "Sent"',
        rb'(\Noselect) "." "NoAccess"',
    ]
    conn = _ListConn(data)
    assert list_folders(conn) == ["INBOX", "Sent"]


def test_list_folders_quoted_name() -> None:
    data = [rb'(\HasNoChildren) "." "My Folder"']
    conn = _ListConn(data)
    assert list_folders(conn) == ["My Folder"]
