"""Tests for batch Message-ID fetch."""

import pytest

from imap_migrate.exceptions import MessageIdBatchFetchError
from imap_migrate.imap_ops import fetch_message_ids_batch


class _FakeImap:
    def __init__(self, status: str, fetch_data: list) -> None:
        self._status = status
        self._fetch_data = fetch_data

    def uid(self, cmd: str, uid_range: str, spec: str) -> tuple[str, list]:
        return self._status, self._fetch_data


def test_fetch_message_ids_batch_ok() -> None:
    uid_list = [b"1", b"2"]
    fetch_data = [
        (
            b"1 (UID 1 BODY[HEADER.FIELDS (MESSAGE-ID)] {38}",
            b"Message-ID: <a@b>\r\n\r\n",
        ),
        b")",
        (
            b"2 (UID 2 BODY[HEADER.FIELDS (MESSAGE-ID)] {38}",
            b"Message-ID: <c@d>\r\n\r\n",
        ),
        b")",
    ]
    conn = _FakeImap("OK", fetch_data)
    out = fetch_message_ids_batch(conn, uid_list)
    assert len(out) == 2
    assert {x[0] for x in out} == {b"1", b"2"}


def test_fetch_message_ids_batch_bad_status_raises() -> None:
    conn = _FakeImap("NO", [])
    with pytest.raises(MessageIdBatchFetchError, match="NO"):
        fetch_message_ids_batch(conn, [b"1"])


def test_fetch_message_ids_batch_incomplete_raises() -> None:
    uid_list = [b"1", b"2"]
    fetch_data = [
        (
            b"1 (UID 1 BODY[HEADER.FIELDS (MESSAGE-ID)] {38}",
            b"Message-ID: <a@b>\r\n\r\n",
        ),
        b")",
    ]
    conn = _FakeImap("OK", fetch_data)
    with pytest.raises(MessageIdBatchFetchError, match="incomplete"):
        fetch_message_ids_batch(conn, uid_list)
