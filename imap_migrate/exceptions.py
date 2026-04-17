"""Domain-specific errors for IMAP migration."""


class MessageIdBatchFetchError(RuntimeError):
    """Raised when UID FETCH for Message-ID headers fails or returns incomplete UID coverage."""
