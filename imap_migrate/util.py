"""Human-readable formatting and connection error messages."""


def human_size(nbytes: int | float) -> str:
    """Human-readable size: 1234567 -> '1.18 MB'."""
    for unit in ("B", "KB", "MB", "GB"):
        if abs(nbytes) < 1024:
            return f"{nbytes:.1f} {unit}" if unit != "B" else f"{int(nbytes)} B"
        nbytes /= 1024
    return f"{nbytes:.2f} TB"


def human_duration(seconds: float) -> str:
    """Human-readable duration in Russian."""
    if seconds < 60:
        return f"{seconds:.0f} сек"
    if seconds < 3600:
        m, s = divmod(int(seconds), 60)
        return f"{m} мин {s} сек"
    h, rem = divmod(int(seconds), 3600)
    m, s = divmod(rem, 60)
    return f"{h} ч {m} мин"


def friendly_error(exc: Exception) -> str:
    """Returns a user-friendly message for known connection/IMAP errors."""
    msg = str(exc).strip()
    if "Broken pipe" in msg or "Errno 32" in msg:
        return "Соединение разорвано (Broken pipe)"
    if "nodename nor servname" in msg or "Errno 8" in msg:
        return "DNS-ошибка: сервер недоступен"
    if "EOF" in msg or "Connection reset" in msg:
        return "Сервер закрыл соединение"
    if "timed out" in msg or "timeout" in msg.lower():
        return "Таймаут соединения"
    if "Connection refused" in msg:
        return "Подключение отклонено (порт закрыт или сервер недоступен)"
    return msg
