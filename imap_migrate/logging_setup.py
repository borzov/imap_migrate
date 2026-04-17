"""Logging configuration and optional tqdm integration."""

import logging
import sys
from typing import Optional

try:
    from tqdm import tqdm

    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False
    tqdm = None  # type: ignore[misc, assignment]

try:
    import yaml

    HAS_YAML = True
except ImportError:
    HAS_YAML = False

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


class TqdmLoggingHandler(logging.StreamHandler):
    """StreamHandler that writes via tqdm.write() so progress bars stay intact."""

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
            if HAS_TQDM and tqdm is not None:
                tqdm.write(msg, file=self.stream)
            else:
                self.stream.write(msg + self.terminator)
            self.flush()
        except Exception:
            self.handleError(record)


def setup_logging(log_file: Optional[str] = None, verbose: bool = False) -> None:
    """Configure root logger for console and optional file output."""
    level = logging.DEBUG if verbose else logging.INFO
    root = logging.getLogger()
    root.setLevel(level)
    root.handlers.clear()

    console = TqdmLoggingHandler(sys.stdout)
    console.setFormatter(logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT))
    root.addHandler(console)

    if log_file:
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setFormatter(logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT))
        root.addHandler(fh)
