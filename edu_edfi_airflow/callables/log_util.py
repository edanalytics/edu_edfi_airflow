import logging
import io
from contextlib import contextmanager
from typing import Generator, Optional

@contextmanager
def capture_log_stream(level=logging.INFO, logger_name: Optional[str] = None) -> Generator[str, None, None]:
    """
    Context manager that captures logs emitted during a block.
    Returns the log contents as a string.
    """
    log_capture_string = io.StringIO()
    handler = logging.StreamHandler(log_capture_string)
    handler.setLevel(level)

    logger = logging.getLogger(logger_name) if logger_name else logging.getLogger()
    logger.addHandler(handler)

    try:
        yield log_capture_string
    finally:
        logger.removeHandler(handler)
        handler.close()
