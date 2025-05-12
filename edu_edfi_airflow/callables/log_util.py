import logging
import json
from datetime import datetime, timezone
from contextlib import contextmanager
from typing import List, Generator


def format_log_record(record: logging.LogRecord) -> str:
    return json.dumps({
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'name': record.name,
        'level': record.levelname,
        'message': record.getMessage(),
        'pathname': record.pathname,
        'lineno': record.lineno
    })


@contextmanager
def capture_logs(logger_name: str = "airflow.task") -> Generator[List[str], None, None]:
    """
    Context manager that captures structured log records and yields them as a list of JSON strings.
    """
    log_records = []

    class StructuredLogHandler(logging.Handler):
        def emit(self, record):
            log_records.append(format_log_record(record))

    handler = StructuredLogHandler()
    logger = logging.getLogger(logger_name)
    logger.addHandler(handler)

    try:
        yield log_records
    finally:
        logger.removeHandler(handler)
        handler.close()
