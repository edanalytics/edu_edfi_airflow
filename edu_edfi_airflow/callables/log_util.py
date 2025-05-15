# log_utils.py

import logging
import json
from datetime import datetime, timezone
from typing import Callable, Optional
from contextlib import contextmanager

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from edu_edfi_airflow.callables import airflow_util


def format_log_record(record, args, kwargs):
    def serialize_argument(arg):
        try:
            return json.dumps(arg)
        except TypeError:
            return str(arg)

    log_record = {
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'name': record.name,
        'level': record.levelname,
        'message': record.getMessage(),
        'pathname': record.pathname,
        'lineno': record.lineno,
        'args': {k: serialize_argument(v) for k, v in enumerate(args)},
        'kwargs': {k: serialize_argument(v) for k, v in kwargs.items()},
    }
    return json.dumps(log_record)


@contextmanager
def structured_log_capture(args, kwargs, logger_name: str = "airflow.task"):
    log_records = []

    class StructuredLogHandler(logging.Handler):
        def emit(self, record):
            if record.levelno >= logging.ERROR:  # only log warnings/errors
                log_records.append(format_log_record(record, args, kwargs))

    handler = StructuredLogHandler()
    logger = logging.getLogger(logger_name)
    logger.addHandler(handler)

    try:
        yield log_records
    finally:
        logger.removeHandler(handler)
        handler.close()


def log_to_snowflake(
    *,
    snowflake_conn_id: str,
    logging_table: str,
    log_data: str,
    tenant_code: str,
    api_year: int,
    run_type: str,
    run_date: str,
    run_timestamp: str,
    grain_update: Optional[str] = None
):
    database, schema = airflow_util.get_snowflake_params_from_conn(snowflake_conn_id)
    grain_update_str = f"'{grain_update}'" if grain_update else "NULL"

    insert_sql = f"""
        INSERT INTO {database}.{schema}.{logging_table}
        (tenant_code, api_year, grain_update, run_type, run_date, run_timestamp, result)
        SELECT
            '{tenant_code}',
            '{api_year}',
            {grain_update_str},
            '{run_type}',
            '{run_date}',
            '{run_timestamp}',
            PARSE_JSON($${log_data}$$)
    """

    hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
    hook.run(insert_sql)


def capture_logs_to_snowflake(
    run_callable: Callable,
    snowflake_conn_id: str,
    logging_table: str,
    tenant_code: str,
    api_year: int,
    run_type: str,
    grain_update: Optional[str] = None,
):
    def wrapper(*args, **kwargs):
        from airflow.utils.context import Context
        context = kwargs.get("context", kwargs)

        def flush_logs(log_records):
            if snowflake_conn_id and log_records:
                structured_logs = "[{}]".format(",".join(log_records))
                from airflow_util import log_to_snowflake
                log_to_snowflake(
                    snowflake_conn_id=snowflake_conn_id,
                    logging_table=logging_table,
                    log_data=structured_logs,
                    tenant_code=tenant_code,
                    api_year=api_year,
                    run_type=run_type,
                    grain_update=grain_update,
                    run_date=context.get("ds"),
                    run_timestamp=context.get("ts"),
                )

        with structured_log_capture(args, kwargs) as log_records:
            try:
                return run_callable(*args, **kwargs)
            except Exception as err:
                logging.getLogger("airflow.task").exception("Earthmover failed during execution")  # <- key line
                raise
            finally:
                flush_logs(log_records)

    return wrapper