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
    *args, **kwargs
):
    """
    Acts as either:
    1. A wrapper that returns a callable (for PythonOperator-style usage)
    2. A direct executor that logs immediately (for Operator .execute() usage)

    Usage:
        - As wrapper: wrapped = capture_logs_to_snowflake(fn, ...)
        - As executor: capture_logs_to_snowflake(fn, ..., arg1, arg2, kwarg1=...)
    """
    def _wrapped(*inner_args, **inner_kwargs):
        from airflow.utils.context import Context
        context = inner_kwargs.get('context', inner_kwargs)

        with structured_log_capture(inner_args, inner_kwargs) as log_records:
            result = run_callable(*inner_args, **inner_kwargs)

        if snowflake_conn_id and log_records:
            structured_logs = "[{}]".format(",".join(log_records))

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

        return result

    # If arguments were passed, run immediately (Operator use-case)
    if args or kwargs:
        return _wrapped(*args, **kwargs)
    else:
        return _wrapped  # return wrapped function for deferred use