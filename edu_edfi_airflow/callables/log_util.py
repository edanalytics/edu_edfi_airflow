# log_util.py

import logging
import json
from datetime import datetime, timezone
from typing import Callable, Optional
from contextlib import contextmanager

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from edu_edfi_airflow.callables import airflow_util


def serialize_argument(arg):
    try:
        return json.dumps(arg)
    except TypeError:
        return str(arg)

def format_log_record(record, args, kwargs):
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
):
    def wrapper(*args, **kwargs):
        
        # Robustly extract the Airflow context
        context = kwargs.get("context") or (kwargs if {"ds", "ts"} <= kwargs.keys() else None)
        if not context:
            raise ValueError("Airflow context not found. Cannot extract ds/ts.")
        
        def flush_logs(log_records, context):
            structured = [json.loads(r) for r in log_records]
            errors = [r for r in structured if r.get("level") == "ERROR" or "ERROR" in r.get("message", "")]
            if not errors:
                return
            last_error = errors[-1]

            # Strip args and kwargs from each record (want to display them only once)
            cleaned_errors = [
                {k: v for k, v in record.items() if k not in ("args", "kwargs")}
                for record in errors
            ]

            # Pull map index if available
            ti = context.get("ti")
            map_index = getattr(ti, "map_index", None) if ti else None

            # Combine: args/kwargs/timestamp from last error, all other context in nested errors{} object
            # (this is to avoid duplicating args/kwargs/timestamp in each error log, while preserving error-specific context)
            merged = {
                "args": last_error.get("args", {}),
                "kwargs": last_error.get("kwargs", {}),
                "last_error_timestamp": last_error.get("timestamp"),
                "errors": cleaned_errors,
            }

            if map_index is not None:
                merged["map_index"] = map_index
                
            logging.getLogger("airflow.task").info(f"[DEBUG] flushing error log record to Snowflake")

            log_to_snowflake(
                snowflake_conn_id=snowflake_conn_id,
                logging_table=logging_table,
                log_data=json.dumps(merged),
                tenant_code=tenant_code,
                api_year=api_year,
                run_type=run_type,
                grain_update=grain_update,
                run_date=context.get("ds"),
                run_timestamp=context.get("ts"),
            )

        with structured_log_capture(args, kwargs=context) as log_records:
            try:
                return run_callable(*args, **kwargs)
            except Exception as err:
                logging.getLogger("airflow.task").exception(f"{type(err).__name__} raised during task execution: {err}")
                raise
            finally:
                flush_logs(log_records, context)

    return wrapper
