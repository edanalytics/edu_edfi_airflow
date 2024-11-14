from typing import Callable, Optional

from edu_edfi_airflow.callables import airflow_util

def log_to_snowflake(
    snowflake_conn_id: str,
    logging_table: str,

    tenant_code: str,
    api_year: int,
    run_type: str,
    grain_update: Optional[str] = None,

    # Mutually-exclusive arguments
    log_filepath: Optional[str] = None,
    log_data: Optional[dict] = None,
    **kwargs
):
    """

    :return:
    """
    # Short-circuit fail before imports.
    if not snowflake_conn_id:
        raise Exception(
            "Snowflake connection required to copy logs into Snowflake."
        )

    from airflow.exceptions import AirflowSkipException
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

    if log_filepath:
        # Assume the results file is overwritten at every run.
        # If not found, raise a skip-exception instead of failing.
        try:
            with open(log_filepath, 'r') as fp:
                log_data = fp.read()
        except FileNotFoundError:
            raise AirflowSkipException(
                f"Results file not found: {log_filepath}\n"
                "Did Earthmover/Lightbeam run without error?"
            )

    # Retrieve the database and schema from the Snowflake hook and build the insert-query.
    database, schema = airflow_util.get_snowflake_params_from_conn(snowflake_conn_id)

    grain_update_str = f"'{grain_update}'" if grain_update else "NULL"

    qry_insert_into = f"""
        INSERT INTO {database}.{schema}.{logging_table}
            (tenant_code, api_year, grain_update, run_type, run_date, run_timestamp, result)
        SELECT
            '{tenant_code}' AS tenant_code,
            '{api_year}' AS api_year,
            {grain_update_str} AS grain_update,
            '{run_type}' AS run_type,
            '{kwargs['ds']}' AS run_date,
            '{kwargs['ts']}' AS run_timestamp,
            PARSE_JSON($${log_data}$$) AS result
    """

    # Insert each row into the table, passing the values as parameters.
    snowflake_hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
    snowflake_hook.run(
        sql=qry_insert_into
    )


def format_log_record(record, args, kwargs):

    from datetime import datetime, timezone
    import json

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


def capture_logs(
    python_callable: Callable,
    snowflake_conn_id: str,
    logging_table: Optional[str],

    tenant_code: str,
    api_year: int,
    grain_update: Optional[str] = None,
):
    def wrapper(*args, **kwargs):

        import logging
        import json
        import io

        # Create a logger
        logger = logging.getLogger(python_callable.__name__)
        logger.setLevel(logging.DEBUG)

        # Create StringIO stream to capture logs
        log_capture_string = io.StringIO()
        ch = logging.StreamHandler(log_capture_string)
        ch.setLevel(logging.DEBUG)
        logger.addHandler(ch)

        try:
            result = python_callable(*args, **kwargs)

        except Exception as err:
            logger.error(f"Error in {python_callable.__name__}: {err}")
            raise

        finally:
            # Ensure all log entries are flushed before closing the stream
            ch.flush()
            log_contents = log_capture_string.getvalue()

            # Remove the handler and close the StringIO stream
            logger.removeHandler(ch)
            log_capture_string.close()

            # Send logs to Snowflake
            log_entries = log_contents.splitlines()
            for entry in log_entries:
                record = logging.LogRecord(
                    name=python_callable.__name__,
                    level=logging.DEBUG,
                    pathname='',
                    lineno=0,
                    msg=entry,
                    args=None,
                    exc_info=None
                )
                log_data = json.loads(format_log_record(record, args, kwargs))
                log_to_snowflake(
                    snowflake_conn_id=snowflake_conn_id,
                    logging_table=logging_table,
                    log_data=log_data,
                    tenant_code=tenant_code,
                    api_year=api_year,
                    grain_update=grain_update,
                    **kwargs
                )

        return result
    return wrapper