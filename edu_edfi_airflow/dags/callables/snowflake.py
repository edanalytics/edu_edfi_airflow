import logging

from typing import List, Union

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from edu_edfi_airflow.dags.dag_util import airflow_util


def insert_into_snowflake(
    snowflake_conn_id: str,
    table_name: str,
    columns: List[str],
    values: Union[list, List[list]]
):
    """

    :param snowflake_conn_id:
    :param table_name:
    :param columns:
    :param values:
    :return:
    """
    # Retrieve the database and schema from the Snowflake hook.
    database, schema = airflow_util.get_snowflake_params_from_conn(snowflake_conn_id)

    # Force a single record into a list for iteration below.
    if not all(isinstance(val, (list, tuple)) for val in values):
        values = [values]

    qry_insert_into = f"""
        INSERT INTO {database}.{schema}.{table_name}
            ({', '.join(columns)})
        VALUES
            ({', '.join(['%s'] * len(columns))})
        ;
    """

    snowflake_hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)

    # Insert each row into the table, passing the values as parameters.
    for row in values:
        cursor_log = snowflake_hook.run(
            sql=qry_insert_into,
            parameters=row
        )
        logging.info(cursor_log)