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
    # Force a single record into a list for iteration below.
    if not all(isinstance(val, (list, tuple)) for val in values):
        values = [values]

    # Retrieve the database and schema from the Snowflake hook.
    database, schema = airflow_util.get_snowflake_params_from_conn(snowflake_conn_id)

    snowflake_hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
    snowflake_hook.insert_rows(
        table=f"{database}.{schema}.{table_name}",
        rows=values,
        target_fields=columns,
    )
