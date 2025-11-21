import logging

from typing import List, Union
from airflow.models.connection import Connection
from edu_edfi_airflow.providers.snowflake.transfers.s3_to_snowflake import ObjectStorageToDatabaseOperator

from edu_edfi_airflow.callables import airflow_util


def insert_into_database(
    database_conn_id: str,
    table_name: str,
    columns: List[str],
    values: Union[list, List[list]],
    **kwargs
):
    """
    Generic database insert function that works with Snowflake, Databricks, or other databases (if added later).
    
    :param database_conn_id: The database connection ID (can be snowflake_conn_id, databricks_conn_id, etc.)
    :param table_name: The table name to insert into
    :param columns: List of column names
    :param values: List of values to insert
    :param kwargs: Additional arguments that may contain connection type info
    :return:
    """
    # Force a single record into a list for iteration below.
    if not all(isinstance(val, (list, tuple)) for val in values):
        values = [values]

    connection = Connection.get_connection_from_secrets(database_conn_id)
    if connection.conn_type == 'snowflake':
        _insert_into_snowflake(database_conn_id, table_name, columns, values)
    elif connection.conn_type == 'databricks':
        _insert_into_databricks(database_conn_id, table_name, columns, values)
    else:
        raise ValueError(f"Unsupported database type: {connection.conn_type}")


def _insert_into_snowflake(conn_id: str, table_name: str, columns: List[str], values: List[list]):
    """Insert into Snowflake database."""
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
    
    # Retrieve the database and schema from the Snowflake hook.
    database, schema = airflow_util.get_database_params_from_conn(conn_id)

    logging_string = f"Inserting the following values into Snowflake table `{database}.{schema}.{table_name}`\nCols: {columns}\n"
    for idx, value in enumerate(values, start=1):
        logging_string += f"   {idx}: {value}\n"
    logging.info(logging_string)

    snowflake_hook = SnowflakeHook(snowflake_conn_id=conn_id)
    snowflake_hook.insert_rows(
        table=f"{database}.{schema}.{table_name}",
        rows=values,
        target_fields=columns,
    )


def _insert_into_databricks(conn_id: str, table_name: str, columns: List[str], values: List[list]):
    """Insert into Databricks database."""
    from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook
    
    # Retrieve the database and schema from the Databricks connection.
    database, schema = airflow_util.get_database_params_from_conn(conn_id)

    logging_string = f"Inserting the following values into Databricks table `{database}.{schema}.{table_name}`\nCols: {columns}\n"
    for idx, value in enumerate(values, start=1):
        logging_string += f"   {idx}: {value}\n"
    logging.info(logging_string)

    # Build INSERT SQL statement
    full_table_name = f"{database}.{schema}.{table_name}"
    columns_str = ", ".join(columns)
    
    # Build VALUES clauses for all rows
    insert_statements = []
    for row in values:
        # Escape and quote string values, handle None/NULL values
        escaped_values = []
        for val in row:
            if val is None:
                escaped_values.append("NULL")
            elif isinstance(val, str):
                # Escape single quotes by doubling them
                escaped_val = val.replace("'", "''")
                escaped_values.append(f"'{escaped_val}'")
            elif isinstance(val, bool):
                escaped_values.append("TRUE" if val else "FALSE")
            else:
                escaped_values.append(str(val))
        
        values_str = ", ".join(escaped_values)
        insert_statements.append(f"INSERT INTO {full_table_name} ({columns_str}) VALUES ({values_str})")

    # Execute all insert statements
    databricks_hook = DatabricksSqlHook(conn_id)
    databricks_hook.run(sql=insert_statements, autocommit=True)


def run_database_query(database_conn_id: str, query: str, **kwargs):
    """
    Generic database query function that works with Snowflake, Databricks, or other databases.
    
    :param database_conn_id: The database connection ID
    :param query: SQL query to execute
    :param kwargs: Additional arguments that may contain connection type info
    :return: Query results
    """

    connection = Connection.get_connection_from_secrets(database_conn_id)
    if connection.conn_type == 'snowflake':
        return _run_snowflake_query(database_conn_id, query)
    elif connection.conn_type == 'databricks':
        return _run_databricks_query(database_conn_id, query)
    else:
        raise ValueError(f"Unsupported database type: {connection.conn_type}")

def _run_snowflake_query(conn_id: str, query: str):
    """Run query on Snowflake and return results."""
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
    
    snowflake_hook = SnowflakeHook(snowflake_conn_id=conn_id)
    return snowflake_hook.get_records(query)


def _run_databricks_query(conn_id: str, query: str):
    """Run query on Databricks and return results."""
    from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook
    
    databricks_hook = DatabricksSqlHook(conn_id)
    return databricks_hook.get_records(query)
