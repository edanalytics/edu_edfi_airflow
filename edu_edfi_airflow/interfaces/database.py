import abc
import logging
import os

from airflow.hooks.base import BaseHook

from typing import List, Union

from edu_edfi_airflow.callables import airflow_util


class DatabaseInterface(abc.ABC):
    """
    
    """
    def __new__(cls, database_conn_id: str, **kwargs):
        conn = BaseHook.get_connection(database_conn_id)
        
        if conn.conn_type == 'snowflake':
            return object.__new__(SnowflakeDatabaseInterface)
        elif conn.conn_type == 'databricks':
            return object.__new__(DatabricksDatabaseInterface)
        else:
            raise ValueError(f"DatabaseInterface type {conn.conn_type} is not defined!")
        
    def __init__(self, database_conn_id: str, **kwargs):
        self.database_conn_id: str = database_conn_id
        self.database, self.schema = airflow_util.get_database_params_from_conn(database_conn_id)

        # Initialized within context manager.
        self.hook = None
        self.sql: List[str] = []

    @abc.abstractmethod
    def __enter__(self):
        raise NotImplementedError

    @abc.abstractmethod
    def __exit__(self, exc_type, exc_value, exc_traceback):
        raise NotImplementedError
    
    @abc.abstractmethod
    def query_database(self, sql: str, **kwargs):
        raise NotImplementedError
    
    @abc.abstractmethod
    def insert_into_database(self, table: str, columns: List[str], values: List[str], **kwargs):
        raise NotImplementedError

    @abc.abstractmethod
    def delete_from_raw(self, tenant_code: str, api_year: str, name: Union[str, List[str]], table: str) -> str:
        raise NotImplementedError
    
    @abc.abstractmethod
    def copy_into_raw(self,
        tenant_code: str, api_year: str, name: str, table: str,
        ods_version: int, data_model_version: str, storage_path: str
    ) -> str:
        raise NotImplementedError
    
    @abc.abstractmethod
    def bulk_copy_into_raw(self,
        tenant_code: str, api_year: str, name: List[str], table: str,
        ods_version: int, data_model_version: str, storage_path: str
    ) -> str:
        """ Same argument signature as singleton method. """
        raise NotImplementedError


class SnowflakeDatabaseInterface(DatabaseInterface):

    def __enter__(self):
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        self.hook = SnowflakeHook(self.database_conn_id)
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.hook.run(sql=self.sql, autocommit=False)

    def query_database(self, sql: str, **kwargs):
        """Run query on Snowflake and return results."""
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        snowflake_hook = SnowflakeHook(snowflake_conn_id=self.database_conn_id)
        return snowflake_hook.get_records(sql)
    
    def insert_into_database(self, table: str, columns: List[str], values: List[list], **kwargs):
        """Insert into Snowflake database."""
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        
        # Force a single record into a list for iteration below.
        if not all(isinstance(val, (list, tuple)) for val in values):
            values = [values]

        # Retrieve the database and schema from the Snowflake hook.
        logging_string = f"Inserting the following values into Snowflake table `{self.database}.{self.schema}.{table}`\nCols: {columns}\n"
        for idx, value in enumerate(values, start=1):
            logging_string += f"   {idx}: {value}\n"
        logging.info(logging_string)

        snowflake_hook = SnowflakeHook(snowflake_conn_id=self.database_conn_id)
        snowflake_hook.insert_rows(
            table=f"{self.database}.{self.schema}.{table}",
            rows=values,
            target_fields=columns,
        )

    # SQL Queries
    def delete_from_raw(self, tenant_code: str, api_year: str, name: Union[str, List[str]], table: str) -> str:
        # Use an array of names to allow same method to be used in single and bulk approaches.
        if isinstance(name, str):
            name = [name]

        names_repr = "', '".join(name)

        qry = f"""
            DELETE FROM {self.database}.{self.schema}.{table}
            WHERE tenant_code = '{tenant_code}'
            AND api_year = '{api_year}'
            AND name in ('{names_repr}')
        """
        self.sql.append(qry)
        return qry
    
    def copy_into_raw(self,
        tenant_code: str, api_year: str, name: str, table: str,
        ods_version: int, data_model_version: str, storage_path: str
    ) -> str:
        # Brackets in regex conflict with string formatting.
        date_regex = "\\\\d{8}"
        ts_regex = "\\\\d{8}T\\\\d{6}"

        qry = f"""
            COPY INTO {self.database}.{self.schema}.{table}
                (tenant_code, api_year, pull_date, pull_timestamp, file_row_number, filename, name, ods_version, data_model_version, v)
            FROM (
                SELECT
                    '{tenant_code}' AS tenant_code,
                    '{api_year}' AS api_year,
                    TO_DATE(REGEXP_SUBSTR(metadata$filename, '{date_regex}'), 'YYYYMMDD') AS pull_date,
                    TO_TIMESTAMP(REGEXP_SUBSTR(metadata$filename, '{ts_regex}'), 'YYYYMMDDTHH24MISS') AS pull_timestamp,
                    metadata$file_row_number AS file_row_number,
                    metadata$filename AS filename,
                    '{name}' AS name,
                    '{ods_version}' AS ods_version,
                    '{data_model_version}' AS data_model_version,
                    t.$1 AS v
                FROM '@{self.database}.util.airflow_stage/{storage_path}'
                (file_format => 'json_default') t
            )
            force = true;
        """
        self.sql.append(qry)
        return qry

    
    def bulk_copy_into_raw(self,
        tenant_code: str, api_year: str, name: List[str], table: str,
        ods_version: int, data_model_version: str, storage_path: str
    ) -> str:
        # Always use the directory for bulk copies into Snowflake
        storage_dir = os.path.dirname(storage_path)

        # Brackets in regex conflict with string formatting.
        date_regex = "\\\\d{8}"
        ts_regex = "\\\\d{8}T\\\\d{6}"

        qry = f"""
            COPY INTO {self.database}.{self.schema}.{table}
                (tenant_code, api_year, pull_date, pull_timestamp, file_row_number, filename, name, ods_version, data_model_version, v)
            FROM (
                SELECT
                    '{tenant_code}' AS tenant_code,
                    '{api_year}' AS api_year,
                    TO_DATE(REGEXP_SUBSTR(metadata$filename, '{date_regex}'), 'YYYYMMDD') AS pull_date,
                    TO_TIMESTAMP(REGEXP_SUBSTR(metadata$filename, '{ts_regex}'), 'YYYYMMDDTHH24MISS') AS pull_timestamp,
                    metadata$file_row_number AS file_row_number,
                    metadata$filename AS filename,
                    REGEXP_SUBSTR(filename, '.+/(\\\\w+).jsonl?', 1, 1, 'c', 1) AS name,
                    '{ods_version}' AS ods_version,
                    '{data_model_version}' AS data_model_version,
                    t.$1 AS v
                FROM '@{self.database}.util.airflow_stage/{storage_dir}'
                (file_format => 'json_default') t
            )
            force = true;
        """
        self.sql.append(qry)
        return qry


class DatabricksDatabaseInterface(DatabaseInterface):

    def __enter__(self):
        from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook
        self.hook = DatabricksSqlHook(self.database_conn_id)
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.hook.run(sql=self.sql, autocommit=False)

    def query_database(self, sql: str, **kwargs):
        """Run query on Databricks and return results."""
        from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook
        databricks_hook = DatabricksSqlHook(self.database_conn_id)
        return databricks_hook.get_records(sql)
    
    def insert_into_database(self, table: str, columns: List[str], values: List[list], **kwargs):
        """Insert into Databricks database."""
        from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook
        
        # Force a single record into a list for iteration below.
        if not all(isinstance(val, (list, tuple)) for val in values):
            values = [values]

        logging_string = f"Inserting the following values into Databricks table `{self.database}.{self.schema}.{table}`\nCols: {columns}\n"
        for idx, value in enumerate(values, start=1):
            logging_string += f"   {idx}: {value}\n"
        logging.info(logging_string)

        # Build INSERT SQL statement
        full_table_name = f"{self.database}.{self.schema}.{table}"
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
        databricks_hook = DatabricksSqlHook(self.database_conn_id)
        databricks_hook.run(sql=insert_statements, autocommit=True)

    # SQL Queries
    def delete_from_raw(self, tenant_code: str, api_year: str, name: Union[str, List[str]], table: str) -> str:
        # Use an array of names to allow same method to be used in single and bulk approaches.
        if isinstance(name, str):
            name = [name]

        names_repr = "', '".join(name)

        qry = f"""
            DELETE FROM {self.database}.{self.schema}.{table}
            WHERE tenant_code = '{tenant_code}'
            AND api_year = '{api_year}'
            AND name in ('{names_repr}')
        """
        self.sql.append(qry)
        return qry
    
    def copy_into_raw(self,
        tenant_code: str, api_year: str, name: str, table: str,
        ods_version: int, data_model_version: str, storage_path: str
    ) -> str:
        qry = f"""
            COPY INTO {self.database}.{self.schema}.{table}
            FROM (
                SELECT 
                    '{tenant_code}' AS tenant_code,
                    {api_year} AS api_year,
                    current_date() AS pull_date,
                    current_timestamp() AS pull_timestamp,
                    -- not available in ADLS
                    null AS file_row_number,
                    _metadata.file_path AS filename,
                    '{name}' AS name,
                    '{ods_version}' AS ods_version,
                    '{data_model_version}' AS data_model_version,
                    v
                FROM '{storage_path}'
            )
            FILEFORMAT = JSON
            FORMAT_OPTIONS ('singleVariantColumn' = 'v')
            COPY_OPTIONS ('force' = 'true', 'mergeSchema' = 'true')
        """
        self.sql.append(qry)
        return qry
    
    def bulk_copy_into_raw(self,
        tenant_code: str, api_year: str, name: List[str], table: str,
        ods_version: int, data_model_version: str, storage_path: str
    ) -> str:
        """
        For now, implement bulk operations by calling the single operation for each name.
        This is a simple implementation that can be optimized later.
        """
        all_queries = []

        for nn in name:
            qry = self.copy_into_raw(tenant_code, api_year, nn, table, ods_version, data_model_version, storage_path)
            all_queries.append(qry)

        return all_queries.join("\n")  # Return all queries at once.
