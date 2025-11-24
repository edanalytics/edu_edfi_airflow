import abc

from airflow.hooks.base import BaseHook

from typing import List, Union

from edu_edfi_airflow.callables import airflow_util


class DatabaseMixin(abc.ABC):
    """
    
    """
    def __new__(cls, database_conn_id: str, **kwargs):
        conn = BaseHook.get_connection(database_conn_id)
        
        # TODO: the logic used to determine connection type will not be this easy.
        if conn.conn_type == 'snowflake':
            return object.__new__(SnowflakeDatabaseMixin)
        elif conn.conn_type == 'databricks':
            return object.__new__(DatabricksDatabaseMixin)
        else:
            raise ValueError(f"DatabaseType type {conn.conn_type} is not defined!")
        
    def __init__(self, database_conn_id: str, **kwargs):
        self.database_conn_id: str = database_conn_id
        self.database, self.schema = airflow_util.get_database_params_from_conn(database_conn_id)

    @abc.abstractmethod
    def run_sql_queries(self, queries: List[str]):
        raise NotImplementedError

    @abc.abstractmethod
    def build_delete_query(self, tenant_code: str, api_year: str, name: str, table: str) -> str:
        raise NotImplementedError
    
    @abc.abstractmethod
    def build_copy_query(self,
        tenant_code: str, api_year: str, name: str, table: str,
        ods_version: int, data_model_version: str, storage_path: str
    ) -> str:
        raise NotImplementedError
    
    @abc.abstractmethod
    def build_bulk_copy_query(self,
        tenant_code: str, api_year: str, name: Union[str, List[str]], table: str,
        ods_version: int, data_model_version: str, storage_path: str
    ) -> str:
        """ Same argument signature as singleton method. """
        raise NotImplementedError


class SnowflakeDatabaseMixin(DatabaseMixin):

    def run_sql_queries(self, sql: List[str]):
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        snowflake_hook = SnowflakeHook(self.database_conn_id)
        snowflake_hook.run(sql=sql, autocommit=False)
    
    def build_delete_query(self, tenant_code: str, api_year: str, name: Union[str, List[str]], table: str) -> str:
        # Use an array of names to allow same method to be used in single and bulk approaches.
        if not isinstance(name, str):
            name = [name]

        names_repr = "', '".join(name)

        return f"""
            DELETE FROM {self.database}.{self.schema}.{table}
            WHERE tenant_code = '{tenant_code}'
            AND api_year = '{api_year}'
            AND name in ('{names_repr}')
        """
    
    def build_copy_query(self,
        tenant_code: str, api_year: str, name: str, table: str,
        ods_version: int, data_model_version: str, storage_path: str
    ) -> str:
        # Brackets in regex conflict with string formatting.
        date_regex = "\\\\d{8}"
        ts_regex = "\\\\d{8}T\\\\d{6}"

        return f"""
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
    
    def build_bulk_copy_query(self,
        tenant_code: str, api_year: str, name: Union[str, List[str]], table: str,
        ods_version: int, data_model_version: str, storage_path: str
    ) -> str:
        # Brackets in regex conflict with string formatting.
        date_regex = "\\\\d{8}"
        ts_regex = "\\\\d{8}T\\\\d{6}"

        return f"""
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
                FROM '@{self.database}.util.airflow_stage/{storage_path}'
                (file_format => 'json_default') t
            )
            force = true;
        """

class DatabricksDatabaseMixin:
    def run_sql_queries(self, sql: List[str]):
        from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook
        databricks_hook = DatabricksSqlHook(self.database_conn_id)
        databricks_hook.run(sql=sql, autocommit=False)
    
    def build_delete_query(self, tenant_code: str, api_year: str, name: str, table: str) -> str:
        # Use an array of names to allow same method to be used in single and bulk approaches.
        if not isinstance(name, str):
            name = [name]

        names_repr = "', '".join(name)

        return f"""
            DELETE FROM {self.database}.{self.schema}.{table}
            WHERE tenant_code = '{tenant_code}'
            AND api_year = '{api_year}'
            AND name in ('{names_repr}')
        """
    
    def build_copy_query(self,
        tenant_code: str, api_year: str, name: str, table: str,
        ods_version: int, data_model_version: str, storage_path: str
    ) -> str:
        return f"""
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
    
    def build_bulk_copy_query(self,
        tenant_code: str, api_year: str, name: Union[str, List[str]], table: str,
        ods_version: int, data_model_version: str, storage_path: str
    ) -> str:
        ### Retrieve the database and schema from the database connection.
        copy_queries = [
            self.build_copy_query(tenant_code, api_year, nn, table, ods_version, data_model_version, storage_path)
            for nn in name
        ]

        return copy_queries.join("\n")
