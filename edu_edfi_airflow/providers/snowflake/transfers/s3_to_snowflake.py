import abc
import logging
import os

from typing import Any, List, Optional, Tuple, Union

from airflow.exceptions import AirflowSkipException
from airflow.models import BaseOperator

from edu_edfi_airflow.callables import airflow_util
from edu_edfi_airflow.providers.edfi.hooks.edfi import EdFiHook

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from airflow.io.path import ObjectStoragePath


class ObjectStorageToDatabaseOperator(BaseOperator, abc.ABC):
    """
    Copy the Ed-Fi files from object storage to raw resource tables in database.
    """
    template_fields = ('resource', 'table_name',)

    
    def __new__(cls, *args, resource: Union[str, List[str]], **kwargs):
        if 'snowflake_conn_id' in kwargs:
            if isinstance(resource, str):
                return object.__new__(ObjectStorageToSnowflakeOperator)
            else:
                return object.__new__(BulkObjectStorageToSnowflakeOperator)

        raise ValueError(
            "No child class of `ObjectStorageToDatabaseOperator` has been defined with the given arguments!" 
        )


    def __init__(self,
        *,
        tenant_code: str,
        api_year: int,
        resource: str,
        table_name: str,

        database_conn_id: str,
        object_storage: 'ObjectStoragePath',

        edfi_conn_id: Optional[str] = None,  # Optional: only used to retrieve unspecified version metadata
        ods_version: Optional[str] = None,
        data_model_version: Optional[str] = None,

        full_refresh: bool = False,
        **kwargs
    ) -> None:
        super(ObjectStorageToDatabaseOperator, self).__init__(**kwargs)

        self.database_conn_id = database_conn_id
        self.object_storage = object_storage

        self.tenant_code = tenant_code
        self.api_year = api_year
        self.resource = resource
        self.table_name = table_name

        self.edfi_conn_id = edfi_conn_id
        self.ods_version = ods_version
        self.data_model_version = data_model_version

        self.full_refresh = full_refresh


    def execute(self, context):
        """

        :param context:
        :return:
        """
        ### Retrieve the Ed-Fi, ODS, and data model versions in execute to prevent excessive API calls.
        self.set_edfi_attributes()

        # Build and run the SQL queries to database. Delete first if EdFi2 or a full-refresh.
        self.run_sql_queries(
            name=self.resource, table=self.table_name, object_storage=self.object_storage,
            full_refresh=self.full_refresh or airflow_util.is_full_refresh(context)
        )

    def set_edfi_attributes(self):
        """
        Retrieve the Ed-Fi, ODS, and data model versions if not provided.
        This needs to occur in execute to not call the API at every Airflow synchronize.
        """
        if self.edfi_conn_id:
            edfi_conn = EdFiHook(edfi_conn_id=self.edfi_conn_id).get_conn()
            if is_edfi2 := edfi_conn.is_edfi2():
                self.full_refresh = True

            if not self.ods_version:
                self.ods_version = 'ED-FI2' if is_edfi2 else edfi_conn.get_ods_version()

            if not self.data_model_version:
                self.data_model_version = 'ED-FI2' if is_edfi2 else edfi_conn.get_data_model_version()

        if not (self.ods_version and self.data_model_version):
            raise Exception(
                f"Arguments `ods_version` and `data_model_version` could not be retrieved and must be provided."
            )
    
    @abc.abstractmethod
    def run_sql_queries(self, name: str, table: str, object_storage: 'ObjectStoragePath', full_refresh: bool = False):
        raise NotImplementedError(
            "Method `run_sql_queries()` must be redefined in database child class!"
        )


class BulkObjectStorageToDatabaseOperator(ObjectStorageToDatabaseOperator):
    """
    Copy the Ed-Fi files saved to S3 to database raw resource tables.
    """
    def execute(self, context):
        """

        :param context:
        :return:
        """
        if not self.resource:
            raise AirflowSkipException("There are no endpoints to copy to database. Skipping task...")

        # Force potential string columns into lists for zipping in execute.
        if isinstance(self.resource, str):
            raise ValueError("Bulk operators require lists of resources to be passed.")

        # we have to do this for the time being because the XCom that produces this list
        # actually returns a lazily-evaluated object with no len() property
        self.resource = list(self.resource)

        ### Retrieve the Ed-Fi, ODS, and data model versions in execute to prevent excessive API calls.
        self.set_edfi_attributes()

        ### Build and run the SQL queries to database. Delete first if EdFi2 or a full-refresh.
        # If all data is sent to the same table, use a single massive SQL query to copy the data from the directory.
        if isinstance(self.table_name, str):
            logging.info("Running bulk statements on a single table.")
            return self.run_bulk_sql_queries(
                names=self.resource, table=self.table_name, object_storage=self.object_storage[0],  # Assume all objects use same parent path.
                full_refresh=self.full_refresh or airflow_util.is_full_refresh(context)
            )
        
        # Otherwise, loop over each S3 destination and copy in sequence.
        for idx, (resource, table, object_storage) in enumerate(zip(self.resource, self.table_name, self.object_storage), start=1):
            logging.info(f"[ENDPOINT {idx} / {len(self.resource)}]")
            self.run_sql_queries(
                name=resource, table=table, object_storage=object_storage,
                full_refresh=self.full_refresh or airflow_util.is_full_refresh(context)
            )
        
    @abc.abstractmethod
    def run_bulk_sql_queries(self, name: str, table: str, object_storage: 'ObjectStoragePath', full_refresh: bool = False):
        raise NotImplementedError(
            "Method `run_bulk_sql_queries()` must be redefined in database child class!"
        )


class SnowflakeMixin:
    def __init__(self, *args, snowflake_conn_id: str, **kwargs):
        super(SnowflakeMixin, self).__init__(*args, database_conn_id=snowflake_conn_id, **kwargs)
        
    def run_sql_queries(self, name: str, table: str, object_storage: 'ObjectStoragePath', full_refresh: bool = False):
        """

        """
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        
        ### Retrieve the database and schema from the database connection.
        database, schema = airflow_util.get_database_params_from_conn(self.database_conn_id, 'extra__snowflake__database')

        ### Build the SQL queries to be passed into `Hook.run()`.
        qry_delete = f"""
            DELETE FROM {database}.{schema}.{table}
            WHERE tenant_code = '{self.tenant_code}'
            AND api_year = '{self.api_year}'
            AND name = '{name}'
        """

        # Brackets in regex conflict with string formatting.
        date_regex = "\\\\d{8}"
        ts_regex = "\\\\d{8}T\\\\d{6}"

        qry_copy_into = f"""
            COPY INTO {database}.{schema}.{table}
                (tenant_code, api_year, pull_date, pull_timestamp, file_row_number, filename, name, ods_version, data_model_version, v)
            FROM (
                SELECT
                    '{self.tenant_code}' AS tenant_code,
                    '{self.api_year}' AS api_year,
                    TO_DATE(REGEXP_SUBSTR(metadata$filename, '{date_regex}'), 'YYYYMMDD') AS pull_date,
                    TO_TIMESTAMP(REGEXP_SUBSTR(metadata$filename, '{ts_regex}'), 'YYYYMMDDTHH24MISS') AS pull_timestamp,
                    metadata$file_row_number AS file_row_number,
                    metadata$filename AS filename,
                    '{name}' AS name,
                    '{self.ods_version}' AS ods_version,
                    '{self.data_model_version}' AS data_model_version,
                    t.$1 AS v
                FROM '@{database}.util.airflow_stage/{object_storage.key}'
                (file_format => 'json_default') t
            )
            force = true;
        """
        
        ### Commit the update queries to database.
        # Incremental runs are only available in EdFi 3+.
        if full_refresh:
            queries_to_run = [qry_delete, qry_copy_into]
        else:
            queries_to_run = [qry_copy_into]

        snowflake_hook = SnowflakeHook(self.database_conn_id)
        snowflake_hook.run(sql=queries_to_run, autocommit=False)

    def run_bulk_sql_queries(self, names: List[str], table: str, object_storage: 'ObjectStoragePath', full_refresh: bool = False):
        """
        Alternative delete and copy queries to be run when all data is sent to the same table in database.
        
        S3 Path Structure:
            /{tenant_code}/{api_year}/{ds_nodash}/{ts_no_dash}/{taskgroup_type}/{name}.jsonl

        Use regex to capture name: ".+/(\\w+).jsonl?"
        Note optional args in REGEXP_SUBSTR(): position (1), occurrence (1), regex_parameters ('c'), group_num
        """
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        ### Retrieve the database and schema from the database connection.
        database, schema = self.get_database_params_from_conn(self.database_conn_id)

        # Infer name from files within directory.
        directory = os.path.dirname(object_storage.key)

        ### Build the SQL queries to be passed into `Hook.run()`.
        # Brackets in regex conflict with string formatting.
        date_regex = "\\\\d{8}"
        ts_regex = "\\\\d{8}T\\\\d{6}"

        qry_copy_into = f"""
            COPY INTO {database}.{schema}.{table}
                (tenant_code, api_year, pull_date, pull_timestamp, file_row_number, filename, name, ods_version, data_model_version, v)
            FROM (
                SELECT
                    '{self.tenant_code}' AS tenant_code,
                    '{self.api_year}' AS api_year,
                    TO_DATE(REGEXP_SUBSTR(metadata$filename, '{date_regex}'), 'YYYYMMDD') AS pull_date,
                    TO_TIMESTAMP(REGEXP_SUBSTR(metadata$filename, '{ts_regex}'), 'YYYYMMDDTHH24MISS') AS pull_timestamp,
                    metadata$file_row_number AS file_row_number,
                    metadata$filename AS filename,
                    REGEXP_SUBSTR(filename, '.+/(\\\\w+).jsonl?', 1, 1, 'c', 1) AS name,
                    '{self.ods_version}' AS ods_version,
                    '{self.data_model_version}' AS data_model_version,
                    t.$1 AS v
                FROM '@{database}.util.airflow_stage/{directory}'
                (file_format => 'json_default') t
            )
            force = true;
        """

        names_string = "', '".join(names)
        qry_delete = f"""
            DELETE FROM {database}.{schema}.{table}
            WHERE tenant_code = '{self.tenant_code}'
            AND api_year = '{self.api_year}'
            AND name in ('{names_string}')
        """

        ### Commit the update queries to database.
        # Incremental runs are only available in EdFi 3+.
        if full_refresh:
            queries_to_run = [qry_delete, qry_copy_into]
        else:
            queries_to_run = [qry_copy_into]

        snowflake_hook = SnowflakeHook(self.database_conn_id)
        snowflake_hook.run(sql=queries_to_run, autocommit=False)
        
    
class ObjectStorageToSnowflakeOperator(ObjectStorageToDatabaseOperator, SnowflakeMixin):
    pass

class BulkObjectStorageToSnowflakeOperator(BulkObjectStorageToDatabaseOperator, SnowflakeMixin):
    pass


class DatabricksMixin:
    def __init__(self, *args, databricks_conn_id: str, **kwargs):
        super(DatabricksMixin, self).__init__(*args, database_conn_id=databricks_conn_id, **kwargs)

    def run_sql_queries(self, name: str, table: str, object_storage: 'ObjectStoragePath', full_refresh: bool = False):
        """

        """
        from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook

        database, schema = self.get_adls_params_from_conn(self.database_conn_id, 'extra__databricks__database')

        ### Build the SQL queries to be passed into `Hook.run()`.
        qry_delete = f"""
            DELETE FROM {database}.{schema}.{table}
            WHERE tenant_code = '{self.tenant_code}'
            AND api_year = '{self.api_year}'
            AND name = '{name}'
        """

        qry_create_table = f"""
            CREATE TABLE IF NOT EXISTS {database}.{schema}.{table}_stage
        """

        qry_drop_table = f"""
            DROP TABLE {database}.{schema}.{table}_stage
        """

        qry_copy_into = f"""
            COPY INTO {database}.{schema}.{table}_stage
            FROM '{object_storage.key}'
            FILEFORMAT = TEXT
            COPY_OPTIONS ('force' = 'true', 'mergeSchema' = 'true')"""

        qry_insert = f"""
            INSERT INTO {database}.{schema}.{table}
            SELECT 
                parse_json(value) as v, 
                '{self.tenant_code}' as tenant_code,
                '{self.api_year}' as `api_year`, 
                current_date() as `pull_date`, 
                current_timestamp() as `pull_timestamp`, 
                ROW_NUMBER() OVER (ORDER BY value) as `file_row_number`, 
                '{object_storage.key}' as `filename`,
                '{name}' as name, 
                '{self.ods_version}' as `ods_version`, 
                '{self.data_model_version}' as `data_model_version`
            FROM {database}.{schema}.{table}_stage    
        """

        ### Commit the update queries to database.
        # Incremental runs are only available in EdFi 3+.
        if full_refresh:
            queries_to_run = [qry_delete, qry_create_table, qry_copy_into, qry_insert, qry_drop_table]
        else:
            queries_to_run = [qry_create_table, qry_copy_into, qry_insert, qry_drop_table]

        databricks_hook = DatabricksSqlHook(self.database_conn_id)
        databricks_hook.run(sql=queries_to_run, autocommit=False)

    def run_bulk_sql_queries(self, names: List[str], table: str, object_storage: 'ObjectStoragePath', full_refresh: bool = False):
        """
        Alternative delete and copy queries to be run when all data is sent to the same table in database.
        
        S3 Path Structure:
            /{tenant_code}/{api_year}/{ds_nodash}/{ts_no_dash}/{taskgroup_type}/{name}.jsonl

        Use regex to capture name: ".+/(\\w+).jsonl?"
        Note optional args in REGEXP_SUBSTR(): position (1), occurrence (1), regex_parameters ('c'), group_num
        """
        pass
        # TODO
    
class ObjectStorageToDatabricksOperator(ObjectStorageToDatabaseOperator, DatabricksMixin):
    pass

class BulkObjectStorageToDatabricksOperator(BulkObjectStorageToDatabaseOperator, DatabricksMixin):
    pass
