import abc
import logging
import os

from typing import Any, List, Optional, Tuple, Union, Type
from airflow.models.connection import Connection

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
    template_fields = ('resource', 'table_name', 'destination_key')

    
    def __new__(cls, *args, resource: Union[str, List[str]] = None, **kwargs):
        # If called with no kwargs (during deserialization/copying), just create the instance
        # The factory pattern already worked during DAG parsing to select the right class
        if not kwargs and not args:
            return object.__new__(cls)
        
        # Extract resource from kwargs if not passed as positional argument
        # if resource is None and 'resource' in kwargs:
            resource = kwargs['resource']
        
        # Find matching database backend from registry (raises ValueError if not found)
        # conn_param, (single_op, bulk_op) = DATABASE_REGISTRY.find_backend_for_kwargs(**kwargs)
        (single_op, bulk_op) = find_ops_for_conn(kwargs['database_conn_id'])
        
        # Check if this is a bulk operation
        # TODO: I'm not sure if this is necessary.
        is_bulk = (
            isinstance(resource, list) or
            'bulk' in cls.__name__.lower() or
            'Bulk' in cls.__name__
        )
        
        if is_bulk:
            return object.__new__(bulk_op)
        else:
            return object.__new__(single_op)


    def __init__(self,
        *,
        tenant_code: str,
        api_year: int,
        resource: str,
        table_name: str,

        database_conn_id: str,
        object_storage: Optional['ObjectStoragePath'] = None,
        destination_key: Optional[str] = None,  # Alternative to object_storage

        edfi_conn_id: Optional[str] = None,  # Optional: only used to retrieve unspecified version metadata
        ods_version: Optional[str] = None,
        data_model_version: Optional[str] = None,

        full_refresh: bool = False,
        **kwargs
    ) -> None:
        super(ObjectStorageToDatabaseOperator, self).__init__(**kwargs)

        self.database_conn_id = database_conn_id
        
        # Store both parameters - will be resolved in execute()
        self.object_storage = object_storage
        self.destination_key = destination_key
        
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

        # Use destination_key if provided, otherwise fall back to object_storage
        storage = self.destination_key if self.destination_key is not None else self.object_storage

        # Build and run the SQL queries to database. Delete first if EdFi2 or a full-refresh.
        self.run_sql_queries(
            name=self.resource, table=self.table_name, destination_key=storage,
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
    def run_sql_queries(self, name: str, table: str, destination_key, full_refresh: bool = False):
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
        
        # Convert destination_key to list if needed
        # Template rendering gives us a list of URL strings
        if self.destination_key is not None:
            if not isinstance(self.destination_key, list):
                self.destination_key = [self.destination_key] * len(self.resource)
        
        elif self.object_storage is not None:
            # Handle legacy object_storage parameter
            if isinstance(self.object_storage, list):
                self.destination_key = self.object_storage
            else:
                self.destination_key = [self.object_storage] * len(self.resource)
        
        else:
            raise ValueError("Either 'object_storage' or 'destination_key' must be provided")

        ### Retrieve the Ed-Fi, ODS, and data model versions in execute to prevent excessive API calls.
        self.set_edfi_attributes()

        ### Build and run the SQL queries to database. Delete first if EdFi2 or a full-refresh.
        # If all data is sent to the same table, use a single massive SQL query to copy the data from the directory.
        if isinstance(self.table_name, str):
            logging.info("Running bulk statements on a single table.")
            return self.run_bulk_sql_queries(
                names=self.resource, table=self.table_name, destination_key=self.destination_key[0],  # Assume all objects use same parent path.
                full_refresh=self.full_refresh or airflow_util.is_full_refresh(context)
            )
        
        # Otherwise, loop over each destination and copy in sequence.
        for idx, (resource, table, destination_key) in enumerate(zip(self.resource, self.table_name, self.destination_key), start=1):
            logging.info(f"[ENDPOINT {idx} / {len(self.resource)}]")
            self.run_sql_queries(
                name=resource, table=table, destination_key=destination_key,
                full_refresh=self.full_refresh or airflow_util.is_full_refresh(context)
            )
        
    @abc.abstractmethod
    def run_bulk_sql_queries(self, name: str, table: str, destination_key, full_refresh: bool = False):
        raise NotImplementedError(
            "Method `run_bulk_sql_queries()` must be redefined in database child class!"
        )


class SnowflakeMixin:
    def __init__(self, *args, database_conn_id: str, **kwargs):
        super(SnowflakeMixin, self).__init__(*args, database_conn_id=database_conn_id, **kwargs)
        
    def run_sql_queries(self, name: str, table: str, destination_key, full_refresh: bool = False):
        """

        """
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        
        ### Retrieve the database and schema from the database connection.
        database, schema = airflow_util.get_database_params_from_conn(self.database_conn_id)

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
        
        # Get the storage path - handle both ObjectStoragePath objects and URL strings
        from airflow.io.path import ObjectStoragePath
        if isinstance(destination_key, ObjectStoragePath):
            storage_path = destination_key.key
        elif isinstance(destination_key, str):
            # Create ObjectStoragePath from URL string to get the key
            storage_path = ObjectStoragePath(destination_key).key
        else:
            storage_path = destination_key

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
                FROM '@{database}.util.airflow_stage/{storage_path}'
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

    def run_bulk_sql_queries(self, names: List[str], table: str, destination_key, full_refresh: bool = False):
        """
        Alternative delete and copy queries to be run when all data is sent to the same table in database.
        
        S3 Path Structure:
            /{tenant_code}/{api_year}/{ds_nodash}/{ts_no_dash}/{taskgroup_type}/{name}.jsonl

        Use regex to capture name: ".+/(\\w+).jsonl?"
        Note optional args in REGEXP_SUBSTR(): position (1), occurrence (1), regex_parameters ('c'), group_num
        """
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        ### Retrieve the database and schema from the database connection.
        database, schema = airflow_util.get_database_params_from_conn(self.database_conn_id)

        # Get the storage path - handle both ObjectStoragePath objects and URL strings
        from airflow.io.path import ObjectStoragePath
        if isinstance(destination_key, ObjectStoragePath):
            storage_path = destination_key.key
        elif isinstance(destination_key, str):
            storage_path = ObjectStoragePath(destination_key).key
        else:
            storage_path = destination_key
        directory = os.path.dirname(storage_path)

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
        


class SnowflakeSingleOperator(SnowflakeMixin, ObjectStorageToDatabaseOperator):
    """Single resource operator for Snowflake backend"""
    pass

class SnowflakeBulkOperator(SnowflakeMixin, BulkObjectStorageToDatabaseOperator):
    """Bulk resource operator for Snowflake backend"""
    pass


class DatabricksMixin:
    def __init__(self, *args, database_conn_id: str, **kwargs):
        super(DatabricksMixin, self).__init__(*args, database_conn_id=database_conn_id, **kwargs)
    
    def run_sql_queries(self, name: str, table: str, destination_key, full_refresh: bool = False):
        """

        """
        from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook

        database, schema = airflow_util.get_database_params_from_conn(self.database_conn_id)

        ### Build the SQL queries to be passed into `Hook.run()`.
        qry_delete = f"""
            DELETE FROM {database}.{schema}.{table}
            WHERE tenant_code = '{self.tenant_code}'
            AND api_year = '{self.api_year}'
            AND name = '{name}'
        """

        # Use the storage URL directly 
        # For ADLS: abfs://container@storage.dfs.core.windows.net/path
        # For S3: s3://bucket/path
        storage_url = str(destination_key) if not isinstance(destination_key, str) else destination_key
        
        qry_copy_into = f"""
            COPY INTO {database}.{schema}.{table}
            FROM (
                SELECT 
                    '{self.tenant_code}' AS tenant_code,
                    {self.api_year} AS api_year,
                    current_date() AS pull_date,
                    current_timestamp() AS pull_timestamp,
                    -- not available in ADLS
                    null AS file_row_number,
                    _metadata.file_path AS filename,
                    '{name}' AS name,
                    '{self.ods_version}' AS ods_version,
                    '{self.data_model_version}' AS data_model_version,
                    v
                FROM '{storage_url}'
            )
            FILEFORMAT = JSON
            FORMAT_OPTIONS ('singleVariantColumn' = 'v')
            COPY_OPTIONS ('force' = 'true', 'mergeSchema' = 'true')
        """

        ### Commit the update queries to database.
        # Incremental runs are only available in EdFi 3+.
        if full_refresh:
            queries_to_run = [qry_delete, qry_copy_into]
        else:
            queries_to_run = [qry_copy_into]

        databricks_hook = DatabricksSqlHook(self.database_conn_id)
        databricks_hook.run(sql=queries_to_run, autocommit=False)

    def run_bulk_sql_queries(self, names: List[str], table: str, destination_key, full_refresh: bool = False):
        """
        Alternative delete and copy queries to be run when all data is sent to the same table in database.
        
        For now, implement bulk operations by calling the single operation for each name.
        This is a simple implementation that can be optimized later.
        """
        for name in names:
            self.run_sql_queries(name, table, destination_key, full_refresh)


class DatabricksSingleOperator(DatabricksMixin, ObjectStorageToDatabaseOperator):
    """Single resource operator for Databricks backend"""
    pass

class DatabricksBulkOperator(DatabricksMixin, BulkObjectStorageToDatabaseOperator):
    """Bulk resource operator for Databricks backend"""
    pass


def find_ops_for_conn(database_conn_id: str) -> Tuple[Type[ObjectStorageToDatabaseOperator], Type[ObjectStorageToDatabaseOperator]]:
    """Find the operators for a given database connection ID."""
    connection = Connection.get_connection_from_secrets(database_conn_id)
    if connection.conn_type == 'snowflake':
        return SnowflakeSingleOperator, SnowflakeBulkOperator
    elif connection.conn_type == 'databricks':
        return DatabricksSingleOperator, DatabricksBulkOperator
    else:
        raise ValueError(f"Unsupported database type: {connection.conn_type}")
