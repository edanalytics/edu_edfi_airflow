import logging

from airflow.models import BaseOperator
from airflow.exceptions import AirflowSkipException

from typing import Optional

from edu_edfi_airflow.callables import airflow_util
from edu_edfi_airflow.mixins.database import DatabaseMixin
from edu_edfi_airflow.providers.edfi.hooks.edfi import EdFiHook


class ObjectStorageToDatabaseOperator(BaseOperator):
    """
    Copy the Ed-Fi files from object storage to raw resource tables in database.
    """
    template_fields = ('resource', 'table_name', 'destination_key')

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
        storage_path = self.destination_key or self.object_storage.key

        ### Commit the update queries to database.
        # Build and run the SQL queries to database. Delete first if EdFi2 or a full-refresh.
        queries_to_run = []

        # Incremental runs are only available in EdFi 3+.
        if self.full_refresh or airflow_util.is_full_refresh(context):
            queries_to_run.append(self.build_delete_query(
                tenant_code=self.tenant_code, api_year=self.api_year, name=self.resource, table=self.table_name
            ))
        
        queries_to_run.append(self.build_copy_query(
            tenant_code=self.tenant_code, api_year=self.api_year, name=self.resource, table=self.table_name,
            ods_version=self.ods_version, data_model_version=self.data_model_version, storage_path=storage_path
        ))
        return DatabaseMixin(self.database_conn_id).run_sql_queries(queries_to_run)


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
        if not (self.destination_key or self.object_storage):
            raise ValueError("Either 'object_storage' or 'destination_key' must be provided")

        if self.destination_key:
            if not isinstance(self.destination_key, list):
                self.destination_key = [self.destination_key] * len(self.resource)
        else:
            # Handle legacy object_storage parameter
            if isinstance(self.object_storage, list):
                self.destination_key = [obj.key for obj in self.object_storage]
            else:
                self.destination_key = [self.object_storage.key] * len(self.resource)

        ### Retrieve the Ed-Fi, ODS, and data model versions in execute to prevent excessive API calls.
        self.set_edfi_attributes()
        
        # Build and run the SQL queries to database. Delete first if EdFi2 or a full-refresh.
        queries_to_run = []
        
        if self.full_refresh or airflow_util.is_full_refresh(context):
            queries_to_run.append(self.build_delete_query(
                tenant_code=self.tenant_code, api_year=self.api_year, name=self.resource, table=self.table_name
            ))

        # If all data is sent to the same table, use a single massive SQL query to copy the data from the directory.
        if isinstance(self.table_name, str):
            logging.info("Running bulk statements on a single table.")
            queries_to_run.append(self.build_bulk_copy_query(
                tenant_code=self.tenant_code, api_year=self.api_year, name=self.resource, table=self.table_name,
                ods_version=self.ods_version, data_model_version=self.data_model_version, storage_path=self.destination_key[0]  # Infer directory if not specified.
            ))
            return DatabaseMixin(self.database_conn_id).run_sql_queries(queries_to_run)
        
        # Otherwise, loop over each destination and copy in sequence.
        for idx, (resource, table, destination_key) in enumerate(zip(self.resource, self.table_name, self.destination_key), start=1):
            logging.info(f"[ENDPOINT {idx} / {len(self.resource)}]")
            queries_to_run.append(self.build_copy_query(
                tenant_code=self.tenant_code, api_year=self.api_year, name=resource, table=table,
                ods_version=self.ods_version, data_model_version=self.data_model_version, storage_path=destination_key
            ))
            DatabaseMixin(self.database_conn_id).run_sql_queries(queries_to_run)
