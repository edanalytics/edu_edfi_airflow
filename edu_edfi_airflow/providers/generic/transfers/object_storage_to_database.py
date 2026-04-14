import logging

from airflow.models import BaseOperator
from airflow.exceptions import AirflowSkipException

from typing import List, Optional, Union

from edu_edfi_airflow.callables import airflow_util
from edu_edfi_airflow.interfaces.database import DatabaseInterface
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
        database_type: Optional[str] = None,
        object_storage: Optional['ObjectStoragePath'] = None,
        destination_key: Optional[str] = None,  # Alternative to object_storage

        edfi_conn_id: Optional[str] = None,  # Optional: only used to retrieve unspecified version metadata
        use_edfi_token_cache: bool = False,
        
        ods_version: Optional[str] = None,
        data_model_version: Optional[str] = None,

        full_refresh: bool = False,
        **kwargs
    ) -> None:
        super(ObjectStorageToDatabaseOperator, self).__init__(**kwargs)
        self.database_conn_id = database_conn_id
        self.database_type = database_type

        # Store both parameters - will be resolved in execute()
        self.object_storage = object_storage
        self.destination_key = destination_key
        
        self.tenant_code = tenant_code
        self.api_year = api_year
        self.resource = resource
        self.table_name = table_name

        self.edfi_conn_id = edfi_conn_id
        self.use_edfi_token_cache = use_edfi_token_cache
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
        with DatabaseInterface(self.database_conn_id, database_type=self.database_type) as db:

            # Incremental runs are only available in EdFi 3+.
            if self.full_refresh or airflow_util.is_full_refresh(context):
                db.delete_from_raw(
                    tenant_code=self.tenant_code, api_year=self.api_year, name=self.resource, table=self.table_name
                )
            
            db.copy_into_raw(
                tenant_code=self.tenant_code, api_year=self.api_year, name=self.resource, table=self.table_name,
                ods_version=self.ods_version, data_model_version=self.data_model_version, storage_path=storage_path
            )


    def set_edfi_attributes(self):
        """
        Retrieve the Ed-Fi, ODS, and data model versions if not provided.
        This needs to occur in execute to not call the API at every Airflow synchronize.
        """
        if self.edfi_conn_id:
            edfi_conn = EdFiHook(edfi_conn_id=self.edfi_conn_id, use_token_cache=self.use_edfi_token_cache).get_conn()
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
    Supports per-resource full_refresh flags to allow selective resource-level refreshes.
    """
    def __init__(self,
        *,
        full_refresh: Union[bool, List[bool]] = False,
        **kwargs
    ) -> None:
        # Store per-resource full_refresh flags if provided as a list
        # Otherwise, store as boolean for backward compatibility
        self.per_resource_full_refresh = full_refresh if isinstance(full_refresh, list) else None
        # Pass the boolean flag to parent for backward compatibility with single-resource case
        super().__init__(full_refresh=full_refresh if isinstance(full_refresh, bool) else False, **kwargs)

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
        
        # Get DAG-level full_refresh flag
        dag_full_refresh = airflow_util.is_full_refresh(context)
        
        # Build and run the SQL queries to database. Delete first if EdFi2 or a full-refresh.
        with DatabaseInterface(self.database_conn_id, database_type=self.database_type) as db:

            # If all data is sent to the same table, use a single massive SQL query to copy the data from the directory.
            if isinstance(self.table_name, str):
                logging.info("Running bulk statements on a single table.")
                # For bulk operations with single table, use DAG-level full_refresh or operator-level full_refresh
                if self.full_refresh or dag_full_refresh:
                    db.delete_from_raw(
                        tenant_code=self.tenant_code, api_year=self.api_year, name=self.resource, table=self.table_name
                    )

                db.bulk_copy_into_raw(
                    tenant_code=self.tenant_code, api_year=self.api_year, name=self.resource, table=self.table_name,
                    ods_version=self.ods_version, data_model_version=self.data_model_version, storage_path=self.destination_key[0]  # Directory is inferred in bulk-copy.
                )
                return  # Break out of the context-manager.
            
            # Otherwise, loop over each destination and copy in sequence.
            for idx, (resource, table, destination_key) in enumerate(zip(self.resource, self.table_name, self.destination_key)):
                # Determine if this resource should be full-refreshed
                # Priority: per-resource flag > DAG-level flag > operator-level flag
                should_full_refresh = dag_full_refresh or self.full_refresh
                
                # Check per-resource full_refresh flags if available
                if self.per_resource_full_refresh is not None and idx < len(self.per_resource_full_refresh):
                    should_full_refresh = self.per_resource_full_refresh[idx]
                
                if should_full_refresh:
                    db.delete_from_raw(
                        tenant_code=self.tenant_code, api_year=self.api_year, name=resource, table=table
                    )

                db.copy_into_raw(
                    tenant_code=self.tenant_code, api_year=self.api_year, name=resource, table=table,
                    ods_version=self.ods_version, data_model_version=self.data_model_version, storage_path=destination_key
                )
