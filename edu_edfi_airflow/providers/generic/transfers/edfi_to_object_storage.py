import json
import logging
import os
import tempfile

from typing import Iterator, List, Optional, Union, Type, Tuple

from airflow.hooks.base import BaseHook
from airflow.io.path import ObjectStoragePath
from airflow.models import BaseOperator
from airflow.exceptions import AirflowSkipException, AirflowFailException

from edu_edfi_airflow.callables import airflow_util
from edu_edfi_airflow.providers.edfi.hooks.edfi import EdFiHook


class EdFiToObjectStorageOperator(BaseOperator):
    """
    Establish a connection to the EdFi ODS using an Airflow Connection.
    Default to pulling the EdFi API configs from the connection if not explicitly provided.

    Use a paged-get to retrieve a particular EdFi resource from the ODS.
    Save the results as JSON lines to `tmp_dir` on the server.
    Once pagination is complete, write the full results to object storage, as defined in a child subclass.

    Deletes and keyChanges are only used in the bulk operator.
    """
    template_fields = (
        'resource', 'namespace', 'page_size', 'num_retries', 'change_version_step_size', 'query_parameters',
        'destination_key', 'destination_dir', 'destination_filename',
        'min_change_version', 'max_change_version', 'enabled_endpoints',
    )

    # def __new__(cls, *args, resource: Union[str, List[str]], **kwargs):
    #     """
    #     Use presence of backend-specific arguments to initialize child class.
    #     Storage backends are automatically detected from the registry.
    #     """
        
    #     # (single_op, bulk_op) = find_ops_for_conn(kwargs['storage_conn_id'])
    #     (single_op, bulk_op) = (EdFiToObjectStorageOperator, BulkEdFiToObjectStorageOperator)
        
    #     # Use registered storage backend
    #     if isinstance(resource, str):
    #         return object.__new__(single_op)
    #     else:
    #         return object.__new__(bulk_op)
        

    def __init__(self,
        edfi_conn_id: str,
        resource: str,

        *,
        tmp_dir: str,
        object_storage_conn_id: Optional[str] = None,
        destination_key: Optional[str] = None,  # Mutually-exclusive with `destination_dir` and `destination_filename`
        destination_dir: Optional[str] = None,
        destination_filename: Optional[str] = None,

        get_deletes: bool = False,
        get_key_changes: bool = False,
        min_change_version: Optional[int] = None,
        max_change_version: Optional[int] = None,

        namespace: str = 'ed-fi',
        page_size: int = 500,
        num_retries: int = 5,
        change_version_step_size: int = 50000,
        reverse_paging: bool = True,
        query_parameters  : Optional[dict] = None,

        enabled_endpoints: Optional[List[str]] = None,

        **kwargs
    ) -> None:
        super(EdFiToObjectStorageOperator, self).__init__(**kwargs)

        # Top-level variables
        self.edfi_conn_id = edfi_conn_id
        self.resource = resource

        self.get_deletes = get_deletes
        self.get_key_changes = get_key_changes
        self.min_change_version = min_change_version
        self.max_change_version = max_change_version

        # Storage variables
        self.tmp_dir = tmp_dir
        self.object_storage_conn_id = object_storage_conn_id
        self.destination_key = destination_key
        self.destination_dir = destination_dir
        self.destination_filename = destination_filename

        # Endpoint-pagination variables
        self.namespace = namespace
        self.page_size = page_size
        self.num_retries = num_retries
        self.change_version_step_size = change_version_step_size
        self.reverse_paging = reverse_paging
        self.query_parameters = query_parameters

        # Optional variable to allow immediate skips when endpoint not specified in dynamic get-change-version output.
        self.enabled_endpoints = enabled_endpoints

        self.kwargs = kwargs


    def execute(self, context) -> str:
        """

        :param context:
        :return:
        """
        # If doing a resource-specific run, confirm resource is in the list.
        # Also confirm resource is in XCom-list if passed (used for dynamic XComs retrieved from get-change-version operator).
        config_endpoints = airflow_util.get_config_endpoints(context)
        if not self.is_endpoint_specified(self.resource, config_endpoints, self.enabled_endpoints):
            raise AirflowSkipException

        # Check the validity of min and max change-versions.
        self.check_change_version_window_validity(self.min_change_version, self.max_change_version)

        # Build the object storage based on passed arguments.
        # Note: this logic is overridden in each of the child classes.
        object_storage, clean_url = self.get_object_storage(
            object_storage_conn_id=self.object_storage_conn_id, destination_key=self.destination_key,
            destination_dir=self.destination_dir, destination_filename=self.destination_filename,
            **self.kwargs
        )

        edfi_conn = EdFiHook(self.edfi_conn_id).get_conn()

        self.pull_edfi_to_object_storage(
            edfi_conn=edfi_conn,
            resource=self.resource, namespace=self.namespace, page_size=self.page_size,
            num_retries=self.num_retries, change_version_step_size=self.change_version_step_size,
            min_change_version=self.min_change_version, max_change_version=self.max_change_version,
            query_parameters=self.query_parameters, object_storage=object_storage
        )

        # Return the clean URL string (not ObjectStoragePath) for downstream operators
        # ObjectStoragePath mangles the URL when conn_id is passed, so extract the original path
        return (self.resource, clean_url)


    @staticmethod
    def is_endpoint_specified(endpoint: str, config_endpoints: List[str], enabled_endpoints: List[str]) -> bool:
        """
        Verify endpoint is enabled in the endpoints YAML and specified to run in the DAG configs.
        """
        if config_endpoints and endpoint not in config_endpoints:
            logging.info(f"    Endpoint {endpoint} not specified in DAG config endpoints. Skipping...")
            return False
        
        if enabled_endpoints and endpoint not in enabled_endpoints:
            logging.info(f"    Endpoint {endpoint} not specified in run endpoints. Skipping...")
            return False

        return True


    @staticmethod
    def check_change_version_window_validity(min_change_version: Optional[int], max_change_version: Optional[int]):
        """
        Logic to pull the min-change version from its upstream operator and check its validity.
        """
        if min_change_version is None and max_change_version is None:
            return
        
        # Run change-version sanity checks to make sure we aren't doing something wrong.
        if min_change_version == max_change_version:
            logging.info("    ODS is unchanged since previous pull.")
            raise AirflowSkipException

        if max_change_version < min_change_version:
            raise AirflowFailException(
                "    Apparent out-of-sequence run: current change version is smaller than previous! Run a full-refresh of this resource to resolve!"
            )


    @classmethod
    def get_object_storage(cls,
        object_storage_conn_id: str,
        destination_key: Optional[str] = None,
        destination_dir: Optional[str] = None,
        destination_filename: Optional[str] = None,
        **kwargs
    ) -> ObjectStoragePath:
        """
        Infer object storage destination path by passed arguments.
        Build destination key from directory and filename if undefined.
        Retrieve bucket name from connection schema if undefined.
        
        Note: This method handles both S3 and ADLS in a single implementation.
        For a mixin-based approach, see S3MixinV2 and ADLSMixinV2 at bottom of file.
        """
        # Optionally set destination key by concatting separate args for dir and filename
        if not destination_key and not (destination_dir and destination_filename):
            raise ValueError(
                f"Argument `destination_key` has not been specified, and `destination_dir` or `destination_filename` is missing."
            )
        
        if not destination_key:
            destination_key = os.path.join(destination_dir, destination_filename)

        # Get connection to determine storage type
        conn = BaseHook.get_connection(object_storage_conn_id)
        print(conn.conn_type)
        
        if conn.conn_type == 'adls':
            # ADLS: Use Airflow's internal format with conn_id embedded in URL
            # Format: abfs://conn_id@container/path
            # Airflow will extract conn_id and use it to get credentials
            container = conn.schema
            account_name = conn.host
            
            if not container:
                raise ValueError(f"Container name not found in connection {object_storage_conn_id}")
            
            # Internal format for ObjectStoragePath - embed conn_id in URL
            internal_storage_path = f"abfs://{object_storage_conn_id}@{container}/{destination_key}"
            
            # Clean URL format for downstream operators (database loading)
            clean_storage_path = f"abfs://{container}@{account_name}.dfs.core.windows.net/{destination_key}"
                
        #TODO faulty assumption that all http connections are S3.. needs to be updated to support other object storage types.
        elif conn.conn_type == 'http':
            # S3 path format: s3://bucket/path
            bucket = conn.schema
            
            if not bucket:
                raise ValueError(f"Bucket name not found in connection {object_storage_conn_id}")
            
            # For S3, both formats are the same
            internal_storage_path = f"s3://{bucket}/{destination_key}"
            clean_storage_path = internal_storage_path
            
        else:
            raise ValueError(f"Unsupported connection type: {conn.conn_type}")

        # Create ObjectStoragePath with internal format (Airflow extracts conn_id from URL)
        # Return both the ObjectStoragePath and the clean URL for downstream use
        return (ObjectStoragePath(internal_storage_path), clean_storage_path)


    def pull_edfi_to_object_storage(self,
        *,
        edfi_conn: 'Connection',
        resource: str,
        namespace: str,
        page_size: int,
        num_retries: int,
        change_version_step_size: int,
        min_change_version: Optional[int],
        max_change_version: Optional[int],
        query_parameters: dict,
        object_storage: ObjectStoragePath
    ):
        """
        Break out load logic to allow code-duplication in bulk version of operator.
        """
        logging.info(
            "    Pulling records for `{}/{}` for change versions `{}` to `{}`. (page_size: {}; CV step size: {})"\
            .format(namespace, resource, min_change_version, max_change_version, page_size, change_version_step_size)
        )

        ### Connect to EdFi and write resource data to a temp file, then copy to object storage.
        # Prepare the EdFiEndpoint for the resource.
        resource_endpoint = edfi_conn.resource(
            resource, namespace=namespace, params=query_parameters,
            get_deletes=self.get_deletes, get_key_changes=self.get_key_changes,
            min_change_version=min_change_version, max_change_version=max_change_version
        )

        # Turn off change version stepping if min and max change versions have not been defined.
        step_change_version = (min_change_version is not None and max_change_version is not None)

        paged_iter = resource_endpoint.get_pages(
            page_size=page_size,
            step_change_version=step_change_version, change_version_step_size=change_version_step_size,
            reverse_paging=self.reverse_paging,
            retry_on_failure=True, max_retries=num_retries
        )

        ### Iterate the ODS, paginating across offset and change version steps.
        # Write each result to the temp file.
        total_rows = 0
        
        os.makedirs(os.path.dirname(self.tmp_dir), exist_ok=True)  # Create its parent-directory if not extant.
        
        with tempfile.NamedTemporaryFile('w+b', dir=self.tmp_dir) as tmp_file:

            # Output each page of results as JSONL strings to the output file.
            for page_result in paged_iter:
                tmp_file.write(self.to_jsonl_string(page_result))
                total_rows += len(page_result)

            # Connect to object storage and copy file
            tmp_file.seek(0)  # Go back to the start of the file before copying to object storage.
            with object_storage.open("wb") as storage_file:
                storage_file.write(tmp_file.read())
        
                
        ### Check whether the number of rows returned matched the number expected.
        logging.info(f"    {total_rows} rows were returned for `{resource}`.")

        try:
            expected_rows = resource_endpoint.total_count()
            if total_rows != expected_rows:
                logging.warning(f"    Expected {expected_rows} rows for `{resource}`.")
            else:
                logging.info("    Number of collected rows matches expected count in the ODS.")

        except Exception:
            logging.warning(f"    Unable to access expected number of rows for `{resource}`.")

        # Raise a Skip if no data was collected.
        if total_rows == 0:
            logging.info(f"Skipping downstream copy to database...")
            raise AirflowSkipException
                

    @staticmethod
    def to_jsonl_string(rows: Iterator[dict]) -> bytes:
        """
        :return:
        """
        return b''.join(
            json.dumps(row).encode('utf8') + b'\n'
            for row in rows
        )


class BulkEdFiToObjectStorageOperator(EdFiToObjectStorageOperator):
    """
    Inherits from EdFiToObjectStorageOperator to reduce code-duplication.

    The following arguments MUST be lists instead of singletons:
    - resource
    - namespace
    - page_size
    - num_retries
    - change_version_step_size
    - query_parameters
    - min_change_version
    - destination_filename

    If all endpoints skip, raise an AirflowSkipException.
    If at least one endpoint fails, push the XCom and raise an AirflowFailException.
    Otherwise, return a successful XCom.
    """
    def execute(self, context) -> str:
        """

        :param context:
        :return:
        """
        # Force destination_dir and destination_filename arguments to be used.
        if self.destination_key or not (self.destination_dir and self.destination_filename):
            raise ValueError(
                "Bulk operators require arguments `destination_dir` and `destination_filename` to be passed."
            )

        # Make connection outside of loop to not re-authenticate at every resource.
        edfi_conn = EdFiHook(self.edfi_conn_id).get_conn()

        # Gather DAG-level endpoints outside of loop.
        config_endpoints = airflow_util.get_config_endpoints(context)

        return_tuples = []
        failed_endpoints = []  # Track which endpoints failed during ingestion.

        # Presume all argument lists are equal length, and iterate each endpoint. Only add to the return in successful pulls.
        zip_arguments = [
            self.resource,
            self.min_change_version,
            self.namespace,
            self.page_size,
            self.num_retries,
            self.change_version_step_size,
            self.query_parameters,
            self.destination_filename,
        ]

        for idx, (resource, min_change_version, namespace, page_size, num_retries, change_version_step_size, query_parameters, destination_filename) \
            in enumerate(zip(*zip_arguments), start=1):

            logging.info(f"[ENDPOINT {idx} / {len(self.resource)}]")

            # If doing a resource-specific run, confirm resource is in the list.
            # Also confirm resource is in XCom-list if passed (used for dynamic XComs retrieved from get-change-version operator).
            if not self.is_endpoint_specified(resource, config_endpoints, self.enabled_endpoints):
                continue

            # Check the validity of min and max change-versions.
            self.check_change_version_window_validity(self.min_change_version, self.max_change_version)

            # Build the object storage based on passed arguments.
            object_storage, clean_url = self.get_object_storage(
                object_storage_conn_id=self.object_storage_conn_id,
                destination_dir=self.destination_dir, destination_filename=destination_filename,
                **self.kwargs
            )

            # Wrap in a try-except to still attempt other endpoints in a skip or failure.
            try:
                self.pull_edfi_to_object_storage(
                    edfi_conn=edfi_conn,
                    resource=resource, namespace=namespace, page_size=page_size,
                    num_retries=num_retries, change_version_step_size=change_version_step_size,
                    min_change_version=min_change_version, max_change_version=self.max_change_version,
                    query_parameters=query_parameters, object_storage=object_storage
                )
                # Return the clean URL string (not ObjectStoragePath) for downstream operators
                return_tuples.append((resource, clean_url))

            except AirflowSkipException:
                continue

            except Exception:
                failed_endpoints.append(resource)
                logging.warning(
                    f"    Unable to complete ingestion of endpoint: {namespace}/{resource}"
                )
                continue

        if failed_endpoints:
            context['ti'].xcom_push(key='return_value', value=return_tuples)
            raise AirflowFailException(
                f"Failed ingestion of one or more endpoints: {failed_endpoints}"
            )

        if not return_tuples:
            raise AirflowSkipException(
                "No new data was ingested for any endpoints. Skipping downstream copy..."
            )

        return return_tuples




# ============================================================================
# UPDATED MIXIN APPROACH (2025-11-21)
# Alternative implementation that matches current base class logic.
# Uncomment these to use mixins instead of if/else in get_object_storage()
# ============================================================================

# class S3MixinV2:
#     """
#     Updated S3 mixin that creates ObjectStoragePath with conn_id.
#     Returns tuple of (ObjectStoragePath, clean_url) for downstream operators.
#     """
#     def __init__(self, *args, s3_conn_id: str, **kwargs) -> None:
#         super(S3MixinV2, self).__init__(*args, object_storage_conn_id=s3_conn_id, **kwargs)
# 
#     @classmethod
#     def get_object_storage(cls,
#         object_storage_conn_id: Optional[str],
#         destination_key: Optional[str] = None,
#         destination_dir: Optional[str] = None,
#         destination_filename: Optional[str] = None,
#         **kwargs
#     ):
#         """Build S3 path and return ObjectStoragePath with clean URL."""
#         # Build destination_key if not provided
#         if not destination_key:
#             if not destination_dir or not destination_filename:
#                 raise ValueError("Must provide destination_key or both destination_dir and destination_filename")
#             destination_key = os.path.join(destination_dir, destination_filename)
#         
#         # Get connection to extract bucket
#         conn = BaseHook.get_connection(object_storage_conn_id)
#         bucket = conn.schema
#         
#         if not bucket:
#             raise ValueError(f"Bucket name not found in connection {object_storage_conn_id}")
#         
#         # S3 format: same for both internal and external use
#         storage_path = f"s3://{bucket}/{destination_key}"
#         
#         return (ObjectStoragePath(storage_path, conn_id=object_storage_conn_id), storage_path)


# class ADLSMixinV2:
#     """
#     Updated ADLS mixin that embeds conn_id in URL (Airflow pattern).
#     Returns tuple of (ObjectStoragePath, clean_url) for downstream operators.
#     """
#     def __init__(self, *args, adls_conn_id: str, **kwargs) -> None:
#         super(ADLSMixinV2, self).__init__(*args, object_storage_conn_id=adls_conn_id, **kwargs)
# 
#     @classmethod
#     def get_object_storage(cls,
#         object_storage_conn_id: Optional[str],
#         destination_key: Optional[str] = None,
#         destination_dir: Optional[str] = None,
#         destination_filename: Optional[str] = None,
#         **kwargs
#     ):
#         """Build ADLS paths and return ObjectStoragePath with clean URL."""
#         # Build destination_key if not provided
#         if not destination_key:
#             if not destination_dir or not destination_filename:
#                 raise ValueError("Must provide destination_key or both destination_dir and destination_filename")
#             destination_key = os.path.join(destination_dir, destination_filename)
#         
#         # Get connection to extract container and storage account
#         conn = BaseHook.get_connection(object_storage_conn_id)
#         container = conn.schema
#         account_name = conn.host
#         
#         if not container:
#             raise ValueError(f"Container name not found in connection {object_storage_conn_id}")
#         
#         # Internal format: embed conn_id in URL so Airflow can extract it
#         internal_storage_path = f"abfs://{object_storage_conn_id}@{container}/{destination_key}"
#         
#         # Clean format: full Azure URL for downstream operators
#         clean_storage_path = f"abfs://{container}@{account_name}.dfs.core.windows.net/{destination_key}"
#         
#         return (ObjectStoragePath(internal_storage_path), clean_storage_path)


# # Operator classes using updated mixins
# class EdFiToS3OperatorV2(S3MixinV2, EdFiToObjectStorageOperator):
#     pass

# class BulkEdFiToS3OperatorV2(S3MixinV2, BulkEdFiToObjectStorageOperator):
#     pass

# class EdFiToADLSOperatorV2(ADLSMixinV2, EdFiToObjectStorageOperator):
#     pass

# class BulkEdFiToADLSOperatorV2(ADLSMixinV2, BulkEdFiToObjectStorageOperator):
#     pass


# def find_ops_for_conn(storage_conn_id: str) -> Tuple[Type[EdFiToObjectStorageOperator], Type[EdFiToObjectStorageOperator]]:
#     """
#     Find the operators for a given storage connection ID.
    
#     Note: This function is not currently used since we removed the __new__ factory pattern.
#     If you uncomment the V2 mixins above, you would also need to:
#     1. Uncomment the __new__ method in EdFiToObjectStorageOperator
#     2. Update the return statements below to use V2 operators
#     """
#     connection = Connection.get_connection_from_secrets(storage_conn_id)
#     if connection.conn_type == 'aws':
#         return EdFiToS3Operator, BulkEdFiToS3Operator  # Or: EdFiToS3OperatorV2, BulkEdFiToS3OperatorV2
#     elif connection.conn_type == 'adls':
#         return EdFiToADLSOperator, BulkEdFiToADLSOperator  # Or: EdFiToADLSOperatorV2, BulkEdFiToADLSOperatorV2
#     else:
#         raise ValueError(f"Unsupported storage type: {connection.conn_type}")
