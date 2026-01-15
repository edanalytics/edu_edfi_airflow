import json
import logging
import os
import tempfile

from typing import Iterator, List, Optional

from airflow.models import BaseOperator
from airflow.exceptions import AirflowSkipException, AirflowFailException

from edu_edfi_airflow.callables import airflow_util
from edu_edfi_airflow.interfaces.object_storage import ObjectStorageInterface
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

    def __init__(self,
        edfi_conn_id: str,
        resource: str,

        *,
        tmp_dir: str,
        object_storage_conn_id: Optional[str] = None,
        object_storage_type: Optional[str] = None,
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
        change_version_step_size: int,
        cursor_paging: bool = True,
        reverse_paging: bool = False,
        query_parameters  : Optional[dict] = None,

        enabled_endpoints: Optional[List[str]] = None,
        use_edfi_token_cache: bool = False,

        **kwargs
    ) -> None:
        super(EdFiToObjectStorageOperator, self).__init__(**kwargs)

        # Top-level variables
        self.edfi_conn_id = edfi_conn_id
        self.use_edfi_token_cache = use_edfi_token_cache
        self.resource = resource

        self.get_deletes = get_deletes
        self.get_key_changes = get_key_changes
        self.min_change_version = min_change_version
        self.max_change_version = max_change_version

        # Storage variables
        self.tmp_dir = tmp_dir
        self.object_storage_conn_id = object_storage_conn_id
        self.object_storage_type = object_storage_type
        self.destination_key = destination_key
        self.destination_dir = destination_dir
        self.destination_filename = destination_filename

        # Endpoint-pagination variables
        self.namespace = namespace
        self.page_size = page_size
        self.num_retries = num_retries
        self.change_version_step_size = change_version_step_size
        self.reverse_paging = reverse_paging
        self.cursor_paging = cursor_paging
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

        # Build the object storage based on passed arguments, using the custom interface implementation.
        object_interface = ObjectStorageInterface(self.object_storage_conn_id, object_storage_type=self.object_storage_type)
        object_storage, clean_url = object_interface.get_object_storage(
            destination_key=self.destination_key, destination_dir=self.destination_dir, destination_filename=self.destination_filename,
            **self.kwargs
        )

        edfi_conn = EdFiHook(self.edfi_conn_id, use_token_cache=self.use_edfi_token_cache).get_conn()

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
        object_storage: 'ObjectStoragePath'
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
        step_change_version = False

        paged_iter = resource_endpoint.get_pages(
            cursor_paging=self.cursor_paging,
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
        edfi_conn = EdFiHook(self.edfi_conn_id, use_token_cache=self.use_edfi_token_cache).get_conn()

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
            self.check_change_version_window_validity(min_change_version, self.max_change_version)

            # Build the object storage based on passed arguments, using the custom interface implementation.
            object_interface = ObjectStorageInterface(self.object_storage_conn_id, object_storage_type=self.object_storage_type)
            object_storage, clean_url = object_interface.get_object_storage(
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

            except Exception as err:
                failed_endpoints.append(resource)
                logging.warning(
                    f"    Unable to complete ingestion of endpoint `{namespace}/{resource}`: {err}"
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
