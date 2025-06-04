import abc
import json
import logging
import os
import tempfile

from typing import Iterator, List, Optional

from airflow.hooks.base import BaseHook
from airflow.io.path import ObjectStoragePath
from airflow.models import BaseOperator
from airflow.exceptions import AirflowSkipException, AirflowFailException

from edu_edfi_airflow.callables import airflow_util
from edu_edfi_airflow.providers.edfi.hooks.edfi import EdFiHook


class EdFiToCloudStorageOperator(BaseOperator, abc.ABC):
    """
    Establish a connection to the EdFi ODS using an Airflow Connection.
    Default to pulling the EdFi API configs from the connection if not explicitly provided.

    Use a paged-get to retrieve a particular EdFi resource from the ODS.
    Save the results as JSON lines to `tmp_dir` on the server.
    Once pagination is complete, write the full results to cloud storage, as defined in a child subclass.

    Deletes and keyChanges are only used in the bulk operator.
    """
    STORAGE_PREFIX: str = ""

    template_fields = (
        'resource', 'namespace', 'page_size', 'num_retries', 'change_version_step_size', 'query_parameters',
        'cloud_destination_key', 'cloud_destination_dir', 'cloud_destination_filename',
        'min_change_version', 'max_change_version', 'enabled_endpoints',
    )

    def __init__(self,
        edfi_conn_id: str,
        resource: str,

        *,
        tmp_dir: str,
        cloud_conn_id: str,
        cloud_destination_key: Optional[str] = None,
        cloud_destination_dir: Optional[str] = None,
        cloud_destination_filename: Optional[str] = None,

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
        super(EdFiToCloudStorageOperator, self).__init__(**kwargs)

        # Top-level variables
        self.edfi_conn_id = edfi_conn_id
        self.resource = resource

        self.get_deletes = get_deletes
        self.get_key_changes = get_key_changes
        self.min_change_version = min_change_version
        self.max_change_version = max_change_version

        # Storage variables
        self.tmp_dir = tmp_dir
        self.cloud_conn_id = cloud_conn_id
        self.cloud_destination_key = cloud_destination_key
        self.cloud_destination_dir = cloud_destination_dir
        self.cloud_destination_filename = cloud_destination_filename

        # Endpoint-pagination variables
        self.namespace = namespace
        self.page_size = page_size
        self.num_retries = num_retries
        self.change_version_step_size = change_version_step_size
        self.reverse_paging = reverse_paging
        self.query_parameters = query_parameters

        # Optional variable to allow immediate skips when endpoint not specified in dynamic get-change-version output.
        self.enabled_endpoints = enabled_endpoints


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

        # Complete the pull and write to cloud storage
        object_storage = self.get_object_storage(
            conn_id=self.cloud_conn_id, destination_key=self.cloud_destination_key,
            destination_dir=self.cloud_destination_dir, destination_filename=self.cloud_destination_filename
        )
        edfi_conn = EdFiHook(self.edfi_conn_id).get_conn()

        self.pull_edfi_to_cloud_storage(
            edfi_conn=edfi_conn,
            resource=self.resource, namespace=self.namespace, page_size=self.page_size,
            num_retries=self.num_retries, change_version_step_size=self.change_version_step_size,
            min_change_version=self.min_change_version, max_change_version=self.max_change_version,
            query_parameters=self.query_parameters, object_storage=object_storage
        )

        return (self.resource, object_storage.key)


    @staticmethod
    def is_endpoint_specified(endpoint: str, config_endpoints: List[str], enabled_endpoints: List[str]) -> bool:
        """
        Verify endpoint is enabled in the endpoints YAML and specified to run in the DAG configs.
        (Both enabled listings are represented as lists.)
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
        conn_id: str,
        destination_key: Optional[str] = None,
        *,
        destination_dir: Optional[str] = None,
        destination_filename: Optional[str] = None,
        bucket_name: Optional[str] = None,
    ) -> ObjectStoragePath:
        """
        Infer cloud storage destination path by passed arguments.
        Build destination key from directory and filename if undefined.
        Retrieve bucket name from connection schema if undefined.
        """
        # Optionally set destination key by concatting separate args for dir and filename
        if not destination_key and not (destination_dir and destination_filename):
            raise ValueError(
                f"Argument `destination_key` has not been specified, and `destination_dir` or `destination_filename` is missing."
            )
        
        if not destination_key:
            destination_key = os.path.join(destination_dir, destination_filename)

        # Infer bucket name from schema if not specified (internal standard that must be maintained during connection setup)
        if not bucket_name:
            bucket_name = BaseHook.get_connection(conn_id).schema
        
        full_destination_key = f"{cls.STORAGE_PREFIX}{bucket_name}/{destination_key}"
        return ObjectStoragePath(full_destination_key, conn_id=conn_id)


    def pull_edfi_to_cloud_storage(self,
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
        Break out EdFi-to-Cloud-Storage logic to allow code-duplication in bulk version of operator.
        """
        logging.info(
            "    Pulling records for `{}/{}` for change versions `{}` to `{}`. (page_size: {}; CV step size: {})"\
            .format(namespace, resource, min_change_version, max_change_version, page_size, change_version_step_size)
        )

        ### Connect to EdFi and write resource data to a temp file, then copy to cloud storage.
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

        # Iterate the ODS, paginating across offset and change version steps.
        # Write each result to the temp file.
        total_rows = 0
        
        os.makedirs(os.path.dirname(self.tmp_dir), exist_ok=True)  # Create its parent-directory in not extant.
        
        with tempfile.TemporaryFile('w+b', dir=self.tmp_dir) as tmp_file:

            # Output each page of results as JSONL strings to the output file.
            for page_result in paged_iter:
                tmp_file.write(self.to_jsonl_string(page_result))
                total_rows += len(page_result)

            ### Connect to cloud storage and copy file
            tmp_file.seek(0)  # Go back to the start of the file before copying to cloud storage.
            object_storage.upload_from(tmp_file, force_overwrite_to_cloud=True)
                
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


class BulkEdFiToCloudStorageOperator(EdFiToCloudStorageOperator):
    """
    Inherits from EdFiToCloudStorageOperator to reduce code-duplication.

    The following arguments MUST be lists instead of singletons:
    - resource
    - namespace
    - page_size
    - num_retries
    - change_version_step_size
    - query_parameters
    - min_change_version
    - cloud_destination_filename

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
        if self.cloud_destination_key or not (self.cloud_destination_dir and self.cloud_destination_filename):
            raise ValueError(
                "Bulk operators require arguments `cloud_destination_dir` and `cloud_destination_filename` to be passed."
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
            self.cloud_destination_filename,
        ]

        for idx, (resource, min_change_version, namespace, page_size, num_retries, change_version_step_size, query_parameters, cloud_destination_filename) \
            in enumerate(zip(*zip_arguments), start=1):

            logging.info(f"[ENDPOINT {idx} / {len(self.resource)}]")

            # If doing a resource-specific run, confirm resource is in the list.
            # Also confirm resource is in XCom-list if passed (used for dynamic XComs retrieved from get-change-version operator).
            if not self.is_endpoint_specified(resource, config_endpoints, self.enabled_endpoints):
                continue

            # Check the validity of min and max change-versions.
            self.check_change_version_window_validity(self.min_change_version, self.max_change_version)

            # Complete the pull and write to cloud storage
            object_storage = self.get_object_storage(
                conn_id=self.cloud_conn_id, destination_key=self.cloud_destination_key,
                destination_dir=self.cloud_destination_dir, destination_filename=cloud_destination_filename
            )

            # Wrap in a try-except to still attempt other endpoints in a skip or failure.
            try:
                self.pull_edfi_to_cloud_storage(
                    edfi_conn=edfi_conn,
                    resource=resource, namespace=namespace, page_size=page_size,
                    num_retries=num_retries, change_version_step_size=change_version_step_size,
                    min_change_version=min_change_version, max_change_version=self.max_change_version,
                    query_parameters=query_parameters, object_storage=object_storage
                )
                return_tuples.append((resource, object_storage.key))

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


class S3Mixin:
    STORAGE_PREFIX: str = "s3://"

    def __init__(self,
        s3_conn_id: str,
        s3_destination_key: Optional[str] = None,
        s3_destination_dir: Optional[str] = None,
        s3_destination_filename: Optional[str] = None,
        **kwargs
    ) -> None:
        super(S3Mixin, self).__init__(
            cloud_conn_id=s3_conn_id,
            cloud_destination_key=s3_destination_key,
            cloud_destination_dir=s3_destination_dir,
            cloud_destination_filename=s3_destination_filename,
            **kwargs
        )

class EdFiToS3Operator(EdFiToCloudStorageOperator, S3Mixin):
    pass

class BulkEdFiToS3Operator(BulkEdFiToCloudStorageOperator, S3Mixin):
    pass
