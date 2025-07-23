import json
import logging
import os

from typing import Iterator, List, Optional, Union, Callable

from airflow.models import BaseOperator
from airflow.exceptions import AirflowSkipException, AirflowFailException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.decorators import apply_defaults

from edu_edfi_airflow.callables import airflow_util
from edu_edfi_airflow.providers.edfi.hooks.edfi import EdFiHook


class EdFiToS3Operator(BaseOperator):
    """
    Establish a connection to the EdFi ODS using an Airflow Connection.
    Default to pulling the EdFi API configs from the connection if not explicitly provided.

    Use a paged-get to retrieve a particular EdFi resource from the ODS.
    Save the results as JSON lines to `tmp_dir` on the server.
    Once pagination is complete, write the full results to S3.

    Deletes and keyChanges are only used in the bulk operator.
    """
    template_fields = (
        'resource', 'namespace', 'page_size', 'num_retries', 'change_version_step_size', 'query_parameters',
        's3_destination_key', 's3_destination_dir', 's3_destination_filename',
        'min_change_version', 'max_change_version', 'enabled_endpoints',
    )

    def __init__(self,
        edfi_conn_id: str,
        resource: str,

        *,
        tmp_dir: str,
        s3_conn_id: str,
        s3_destination_key: Optional[str] = None,
        s3_destination_dir: Optional[str] = None,
        s3_destination_filename: Optional[str] = None,

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

        edfi_token_factory: Callable[[dict], Optional[Callable[[], str]]],

        **kwargs
    ) -> None:
        super(EdFiToS3Operator, self).__init__(**kwargs)

        # Top-level variables
        self.edfi_conn_id = edfi_conn_id
        self.resource = resource

        self.get_deletes = get_deletes
        self.get_key_changes = get_key_changes
        self.min_change_version = min_change_version
        self.max_change_version = max_change_version

        # Storage variables
        self.tmp_dir = tmp_dir
        self.s3_conn_id = s3_conn_id
        self.s3_destination_key = s3_destination_key
        self.s3_destination_dir = s3_destination_dir
        self.s3_destination_filename = s3_destination_filename
        self.edfi_token_factory = edfi_token_factory

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
        config_endpoints = airflow_util.get_config_endpoints(context)
        if config_endpoints and self.resource not in config_endpoints:
            raise AirflowSkipException("Endpoint not specified in DAG config endpoints.")
        
        # Confirm resource is in XCom-list if passed (used for dynamic XComs retrieved from get-change-version operator).
        if self.enabled_endpoints and self.resource not in self.enabled_endpoints:
            raise AirflowSkipException("Endpoint not specified in run endpoints.")
        # Optionally set destination key by concatting separate args for dir and filename
        if not self.s3_destination_key:
            if not (self.s3_destination_dir and self.s3_destination_filename):
                raise ValueError(
                    f"Argument `s3_destination_key` has not been specified, and `s3_destination_dir` or `s3_destination_filename` is missing."
                )
            self.s3_destination_key = os.path.join(self.s3_destination_dir, self.s3_destination_filename)

        # Check the validity of min and max change-versions.
        self.check_change_version_window_validity(self.min_change_version, self.max_change_version)

        # Complete the pull and write to S3
        access_token = self.edfi_token_factory(context)
        edfi_conn = EdFiHook(self.edfi_conn_id, access_token=access_token).get_conn()

        self.pull_edfi_to_s3(
            edfi_conn=edfi_conn,
            resource=self.resource, namespace=self.namespace, page_size=self.page_size,
            num_retries=self.num_retries, change_version_step_size=self.change_version_step_size,
            min_change_version=self.min_change_version, max_change_version=self.max_change_version,
            query_parameters=self.query_parameters, s3_destination_key=self.s3_destination_key
        )

        return (self.resource, self.s3_destination_key)


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

    def pull_edfi_to_s3(self,
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
        s3_destination_key: str
    ):
        """
        Break out EdFi-to-S3 logic to allow code-duplication in bulk version of operator.
        """
        ### Connect to EdFi and write resource data to a temp file.
        # Prepare the EdFiEndpoint for the resource.
        logging.info(
            "    Pulling records for `{}/{}` for change versions `{}` to `{}`. (page_size: {}; CV step size: {})"\
            .format(namespace, resource, min_change_version, max_change_version, page_size, change_version_step_size)
        )

        resource_endpoint = edfi_conn.resource(
            resource, namespace=namespace, params=query_parameters,
            get_deletes=self.get_deletes, get_key_changes=self.get_key_changes,
            min_change_version=min_change_version, max_change_version=max_change_version
        )

        # Iterate the ODS, paginating across offset and change version steps.
        # Write each result to the temp file.
        tmp_file = os.path.join(self.tmp_dir, s3_destination_key)
        total_rows = 0

        try:
            # Turn off change version stepping if min and max change versions have not been defined.
            step_change_version = (min_change_version is not None and max_change_version is not None)

            paged_iter = resource_endpoint.get_pages(
                page_size=page_size,
                step_change_version=step_change_version, change_version_step_size=change_version_step_size,
                reverse_paging=(self.reverse_paging),
                retry_on_failure=True, max_retries=num_retries
            )

            # Output each page of results as JSONL strings to the output file.
            os.makedirs(os.path.dirname(tmp_file), exist_ok=True)  # Create its parent-directory in not extant.

            with open(tmp_file, 'wb') as fp:
                for page_result in paged_iter:
                    fp.write(self.to_jsonl_string(page_result))
                    total_rows += len(page_result)

        # In the case of any failures, we need to delete the temporary files written, then reraise the error.
        except Exception as err:
            self.delete_path(tmp_file)
            raise err

        # Check whether the number of rows returned matched the number expected.
        try:
            expected_rows = resource_endpoint.total_count()
            if total_rows != expected_rows:
                logging.warning(f"    Expected {expected_rows} rows for `{resource}`.")
            else:
                logging.info("    Number of collected rows matches expected count in the ODS.")

        except Exception:
            logging.warning(f"    Unable to access expected number of rows for `{resource}`.")

        finally:
            logging.info(f"    {total_rows} rows were returned for `{resource}`.")

        # Raise a Skip if no data was collected.
        if total_rows == 0:
            logging.info(f"    No results returned for `{resource}`")
            self.delete_path(tmp_file)
            raise AirflowSkipException

        ### Connect to S3 and push
        try:
            s3_hook = S3Hook(aws_conn_id=self.s3_conn_id)
            s3_bucket = s3_hook.get_connection(self.s3_conn_id).schema

            s3_hook.load_file(
                filename=tmp_file,
                bucket_name=s3_bucket,
                key=s3_destination_key,
                encrypt=True,
                replace=True
            )
        finally:
            self.delete_path(tmp_file)

    @staticmethod
    def delete_path(path: str):
        logging.info(f"    Removing temporary files written to `{path}`")
        try:
            os.remove(path)
        except FileNotFoundError:
            pass

    @staticmethod
    def to_jsonl_string(rows: Iterator[dict]) -> bytes:
        """
        :return:
        """
        return b''.join(
            json.dumps(row).encode('utf8') + b'\n'
            for row in rows
        )


class BulkEdFiToS3Operator(EdFiToS3Operator):
    """
    Inherits from EdFiToS3Operator to reduce code-duplication.

    The following arguments MUST be lists instead of singletons:
    - resource
    - namespace
    - page_size
    - num_retries
    - change_version_step_size
    - query_parameters
    - min_change_version
    - s3_destination_filename

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
        if self.s3_destination_key or not (self.s3_destination_dir and self.s3_destination_filename):
            raise ValueError(
                "Bulk operators require arguments `s3_destination_dir` and `s3_destination_filename` to be passed."
            )

        # Make connection outside of loop to not re-authenticate at every resource.
        # ignore token factory; get own tokens
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
            self.s3_destination_filename,
        ]

        for idx, (resource, min_change_version, namespace, page_size, num_retries, change_version_step_size, query_parameters, s3_destination_filename) \
            in enumerate(zip(*zip_arguments), start=1):

            logging.info(f"[ENDPOINT {idx} / {len(self.resource)}]")

            # If doing a resource-specific run, confirm resource is in the list.
            if config_endpoints and resource not in config_endpoints:
                logging.info(f"    Endpoint {resource} not specified in DAG config endpoints. Skipping...")
                continue

            # Confirm resource is in XCom-list if passed (used for dynamic XComs retrieved from get-change-version operator).
            if self.enabled_endpoints and resource not in self.enabled_endpoints:
                logging.info(f"    Endpoint {resource} not specified in run endpoints. Skipping...")
                continue

            try:
                # Retrieve the min_change_version for this resource specifically.
                self.check_change_version_window_validity(min_change_version, self.max_change_version)

                # Complete the pull and write to S3
                s3_destination_key = os.path.join(self.s3_destination_dir, s3_destination_filename)

                self.pull_edfi_to_s3(
                    edfi_conn=edfi_conn,
                    resource=resource, namespace=namespace, page_size=page_size,
                    num_retries=num_retries, change_version_step_size=change_version_step_size,
                    min_change_version=min_change_version, max_change_version=self.max_change_version,
                    query_parameters=query_parameters, s3_destination_key=s3_destination_key
                )
                return_tuples.append((resource, s3_destination_key))

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
