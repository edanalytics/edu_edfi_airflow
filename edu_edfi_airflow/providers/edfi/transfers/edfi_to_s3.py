import json
import logging
import os

from typing import Iterator, Optional

from airflow.models import BaseOperator
from airflow.exceptions import AirflowSkipException, AirflowFailException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.decorators import apply_defaults

from edfi_api_client import camel_to_snake
from edu_edfi_airflow.dags.dag_util import airflow_util
from edu_edfi_airflow.providers.edfi.hooks.edfi import EdFiHook


class EdFiToS3Operator(BaseOperator):
    """
    Establish a connection to the EdFi ODS using an Airflow Connection.
    Default to pulling the EdFi API configs from the connection if not explicitly provided.

    Use a paged-get to retrieve a particular EdFi resource from the ODS.
    Save the results as JSON lines to `tmp_dir` on the server.
    Once pagination is complete, write the full results to S3.
    """
    template_fields = ('s3_destination_key', 'query_parameters', 'min_change_version', 'max_change_version',)

    @apply_defaults
    def __init__(self,
        edfi_conn_id: str,
        resource    : str,

        *,
        tmp_dir   : str,
        s3_conn_id: str,
        s3_destination_key: str,

        query_parameters  : Optional[dict] = None,
        min_change_version: Optional[int ] = None,
        max_change_version: Optional[int ] = None,
        change_version_step_size: int = 50000,

        api_namespace  : str = 'ed-fi',  # Should these variables line up 1-to-1 with EdFiClient?
        api_get_deletes: bool = False,
        api_retries    : int = 5,
        page_size      : int = None,

        **kwargs
    ) -> None:
        super(EdFiToS3Operator, self).__init__(**kwargs)

        # EdFi variables
        self.edfi_conn_id = edfi_conn_id
        self.resource = resource

        self.query_parameters   = query_parameters
        self.min_change_version = min_change_version
        self.max_change_version = max_change_version
        self.change_version_step_size = change_version_step_size

        self.api_namespace  = api_namespace
        self.api_get_deletes= api_get_deletes
        self.api_retries    = api_retries
        self.page_size      = page_size

        # Storage variables
        self.tmp_dir   = tmp_dir
        self.s3_conn_id= s3_conn_id
        self.s3_destination_key= s3_destination_key

        # Force min-change-version if no XCom was found.
        if self.max_change_version and self.min_change_version is None:
            self.min_change_version = 0


    def execute(self, context) -> str:
        """

        :param context:
        :return:
        """
        if airflow_util.is_full_refresh(context) and self.api_get_deletes:
            raise AirflowSkipException(
                "Skipping delete pull for full_refresh run."
            )

        # If doing a resource-specific run, confirm resource is in the list.
        config_endpoints = airflow_util.get_config_endpoints(context)  # Returns `[]` if none explicitly specified.
        if config_endpoints and camel_to_snake(self.resource) in config_endpoints:
            raise AirflowSkipException(
                "Endpoint not specified in DAG config `endpoints`."
            )

        # Run sanity checks to make sure we aren't doing something wrong.
        if self.min_change_version is not None and self.max_change_version is not None:
            if self.min_change_version == self.max_change_version:
                raise AirflowSkipException(
                    "ODS is unchanged since previous pull."
                )
            elif self.max_change_version < self.min_change_version:
                raise AirflowFailException(
                    "Apparent out-of-sequence run: current change version is smaller than previous!"
                )

            logging.info(
                "Pulling records for `{}/{}` for change versions `{}` to `{}`.".format(
                    self.api_namespace, self.resource, self.min_change_version, self.max_change_version
                )
            )
        
        ### Connect to EdFi and write resource data to a temp file.
        # Establish a hook to the ODS
        edfi_conn = EdFiHook(self.edfi_conn_id).get_conn()

        # Prepare the EdFiEndpoint for the resource.
        resource_endpoint = edfi_conn.resource(
            self.resource, namespace=self.api_namespace, get_deletes=self.api_get_deletes, params=self.query_parameters,
            min_change_version=self.min_change_version, max_change_version=self.max_change_version
        )

        # # Fail immediately if the endpoint is forbidden.
        # _response = resource_endpoint.ping()
        # if not _response.ok:
        #     raise Exception(f"Unable to connect to `{self.resource}`! {_response.status_code}: {_response.reason}")

        # Iterate the ODS, paginating across offset and change version steps.
        # Write each result to the temp file.
        tmp_file = os.path.join(self.tmp_dir, self.s3_destination_key)
        total_rows = 0

        try:
            # Turn off change version stepping if min and max change versions have not been defined.
            step_change_version = (self.min_change_version is not None and self.max_change_version is not None)

            paged_iter = resource_endpoint.get_pages(
                page_size=self.page_size,
                step_change_version=step_change_version, change_version_step_size=self.change_version_step_size,
                reverse_paging=(not self.api_get_deletes),  # Reverse_paging is true for resources and false for deletes
                retry_on_failure=True, max_retries=self.api_retries
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
                logging.warning(f"Expected {expected_rows} rows for `{self.resource}`.")
            else:
                logging.info("Number of collected rows matches expected count in the ODS.")

        except Exception:
            logging.warning(f"Unable to access expected number of rows for `{self.resource}`.")

        finally:
            logging.info(f"{total_rows} rows were returned for `{self.resource}`.")

        # Raise a Skip if no data was collected.
        if total_rows == 0:
            logging.info(f"No results returned for `{self.resource}`")
            self.delete_path(tmp_file)
            raise AirflowSkipException

        ### Connect to S3 and push
        try:
            s3_hook = S3Hook(aws_conn_id=self.s3_conn_id)
            s3_bucket = s3_hook.get_connection(self.s3_conn_id).schema

            s3_hook.load_file(
                filename=tmp_file,

                bucket_name=s3_bucket,
                key=self.s3_destination_key,

                encrypt=True,
                replace=True
            )
        finally:
            self.delete_path(tmp_file)

        return self.s3_destination_key


    @staticmethod
    def delete_path(path: str):
        logging.info(f"Removing temporary files written to `{path}`")
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
