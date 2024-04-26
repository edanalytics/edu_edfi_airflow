import logging
import os

from typing import Any, Optional

from airflow.exceptions import AirflowSkipException
from airflow.models import BaseOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.decorators import apply_defaults

from edu_edfi_airflow.callables import airflow_util
from edu_edfi_airflow.providers.edfi.hooks.edfi import EdFiHook


class S3ToSnowflakeOperator(BaseOperator):
    """
    Copy the Ed-Fi files saved to S3 to Snowflake raw resource tables.
    """
    template_fields = ('resource', 'table_name', 's3_destination_key', 's3_destination_dir', 's3_destination_filename',)

    @apply_defaults
    def __init__(self,
        *,
        tenant_code: str,
        api_year: int,
        resource: str,
        table_name: str,

        s3_destination_key: Optional[str] = None,
        s3_destination_dir: Optional[str] = None,
        s3_destination_filename: Optional[str] = None,

        snowflake_conn_id: str,

        edfi_conn_id: Optional[str] = None,
        ods_version: Optional[str] = None,
        data_model_version: Optional[str] = None,

        full_refresh: bool = False,
        xcom_return: Optional[Any] = None,
        **kwargs
    ) -> None:
        super(S3ToSnowflakeOperator, self).__init__(**kwargs)

        self.edfi_conn_id = edfi_conn_id
        self.snowflake_conn_id = snowflake_conn_id

        self.tenant_code = tenant_code
        self.api_year = api_year
        self.resource = resource
        self.table_name = table_name

        self.s3_destination_key = s3_destination_key
        self.s3_destination_dir = s3_destination_dir
        self.s3_destination_filename = s3_destination_filename

        self.ods_version = ods_version
        self.data_model_version = data_model_version

        self.full_refresh = full_refresh
        self.xcom_return = xcom_return


    def execute(self, context):
        """

        :param context:
        :return:
        """
        ### Optionally set destination key by concatting separate args for dir and filename
        if not self.s3_destination_key:
            if not (self.s3_destination_dir and self.s3_destination_filename):
                raise ValueError(
                    f"Argument `s3_destination_key` has not been specified, and `s3_destination_dir` or `s3_destination_filename` is missing."
                )
            self.s3_destination_key = os.path.join(self.s3_destination_dir, self.s3_destination_filename)

        ### Retrieve the Ed-Fi, ODS, and data model versions in execute to prevent excessive API calls.
        self.set_edfi_attributes()

        # Build and run the SQL queries to Snowflake. Delete first if EdFi2 or a full-refresh.
        self.run_sql_queries(
            name=self.resource, table=self.table_name,
            s3_key=self.s3_destination_key, full_refresh=airflow_util.is_full_refresh(context)
        )

        return self.xcom_return

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

    def run_sql_queries(self, name: str, table: str, s3_key: str, full_refresh: bool = False):
        """

        """
        ### Retrieve the database and schema from the Snowflake hook.
        snowflake_hook = SnowflakeHook(snowflake_conn_id=self.snowflake_conn_id)
        database, schema = airflow_util.get_snowflake_params_from_conn(self.snowflake_conn_id)

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
                    '{name}' AS name,
                    '{self.ods_version}' AS ods_version,
                    '{self.data_model_version}' AS data_model_version,
                    t.$1 AS v
                FROM @{database}.util.airflow_stage/{s3_key}
                (file_format => 'json_default') t
            )
            force = true;
        """

        ### Commit the update queries to Snowflake.
        # Incremental runs are only available in EdFi 3+.
        if self.full_refresh or full_refresh:
            qry_delete = f"""
                DELETE FROM {database}.{schema}.{table}
                WHERE tenant_code = '{self.tenant_code}'
                AND api_year = '{self.api_year}'
                AND name = '{name}'
            """
            snowflake_hook.run(sql=[qry_delete, qry_copy_into], autocommit=False)
        
        else:
            snowflake_hook.run(sql=qry_copy_into)


class BulkS3ToSnowflakeOperator(S3ToSnowflakeOperator):
    """
    Copy the Ed-Fi files saved to S3 to Snowflake raw resource tables.
    """
    def execute(self, context):
        """

        :param context:
        :return:
        """
        if not self.resource:
            raise AirflowSkipException("There are no endpoints to copy to Snowflake. Skipping task...")

        # Force potential string columns into lists for zipping in execute.
        if isinstance(self.resource, str):
            raise ValueError("Bulk operators require lists of resources to be passed.")

        if isinstance(self.table_name, str):
            self.table_name = [self.table_name] * len(self.resource)

        # Force destination_dir and destination_filename arguments to be used.
        if self.s3_destination_key or not (self.s3_destination_dir and self.s3_destination_filename):
            raise ValueError(
                "Bulk operators require arguments `s3_destination_dir` and `s3_destination_filename` to be passed."
            )
        
        if isinstance(self.s3_destination_filename, str):
            raise ValueError(
                "Bulk operators require argument `s3_destination_filename` to be a list."
            )

        ### Retrieve the Ed-Fi, ODS, and data model versions in execute to prevent excessive API calls.
        self.set_edfi_attributes()

        # Build and run the SQL queries to Snowflake. Delete first if EdFi2 or a full-refresh.
        xcom_returns = []

        for idx, (resource, table, s3_destination_filename) in enumerate(zip(self.resource, self.table_name, self.s3_destination_filename), start=1):
            logging.info(f"[ENDPOINT {idx} / {len(self.resource)}]")

            s3_key = os.path.join(self.s3_destination_dir, s3_destination_filename)
            self.run_sql_queries(
                name=resource, table=table,
                s3_key=s3_key, full_refresh=airflow_util.is_full_refresh(context)
            )

            if self.xcom_return:
                xcom_returns.append(self.xcom_return(resource))

        return xcom_returns
