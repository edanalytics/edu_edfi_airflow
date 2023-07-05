import logging

from typing import Optional

from airflow.models import BaseOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.decorators import apply_defaults

from edu_edfi_airflow.dags.dag_util import airflow_util
from edu_edfi_airflow.providers.edfi.hooks.edfi import EdFiHook


class S3ToSnowflakeOperator(BaseOperator):
    """
    Copy the Ed-Fi files saved to S3 to Snowflake raw resource tables.
    """
    template_fields = ('s3_destination_key',)

    @apply_defaults
    def __init__(self,
        *,
        tenant_code: str,
        api_year: int,
        resource: str,
        table_name: str,

        s3_destination_key: str,

        snowflake_conn_id: str,

        edfi_conn_id: Optional[str] = None,
        ods_version: Optional[str] = None,
        data_model_version: Optional[str] = None,

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

        self.ods_version = ods_version
        self.data_model_version = data_model_version


    def execute(self, context):
        """

        :param context:
        :return:
        """
        ### Retrieve the database and schema from the Snowflake hook.
        snowflake_hook = SnowflakeHook(snowflake_conn_id=self.snowflake_conn_id)
        database, schema = airflow_util.get_snowflake_params_from_conn(self.snowflake_conn_id)

        ### Retrieve the Ed-Fi, ODS, and data model versions if not provided.
        # (This needs to occur in execute to not call the API at every Airflow synchronize.)
        if self.edfi_conn_id:
            edfi_conn = EdFiHook(edfi_conn_id=self.edfi_conn_id).get_conn()
            is_edfi2 = edfi_conn.is_edfi2()

            if not self.ods_version:
                self.ods_version = 'ED-FI2' if is_edfi2 else edfi_conn.get_ods_version()

            if not self.data_model_version:
                self.data_model_version = 'ED-FI2' if is_edfi2 else edfi_conn.get_data_model_version()

        else:
            is_edfi2 = False

        if not (self.ods_version and self.data_model_version):
            raise Exception(
                f"Arguments `ods_version` and `data_model_version` could not be retrieved and must be provided."
            )

        ### Build the SQL queries to be passed into `Hook.run()`.
        qry_delete = f"""
            DELETE FROM {database}.{schema}.{self.table_name}
            WHERE tenant_code = '{self.tenant_code}'
            AND api_year = '{self.api_year}'
            AND name = '{self.resource}'
        """

        # Brackets in regex conflict with string formatting.
        date_regex = "\\d{8}"
        ts_regex   = "\\d{8}T\\d{6}"

        qry_copy_into = f"""
            COPY INTO {database}.{schema}.{self.table_name}
                (tenant_code, api_year, pull_date, pull_timestamp, file_row_number, filename, name, ods_version, data_model_version, v)
            FROM (
                SELECT
                    '{self.tenant_code}' AS tenant_code,
                    '{self.api_year}' AS api_year,
                    TO_DATE(REGEXP_SUBSTR(metadata$filename, '{date_regex}'), 'YYYYMMDD') AS pull_date,
                    TO_TIMESTAMP(REGEXP_SUBSTR(metadata$filename, '{ts_regex}'), 'YYYYMMDDTHH24MISS') AS pull_timestamp,
                    metadata$file_row_number AS file_row_number,
                    metadata$filename AS filename,
                    '{self.resource}' AS name,
                    '{self.ods_version}' AS ods_version,
                    '{self.data_model_version}' AS data_model_version,
                    t.$1 AS v
                FROM @{database}.util.airflow_stage/{self.s3_destination_key}
                (file_format => 'json_default') t
            )
            force = true;
        """

        ### Commit the update queries to Snowflake.
        # Incremental runs are only available in EdFi 3+.
        if is_edfi2 or airflow_util.is_full_refresh(context):
            cursor_log = snowflake_hook.run(
                sql=[qry_delete, qry_copy_into],
                autocommit=False
            )

        else:
            cursor_log = snowflake_hook.run(
                sql=qry_copy_into
            )

        logging.info(cursor_log)
        return True  # Return for update_change_versions() xcom pull
