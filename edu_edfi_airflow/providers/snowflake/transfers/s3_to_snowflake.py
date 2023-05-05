import logging

from typing import Optional

from airflow.models import BaseOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.decorators import apply_defaults

from edu_edfi_airflow.dags.dag_util.airflow_util import is_full_refresh, get_snowflake_params_from_conn
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
        full_refresh: bool = False,

        edfi_conn_id: Optional[str] = None,
        ods_version: Optional[str] = None,
        data_model_version: Optional[str] = None,

        **kwargs
    ) -> None:
        super(S3ToSnowflakeOperator, self).__init__(**kwargs)

        self.tenant_code = tenant_code
        self.api_year = api_year
        self.resource = resource
        self.table_name = table_name

        self.s3_destination_key = s3_destination_key

        self.snowflake_conn_id = snowflake_conn_id
        self.full_refresh = full_refresh

        self.edfi_conn_id = edfi_conn_id
        self.ods_version = ods_version
        self.data_model_version = data_model_version


    def execute(self, context):
        """

        :param context:
        :return:
        """
        ### Retrieve the database and schema from the Snowflake hook.
        snowflake_hook = SnowflakeHook(snowflake_conn_id=self.snowflake_conn_id)
        database, schema = get_snowflake_params_from_conn(self.snowflake_conn_id)

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

        qry_copy_into = f"""
            COPY INTO {database}.{schema}.{self.table_name}
                (tenant_code, api_year, pull_date, pull_timestamp, file_row_number, filename, name, ods_version, data_model_version, v)
            FROM (
                SELECT
                    SPLIT_PART(metadata$filename, '/', 1) as tenant_code,
                    SPLIT_PART(metadata$filename, '/', 2) as api_year,
                    TO_DATE(SPLIT_PART(metadata$filename, '/', 3), 'YYYYMMDD') AS pull_date,
                    TO_TIMESTAMP(SPLIT_PART(metadata$filename, '/', 4), 'YYYYMMDDTHH24MISS') AS pull_timestamp,
                    metadata$file_row_number AS file_row_number,
                    metadata$filename AS filename,
                    '{self.resource}' AS name,
                    '{self.ods_version}' AS ods_version,
                    '{self.data_model_version}' as data_model_version,
                    t.$1 AS v
                FROM @{database}.util.airflow_stage/{self.s3_destination_key}
                (file_format => 'json_default') t
            )
            force = true;
        """

        ### Commit the update queries to Snowflake.
        # Incremental runs are only available in EdFi 3+.
        if is_edfi2 or (self.full_refresh or is_full_refresh(context)):
            cursor_log = snowflake_hook.run(
                sql=[qry_delete, qry_copy_into],
                autocommit=False
            )

        else:
            cursor_log = snowflake_hook.run(
                sql=qry_copy_into
            )

        logging.info(cursor_log)
