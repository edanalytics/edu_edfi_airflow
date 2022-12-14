import logging

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
        edfi_conn_id: str,
        snowflake_conn_id: str,

        tenant_code: str,
        api_year: int,
        resource: str,
        table_name: str,

        s3_destination_key: str,

        full_refresh: bool = False,

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

        self.full_refresh = full_refresh


    def execute(self, context):
        """

        :param context:
        :return:
        """
        edfi_conn = EdFiHook(edfi_conn_id=self.edfi_conn_id).get_conn()
        snowflake_hook = SnowflakeHook(snowflake_conn_id=self.snowflake_conn_id)

        # Retrieve the database and schema from the Snowflake hook.
        database, schema = get_snowflake_params_from_conn(self.snowflake_conn_id)

        if edfi_conn.is_edfi2():
            ods_version = "ED-FI2"
            data_model_version = "ED-FI2"
        else:
            ods_version = edfi_conn.get_ods_version()
            data_model_version = edfi_conn.get_data_model_version()

        # Build the SQL queries to be passed into `Hook.run()`.
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
                    '{ods_version}' AS ods_version,
                    '{data_model_version}' as data_model_version,
                    t.$1 AS v
                FROM @{database}.util.airflow_stage/{self.s3_destination_key}
                (file_format => 'json_default') t
            )
            force = true;
        """

        # Break off prematurely if EdFi 3+ and not a full refresh.
        # Incremental runs are only available in EdFi 3+.
        if edfi_conn.is_edfi2() or (self.full_refresh or is_full_refresh(context)):

            cursor_log = snowflake_hook.run(
                sql=[qry_delete, qry_copy_into],
                autocommit=False
            )

        else:
            cursor_log = snowflake_hook.run(
                sql=qry_copy_into
            )

        logging.info(cursor_log)
