import os
from functools import partial
from typing import Optional

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup

from ea_airflow_util import slack_callbacks
from edfi_api_client import camel_to_snake

from edu_edfi_airflow.dags.callables import edfi as edfi_callables
from edu_edfi_airflow.dags.callables import snowflake as snowflake_callables
from edu_edfi_airflow.dags.dag_util.airflow_util import xcom_pull_template as pull_xcom
from edu_edfi_airflow.providers.edfi.transfers.edfi_to_s3 import EdFiToS3Operator
from edu_edfi_airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator


class EdFiResourceDAG:
    """

    """
    def __init__(self,
        *,
        tenant_code: str,
        api_year   : int,

        edfi_conn_id     : str,
        s3_conn_id       : str,
        snowflake_conn_id: str,

        pool     : str,
        tmp_dir  : str,

        use_change_version: bool = True,
        change_version_table: str = '_meta_change_versions',
        multiyear: bool = False,
        full_refresh: bool = False,

        slack_conn_id: str = None,

        **kwargs
    ) -> None:
        self.tenant_code = tenant_code
        self.api_year = api_year

        self.edfi_conn_id = edfi_conn_id
        self.s3_conn_id = s3_conn_id
        self.snowflake_conn_id = snowflake_conn_id
        self.slack_conn_id = slack_conn_id

        self.pool = pool
        self.tmp_dir = tmp_dir

        self.use_change_version = use_change_version
        self.change_version_table = change_version_table
        self.multiyear = multiyear
        self.full_refresh = full_refresh

        # Force full-refreshes if `use_change_version is False`.
        if self.use_change_version is False:
            self.full_refresh = True

        self.dag = self.initialize_dag(**kwargs)

        # If change-versions operations are turned off, don't build the operator.
        if use_change_version:
            self.edfi_change_version_operator = self.build_edfi_change_version_operator()
        else:
            self.edfi_change_version_operator = None


    def initialize_dag(self,
        dag_id: str,
        schedule_interval: str,
        default_args: dict,
        **kwargs
    ) -> DAG:
        """

        :param dag_id:
        :param schedule_interval:
        :param default_args:
        :return:
        """
        # If a Slack connection has been defined, add the failure callback to the default_args.
        if self.slack_conn_id:
            slack_failure_callback = partial(slack_callbacks.slack_alert_failure, http_conn_id=self.slack_conn_id)
            default_args['on_failure_callback'] = slack_failure_callback

            # Define an SLA-miss callback as well.
            slack_sla_miss_callback = partial(slack_callbacks.slack_alert_sla_miss, http_conn_id=self.slack_conn_id)
        else:
            slack_sla_miss_callback = None

        # If a Slack connection is defined, send a callback in the event of a DAG failure.
        return DAG(
            dag_id=dag_id,
            schedule_interval=schedule_interval,
            default_args=default_args,
            catchup=False,
            # user_defined_macros= {  # Note: none of these UDMs are currently used. (These are beautiful, but antithetical to Airflow's Scheduler design!)
            #     'tenant_code': self.tenant_code,
            #     'api_year'   : self.api_year,
            # },
            render_template_as_native_obj=True,
            max_active_runs=1,
            sla_miss_callback=slack_sla_miss_callback,
        )


    def build_edfi_change_version_operator(self) -> PythonOperator:
        """
        :return:
        """
        task_id = "get_latest_edfi_change_version"

        return PythonOperator(
            task_id=task_id,
            python_callable=edfi_callables.get_edfi_change_version,
            op_kwargs={
                'edfi_conn_id': self.edfi_conn_id,
            },
            dag=self.dag
        )


    def build_edfi_to_snowflake_task_group(self,
        resource : str,
        namespace: str = 'ed-fi',

        *,
        deletes    : bool = False,
        table      : Optional[str] = None,
        page_size  : int = 500,
        max_retries: int = 5,

        use_change_version: bool = True,
        change_version_step_size: int = 50000,

        parent_group: Optional[TaskGroup] = None,
    ) -> TaskGroup:
        """
        Pulling an EdFi resource/descriptor requires knowing its camelCased name and namespace.
        Deletes are optionally specified.
        Specify `table` to overwrite the final Snowflake table location.

        :param resource :
        :param namespace:
        :param deletes  :
        :param table    : Overwrite the table to output the rows to (exception case for descriptors).
        :param page_size:
        :param max_retries:
        :param use_change_version:
        :param change_version_step_size:
        :param parent_group:
        :return:
        """
        # Snowflake tables and Airflow tasks use snake_cased resources for readability.
        # Apply the deletes suffix for logging deletes across the Airflow DAG.
        snake_resource = camel_to_snake(resource)
        display_resource = snake_resource + '_deletes' if deletes else snake_resource

        # Wrap the branch in a task group
        with TaskGroup(
            group_id=display_resource,
            prefix_group_id=False,
            parent_group=parent_group,
            dag=self.dag
        ) as resource_task_group:

            ### EDFI3 CHANGE_VERSION LOGIC
            if self.edfi_change_version_operator is None:
                use_change_version = False
            else:
                max_change_version = pull_xcom(self.edfi_change_version_operator.task_id)

                # EdFi2 causes an AirflowSkipException during the run.
                # A None change version from this task presumes EdFi2.
                # Force `use_change_version = False` to account for this.
                if max_change_version is None or max_change_version == 'None':
                    use_change_version = False

            if use_change_version:

                ### GET LAST CHANGE VERSION FROM SNOWFLAKE
                get_change_version_snowflake = PythonOperator(
                    task_id=f"get_last_change_version_{display_resource}",
                    python_callable=snowflake_callables.get_resource_change_version,

                    op_kwargs={
                        'edfi_change_version': max_change_version,
                        'snowflake_conn_id': self.snowflake_conn_id,

                        'tenant_code': self.tenant_code,
                        'api_year': self.api_year,
                        'resource': snake_resource,
                        'deletes' : deletes,

                        'change_version_table': self.change_version_table,
                        'full_refresh': self.full_refresh,
                    },

                    provide_context=True,
                    trigger_rule='all_success',
                    dag=self.dag,
                )

                min_change_version = pull_xcom(get_change_version_snowflake.task_id, key='prev_change_version')


                ### UPDATE CHANGE VERSION TABLE ON SNOWFLAKE
                update_change_version_snowflake = PythonOperator(
                    task_id=f"update_change_version_{display_resource}",
                    python_callable=snowflake_callables.update_change_version_table,

                    op_kwargs={
                        'tenant_code': self.tenant_code,
                        'api_year': self.api_year,
                        'resource': snake_resource,
                        'deletes': deletes,

                        'snowflake_conn_id': self.snowflake_conn_id,
                        'change_version_table': self.change_version_table,

                        'edfi_change_version': pull_xcom(self.edfi_change_version_operator.task_id),
                    },

                    provide_context=True,
                    trigger_rule='all_success',
                    dag=self.dag
                )

            else:
                get_change_version_snowflake = None
                update_change_version_snowflake = None
                min_change_version = None
                max_change_version = None


            ### EDFI TO S3
            s3_destination_key = os.path.join(
                self.tenant_code, str(self.api_year), "{{ ds_nodash }}", "{{ ts_nodash }}",
                f'{display_resource}.json'
            )

            # For a multiyear ODS, we need to specify school year as an additional query parameter.
            # (This is an exception-case; we push all tenants to build year-specific ODSes when possible.)
            edfi_query_params = {}
            if self.multiyear:
                edfi_query_params['schoolYear'] = self.api_year

            pull_edfi_to_s3 = EdFiToS3Operator(
                task_id= f"pull_{display_resource}",

                edfi_conn_id    = self.edfi_conn_id,
                page_size       = page_size,
                resource        = resource,
                api_namespace   = namespace,
                api_get_deletes = deletes,
                api_retries     = max_retries,
                
                query_parameters= edfi_query_params,
                min_change_version=min_change_version,
                max_change_version=max_change_version,
                change_version_step_size=change_version_step_size,

                pool      = self.pool,
                tmp_dir   = self.tmp_dir,
                s3_conn_id= self.s3_conn_id,
                s3_destination_key= s3_destination_key,

                trigger_rule='all_success',
                dag=self.dag
            )


            ### COPY FROM S3 TO SNOWFLAKE
            copy_s3_to_snowflake = S3ToSnowflakeOperator(
                task_id=f"copy_into_snowflake_{display_resource}",

                edfi_conn_id=self.edfi_conn_id,
                snowflake_conn_id=self.snowflake_conn_id,

                tenant_code=self.tenant_code,
                api_year=self.api_year,
                resource=snake_resource,

                table_name=table or snake_resource,  # Use the provided table name, or default to resource.

                s3_destination_key=pull_xcom(pull_edfi_to_s3.task_id),

                full_refresh=self.full_refresh,

                trigger_rule='all_success',
                dag=self.dag
            )


            ### ORDER OPERATORS
            # If change versions are present, we can use them to only ingest any updates since the last pull.
            if get_change_version_snowflake and update_change_version_snowflake:
                get_change_version_snowflake >> pull_edfi_to_s3 >> copy_s3_to_snowflake >> update_change_version_snowflake

            # A branch consists of a pull from Ed-Fi to S3 and a copy (and optional reset) from S3 to the data lake.
            else:
                pull_edfi_to_s3 >> copy_s3_to_snowflake

            return resource_task_group
