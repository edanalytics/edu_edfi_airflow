import os
from functools import partial
from typing import Optional

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from ea_airflow_util import build_variable_update_operator
from ea_airflow_util import slack_callbacks
from edfi_api_client import camel_to_snake

from edu_edfi_airflow.dags.callables import change_version
from edu_edfi_airflow.dags.dag_util import airflow_util
from edu_edfi_airflow.providers.edfi.transfers.edfi_to_s3 import EdFiToS3Operator
from edu_edfi_airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator


class EdFiResourceDAG:
    """

    """
    newest_edfi_cv_task_id = "get_latest_edfi_change_version"  # Original name for historic run compatibility
    previous_snowflake_cv_task_id = "get_previous_change_versions_from_snowflake"

    s3_to_snowflake_task_id_prefix = "copy_into_snowflake_"  # This prefix is passed to `update_change_versions`.

    params_dict = {
        "full_refresh": Param(False, type="boolean"),
        "endpoints": Param([], type="array"),
    }

    def __init__(self,
        *,
        tenant_code: str,
        api_year   : int,

        edfi_conn_id     : str,
        s3_conn_id       : str,
        snowflake_conn_id: str,

        pool     : str,
        tmp_dir  : str,

        multiyear: bool = False,

        use_change_version: bool = True,
        change_version_table: str = '_meta_change_versions',

        slack_conn_id: str = None,
        dbt_incrementer_var: str = None,

        **kwargs
    ) -> None:
        self.tenant_code = tenant_code
        self.api_year = api_year
        self.multiyear = multiyear

        self.edfi_conn_id = edfi_conn_id
        self.s3_conn_id = s3_conn_id
        self.snowflake_conn_id = snowflake_conn_id
        self.slack_conn_id = slack_conn_id

        self.pool = pool
        self.tmp_dir = tmp_dir

        self.use_change_version = use_change_version
        self.change_version_table = change_version_table

        # Initialize the DAG scaffolding for TaskGroup declaration.
        self.dag = self.initialize_dag(**kwargs)

        # Retrieve current and previous change versions to define an ingestion window.
        if self.use_change_version:
            self.cv_task_group      = self.build_change_version_task_group()
            self.cv_update_operator = self.build_change_version_update_operator()
        else:
            self.cv_task_group = None
            self.cv_update_operator = None

        # Build an operator to increment the DBT var at the end of the run.
        if dbt_incrementer_var:
            self.dbt_var_increment_operator = build_variable_update_operator(
                dbt_incrementer_var, lambda x: int(x) + 1,
                task_id='increment_dbt_variable', trigger_rule='all_done', dag=self.dag
            )
        else:
            self.dbt_var_increment_operator = None

        # Create nested task-groups for cleaner webserver UI
        # (Make these lazy to only show populated TaskGroups in the UI.)
        self.resources_task_group = None
        self.resource_deletes_task_group = None
        self.descriptors_task_group = None


    def add_resource(self,
        resource: str,
        namespace: str = 'ed-fi',
        **kwargs
    ):
        if not self.resources_task_group:  # Initialize the task group if still undefined.
            self.resources_task_group = TaskGroup(
                group_id="Ed-Fi Resources",
                prefix_group_id=False,
                parent_group=None,
                dag=self.dag
            )

        self.build_edfi_to_snowflake_task_group(
            resource, namespace,
            parent_group=self.resources_task_group,
            **kwargs
        )

    def add_resource_deletes(self,
        resource: str,
        namespace: str = 'ed-fi',
        **kwargs
    ):
        if not self.resource_deletes_task_group:  # Initialize the task group if still undefined.
            self.resource_deletes_task_group = TaskGroup(
                group_id="Ed-Fi Resource Deletes",
                prefix_group_id=False,
                parent_group=None,
                dag=self.dag
            )

        self.build_edfi_to_snowflake_task_group(
            resource, namespace, deletes=True, table="_deletes",
            parent_group=self.resource_deletes_task_group,
            **kwargs
        )

    def add_descriptor(self,
        resource: str,
        namespace: str = 'ed-fi',
        **kwargs
    ):
        if not self.descriptors_task_group:  # Initialize the task group if still undefined.
            self.descriptors_task_group = TaskGroup(
                group_id="Ed-Fi Descriptors",
                prefix_group_id=False,
                parent_group=None,
                dag=self.dag
            )

        self.build_edfi_to_snowflake_task_group(
            resource, namespace, table="_descriptors",
            parent_group=self.descriptors_task_group,
            **kwargs
        )

    def chain_task_groups_into_dag(self):
        """
        Chain the optional endpoint task groups with the change-version operator and DBT incrementer if defined.
        Ideally, we'd use `airflow.util.helpers.chain()`, but Airflow2.6 logs dependency warnings when chaining already-included tasks.

        :return:
        """
        for task_group in (self.resources_task_group, self.resource_deletes_task_group, self.descriptors_task_group):

            # Ignore undefined task groups
            if not task_group:
                continue

            if self.use_change_version:
                self.cv_task_group >> task_group >> self.cv_update_operator
            elif self.dbt_var_increment_operator:
                task_group >> self.dbt_var_increment_operator

        # Be extremely intentional with connections to prevent dependency warnings.
        if self.use_change_version and self.dbt_var_increment_operator:
            self.cv_update_operator >> self.dbt_var_increment_operator


    ### Internal methods that should probably not be called directly.
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
            params=self.params_dict,
            render_template_as_native_obj=True,
            max_active_runs=1,
            sla_miss_callback=slack_sla_miss_callback,
        )


    def build_change_version_task_group(self) -> TaskGroup:
        """

        :return:
        """
        with TaskGroup(
            group_id="Ed-Fi3 Change Version Window",
            prefix_group_id=False,
            parent_group=None,
            dag=self.dag
        ) as cv_task_group:

            # Pull the newest change version recorded in Ed-Fi.
            get_newest_edfi_cv = PythonOperator(
                task_id=self.newest_edfi_cv_task_id,  # Class attribute for easier XCom retrieval
                python_callable=change_version.get_newest_edfi_change_version,
                op_kwargs={
                    'edfi_conn_id': self.edfi_conn_id,
                },
                dag=self.dag
            )

            # Reset the Snowflake change version table (if a full-refresh).
            reset_snowflake_cvs = PythonOperator(
                task_id="reset_previous_change_versions_in_snowflake",
                python_callable=change_version.reset_change_versions,
                op_kwargs={
                    'tenant_code': self.tenant_code,
                    'api_year': self.api_year,
                    'snowflake_conn_id': self.snowflake_conn_id,
                    'change_version_table': self.change_version_table,
                },
                trigger_rule='all_success',
                dag=self.dag
            )

            # Retrieve the latest active pulls from the Snowflake change version table.
            get_previous_snowflake_cvs = PythonOperator(
                task_id=self.previous_snowflake_cv_task_id,  # Class attribute for easier XCom retrieval
                python_callable=change_version.get_previous_change_versions,
                op_kwargs={
                    'tenant_code': self.tenant_code,
                    'api_year': self.api_year,
                    'snowflake_conn_id': self.snowflake_conn_id,
                    'change_version_table': self.change_version_table,
                },
                trigger_rule='all_done',  # Run regardless of whether the CV table was reset.
                dag=self.dag
            )

            get_newest_edfi_cv >> get_previous_snowflake_cvs
            get_newest_edfi_cv >> reset_snowflake_cvs >> get_previous_snowflake_cvs

        return cv_task_group


    def build_change_version_update_operator(self) -> PythonOperator:
        """

        :return:
        """
        ### UPDATE CHANGE VERSION TABLE ON SNOWFLAKE
        return PythonOperator(
            task_id=f"update_change_versions_in_snowflake",
            python_callable=change_version.update_change_versions,

            op_kwargs={
                'tenant_code': self.tenant_code,
                'api_year': self.api_year,

                'snowflake_conn_id': self.snowflake_conn_id,
                'change_version_table': self.change_version_table,

                'edfi_change_version': airflow_util.xcom_pull_template(self.newest_edfi_cv_task_id),

                'task_id_prefix': self.s3_to_snowflake_task_id_prefix,
            },

            provide_context=True,
            trigger_rule='all_done',
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
        change_version_step_size: int = 50000,

        parent_group: Optional[TaskGroup] = None,
        **kwargs
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
        :param change_version_step_size:
        :param parent_group:
        :return:
        """
        # Snowflake tables and Airflow tasks use snake_cased resources for readability.
        # Apply the deletes suffix for logging deletes across the Airflow DAG.
        snake_resource = camel_to_snake(resource)
        display_resource = airflow_util.build_display_name(snake_resource, deletes)

        # Wrap the branch in a task group
        with TaskGroup(
            group_id=display_resource,
            prefix_group_id=False,
            parent_group=parent_group,
            dag=self.dag
        ) as resource_task_group:

            ### EDFI3 CHANGE_VERSION LOGIC
            if self.use_change_version:
                max_change_version = airflow_util.xcom_pull_template(self.newest_edfi_cv_task_id)
                min_change_version = airflow_util.xcom_pull_template(
                    self.previous_snowflake_cv_task_id,
                    key=display_resource
                )
            else:
                max_change_version = None
                min_change_version = None


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
                task_id=f"{self.s3_to_snowflake_task_id_prefix}{display_resource}",

                edfi_conn_id=self.edfi_conn_id,
                snowflake_conn_id=self.snowflake_conn_id,

                tenant_code=self.tenant_code,
                api_year=self.api_year,
                resource=snake_resource,

                table_name=table or snake_resource,  # Use the provided table name, or default to resource.

                s3_destination_key=airflow_util.xcom_pull_template(pull_edfi_to_s3.task_id),

                trigger_rule='all_success',
                dag=self.dag
            )

            pull_edfi_to_s3 >> copy_s3_to_snowflake

        return resource_task_group
