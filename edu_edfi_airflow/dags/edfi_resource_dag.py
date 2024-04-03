import os
from functools import partial
from typing import Dict, List, Optional, Tuple

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from ea_airflow_util import slack_callbacks, update_variable
from edfi_api_client import camel_to_snake

from edu_edfi_airflow.dags.callables import change_version
from edu_edfi_airflow.dags.dag_util import airflow_util
from edu_edfi_airflow.providers.edfi.transfers.edfi_to_s3 import EdFiToS3Operator, BulkEdFiToS3Operator
from edu_edfi_airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator, BulkS3ToSnowflakeOperator


class EdFiResourceDAG:
    """

    """
    newest_edfi_cv_task_id = "get_latest_edfi_change_version"  # Original name for historic run compatibility
    previous_snowflake_cv_task_id = "get_previous_change_versions_from_snowflake"

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

        resource_configs: Optional[List[dict]] = None,
        descriptor_configs: Optional[List[dict]] = None,

        change_version_table: str = '_meta_change_versions',
        deletes_table: str = '_deletes',
        key_changes_table: str = '_key_changes',
        descriptors_table: str = '_descriptors',

        slack_conn_id: str = None,
        dbt_incrementer_var: str = None,

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
        self.multiyear = multiyear
        self.use_change_version = use_change_version

        self.change_version_table = change_version_table
        self.deletes_table = deletes_table
        self.key_changes_table = key_changes_table
        self.descriptors_table = descriptors_table

        self.dbt_incrementer_var = dbt_incrementer_var


        ### Parse endpoint configs into dictionaries if passed.
        if resource_configs:
            if isinstance(resource_configs, dict):
                self.resource_configs = resource_configs
            else:  # A list of resources has been passed without run-metadata
                self.resource_configs = {endpoint: {"enabled": True, "namespace": ns, 'fetch_deletes': True} for endpoint, ns in resource_configs}

            self.deletes_to_ingest = set([resource for resource, config in self.resource_configs.items() if config.get('fetch_deletes')])
            self.key_changes_to_ingest = set([resource for resource, config in self.resource_configs.items() if config.get('fetch_deletes')])

        else:  # Otherwise, add endpoints manually (backwards-compatible with current DAG init code).
            self.resource_configs = {}
            self.deletes_to_ingest = set()
            self.key_changes_to_ingest = set()

        if descriptor_configs:
            if isinstance(resource_configs, dict):
                self.descriptor_configs = descriptor_configs
            else:  # A list of descriptors has been passed without run-metadata
                self.descriptor_configs = {endpoint: {"enabled": True, "namespace": ns, 'fetch_deletes': False} for endpoint, ns in descriptor_configs}

        else:  # Otherwise, add endpoints manually (backwards-compatible with current DAG init code).
            self.descriptor_configs = {}

        # Populate DAG params with defined resources and descriptors; default to empty-list (i.e., run all).
        enabled_endpoints = [
            camel_to_snake(endpoint)
            for endpoint, config in {**self.resource_configs, **self.descriptor_configs}
            if config.get('enabled')
        ]

        self.params_dict = {
            "full_refresh": Param(
                default=False,
                type="boolean",
                description="If true, deletes endpoint data in Snowflake before ingestion"
            ),
            "endpoints": Param(
                default=sorted(enabled_endpoints),
                type="array",
                description="Newline-separated list of specific endpoints to ingest (case-agnostic)\n(Bug: even if unused, enter a newline)"
            ),
        }

        ### Initialize the DAG scaffolding for TaskGroup declaration.
        self.dag = self.initialize_dag(params=self.params_dict, **kwargs)

        # Retrieve current and previous change versions to define an ingestion window.
        if self.use_change_version:
            self.cv_task_group      = self.build_change_version_task_group()
            self.cv_update_operator = self.build_change_version_update_operator()
        else:
            self.cv_task_group = None
            self.cv_update_operator = None

        # Build an operator to increment the DBT var at the end of the run.
        if self.dbt_incrementer_var:
            self.dbt_var_increment_operator = self.build_dbt_var_increment_operator()
        else:
            self.dbt_var_increment_operator = None

        ### Create nested task-groups for cleaner webserver UI
        # (Make these lazy to only show populated TaskGroups in the UI.)
        self.resources_task_group = None
        self.descriptors_task_group = None
        self.resource_deletes_task_group = None
        self.resource_key_changes_task_group = None


    # Original methods to manually build task-groups (deprecated in favor of `resource_configs` and `descriptor_configs`).
    def add_resource(self, resource: str, **kwargs):
        self.resource_configs[resource] = kwargs

    def add_descriptor(self, resource: str, **kwargs):
        self.descriptor_configs[resource] = kwargs

    def add_resource_deletes(self, resource: str, **kwargs):
        self.deletes_to_ingest.add(resource)
        self.key_changes_to_ingest.add(resource)

    def chain_task_groups_into_dag(self):
        """
        Chain the optional endpoint task groups with the change-version operator and DBT incrementer if defined.

        Originally, we chained the empty task groups at init, but tasks are only registered if added to the group before downstream dependencies.
        See `https://github.com/apache/airflow/issues/16764` for more information.

        Ideally, we'd use `airflow.util.helpers.chain()`, but Airflow2.6 logs dependency warnings when chaining already-included tasks.
        See `https://github.com/apache/airflow/discussions/20693` for more information.

        :return:
        """
        ### Initialize resource and descriptor task groups if configs are defined.
        # Resources
        if self.resource_configs:
            self.resources_task_group = TaskGroup(
                group_id="Ed-Fi Resources",
                prefix_group_id=False,
                parent_group=None,
                dag=self.dag
            )

            for resource, configs in self.resource_configs.items():
                self.build_edfi_to_snowflake_task_group(resource, parent_group=self.resources_task_group)

        # Descriptors
        self.descriptors_task_group = self.build_bulk_edfi_to_snowflake_task_group(
            group_id="Ed-Fi Descriptors",
            endpoints=list(self.descriptor_configs.keys()),
            configs=self.descriptor_configs,
            table=self.descriptors_table
        )

        # Resource Deletes
        self.resource_deletes_task_group = self.build_bulk_edfi_to_snowflake_task_group(
            group_id="Ed-Fi Resource Deletes",
            endpoints=list(self.deletes_to_ingest),
            configs=self.resource_configs,
            table=self.deletes_table,
            get_deletes=True
        )

        # Resource Key-Changes
        self.resource_key_changes_task_group = self.build_bulk_edfi_to_snowflake_task_group(
            group_id="Ed-Fi Resource Key Changes",
            endpoints=list(self.key_changes_to_ingest),
            configs=self.resource_configs,
            table=self.key_changes_table,
            get_key_changes=True
        )

        ### Chain task groups into the DAG between CV operators and Airflow state operators.
        # Create a dummy sentinel to display the success of the endpoint taskgroups.
        dag_state_sentinel = DummyOperator(
            task_id='dag_state_sentinel',
            trigger_rule='none_failed',
            dag=self.dag
        )

        task_groups_to_chain = [
            self.resources_task_group,
            self.descriptors_task_group,
            self.resource_deletes_task_group,
            self.resource_key_changes_task_group,
        ]

        for task_group in task_groups_to_chain:

            if not task_group:  # Ignore undefined task groups
                continue

            if self.use_change_version:
                self.cv_task_group >> task_group >> self.cv_update_operator

            if self.dbt_var_increment_operator:
                task_group >> self.dbt_var_increment_operator

            # Always apply the state sentinel.
            task_group >> dag_state_sentinel

        # The sentinel also holds the state of the CV and DBT var operators.
        if self.use_change_version:
            self.cv_update_operator >> dag_state_sentinel

        if self.dbt_var_increment_operator:
            self.dbt_var_increment_operator >> dag_state_sentinel


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
            render_template_as_native_obj=True,
            max_active_runs=1,
            sla_miss_callback=slack_sla_miss_callback,
            **airflow_util.subset_kwargs_to_class(DAG, kwargs)  # Remove kwargs not expected in DAG.
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
                trigger_rule='none_failed',  # Run regardless of whether the CV table was reset.
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
            },

            provide_context=True,
            trigger_rule='all_done',
            dag=self.dag
        )


    def build_dbt_var_increment_operator(self):
        """

        :return:
        """
        def short_circuit_update_variable(**kwargs):
            """
            Helper to build short-circuit logic into the update_variable callable if no new data was ingested.
            :return:
            """
            from airflow.exceptions import AirflowSkipException

            for task_id in kwargs['task'].upstream_task_ids:
                if kwargs['ti'].xcom_pull(task_id):
                    return update_variable(**kwargs)
            else:
                raise AirflowSkipException(
                    "There is no new data to process using DBT. All upstream tasks skipped or failed."
                )

        return PythonOperator(
            task_id='increment_dbt_variable',
            python_callable=short_circuit_update_variable,
            op_kwargs={
                'var': self.dbt_incrementer_var,
                'value': lambda x: int(x) + 1,
            },
            trigger_rule='all_done',
            dag=self.dag
        )


    def build_edfi_to_snowflake_task_group(self,
        resource : str,
        namespace: str = 'ed-fi',

        *,
        get_deletes: bool = False,
        get_key_changes: bool = False,

        page_size: int = 500,
        max_retries: int = 5,
        change_version_step_size: int = 50000,

        parent_group: Optional[TaskGroup] = None,
        **kwargs
    ) -> TaskGroup:
        """
        Pull a single Ed-Fi resource and write to S3.
        Copy the data to its own table in Snowflake.

        Note: Descriptors, deletes, and keyChanges use the BulkOperator instead.

        :param resource :
        :param namespace:
        :param get_deletes:
        :param get_key_changes:
        :param page_size:
        :param max_retries:
        :param change_version_step_size:
        :param parent_group:
        :return:
        """
        # Snowflake tables and Airflow tasks use snake_cased resources for readability.
        snake_resource = camel_to_snake(resource)
        display_resource = airflow_util.build_display_name(resource, is_deletes=get_deletes, is_key_changes=get_key_changes)

        # Wrap the branch in a task group
        with TaskGroup(
            group_id=display_resource,
            prefix_group_id=False,
            parent_group=parent_group,
            dag=self.dag
        ) as resource_task_group:

            ### EDFI TO S3
            s3_destination_key = os.path.join(
                self.tenant_code, str(self.api_year), "{{ ds_nodash }}", "{{ ts_nodash }}",
                f"{display_resource}.jsonl"
            )

            # For a multiyear ODS, we need to specify school year as an additional query parameter.
            # (This is an exception-case; we push all tenants to build year-specific ODSes when possible.)
            edfi_query_params = {}
            if self.multiyear:
                edfi_query_params['schoolYear'] = self.api_year

            pull_edfi_to_s3 = EdFiToS3Operator(
                task_id=f"pull_{display_resource}",

                edfi_conn_id=self.edfi_conn_id,
                resource=resource,
                namespace=namespace,

                tmp_dir= self.tmp_dir,
                s3_conn_id= self.s3_conn_id,
                s3_destination_key= s3_destination_key,

                get_deletes=get_deletes,
                get_key_changes=get_key_changes,

                page_size=page_size,
                num_retries=max_retries,
                query_parameters=edfi_query_params,
                min_change_version=airflow_util.xcom_pull_template(self.previous_snowflake_cv_task_id, key=display_resource),
                max_change_version=airflow_util.xcom_pull_template(self.newest_edfi_cv_task_id),
                change_version_step_size=change_version_step_size,

                pool=self.pool,
                trigger_rule='all_success',
                dag=self.dag
            )

            ### COPY FROM S3 TO SNOWFLAKE
            copy_s3_to_snowflake = S3ToSnowflakeOperator(
                task_id=f"copy_into_snowflake_{display_resource}",

                tenant_code=self.tenant_code,
                api_year=self.api_year,
                resource=snake_resource,
                table_name=snake_resource,

                edfi_conn_id=self.edfi_conn_id,
                snowflake_conn_id=self.snowflake_conn_id,

                s3_destination_key=airflow_util.xcom_pull_template(pull_edfi_to_s3.task_id),
                xcom_return=[(snake_resource, get_deletes, get_key_changes)],  # Force return structure for downstream XCom.

                trigger_rule='all_success',
                dag=self.dag
            )

            pull_edfi_to_s3 >> copy_s3_to_snowflake

        return resource_task_group

    def build_bulk_edfi_to_snowflake_task_group(self,
        endpoints: List[str],
        configs: Dict[str, dict],
        group_id: str,

        *,
        table: Optional[str] = None,
        get_deletes: bool = False,
        get_key_changes: bool = False,

        max_retries: int = 5,
        change_version_step_size: int = 50000,
        **kwargs
    ):
        if not endpoints:
            return None

        # Wrap the branch in a task group
        with TaskGroup(
            group_id=group_id,
            prefix_group_id=False,
            parent_group=None,
            dag=self.dag
        ) as bulk_task_group:

            cleaned_group_id = group_id.replace(' ', "_").lower()

            ### EDFI TO S3
            s3_destination_directory = os.path.join(
                self.tenant_code, str(self.api_year), "{{ ds_nodash }}", "{{ ts_nodash }}"
            )

            s3_destination_filename_lambda = lambda resource: "{}.jsonl".format(
                airflow_util.build_display_name(resource, is_deletes=get_deletes, is_key_changes=get_key_changes)
            )

            # For a multiyear ODS, we need to specify school year as an additional query parameter.
            # (This is an exception-case; we push all tenants to build year-specific ODSes when possible.)
            edfi_query_params = {}
            if self.multiyear:
                edfi_query_params['schoolYear'] = self.api_year

            # Build a lambda to retrieve the min-change-version from XComs
            # This allows no knowledge of resource or path in the operator.
            min_change_version_lambda = lambda context, resource: context['ti'].xcom_pull(
                key=airflow_util.build_display_name(resource, is_deletes=get_deletes, is_key_changes=get_key_changes),
                task_ids=self.previous_snowflake_cv_task_id
            )

            pull_edfi_to_s3 = BulkEdFiToS3Operator(
                task_id=f"{cleaned_group_id}__pull_bulk_endpoints",

                edfi_conn_id=self.edfi_conn_id,
                resource=endpoints,
                namespace=[configs[endpoint].get('namespace', 'ed-fi') for endpoint in endpoints],
                page_size=[configs[endpoint].get('page_size', 500) for endpoint in endpoints],

                get_deletes=get_deletes,
                get_key_changes=get_key_changes,

                tmp_dir=self.tmp_dir,
                s3_conn_id=self.s3_conn_id,
                s3_destination_dir=s3_destination_directory,
                s3_destination_filename=s3_destination_filename_lambda,

                num_retries=max_retries,
                query_parameters=edfi_query_params,
                min_change_version = min_change_version_lambda,
                max_change_version=airflow_util.xcom_pull_template(self.newest_edfi_cv_task_id),
                change_version_step_size=change_version_step_size,

                pool=self.pool,
                trigger_rule='all_success',
                dag=self.dag
            )

            ### COPY FROM S3 TO SNOWFLAKE
            copy_s3_to_snowflake = BulkS3ToSnowflakeOperator(
                task_id=f"{cleaned_group_id}__copy_into_snowflake_bulk_endpoints",

                tenant_code=self.tenant_code,
                api_year=self.api_year,
                resource=[camel_to_snake(resource) for resource in endpoints],
                table_name=table,

                edfi_conn_id=self.edfi_conn_id,
                snowflake_conn_id=self.snowflake_conn_id,

                s3_destination_dir=s3_destination_directory,
                s3_destination_filename=s3_destination_filename_lambda,

                xcom_return=lambda resource: (camel_to_snake(resource), get_deletes, get_key_changes),

                trigger_rule='all_success',
                dag=self.dag
            )

            pull_edfi_to_s3 >> copy_s3_to_snowflake

        return bulk_task_group