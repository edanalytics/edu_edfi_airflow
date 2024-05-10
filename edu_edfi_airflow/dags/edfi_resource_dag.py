import os
from typing import Dict, List, Optional, Tuple, Union

from airflow.exceptions import AirflowFailException
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from ea_airflow_util import EACustomDAG
from ea_airflow_util import update_variable
from edfi_api_client import camel_to_snake

from edu_edfi_airflow.callables import airflow_util, change_version
from edu_edfi_airflow.providers.edfi.transfers.edfi_to_s3 import EdFiToS3Operator, BulkEdFiToS3Operator
from edu_edfi_airflow.providers.snowflake.transfers.s3_to_snowflake import BulkS3ToSnowflakeOperator


class EdFiResourceDAG:
    """
    If use_change_version is True, initialize a change group that retrieves the latest Ed-Fi change version.
    If full_refresh is triggered in DAG configs, reset change versions for the resources being processed in Snowflake.

    DAG Structure:
        (Ed-Fi3 Change Version Window) >> [Ed-Fi Resources/Descriptors (Deletes/KeyChanges)] >> (increment_dbt_variable) >> dag_state_sentinel
    
    "Ed-Fi3 Change Version Window" TaskGroup:
        get_latest_edfi_change_version >> reset_previous_change_versions_in_snowflake

    "Ed-Fi Resources/Descriptors (Deletes/KeyChanges)" TaskGroup:
        (get_cv_operator) >> [Ed-Fi Endpoint Task] >> copy_all_endpoints_into_snowflake >> (update_change_versions_in_snowflake)

    There are three types of Ed-Fi Endpoint Tasks. All take the same inputs and return the same outputs (i.e., polymorphism).
        "Default" TaskGroup
        - Create one task per endpoint.
        "Dynamic" TaskGroup
        - Dynamically task-map all endpoints. This function presumes that only endpoints with deltas to ingest are passed as input.
        "Bulk" TaskGroup
        - Loop over each endpoint in a single task.

    All task groups receive a list of (endpoint, last_change_version) tuples as input.
    All that successfully retrieve records are passed onward as a (endpoint, filename) tuples to the S3ToSnowflake and UpdateSnowflakeCV operators.
    """
    DEFAULT_NAMESPACE: str = 'ed-fi'
    DEFAULT_PAGE_SIZE: int = 500
    DEFAULT_CHANGE_VERSION_STEP_SIZE: int = 50000
    DEFAULT_MAX_RETRIES: int = 5

    newest_edfi_cv_task_id = "get_latest_edfi_change_version"  # Original name for historic run compatibility


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
        schedule_interval_full_refresh: Optional[str] = None,

        use_change_version: bool = True,
        get_key_changes: bool = False,
        run_type: str = "default",
        resource_configs: Optional[List[dict]] = None,
        descriptor_configs: Optional[List[dict]] = None,

        change_version_table: str = '_meta_change_versions',
        deletes_table: str = '_deletes',
        key_changes_table: str = '_key_changes',
        descriptors_table: str = '_descriptors',

        dbt_incrementer_var: Optional[str] = None,

        **kwargs
    ) -> None:
        self.run_type = run_type
        self.use_change_version = use_change_version
        self.get_key_changes = get_key_changes

        self.tenant_code = tenant_code
        self.api_year = api_year

        self.edfi_conn_id = edfi_conn_id
        self.s3_conn_id = s3_conn_id
        self.snowflake_conn_id = snowflake_conn_id

        self.pool = pool
        self.tmp_dir = tmp_dir
        self.multiyear = multiyear
        self.schedule_interval_full_refresh = schedule_interval_full_refresh  # Force full-refresh on a scheduled cadence

        self.change_version_table = change_version_table
        self.deletes_table = deletes_table
        self.key_changes_table = key_changes_table
        self.descriptors_table = descriptors_table

        self.dbt_incrementer_var = dbt_incrementer_var

        ### For a multiyear ODS, we need to specify school year as an additional query parameter.
        # (This is an exception-case; we push all tenants to build year-specific ODSes when possible.)
        # Set defaults before parsing configs.
        self.default_params = {}
        if self.multiyear:
            self.default_params['schoolYear'] = self.api_year
        
        ### Parse optional config objects (improved performance over adding resources manually)
        resource_configs = self.parse_endpoint_configs(resource_configs)
        descriptor_configs = self.parse_endpoint_configs(descriptor_configs)
        self.endpoint_configs = {**resource_configs, **descriptor_configs}
        
        # Build lists of each enabled endpoint type. (only collect deletes and key-changes for resources).
        self.resources = set([resource for resource, config in resource_configs.items() if config.get('enabled')])
        self.descriptors = set([resource for resource, config in descriptor_configs.items() if config.get('enabled')])
        self.deletes_to_ingest = set([resource for resource in self.resources if self.get_endpoint_configs(resource, 'fetch_deletes')])
        self.key_changes_to_ingest = set([resource for resource in self.resources if self.get_endpoint_configs(resource, 'fetch_deletes')])

        # Populate DAG params with optionally-defined resources and descriptors; default to empty-list (i.e., run all).
        dag_params = {
            "full_refresh": Param(
                default=False,
                type="boolean",
                description="If true, deletes endpoint data in Snowflake before ingestion"
            ),
            "endpoints": Param(
                default=sorted(list(self.resources.union(self.descriptors))),
                type="array",
                description="Newline-separated list of specific endpoints to ingest (case-agnostic)\n(Bug: even if unused, enter a newline)"
            ),
        }

        self.dag = EACustomDAG(params=dag_params, **kwargs)


    # Helper methods for putting and getting DAG endpointconfigs.
    @staticmethod
    def parse_endpoint_configs(configs: Optional[Union[dict, list]] = None) -> Dict[str, dict]:
        """
        Parse endpoint configs into dictionaries if passed.
        Force all endpoints to snake-case for consistency.
        """
        if not configs:
            return {}
        
        elif isinstance(configs, dict):
            return {camel_to_snake(endpoint): config for endpoint, config in configs.items()}
        
        # A list of resources has been passed without run-metadata
        elif isinstance(configs, list):
            return {camel_to_snake(endpoint): {"enabled": True, "namespace": ns, 'fetch_deletes': True} for endpoint, ns in configs}
        
        else:
            raise ValueError(
                f"Passed configs are an unknown datatype! Expected Dict[endpoint: metadata] or List[(namespace, endpoint)] but received {type(configs)}"
            )
        
    def get_endpoint_configs(self,
        endpoint: Optional[str] = None,
        key: Optional[str] = None
    ) -> Union[dict, object]:
        """
        Helper to retrieve endpoint metadata from globally-defined configs.
        The keys to this dictionary align with arguments passed in EdFiToS3Operator.

        Pass a key to get a specific value from the dictionary.
        Pass no endpoint to get the default config values.
        """
        configs = {
            'namespace': self.endpoint_configs.get(endpoint, {}).get('namespace', self.DEFAULT_NAMESPACE),
            'page_size': self.endpoint_configs.get(endpoint, {}).get('page_size', self.DEFAULT_PAGE_SIZE),
            'num_retries': self.endpoint_configs.get(endpoint, {}).get('num_retries', self.DEFAULT_MAX_RETRIES),
            'change_version_step_size': self.endpoint_configs.get(endpoint, {}).get('change_version_step_size', self.DEFAULT_CHANGE_VERSION_STEP_SIZE),
            'query_parameters': {**self.endpoint_configs.get(endpoint, {}).get('params', {}), **self.default_params},
        }

        if key:
            return configs.get(key)
        else:
            return configs


    # Original methods to manually build task-groups (deprecated in favor of `resource_configs` and `descriptor_configs`).
    def add_resource(self, resource: str, **kwargs):
        snake_resource = camel_to_snake(resource)
        if kwargs.get('enabled'):
            self.resources.add(snake_resource)
        self.endpoint_configs[snake_resource] = kwargs

    def add_descriptor(self, resource: str, **kwargs):
        snake_resource = camel_to_snake(resource)
        if kwargs.get('enabled'):
            self.descriptors.add(snake_resource)
        self.endpoint_configs[snake_resource] = kwargs

    def add_resource_deletes(self, resource: str, **kwargs):
        snake_resource = camel_to_snake(resource)
        if kwargs.get('enabled'):
            self.deletes_to_ingest.add(snake_resource)
            self.key_changes_to_ingest.add(snake_resource)

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
        if self.run_type == 'default':
            task_group_callable = self.build_default_edfi_to_snowflake_task_group
        elif self.run_type == 'dynamic':
            task_group_callable = self.build_dynamic_edfi_to_snowflake_task_group
        elif self.run_type == 'bulk':
            task_group_callable = self.build_bulk_edfi_to_snowflake_task_group
        else:
            raise ValueError(f"Run type {self.run_type} is not one of the expected values: [default, dynamic, bulk].")

        # Set parent directory and create subfolders for each task group.
        s3_parent_directory = os.path.join(
            self.tenant_code, str(self.api_year), "{{ ds_nodash }}", "{{ ts_nodash }}"
        )

        # Resources
        resources_task_group: Optional[TaskGroup] = task_group_callable(
            group_id = "Ed-Fi_Resources",
            endpoints=list(self.resources),
            s3_destination_dir=os.path.join(s3_parent_directory, 'resources')
            # Tables are built dynamically from the names of the endpoints.
        )

        # Descriptors
        descriptors_task_group: Optional[TaskGroup] = task_group_callable(
            group_id="Ed-Fi_Descriptors",
            endpoints=list(self.descriptors),
            table=self.descriptors_table,
            s3_destination_dir=os.path.join(s3_parent_directory, 'descriptors')
        )

        # Resource Deletes
        resource_deletes_task_group: Optional[TaskGroup] = task_group_callable(
            group_id="Ed-Fi_Resource_Deletes",
            endpoints=list(self.deletes_to_ingest),
            table=self.deletes_table,
            s3_destination_dir=os.path.join(s3_parent_directory, 'resource_deletes'),
            get_deletes=True
        )

        # Resource Key-Changes (only applicable in Ed-Fi v6.x and up)
        if self.get_key_changes:
            resource_key_changes_task_group: Optional[TaskGroup] = task_group_callable(
                group_id="Ed-Fi Resource Key Changes",
                endpoints=list(self.key_changes_to_ingest),
                table=self.key_changes_table,
                s3_destination_dir=os.path.join(s3_parent_directory, 'resource_key_changes'),
                get_key_changes=True
            )
        else:
            resource_key_changes_task_group = None

        ### Chain Ed-Fi task groups into the DAG between CV operators and Airflow state operators.
        edfi_task_groups = [
            resources_task_group,
            descriptors_task_group,
            resource_deletes_task_group,
            resource_key_changes_task_group,
        ]

        # Retrieve current and previous change versions to define an ingestion window.
        if self.use_change_version:
            cv_task_group: TaskGroup = self.build_change_version_task_group()
        else:
            cv_task_group = None

        # Build an operator to increment the DBT var at the end of the run.
        if self.dbt_incrementer_var:
            dbt_var_increment_operator = PythonOperator(
                task_id='increment_dbt_variable',
                python_callable=update_variable,
                op_kwargs={
                    'var': self.dbt_incrementer_var,
                    'value': lambda x: int(x) + 1,
                },
                trigger_rule='one_success',
                dag=self.dag
            )
        else:
            dbt_var_increment_operator = None

        # Create a dummy sentinel to display the success of the endpoint taskgroups.
        dag_state_sentinel = PythonOperator(
            task_id='dag_state_sentinel',
            python_callable=airflow_util.fail_if_any_task_failed,
            trigger_rule='all_done',
            dag=self.dag
        )

        # Chain tasks and taskgroups into the DAG
        airflow_util.chain_tasks(cv_task_group, edfi_task_groups, dbt_var_increment_operator, dag_state_sentinel)


    ### Internal methods that should not be called directly.
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

            get_newest_edfi_cv >> reset_snowflake_cvs

        return cv_task_group

    def build_change_version_get_operator(self,
        task_id: str,
        endpoints: List[Tuple[str, str]],
        get_deletes: bool = False,
        get_key_changes: bool = False
    ) -> PythonOperator:
        """

        :return:
        """
        get_cv_operator = PythonOperator(
            task_id=task_id,
            python_callable=change_version.get_previous_change_versions_with_deltas,
            op_kwargs={
                'tenant_code': self.tenant_code,
                'api_year': self.api_year,
                'endpoints': endpoints,
                'snowflake_conn_id': self.snowflake_conn_id,
                'change_version_table': self.change_version_table,
                'get_deletes': get_deletes,
                'get_key_changes': get_key_changes,
                'edfi_conn_id': self.edfi_conn_id,
                'max_change_version': airflow_util.xcom_pull_template(self.newest_edfi_cv_task_id),
            },
            trigger_rule='none_failed',  # Run regardless of whether the CV table was reset.
            dag=self.dag
        )

        # One or more endpoints can fail total-get count. Create a second operator to track that failed status.
        # This should NOT be necessary, but we encountered a bug where a downstream "none_skipped" task skipped with "upstream_failed" status.
        def fail_if_xcom(xcom_value, **context):
            if xcom_value:
                raise AirflowFailException

        failed_sentinel = PythonOperator(
            task_id=f"{task_id}__failed_total_counts",
            python_callable=fail_if_xcom,
            op_args=[airflow_util.xcom_pull_template(get_cv_operator, key='failed_endpoints')],
            trigger_rule='all_done',
            dag=self.dag
        )
        get_cv_operator >> failed_sentinel

        return get_cv_operator

    def build_change_version_update_operator(self,
        task_id: str,
        endpoints: List[str],
        get_deletes: bool,
        get_key_changes: bool,
        **kwargs
    ) -> PythonOperator:
        """

        :return:
        """
        return PythonOperator(
            task_id=task_id, 
            python_callable=change_version.update_change_versions,
            op_kwargs={
                'tenant_code': self.tenant_code,
                'api_year': self.api_year,
                'snowflake_conn_id': self.snowflake_conn_id,
                'change_version_table': self.change_version_table,
                
                'edfi_change_version': airflow_util.xcom_pull_template(self.newest_edfi_cv_task_id),
                'endpoints': endpoints,
                'get_deletes': get_deletes,
                'get_key_changes': get_key_changes,
            },
            provide_context=True,
            dag=self.dag,
            **kwargs
        )


    # Polymorphic Ed-Fi TaskGroups
    @staticmethod
    def xcom_pull_template_map_idx(task_ids, idx: int):
        """
        Many XComs in this DAG are lists of tuples. This overloads xcom_pull_template to retrieve a list of items at a given index.
        """
        return airflow_util.xcom_pull_template(
            task_ids, suffix=f" | map(attribute={idx}) | list"
        )
    
    @staticmethod
    def xcom_pull_template_get_key(task_ids, key: str):
        """
        Many XComs in this DAG are lists of tuples. This converts the XCom to a dictionary and returns the value for a given key.
        """
        return airflow_util.xcom_pull_template(
            task_ids, prefix="dict(", suffix=f").get('{key}')"
        )

    def build_default_edfi_to_snowflake_task_group(self,
        endpoints: List[str],
        group_id: str,

        *,
        s3_destination_dir: str,
        table: Optional[str] = None,
        get_deletes: bool = False,
        get_key_changes: bool = False,
        **kwargs
    ) -> TaskGroup:
        """
        Build one EdFiToS3 task per endpoint
        Bulk copy the data to its respective table in Snowflake.

        :param endpoints:
        :param group_id:
        :param s3_destination_dir:
        :param table:
        :param get_deletes:
        :param get_key_changes:
        :return:
        """
        if not endpoints:
            return None
        
        with TaskGroup(
            group_id=group_id,
            prefix_group_id=True,
            parent_group=None,
            dag=self.dag
        ) as default_task_group:

            ### LATEST SNOWFLAKE CHANGE VERSIONS: Output Dict[endpoint, last_change_version]
            if self.use_change_version:
                get_cv_operator = self.build_change_version_get_operator(
                    task_id=f"get_last_change_versions_from_snowflake",
                    endpoints=[(self.get_endpoint_configs(endpoint, 'namespace'), endpoint) for endpoint in endpoints],
                    get_deletes=get_deletes,
                    get_key_changes=get_key_changes,
                )
                enabled_endpoints = self.xcom_pull_template_map_idx(get_cv_operator, 0)
            else:
                get_cv_operator = None
                enabled_endpoints = endpoints

            ### EDFI TO S3: Output Tuple[endpoint, filename] per successful task
            pull_operators_list = []

            for endpoint in endpoints:
                pull_edfi_to_s3 = EdFiToS3Operator(
                    task_id=endpoint,
                    edfi_conn_id=self.edfi_conn_id,
                    resource=endpoint,

                    tmp_dir=self.tmp_dir,
                    s3_conn_id=self.s3_conn_id,
                    s3_destination_dir=s3_destination_dir,
                    s3_destination_filename=f"{endpoint}.jsonl",
                    
                    get_deletes=get_deletes,
                    get_key_changes=get_key_changes,
                    min_change_version=self.xcom_pull_template_get_key(get_cv_operator, endpoint) if get_cv_operator else None,
                    max_change_version=airflow_util.xcom_pull_template(self.newest_edfi_cv_task_id),

                    # Optional config-specified run-attributes (overridden by those in configs)
                    **self.get_endpoint_configs(endpoint),

                    # Only run endpoints specified at DAG or delta-level.
                    enabled_endpoints=enabled_endpoints,

                    pool=self.pool,
                    trigger_rule='none_skipped',
                    dag=self.dag
                )

                pull_operators_list.append(pull_edfi_to_s3)

            ### COPY FROM S3 TO SNOWFLAKE
            copy_s3_to_snowflake = BulkS3ToSnowflakeOperator(
                task_id=f"copy_all_endpoints_into_snowflake",
                tenant_code=self.tenant_code,
                api_year=self.api_year,
                
                resource=self.xcom_pull_template_map_idx(pull_operators_list, 0),
                table_name=table or self.xcom_pull_template_map_idx(pull_operators_list, 0),
                edfi_conn_id=self.edfi_conn_id,
                snowflake_conn_id=self.snowflake_conn_id,
                s3_destination_key=self.xcom_pull_template_map_idx(pull_operators_list, 1),

                trigger_rule='all_done',
                dag=self.dag
            )

            ### UPDATE SNOWFLAKE CHANGE VERSIONS
            if self.use_change_version:
                update_cv_operator = self.build_change_version_update_operator(
                    task_id=f"update_change_versions_in_snowflake",
                    endpoints=self.xcom_pull_template_map_idx(pull_operators_list, 0),
                    get_deletes=get_deletes,
                    get_key_changes=get_key_changes,
                    trigger_rule='all_success'
                )
            else:
                update_cv_operator = None

            ### Chain tasks into final task-group
            airflow_util.chain_tasks(get_cv_operator, pull_operators_list, copy_s3_to_snowflake, update_cv_operator)

        return default_task_group


    def build_dynamic_edfi_to_snowflake_task_group(self,
        endpoints: List[str],
        group_id: str,

        *,
        s3_destination_dir: str,
        table: Optional[str] = None,
        get_deletes: bool = False,
        get_key_changes: bool = False,
        **kwargs
    ):
        """
        Build one EdFiToS3 task per endpoint
        Bulk copy the data to its respective table in Snowflake.

        :param endpoints:
        :param group_id:
        :param s3_destination_dir:
        :param table:
        :param get_deletes:
        :param get_key_changes:
        :return:
        """
        if not endpoints:
            return None
        
        with TaskGroup(
            group_id=group_id,
            prefix_group_id=True,
            parent_group=None,
            dag=self.dag
        ) as dynamic_task_group:

            ### LATEST SNOWFLAKE CHANGE VERSIONS: Output Dict[endpoint, last_change_version]
            # If change versions are enabled, dynamically expand the output of the CV operator task into the Ed-Fi partial.
            if self.use_change_version:
                get_cv_operator = self.build_change_version_get_operator(
                    task_id=f"get_last_change_versions_from_snowflake",
                    endpoints=[(self.get_endpoint_configs(endpoint, 'namespace'), endpoint) for endpoint in endpoints],
                    get_deletes=get_deletes,
                    get_key_changes=get_key_changes
                )
                enabled_endpoints = self.xcom_pull_template_map_idx(get_cv_operator, 0)
                kwargs_dicts = get_cv_operator.output.map(lambda endpoint__cv: {
                    'resource': endpoint__cv[0],
                    'min_change_version': endpoint__cv[1],
                    's3_destination_filename': f"{endpoint__cv[0]}.jsonl",
                    **self.get_endpoint_configs(endpoint__cv[0]),
                })
            
            # Otherwise, iterate all endpoints.
            else:
                get_cv_operator = None
                enabled_endpoints = endpoints
                kwargs_dicts = list(map(lambda endpoint: {
                    'resource': endpoint,
                    'min_change_version': None,
                    's3_destination_filename': f"{endpoint}.jsonl",
                    **self.get_endpoint_configs(endpoint),
                }, endpoints))


            ### EDFI TO S3: Output Tuple[endpoint, filename] per successful task
            pull_edfi_to_s3 = (EdFiToS3Operator
                .partial(
                    task_id=f"pull_dynamic_endpoints_to_s3",
                    edfi_conn_id=self.edfi_conn_id,

                    tmp_dir= self.tmp_dir,
                    s3_conn_id= self.s3_conn_id,
                    s3_destination_dir=s3_destination_dir,

                    get_deletes=get_deletes,
                    get_key_changes=get_key_changes,
                    max_change_version=airflow_util.xcom_pull_template(self.newest_edfi_cv_task_id),

                    # Only run endpoints specified at DAG or delta-level.
                    enabled_endpoints=enabled_endpoints,

                    sla=None,  # "SLAs are unsupported with mapped tasks."
                    pool=self.pool,
                    trigger_rule='none_skipped',
                    dag=self.dag
                )
                .expand_kwargs(kwargs_dicts)
            )

            ### COPY FROM S3 TO SNOWFLAKE
            copy_s3_to_snowflake = BulkS3ToSnowflakeOperator(
                task_id=f"copy_all_endpoints_into_snowflake",
                tenant_code=self.tenant_code,
                api_year=self.api_year,
                
                resource=self.xcom_pull_template_map_idx(pull_edfi_to_s3, 0),
                table_name=table or self.xcom_pull_template_map_idx(pull_edfi_to_s3, 0),
                edfi_conn_id=self.edfi_conn_id,
                snowflake_conn_id=self.snowflake_conn_id,
                s3_destination_key=self.xcom_pull_template_map_idx(pull_edfi_to_s3, 1),

                trigger_rule='all_done',
                dag=self.dag
            )

            ### UPDATE SNOWFLAKE CHANGE VERSIONS
            if self.use_change_version:
                update_cv_operator = self.build_change_version_update_operator(
                    task_id=f"update_change_versions_in_snowflake",
                    endpoints=self.xcom_pull_template_map_idx(pull_edfi_to_s3, 0),
                    get_deletes=get_deletes,
                    get_key_changes=get_key_changes,
                    trigger_rule='all_success'
                )
            else:
                update_cv_operator = None

            ### Chain tasks into final task-group
            airflow_util.chain_tasks(get_cv_operator, pull_edfi_to_s3, copy_s3_to_snowflake, update_cv_operator)

        return dynamic_task_group
    

    def build_bulk_edfi_to_snowflake_task_group(self,
        endpoints: List[str],
        group_id: str,

        *,
        s3_destination_dir: str,
        table: Optional[str] = None,
        get_deletes: bool = False,
        get_key_changes: bool = False,
        **kwargs
    ):
        """
        Build one EdFiToS3 task (with inner for-loop across endpoints).
        Bulk copy the data to its respective table in Snowflake.

        :param endpoints:
        :param group_id:
        :param s3_destination_dir:
        :param table:
        :param get_deletes:
        :param get_key_changes:
        :return:
        """
        if not endpoints:
            return None

        with TaskGroup(
            group_id=group_id,
            prefix_group_id=True,
            parent_group=None,
            dag=self.dag
        ) as bulk_task_group:

            ### LATEST SNOWFLAKE CHANGE VERSIONS: Output Dict[endpoint, last_change_version]
            # If change versions are enabled, dynamically expand the output of the CV operator task into the Ed-Fi bulk operator.
            if self.use_change_version:
                get_cv_operator = self.build_change_version_get_operator(
                    task_id=f"get_last_change_versions",
                    endpoints=[(self.get_endpoint_configs(endpoint, 'namespace'), endpoint) for endpoint in endpoints],
                    get_deletes=get_deletes,
                    get_key_changes=get_key_changes
                )
                min_change_versions = [
                    self.xcom_pull_template_get_key(get_cv_operator, endpoint)
                    for endpoint in endpoints
                ]
                enabled_endpoints = self.xcom_pull_template_map_idx(get_cv_operator, 0)
            
            # Otherwise, iterate all endpoints.
            else:
                get_cv_operator = None
                min_change_versions = [None] * len(endpoints)
                enabled_endpoints = endpoints

            ### EDFI TO S3: Output Dict[endpoint, filename] with all successful tasks
            # Build a dictionary of lists to pass into bulk operator.
            endpoint_config_lists = {
                key: [self.get_endpoint_configs(endpoint, key) for endpoint in endpoints]
                for key in self.get_endpoint_configs().keys()  # Retrieve default keys.
            }

            pull_edfi_to_s3 = BulkEdFiToS3Operator(
                task_id=f"pull_all_endpoints_to_s3",
                edfi_conn_id=self.edfi_conn_id,

                tmp_dir=self.tmp_dir,
                s3_conn_id=self.s3_conn_id,
                s3_destination_dir=s3_destination_dir,
                
                get_deletes=get_deletes,
                get_key_changes=get_key_changes,
                max_change_version=airflow_util.xcom_pull_template(self.newest_edfi_cv_task_id),

                # Arguments that are required to be lists in Ed-Fi bulk-operator.
                resource=endpoints,
                min_change_version=min_change_versions,
                s3_destination_filename=[f"{endpoint}.jsonl" for endpoint in endpoints],

                # Optional config-specified run-attributes (overridden by those in configs)
                **endpoint_config_lists,

                # Only run endpoints specified at DAG or delta-level.
                enabled_endpoints=enabled_endpoints,

                pool=self.pool,
                trigger_rule='none_skipped',
                dag=self.dag
            )

            ### COPY FROM S3 TO SNOWFLAKE
            copy_s3_to_snowflake = BulkS3ToSnowflakeOperator(
                task_id=f"copy_all_endpoints_into_snowflake",
                tenant_code=self.tenant_code,
                api_year=self.api_year,

                resource=self.xcom_pull_template_map_idx(pull_edfi_to_s3, 0),
                table_name=table or self.xcom_pull_template_map_idx(pull_edfi_to_s3, 0),
                edfi_conn_id=self.edfi_conn_id,
                snowflake_conn_id=self.snowflake_conn_id,
                s3_destination_key=self.xcom_pull_template_map_idx(pull_edfi_to_s3, 1),

                trigger_rule='none_skipped',  # Different trigger rule than default.
                dag=self.dag
            )

            ### UPDATE SNOWFLAKE CHANGE VERSIONS
            if self.use_change_version:
                update_cv_operator = self.build_change_version_update_operator(
                    task_id=f"update_change_versions_in_snowflake",
                    endpoints=self.xcom_pull_template_map_idx(pull_edfi_to_s3, 0),
                    get_deletes=get_deletes,
                    get_key_changes=get_key_changes,
                    trigger_rule='all_success'
                )
            else:
                update_cv_operator = None

            ### Chain tasks into final task-group
            airflow_util.chain_tasks(get_cv_operator, pull_edfi_to_s3, copy_s3_to_snowflake, update_cv_operator)

        return bulk_task_group
