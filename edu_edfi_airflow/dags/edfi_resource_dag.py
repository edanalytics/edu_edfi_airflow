import os
from typing import Dict, List, Optional, Tuple, Union

from airflow.models.param import Param
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from ea_airflow_util import EACustomDAG
from ea_airflow_util import update_variable
from edfi_api_client import camel_to_snake

from edu_edfi_airflow.dags.callables import change_version
from edu_edfi_airflow.dags.dag_util import airflow_util
from edu_edfi_airflow.providers.edfi.transfers.edfi_to_s3 import EdFiToS3Operator, BulkEdFiToS3Operator
from edu_edfi_airflow.providers.snowflake.transfers.s3_to_snowflake import BulkS3ToSnowflakeOperator


class EdFiResourceDAG:
    """
    "Ed-Fi3 Change Version Window" TaskGroup

    [Ed-Fi Max Change Version] -> [Reset Snowflake Change Versions]

    If use_change_version is True, initialize a change group that retrieves the latest Ed-Fi change version.
    If full_refresh is triggered in DAG configs, reset change versions for the resources being processed in Snowflake.
    

    There are three types of Ed-Fi endpoint TaskGroups. All take the same inputs and return the same outputs (polymorphism).

        "Default" TaskGroup
        - Create one task per endpoint.

        "Dynamic" TaskGroup
        - Dynamically task-map all endpoints. This function presumes that only endpoints with deltas to ingest are passed as input.
        
        "Bulk" TaskGroup
        - Loop over each endpoint in a single task.

    All task groups receive a list of (endpoint, last_change_version) tuples as input.
    These are referenced in EdFiToS3 operators, and ingestion from Ed-Fi is attempted for each.
    All that succeed are passed onward as a {endpoint: filename} dictionary to the S3ToSnowflake and UpdateSnowflakeCV operators.
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
        use_change_version: bool = True,

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

        self.tenant_code = tenant_code
        self.api_year = api_year

        self.edfi_conn_id = edfi_conn_id
        self.s3_conn_id = s3_conn_id
        self.snowflake_conn_id = snowflake_conn_id

        self.pool = pool
        self.tmp_dir = tmp_dir
        self.multiyear = multiyear

        self.change_version_table = change_version_table
        self.deletes_table = deletes_table
        self.key_changes_table = key_changes_table
        self.descriptors_table = descriptors_table

        self.dbt_incrementer_var = dbt_incrementer_var
        
        ### Parse optional config objects (improved performance over adding resources manually)
        self.resource_configs = self.parse_endpoint_configs(resource_configs)
        self.descriptor_configs = self.parse_endpoint_configs(descriptor_configs)

        # Only collect deletes and key-changes for resources
        self.deletes_to_ingest = set([resource for resource, config in self.resource_configs.items() if config.get('fetch_deletes')])
        self.key_changes_to_ingest = set([resource for resource, config in self.resource_configs.items() if config.get('fetch_deletes')])

        # Populate DAG params with optionally-defined resources and descriptors; default to empty-list (i.e., run all).
        enabled_endpoints = [
            endpoint for endpoint, config in {**self.resource_configs, **self.descriptor_configs}.items()
            if config.get('enabled')
        ]

        dag_params = {
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

        self.dag = EACustomDAG(params=dag_params, **kwargs)

        ### Create nested task-groups for cleaner webserver UI
        # (Make these lazy to only show populated TaskGroups in the UI.)
        self.resources_task_group = None
        self.descriptors_task_group = None
        self.resource_deletes_task_group = None
        self.resource_key_changes_task_group = None

        ### Populate variables static across all run-types.
        self.s3_destination_directory = os.path.join(
            self.tenant_code, str(self.api_year), "{{ ds_nodash }}", "{{ ts_nodash }}"
        )

        # For a multiyear ODS, we need to specify school year as an additional query parameter.
        # (This is an exception-case; we push all tenants to build year-specific ODSes when possible.)
        self.default_params = {}
        if self.multiyear:
            self.default_params['schoolYear'] = self.api_year


    # Helper method for parsing new optional DAG arguments resource_configs and descriptor_configs
    @staticmethod
    def parse_endpoint_configs(configs: Optional[Union[dict, list]] = None):
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


    # Original methods to manually build task-groups (deprecated in favor of `resource_configs` and `descriptor_configs`).
    def add_resource(self, resource: str, **kwargs):
        self.resource_configs[camel_to_snake(resource)] = kwargs

    def add_descriptor(self, resource: str, **kwargs):
        self.descriptor_configs[camel_to_snake(resource)] = kwargs

    def add_resource_deletes(self, resource: str, **kwargs):
        self.deletes_to_ingest.add(camel_to_snake(resource))
        self.key_changes_to_ingest.add(camel_to_snake(resource))

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

        # Resources
        self.resources_task_group: Optional[TaskGroup] = task_group_callable(
            group_id = "Ed-Fi Resources",
            endpoints=list(self.resource_configs.keys()),
            configs=self.resource_configs
            # Tables are built dynamically from the names of the endpoints.
        )

        # Descriptors
        self.descriptors_task_group: Optional[TaskGroup] = task_group_callable(
            group_id="Ed-Fi Descriptors",
            endpoints=list(self.descriptor_configs.keys()),
            configs=self.descriptor_configs,
            table=self.descriptors_table
        )

        # Resource Deletes
        self.resource_deletes_task_group: Optional[TaskGroup] = task_group_callable(
            group_id="Ed-Fi Resource Deletes",
            endpoints=list(self.deletes_to_ingest),
            configs=self.resource_configs,
            table=self.deletes_table,
            get_deletes=True
        )

        # Resource Key-Changes
        self.resource_key_changes_task_group: Optional[TaskGroup] = task_group_callable(
            group_id="Ed-Fi Resource Key Changes",
            endpoints=list(self.key_changes_to_ingest),
            configs=self.resource_configs,
            table=self.key_changes_table,
            get_key_changes=True
        )

        ### Chain task groups into the DAG between CV operators and Airflow state operators.
        # Retrieve current and previous change versions to define an ingestion window.
        if self.use_change_version:
            cv_task_group: TaskGroup = self.build_change_version_task_group()

        # Build an operator to increment the DBT var at the end of the run.
        if self.dbt_incrementer_var:
            dbt_var_increment_operator: PythonOperator = self.build_dbt_var_increment_operator()

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
                cv_task_group >> task_group

            if self.dbt_incrementer_var:
                task_group >> dbt_var_increment_operator

            # Always apply the state sentinel.
            task_group >> dag_state_sentinel

        # The sentinel also holds the state of the CV and DBT var operators.
        if self.dbt_incrementer_var:
            dbt_var_increment_operator >> dag_state_sentinel


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
        get_key_changes: bool = False,
        return_only_deltas: bool = False
    ) -> PythonOperator:
        op_kwargs = {
            'tenant_code': self.tenant_code,
            'api_year': self.api_year,
            'endpoints': endpoints,
            'snowflake_conn_id': self.snowflake_conn_id,
            'change_version_table': self.change_version_table,
            'get_deletes': get_deletes,
            'get_key_changes': get_key_changes,
        }

        # If Ed-Fi connection is provided, total-count checks will be made for each endpoint.
        if return_only_deltas:
            op_kwargs.update({
                'edfi_conn_id': self.edfi_conn_id,
                'max_change_version': airflow_util.xcom_pull_template(self.newest_edfi_cv_task_id),
            })

        return PythonOperator(
            task_id=task_id,
            python_callable=change_version.get_previous_change_versions,
            op_kwargs=op_kwargs,
            trigger_rule='none_failed',  # Run regardless of whether the CV table was reset.
            dag=self.dag
        )

    def build_change_version_update_operator(self, task_id: str, endpoints: List[str], get_deletes: bool, get_key_changes: bool) -> PythonOperator:
        """

        :return:
        """
        ### UPDATE CHANGE VERSION TABLE ON SNOWFLAKE
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
            trigger_rule='all_success',
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


    def build_default_edfi_to_snowflake_task_group(self,
        endpoints: List[str],
        configs: Dict[str, dict],
        group_id: str,

        *,
        table: Optional[str] = None,
        get_deletes: bool = False,
        get_key_changes: bool = False,
        **kwargs
    ) -> TaskGroup:
        """
        Build one EdFiToS3 task per endpoint
        Bulk copy the data to its respective table in Snowflake.

        len(get_cv_operator.input) == len(get_cv_operator.output) == len(pull_edfi_to_s3.input) >= len(pull_edfi_to_s3.output) == len(copy_s3_to_snowflake.input)

        :param endpoints:
        :param configs:
        :param get_deletes:
        :param get_key_changes:
        :param parent_group:
        :return:
        """
        if not endpoints:
            return None
        
        # Wrap the branch in a task group
        with TaskGroup(
            group_id=group_id,
            prefix_group_id=False,
            parent_group=None,
            dag=self.dag
        ) as default_task_group:
            
            cleaned_group_id = group_id.replace(' ', "_").lower()

            ### LATEST SNOWFLAKE CHANGE VERSIONS: Output Dict[endpoint, last_change_version]
            if self.use_change_version:
                get_cv_operator = self.build_change_version_get_operator(
                    task_id=f"{cleaned_group_id}__get_last_change_versions_from_snowflake",
                    endpoints=[(configs[endpoint].get('namespace', self.DEFAULT_NAMESPACE), endpoint) for endpoint in endpoints],
                    get_deletes=get_deletes,
                    get_key_changes=get_key_changes,
                )
            else:
                get_cv_operator = None

            ### EDFI TO S3: Output Tuple[endpoint, filename] per successful task
            pull_operators_list = []

            for endpoint in endpoints:
                display_resource = airflow_util.build_display_name(endpoint, get_deletes=get_deletes, get_key_changes=get_key_changes)
                endpoint_configs = configs.get(endpoint, {})

                pull_edfi_to_s3 = EdFiToS3Operator(
                    task_id=f"pull_{display_resource}_to_s3",

                    edfi_conn_id=self.edfi_conn_id,
                    resource=endpoint,

                    tmp_dir=self.tmp_dir,
                    s3_conn_id=self.s3_conn_id,
                    s3_destination_dir=self.s3_destination_directory,
                    s3_destination_filename=f"{display_resource}.jsonl",
                    
                    get_deletes=get_deletes,
                    get_key_changes=get_key_changes,
                    min_change_version=airflow_util.xcom_pull_template(get_cv_operator.task_id, prefix="dict(", suffix=f")['{endpoint}']") if get_cv_operator else None,
                    max_change_version=airflow_util.xcom_pull_template(self.newest_edfi_cv_task_id),

                    # Optional config-specified run-attributes (overridden by those in configs)
                    namespace=endpoint_configs.get('namespace', self.DEFAULT_NAMESPACE),
                    page_size=endpoint_configs.get('page_size', self.DEFAULT_PAGE_SIZE),
                    num_retries=endpoint_configs.get('num_retries', self.DEFAULT_MAX_RETRIES),
                    change_version_step_size=endpoint_configs.get('change_version_step_size', self.DEFAULT_CHANGE_VERSION_STEP_SIZE),
                    query_parameters={**endpoint_configs.get('params', {}), **self.default_params},

                    pool=self.pool,
                    trigger_rule='none_skipped',
                    dag=self.dag
                )

                pull_operators_list.append(pull_edfi_to_s3)

            ### COPY FROM S3 TO SNOWFLAKE
            map_xcom_attribute_by_index = lambda idx: airflow_util.xcom_pull_template(
                [operator.task_id for operator in pull_operators_list], suffix=f" | map(attribute={idx}) | list"
            )

            copy_s3_to_snowflake = BulkS3ToSnowflakeOperator(
                task_id=f"{cleaned_group_id}__copy_all_endpoints_into_snowflake",
                tenant_code=self.tenant_code,
                api_year=self.api_year,
                resource=map_xcom_attribute_by_index(0),
                table_name=table or map_xcom_attribute_by_index(0),
                edfi_conn_id=self.edfi_conn_id,
                snowflake_conn_id=self.snowflake_conn_id,

                s3_destination_dir=self.s3_destination_directory,
                s3_destination_filename=map_xcom_attribute_by_index(1),

                trigger_rule='all_done',
                dag=self.dag
            )

            ### UPDATE SNOWFLAKE CHANGE VERSIONS
            if self.use_change_version:
                update_cv_operator = self.build_change_version_update_operator(
                    task_id=f"{cleaned_group_id}__update_change_versions_in_snowflake",
                    endpoints=map_xcom_attribute_by_index(0),
                    get_deletes=get_deletes,
                    get_key_changes=get_key_changes
                )
            else:
                update_cv_operator = None

            ### Chain tasks into final task-group
            for operator in pull_operators_list:
                if get_cv_operator:
                    get_cv_operator >> operator >> copy_s3_to_snowflake
                else:
                    operator >> copy_s3_to_snowflake

            if update_cv_operator:
                copy_s3_to_snowflake >> update_cv_operator

        return default_task_group


    def build_dynamic_edfi_to_snowflake_task_group(self,
        endpoints: List[str],
        configs: Dict[str, dict],
        group_id: str,

        *,
        table: Optional[str] = None,
        get_deletes: bool = False,
        get_key_changes: bool = False,
        **kwargs
    ):
        """
        Build one EdFiToS3 task per endpoint
        Bulk copy the data to its respective table in Snowflake.

        len(get_cv_operator.input) >= len(get_cv_operator.output) == len(pull_edfi_to_s3.input) >= len(pull_edfi_to_s3.output) == len(copy_s3_to_snowflake.input)

        :param endpoints:
        :param configs:
        :param get_deletes:
        :param get_key_changes:
        :param parent_group:
        :return:
        """
        if not endpoints:
            return None

        # Wrap the branch in a task group
        with TaskGroup(
            group_id=group_id,
            prefix_group_id=False,
            parent_group=None,
            dag=self.dag
        ) as dynamic_task_group:

            # Dynamic runs only make sense in the context of change-versions.
            if not self.use_change_version:
                raise ValueError("Dynamic run-type requires `use_change_version to be True`.")

            cleaned_group_id = group_id.replace(' ', "_").lower()

            ### LATEST SNOWFLAKE CHANGE VERSIONS
            get_cv_operator = self.build_change_version_get_operator(
                task_id=f"{cleaned_group_id}__get_last_change_versions",
                endpoints=[(configs[endpoint].get('namespace', 'ed-fi'), endpoint) for endpoint in endpoints],
                get_deletes=get_deletes,
                get_key_changes=get_key_changes,
                return_only_deltas=True  # For dynamic mapping, only process endpoints with new data to ingest.
            )

            ### EDFI TO S3
            pull_edfi_to_s3 = (EdFiToS3Operator
                .partial(
                    task_id=f"{cleaned_group_id}__pull_dynamic_endpoints_to_s3",

                    edfi_conn_id=self.edfi_conn_id,

                    tmp_dir= self.tmp_dir,
                    s3_conn_id= self.s3_conn_id,
                    s3_destination_dir=self.s3_destination_directory,

                    get_deletes=get_deletes,
                    get_key_changes=get_key_changes,
                    max_change_version=airflow_util.xcom_pull_template(self.newest_edfi_cv_task_id),

                    sla=None,  # "SLAs are unsupported with mapped tasks."
                    pool=self.pool,
                    trigger_rule='none_skipped',
                    dag=self.dag
                )
                .expand_kwargs(
                    get_cv_operator.output.map(lambda endpoint__last_cv: {
                        'resource': endpoint__last_cv[0],
                        'min_change_version': endpoint__last_cv[1],
                        'namespace': configs[endpoint__last_cv[0]].get('namespace', self.DEFAULT_NAMESPACE),
                        'page_size': configs[endpoint__last_cv[0]].get('page_size', self.DEFAULT_PAGE_SIZE),
                        'num_retries': configs[endpoint__last_cv[0]].get('num_retries', self.DEFAULT_MAX_RETRIES),
                        'change_version_step_size': configs[endpoint__last_cv[0]].get('change_version_step_size', self.DEFAULT_CHANGE_VERSION_STEP_SIZE),
                        'query_parameters': {**configs[endpoint__last_cv[0]].get('params', {}), **self.default_params},
                        's3_destination_filename': "{}.jsonl".format(airflow_util.build_display_name(endpoint__last_cv[0], get_deletes=get_deletes, get_key_changes=get_key_changes)),
                    })
                )
            )

            ### COPY FROM S3 TO SNOWFLAKE
            map_xcom_attribute_by_index = lambda idx: airflow_util.xcom_pull_template(
                pull_edfi_to_s3.task_id, suffix=f" | map(attribute={idx}) | list"
            )

            copy_s3_to_snowflake = BulkS3ToSnowflakeOperator(
                task_id=f"{cleaned_group_id}__copy_all_endpoints_into_snowflake",
                tenant_code=self.tenant_code,
                api_year=self.api_year,
                resource=map_xcom_attribute_by_index(0),
                table_name=table or map_xcom_attribute_by_index(0),
                edfi_conn_id=self.edfi_conn_id,
                snowflake_conn_id=self.snowflake_conn_id,

                s3_destination_dir=self.s3_destination_directory,
                s3_destination_filename=map_xcom_attribute_by_index(1),

                trigger_rule='all_done',
                dag=self.dag
            )

            ### UPDATE SNOWFLAKE CHANGE VERSIONS
            update_cv_operator = self.build_change_version_update_operator(
                task_id=f"{cleaned_group_id}__update_change_versions_in_snowflake",
                endpoints=map_xcom_attribute_by_index(0),
                get_deletes=get_deletes,
                get_key_changes=get_key_changes
            )

            ### Chain tasks into final task-group
            get_cv_operator >> pull_edfi_to_s3 >> copy_s3_to_snowflake >> update_cv_operator

        return dynamic_task_group
    

    def build_bulk_edfi_to_snowflake_task_group(self,
        endpoints: List[str],
        configs: Dict[str, dict],
        group_id: str,

        *,
        table: Optional[str] = None,
        get_deletes: bool = False,
        get_key_changes: bool = False,
        **kwargs
    ):
        """
        Build one EdFiToS3 task (with inner for-loop across endpoints).
        Bulk copy the data to its respective table in Snowflake.

        len(get_cv_operator.input) == len(get_cv_operator.output) == len(pull_edfi_to_s3.input) >= len(pull_edfi_to_s3.output) == len(copy_s3_to_snowflake.input)

        :param endpoints:
        :param configs:
        :param get_deletes:
        :param get_key_changes:
        :param parent_group:
        :return:
        """
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

            ### LATEST SNOWFLAKE CHANGE VERSIONS: Output Dict[endpoint, last_change_version]
            if self.use_change_version:
                get_cv_operator = self.build_change_version_get_operator(
                    task_id=f"{cleaned_group_id}__get_last_change_versions",
                    endpoints=[(configs[endpoint].get('namespace', self.DEFAULT_NAMESPACE), endpoint) for endpoint in endpoints],
                    get_deletes=get_deletes,
                    get_key_changes=get_key_changes,
                )
                min_change_versions = [
                    airflow_util.xcom_pull_template(get_cv_operator.task_id, prefix="dict(", suffix=f")['{endpoint}']")
                    for endpoint in endpoints
                ]
            else:
                get_cv_operator = None
                min_change_versions = None

            ### EDFI TO S3: Output Dict[endpoint, filename] with all successful tasks
            pull_edfi_to_s3 = BulkEdFiToS3Operator(
                task_id=f"{cleaned_group_id}__pull_all_endpoints_to_s3",

                edfi_conn_id=self.edfi_conn_id,
                resource=endpoints,

                tmp_dir=self.tmp_dir,
                s3_conn_id=self.s3_conn_id,
                s3_destination_dir=self.s3_destination_directory,
                s3_destination_filename=[
                    "{}.jsonl".format(airflow_util.build_display_name(endpoint, get_deletes=get_deletes, get_key_changes=get_key_changes))
                    for endpoint in endpoints        
                ],
                
                get_deletes=get_deletes,
                get_key_changes=get_key_changes,
                min_change_version=min_change_versions,
                max_change_version=airflow_util.xcom_pull_template(self.newest_edfi_cv_task_id),

                # Optional config-specified run-attributes (overridden by those in configs)
                namespace=[
                    configs.get(endpoint, {}).get('namespace', self.DEFAULT_NAMESPACE)
                    for endpoint in endpoints
                ],
                page_size=[
                    configs.get(endpoint, {}).get('page_size', self.DEFAULT_PAGE_SIZE)
                    for endpoint in endpoints
                ],
                num_retries=[
                    configs.get(endpoint, {}).get('num_retries', self.DEFAULT_MAX_RETRIES)
                    for endpoint in endpoints
                ],
                change_version_step_size=[
                    configs.get(endpoint, {}).get('change_version_step_size', self.DEFAULT_CHANGE_VERSION_STEP_SIZE)
                    for endpoint in endpoints
                ],
                query_parameters=[
                    {**configs.get(endpoint, {}).get('params', {}), **self.default_params}
                    for endpoint in endpoints
                ],

                pool=self.pool,
                trigger_rule='none_skipped',
                dag=self.dag
            )

            ### COPY FROM S3 TO SNOWFLAKE
            map_xcom_attribute_by_index = lambda idx: airflow_util.xcom_pull_template(
                pull_edfi_to_s3.task_id, suffix=f" | map(attribute={idx}) | list"
            )

            copy_s3_to_snowflake = BulkS3ToSnowflakeOperator(
                task_id=f"{cleaned_group_id}__copy_all_endpoints_into_snowflake",
                tenant_code=self.tenant_code,
                api_year=self.api_year,
                resource=map_xcom_attribute_by_index(0),
                table_name=table or map_xcom_attribute_by_index(0),
                edfi_conn_id=self.edfi_conn_id,
                snowflake_conn_id=self.snowflake_conn_id,

                s3_destination_dir=self.s3_destination_directory,
                s3_destination_filename=map_xcom_attribute_by_index(1),

                trigger_rule='none_skipped',  # Different trigger rule than default.
                dag=self.dag
            )

            ### UPDATE SNOWFLAKE CHANGE VERSIONS
            if self.use_change_version:
                update_cv_operator = self.build_change_version_update_operator(
                    task_id=f"{cleaned_group_id}__update_change_versions_in_snowflake",
                    endpoints=map_xcom_attribute_by_index(0),
                    get_deletes=get_deletes,
                    get_key_changes=get_key_changes
                )
            else:
                update_cv_operator = None

            ### Chain tasks into final task-group
            if get_cv_operator and update_cv_operator:
                get_cv_operator >> pull_edfi_to_s3 >> copy_s3_to_snowflake >> update_cv_operator
            else:
                pull_edfi_to_s3 >> copy_s3_to_snowflake

        return bulk_task_group
