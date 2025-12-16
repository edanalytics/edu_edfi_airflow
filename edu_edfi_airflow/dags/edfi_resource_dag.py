import copy
import os
from functools import partial
from typing import Dict, List, Optional, Set, Tuple, Union

from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from ea_airflow_util import EACustomDAG
from ea_airflow_util import update_variable
from edfi_api_client import camel_to_snake

from edu_edfi_airflow.callables import airflow_util, change_version, total_counts
from edu_edfi_airflow.providers.generic.transfers.edfi_to_object_storage import EdFiToObjectStorageOperator, BulkEdFiToObjectStorageOperator
# Generic database operator - will be determined by connection parameters
from edu_edfi_airflow.providers.generic.transfers.object_storage_to_database import BulkObjectStorageToDatabaseOperator


class EdFiResourceDAG:
    """
    If use_change_version is True, initialize a change group that retrieves the latest Ed-Fi change version.
    If full_refresh is triggered in DAG configs, reset change versions for the resources being processed in the database.

    DAG Structure:
        (Ed-Fi3 Change Version Window) >> [Ed-Fi Resources/Descriptors (Deletes/KeyChanges)] >> (increment_dbt_variable) >> dag_state_sentinel
    
    "Ed-Fi3 Change Version Window" TaskGroup:
        get_latest_edfi_change_version >> reset_previous_change_versions_in_database

    "Ed-Fi Resources/Descriptors (Deletes/KeyChanges)" TaskGroup:
        (get_cv_operator) >> [Ed-Fi Endpoint Task] >> copy_all_endpoints_into_database >> (update_change_versions_in_database)

    There are three types of Ed-Fi Endpoint Tasks. All take the same inputs and return the same outputs (i.e., polymorphism).
        "Default" TaskGroup
        - Create one task per endpoint.
        "Dynamic" TaskGroup
        - Dynamically task-map all endpoints. This function presumes that only endpoints with deltas to ingest are passed as input.
        "Bulk" TaskGroup
        - Loop over each endpoint in a single task.

    All task groups receive a list of (endpoint, last_change_version) tuples as input.
    All that successfully retrieve records are passed onward as (endpoint, filename) tuples to the ObjectStorageToDatabase and UpdateDatabaseCV operators.
    """
    DEFAULT_CONFIGS = {
        'namespace': 'ed-fi',
        'page_size': 500,
        'change_version_step_size': 50000,
        'num_retries': 5,
        'query_parameters': {},
    }

    newest_edfi_cv_task_id = "get_latest_edfi_change_version"  # Original name for historic run compatibility


    def __init__(self,
        *,
        tenant_code: str,
        api_year   : int,

        edfi_conn_id     : str,
        use_edfi_token_cache: bool = True,

        object_storage_conn_id  : Optional[str] = None,
        database_conn_id: Optional[str] = None,
        object_storage_type: str = "data_lake",  # Generic type name if not explicitly specified
        database_type: str = "database",  # Generic type name if not explicitly specified
        snowflake_conn_id: Optional[str] = None,  # Deprecated, use database_conn_id
        s3_conn_id       : Optional[str] = None,  # Deprecated, use storage_conn_id

        pool     : str,
        tmp_dir  : str,
        multiyear: bool = False,
        schedule_interval_full_refresh: Optional[str] = None,

        use_change_version: bool = True,
        get_key_changes: bool = False,
        get_deletes_cv_with_deltas: bool = True,
        pull_all_deletes: bool = False,  # Deprecated in 0.5.0
        pull_total_counts: bool = False,
        run_type: str = "default",
        resource_configs: Optional[List[dict]] = None,
        descriptor_configs: Optional[List[dict]] = None,

        change_version_table: str = '_meta_change_versions',
        deletes_table: str = '_deletes',
        key_changes_table: str = '_key_changes',
        descriptors_table: str = '_descriptors',
        total_counts_table: str = '_meta_total_counts',

        dbt_incrementer_var: Optional[str] = None,


        **kwargs
    ) -> None:
        self.run_type = run_type
        self.use_change_version = use_change_version
        self.get_key_changes = get_key_changes

        self.tenant_code = tenant_code
        self.api_year = api_year

        self.edfi_conn_id = edfi_conn_id
        self.use_edfi_token_cache = use_edfi_token_cache
        
        self.object_storage_conn_id = object_storage_conn_id or s3_conn_id
        self.database_conn_id = database_conn_id or snowflake_conn_id
        self.object_storage_type = object_storage_type
        self.database_type = database_type
        
        # Store additional kwargs for flexible database/storage connections
        self.additional_kwargs = kwargs
        
        self.pool = pool
        self.tmp_dir = tmp_dir
        self.multiyear = multiyear
        self.schedule_interval_full_refresh = schedule_interval_full_refresh  # Force full-refresh on a scheduled cadence

        self.change_version_table = change_version_table
        self.deletes_table = deletes_table
        self.key_changes_table = key_changes_table
        self.descriptors_table = descriptors_table
        self.get_deletes_cv_with_deltas = get_deletes_cv_with_deltas
        self.total_counts_table = total_counts_table
        self.pull_total_counts = pull_total_counts

        self.dbt_incrementer_var = dbt_incrementer_var

        
        ### Parse optional config objects (improved performance over adding resources manually).
        resource_configs, resource_deletes, resource_key_changes = self.parse_endpoint_configs(resource_configs)
        descriptor_configs, descriptor_deletes, descriptor_key_changes = self.parse_endpoint_configs(descriptor_configs)
        self.endpoint_configs = {**resource_configs, **descriptor_configs}

        # Build lists of each enabled endpoint type (only collect deletes and key-changes for resources).
        self.resources = set(resource_configs.keys())
        self.descriptors = set(descriptor_configs.keys())
        self.deletes_to_ingest = resource_deletes
        self.key_changes_to_ingest = resource_key_changes

        # Populate DAG params with optionally-defined resources and descriptors; default to empty-list (i.e., run all).
        dag_params = {
            "full_refresh": Param(
                default=False,
                type="boolean",
                description="If true, deletes endpoint data in Snowflake before ingestion"
            ),
            "endpoints": Param(
                default=sorted(list(self.resources | self.descriptors)),
                type=["array", "null"],
                description="Newline-separated list of specific endpoints to ingest (case-agnostic). If left blank, ingests all endpionts"
            ),
        }

        user_defined_macros={
            "is_scheduled_full_refresh": partial(airflow_util.run_matches_cron, cron=self.schedule_interval_full_refresh)
        }

        self.dag = EACustomDAG(params=dag_params, user_defined_macros=user_defined_macros, **kwargs)

    # Helper methods for parsing and building DAG endpoint configs.
    def build_endpoint_configs(self, enabled: bool = True, fetch_deletes: bool = True, **kwargs):
        """
        Unify kwargs with default config arguments.
        Add schoolYear filter in multiYear ODSes.
        `enabled` and `fetch_deletes` are not passed into configs.
        """
        configs = {**self.DEFAULT_CONFIGS, **kwargs}
        configs["query_parameters"] = copy.deepcopy(configs["query_parameters"])  # Prevent query_parameters from being shared across DAGs.

        ### For a multiyear ODS, we need to specify school year as an additional query parameter.
        # (This is an exception-case; we push all tenants to build year-specific ODSes when possible.)
        if self.multiyear:
            configs['query_parameters']['schoolYear'] = self.api_year

        return configs

    def parse_endpoint_configs(self, raw_configs: Optional[Union[dict, list]] = None) -> Tuple[Dict[str, dict], Set[str], Set[str]]:
        """
        Parse endpoint configs into kwarg bundles if passed.
        Keep list of deletes and keyChanges to fetch.
        Force all endpoints to snake-case for consistency.
        Return only enabled configs (enabled by default).
        """
        if not raw_configs:
            return {}, set(), set()
        
        # A dictionary of endpoints has been passed with run-metadata.
        elif isinstance(raw_configs, dict):
            configs = {}
            deletes = set()
            key_changes = set()

            for endpoint, kwargs in raw_configs.items():
                if not kwargs.get('enabled', True):
                    continue

                snake_endpoint = camel_to_snake(endpoint)
                configs[snake_endpoint] = self.build_endpoint_configs(**kwargs)
                if kwargs.get('fetch_deletes'):
                    deletes.add(snake_endpoint)
                    key_changes.add(snake_endpoint)
            
            return configs, deletes, key_changes

        # A list of resources has been passed without run-metadata
        elif isinstance(raw_configs, list):
            # Use default configs and mark all as enabled with deletes/keyChanges.
            configs = {camel_to_snake(endpoint): self.build_endpoint_configs(namespace=ns) for endpoint, ns in raw_configs}
            deletes = set(configs.keys())
            key_changes = set(configs.keys())
            return configs, deletes, key_changes
        
        else:
            raise ValueError(
                f"Passed configs are an unknown datatype! Expected Dict[endpoint: metadata] or List[(namespace, endpoint)] but received {type(configs)}"
            )
    
    # Original methods to manually build task-groups (deprecated in favor of `resource_configs` and `descriptor_configs` DAG arguments).
    def add_resource(self, resource: str, **kwargs):
        if kwargs.get('enabled', True):
            snake_resource = camel_to_snake(resource)
            self.resources.add(snake_resource)
            self.endpoint_configs[snake_resource] = self.build_endpoint_configs(**kwargs)

    def add_descriptor(self, resource: str, **kwargs):
        if kwargs.get('enabled', True):
            snake_resource = camel_to_snake(resource)
            self.descriptors.add(snake_resource)
            self.endpoint_configs[snake_resource] = self.build_endpoint_configs(**kwargs)

    def add_resource_deletes(self, resource: str, **kwargs):
        if kwargs.get('enabled', True):
            snake_resource = camel_to_snake(resource)
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
            task_group_callable = self.build_default_edfi_to_database_task_group
        elif self.run_type == 'dynamic':
            task_group_callable = self.build_dynamic_edfi_to_database_task_group
        elif self.run_type == 'bulk':
            task_group_callable = self.build_bulk_edfi_to_database_task_group
        else:
            raise ValueError(f"Run type {self.run_type} is not one of the expected values: [default, dynamic, bulk].")

        # Set parent directory and create subfolders for each task group.
        parent_directory = os.path.join(
            self.tenant_code, str(self.api_year), "{{ ds_nodash }}", "{{ ts_nodash }}"
        )

        # Resources
        resources_task_group: Optional[TaskGroup] = task_group_callable(
            group_id = "Ed-Fi_Resources",
            endpoints=sorted(list(self.resources)),
            destination_dir=os.path.join(parent_directory, 'resources')
            # Tables are built dynamically from the names of the endpoints.
        )

        # Descriptors
        descriptors_task_group: Optional[TaskGroup] = task_group_callable(
            group_id="Ed-Fi_Descriptors",
            endpoints=sorted(list(self.descriptors)),
            table=self.descriptors_table,
            destination_dir=os.path.join(parent_directory, 'descriptors')
        )

        # Resource Deletes
        resource_deletes_task_group: Optional[TaskGroup] = task_group_callable(
            group_id="Ed-Fi_Resource_Deletes",
            endpoints=sorted(list(self.deletes_to_ingest)),
            table=self.deletes_table,
            destination_dir=os.path.join(parent_directory, 'resource_deletes'),
            get_deletes=True,
            get_with_deltas=self.get_deletes_cv_with_deltas
        )

        # Resource Key-Changes (only applicable in Ed-Fi v6.x and up)
        if self.get_key_changes:
            resource_key_changes_task_group: Optional[TaskGroup] = task_group_callable(
                group_id="Ed-Fi Resource Key Changes",
                endpoints=sorted(list(self.key_changes_to_ingest)),
                table=self.key_changes_table,
                destination_dir=os.path.join(parent_directory, 'resource_key_changes'),
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

        if self.pull_total_counts: 
            total_counts_taskgroup: TaskGroup = self.build_total_counts_task_group(endpoints=sorted(list(self.resources)))
        else:
            total_counts_taskgroup = None

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

        # Chain tasks and taskgroups into the DAG; chain sentinel after all task groups.
        airflow_util.chain_tasks(cv_task_group, total_counts_taskgroup, edfi_task_groups, dbt_var_increment_operator)
        airflow_util.chain_tasks(edfi_task_groups, dag_state_sentinel)


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
                    'use_edfi_token_cache': self.use_edfi_token_cache,
                },
                dag=self.dag
            )

            # Reset the database change version table (if a full-refresh).
            reset_database_cvs = PythonOperator(
                task_id=f"reset_previous_change_versions_in_{self.database_type}",
                python_callable=change_version.reset_change_versions,
                op_kwargs={
                    'tenant_code': self.tenant_code,
                    'api_year': self.api_year,
                    'database_conn_id': self.database_conn_id,
                    'database_type': self.database_type,
                    'change_version_table': self.change_version_table,
                },
                trigger_rule='all_success',
                dag=self.dag
            )

            get_newest_edfi_cv >> reset_database_cvs

        return cv_task_group

    def build_change_version_get_operator(self,
        task_id: str,
        endpoints: List[Tuple[str, str]],
        get_deletes: bool = False,
        get_key_changes: bool = False,
        get_with_deltas: bool = True
    ) -> PythonOperator:
        """

        :return:
        """
        get_cv_operator = PythonOperator(
            task_id=task_id,
            python_callable=change_version.get_previous_change_versions_with_deltas if get_with_deltas else change_version.get_previous_change_versions,
            op_kwargs={
                'tenant_code': self.tenant_code,
                'api_year': self.api_year,
                'endpoints': endpoints,
                'database_conn_id': self.database_conn_id,
                'database_type': self.database_type,
                'change_version_table': self.change_version_table,
                'get_deletes': get_deletes,
                'get_key_changes': get_key_changes,
                'has_key_changes': self.get_key_changes, # Indicates whether to add keyChanges records on full refresh, since this column is not yet required
                'edfi_conn_id': self.edfi_conn_id,
                'use_edfi_token_cache': self.use_edfi_token_cache,
                'max_change_version': airflow_util.xcom_pull_template(self.newest_edfi_cv_task_id),
            },
            trigger_rule='none_failed',  # Run regardless of whether the CV table was reset.
            dag=self.dag
        )

        # One or more endpoints can fail total-get count. Create a second operator to track that failed status.
        # This should NOT be necessary, but we encountered a bug where a downstream "none_skipped" task skipped with "upstream_failed" status.
        def fail_if_xcom(xcom_value, **context):
            if xcom_value:
                raise AirflowFailException(f"The following endpoints failed when pulling total counts: {xcom_value}")
            else:
                raise AirflowSkipException  # Force a skip to not mark the taskgroup as a success when all tasks skip.

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
                'database_conn_id': self.database_conn_id,
                'database_type': self.database_type,
                'change_version_table': self.change_version_table,
                'edfi_change_version': airflow_util.xcom_pull_template(self.newest_edfi_cv_task_id),
                'endpoints': endpoints,
                'get_deletes': get_deletes,
                'get_key_changes': get_key_changes,
                'has_key_changes': self.get_key_changes # Indicates whether to add keyChanges records on full refresh, since this column is not yet required
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

    def build_default_edfi_to_database_task_group(self,
        endpoints: List[str],
        group_id: str,

        *,
        destination_dir: str,
        table: Optional[str] = None,
        get_deletes: bool = False,
        get_key_changes: bool = False,
        get_with_deltas: bool = True,                                         
        **kwargs
    ) -> TaskGroup:
        """
        Build one EdFiToObjectStorage task per endpoint
        Bulk copy the data to its respective table in Snowflake.

        :param endpoints:
        :param group_id:
        :param destination_dir:
        :param table:
        :param get_deletes:
        :param get_key_changes:
        :param get_with_deltas:
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
                    task_id=f"get_last_change_versions_from_{self.database_type}",
                    endpoints=[(self.endpoint_configs[endpoint]['namespace'], endpoint) for endpoint in endpoints],
                    get_deletes=get_deletes,
                    get_key_changes=get_key_changes,
                    get_with_deltas=get_with_deltas
                )
                enabled_endpoints = self.xcom_pull_template_map_idx(get_cv_operator, 0)
            else:
                get_cv_operator = None
                enabled_endpoints = endpoints

            ### EDFI TO Object Storage: Output Tuple[endpoint, filename] per successful task
            pull_operators_list = []

            for endpoint in endpoints:

                pull_edfi_to_object_storage = EdFiToObjectStorageOperator(
                    task_id=endpoint,
                    edfi_conn_id=self.edfi_conn_id,
                    use_edfi_token_cache=self.use_edfi_token_cache,
                    resource=endpoint,

                    tmp_dir=self.tmp_dir,
                    object_storage_conn_id=self.object_storage_conn_id,
                    object_storage_type=self.object_storage_type,
                    destination_dir=destination_dir,
                    destination_filename=f"{endpoint}.jsonl",
                    
                    get_deletes=get_deletes,
                    get_key_changes=get_key_changes,
                    min_change_version=self.xcom_pull_template_get_key(get_cv_operator, endpoint) if get_cv_operator else None,
                    max_change_version=airflow_util.xcom_pull_template(self.newest_edfi_cv_task_id),
                    reverse_paging=self.get_deletes_cv_with_deltas if get_deletes else True,

                    # Optional config-specified run-attributes (overridden by those in configs)
                    **self.endpoint_configs[endpoint],

                    # Only run endpoints specified at DAG or delta-level.
                    enabled_endpoints=enabled_endpoints,

                    pool=self.pool,
                    trigger_rule='none_skipped',
                    dag=self.dag
                )

                pull_operators_list.append(pull_edfi_to_object_storage)

            ### COPY FROM OBJECT STORAGE TO DATABASE
            copy_stage_to_database = BulkObjectStorageToDatabaseOperator(
                task_id=f"copy_all_endpoints_into_{self.database_type}",
                tenant_code=self.tenant_code,
                api_year=self.api_year,
                
                resource=self.xcom_pull_template_map_idx(pull_operators_list, 0),
                table_name=table or self.xcom_pull_template_map_idx(pull_operators_list, 0),
                edfi_conn_id=self.edfi_conn_id,
                use_edfi_token_cache=self.use_edfi_token_cache,
              
                database_conn_id=self.database_conn_id,
                database_type=self.database_type,
                destination_key=self.xcom_pull_template_map_idx(pull_operators_list, 1),

                trigger_rule='all_done',
                dag=self.dag
            )

            ### UPDATE SNOWFLAKE CHANGE VERSIONS
            if self.use_change_version:
                update_cv_operator = self.build_change_version_update_operator(
                    task_id=f"update_change_versions_in_{self.database_type}",
                    endpoints=self.xcom_pull_template_map_idx(pull_operators_list, 0),
                    get_deletes=get_deletes,
                    get_key_changes=get_key_changes,
                    trigger_rule='all_success'
                )
            else:
                update_cv_operator = None

            ### Chain tasks into final task-group
            airflow_util.chain_tasks(get_cv_operator, pull_operators_list, copy_stage_to_database, update_cv_operator)

        return default_task_group


    def build_dynamic_edfi_to_database_task_group(self,
        endpoints: List[str],
        group_id: str,

        *,
        destination_dir: str,
        table: Optional[str] = None,
        get_deletes: bool = False,
        get_key_changes: bool = False,
        get_with_deltas: bool = True,              
        **kwargs
    ):
        """
        Build one EdFiToObjectStorage task per endpoint
        Bulk copy the data to its respective table in Snowflake.

        :param endpoints:
        :param group_id:
        :param destination_dir:
        :param table:
        :param get_deletes:
        :param get_key_changes:                    
        :param get_with_deltas:
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
                    task_id=f"get_last_change_versions_from_{self.database_type}",
                    endpoints=[(self.endpoint_configs[endpoint]['namespace'], endpoint) for endpoint in endpoints],
                    get_deletes=get_deletes,
                    get_key_changes=get_key_changes,
                    get_with_deltas=get_with_deltas
                )
                enabled_endpoints = self.xcom_pull_template_map_idx(get_cv_operator, 0)
                kwargs_dicts = get_cv_operator.output.map(lambda endpoint__cv: {
                    'resource': endpoint__cv[0],
                    'min_change_version': endpoint__cv[1],
                    'destination_filename': f"{endpoint__cv[0]}.jsonl",
                    **self.endpoint_configs[endpoint__cv[0]],
                })
            
            # Otherwise, iterate all endpoints.
            else:
                get_cv_operator = None
                enabled_endpoints = endpoints
                kwargs_dicts = list(map(lambda endpoint: {
                    'resource': endpoint,
                    'min_change_version': None,
                    'destination_filename': f"{endpoint}.jsonl",
                    **self.endpoint_configs[endpoint],
                }, endpoints))


            ### EDFI TO OBJECT STORAGE: Output Tuple[endpoint, filename] per successful task
            pull_edfi_to_object_storage = (EdFiToObjectStorageOperator
                .partial(
                    task_id=f"pull_dynamic_endpoints_to_{self.object_storage_type}",
                    map_index_template="""{{ task.resource }}""",
                    edfi_conn_id=self.edfi_conn_id,
                    use_edfi_token_cache=self.use_edfi_token_cache,

                    tmp_dir= self.tmp_dir,
                    object_storage_conn_id=self.object_storage_conn_id,
                    object_storage_type=self.object_storage_type,
                    destination_dir=destination_dir,

                    get_deletes=get_deletes,
                    get_key_changes=get_key_changes,
                    max_change_version=airflow_util.xcom_pull_template(self.newest_edfi_cv_task_id),
                    reverse_paging=self.get_deletes_cv_with_deltas if get_deletes else True,

                    # Only run endpoints specified at DAG or delta-level.
                    enabled_endpoints=enabled_endpoints,

                    sla=None,  # "SLAs are unsupported with mapped tasks."
                    pool=self.pool,
                    trigger_rule='none_skipped',
                    dag=self.dag
                )
                .expand_kwargs(kwargs_dicts)
            )

            ### COPY FROM OBJECT STORAGE TO DATABASE
            copy_stage_to_database = BulkObjectStorageToDatabaseOperator(
                task_id=f"copy_all_endpoints_into_{self.database_type}",
                tenant_code=self.tenant_code,
                api_year=self.api_year,

                resource=self.xcom_pull_template_map_idx(pull_edfi_to_object_storage, 0),
                table_name=table or self.xcom_pull_template_map_idx(pull_edfi_to_object_storage, 0),
                edfi_conn_id=self.edfi_conn_id,
                use_edfi_token_cache=self.use_edfi_token_cache,

                database_conn_id=self.database_conn_id,
                database_type=self.database_type,
                destination_key=self.xcom_pull_template_map_idx(pull_edfi_to_object_storage, 1),

                trigger_rule='all_done',
                dag=self.dag
            )

            ### UPDATE SNOWFLAKE CHANGE VERSIONS
            if self.use_change_version:
                update_cv_operator = self.build_change_version_update_operator(
                    task_id=f"update_change_versions_in_{self.database_type}",
                    endpoints=self.xcom_pull_template_map_idx(pull_edfi_to_object_storage, 0),
                    get_deletes=get_deletes,
                    get_key_changes=get_key_changes,
                    trigger_rule='all_success'
                )
            else:
                update_cv_operator = None

            ### Chain tasks into final task-group
            airflow_util.chain_tasks(get_cv_operator, pull_edfi_to_object_storage, copy_stage_to_database, update_cv_operator)

        return dynamic_task_group
    

    def build_bulk_edfi_to_database_task_group(self,
        endpoints: List[str],
        group_id: str,

        *,
        destination_dir: str,
        table: Optional[str] = None,
        get_deletes: bool = False,
        get_key_changes: bool = False,
        get_with_deltas: bool = True,
        **kwargs
    ):
        """
        Build one EdFiToObjectStorage task (with inner for-loop across endpoints).
        Bulk copy the data to its respective table in Snowflake.

        :param endpoints:
        :param group_id:
        :param destination_dir:
        :param table:
        :param get_deletes:
        :param get_key_changes:
        :param get_with_deltas:
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
                    endpoints=[(self.endpoint_configs[endpoint]['namespace'], endpoint) for endpoint in endpoints],
                    get_deletes=get_deletes,
                    get_key_changes=get_key_changes,
                    get_with_deltas=get_with_deltas
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

            ### EDFI TO OBJECT STORAGE: Output Dict[endpoint, filename] with all successful tasks
            # Build a dictionary of lists to pass into bulk operator.
            endpoint_config_lists = {
                key: [self.endpoint_configs[endpoint][key] for endpoint in endpoints]
                for key in self.DEFAULT_CONFIGS.keys()  # Create lists for all keys in config.
            }

            pull_edfi_to_object_storage = BulkEdFiToObjectStorageOperator(
                task_id=f"pull_all_endpoints_to_{self.object_storage_type}",
                edfi_conn_id=self.edfi_conn_id,
                use_edfi_token_cache=self.use_edfi_token_cache,

                tmp_dir=self.tmp_dir,
                object_storage_conn_id=self.object_storage_conn_id,
                object_storage_type=self.object_storage_type,
                destination_dir=destination_dir,
                
                get_deletes=get_deletes,
                get_key_changes=get_key_changes,
                max_change_version=airflow_util.xcom_pull_template(self.newest_edfi_cv_task_id),
                reverse_paging=self.get_deletes_cv_with_deltas if get_deletes else True,

                # Arguments that are required to be lists in Ed-Fi bulk-operator.
                resource=endpoints,  # List datatype initializes bulk child class
                min_change_version=min_change_versions,
                destination_filename=[f"{endpoint}.jsonl" for endpoint in endpoints],

                # Optional config-specified run-attributes (overridden by those in configs)
                **endpoint_config_lists,

                # Only run endpoints specified at DAG or delta-level.
                enabled_endpoints=enabled_endpoints,

                pool=self.pool,
                trigger_rule='none_skipped',
                dag=self.dag
            )

            ### COPY FROM OBJECT STORAGE TO DATABASE
            copy_stage_to_database = BulkObjectStorageToDatabaseOperator(
                task_id=f"copy_all_endpoints_into_{self.database_type}",
                tenant_code=self.tenant_code,
                api_year=self.api_year,

                resource=self.xcom_pull_template_map_idx(pull_edfi_to_object_storage, 0),
                table_name=table or self.xcom_pull_template_map_idx(pull_edfi_to_object_storage, 0),
                edfi_conn_id=self.edfi_conn_id,
                use_edfi_token_cache=self.use_edfi_token_cache,

                database_conn_id=self.database_conn_id,
                database_type=self.database_type,
                destination_key=self.xcom_pull_template_map_idx(pull_edfi_to_object_storage, 1),

                trigger_rule='none_skipped',  # Different trigger rule than default.
                dag=self.dag
            )

            ### UPDATE SNOWFLAKE CHANGE VERSIONS
            if self.use_change_version:
                update_cv_operator = self.build_change_version_update_operator(
                    task_id=f"update_change_versions_in_{self.database_type}",
                    endpoints=self.xcom_pull_template_map_idx(pull_edfi_to_object_storage, 0),
                    get_deletes=get_deletes,
                    get_key_changes=get_key_changes,
                    trigger_rule='all_success'
                )
            else:
                update_cv_operator = None

            ### Chain tasks into final task-group
            airflow_util.chain_tasks(get_cv_operator, pull_edfi_to_object_storage, copy_stage_to_database, update_cv_operator)

        return bulk_task_group


    def build_total_counts_task_group(self,
        endpoints: List[str],
        **kwargs
    ):
        """
        :param endpoints:
        :return:
        """
        if not endpoints:
            return None

        with TaskGroup(
            group_id="Ed-Fi Total Counts",
            prefix_group_id=False,
            parent_group=None,
            dag=self.dag
        ) as total_counts_task_group:
            
            get_total_counts = PythonOperator(
                task_id="get_edfi_total_counts",
                python_callable=total_counts.get_total_counts,
                op_kwargs={
                    'endpoints': [(self.endpoint_configs[endpoint]['namespace'], endpoint) for endpoint in endpoints],
                    'edfi_conn_id': self.edfi_conn_id,
                    'use_edfi_token_cache': self.use_edfi_token_cache,
                    'max_change_version': airflow_util.xcom_pull_template(self.newest_edfi_cv_task_id),
                },
                trigger_rule='none_failed',
                dag=self.dag
            )

            # Clear the database total counts table (if a full-refresh).
            delete_total_counts = PythonOperator(
                task_id=f"delete_previous_total_counts_in_{self.database_type}",
                python_callable=total_counts.delete_total_counts,
                op_kwargs={
                    'tenant_code': self.tenant_code,
                    'api_year': self.api_year,
                    'database_conn_id': self.database_conn_id,
                    'database_type': self.database_type,
                    'total_counts_table': self.total_counts_table,
                },
                dag=self.dag
            )

            load_total_counts = PythonOperator(
                task_id=f"load_total_counts_to_{self.database_type}", 
                python_callable=total_counts.insert_total_counts,
                op_kwargs={
                    'tenant_code': self.tenant_code,
                    'api_year': self.api_year,
                    'database_conn_id': self.database_conn_id,
                    'database_type': self.database_type,
                    'total_counts_table': self.total_counts_table,
                    'endpoint_counts': airflow_util.xcom_pull_template(get_total_counts),
                },
                trigger_rule='none_failed',
                provide_context=True,
                dag=self.dag,
                **kwargs
            )

            get_total_counts >> delete_total_counts >> load_total_counts

        return total_counts_task_group
