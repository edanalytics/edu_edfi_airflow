from typing import Callable, List, Optional, Union
import logging
import os
import re


from airflow.decorators import task, task_group
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.helpers import chain
from airflow.utils.task_group import TaskGroup

import edfi_api_client
from ea_airflow_util import EACustomDAG

from edu_edfi_airflow.callables.s3 import local_filepath_to_s3, remove_filepaths
from edu_edfi_airflow.callables import airflow_util
from edu_edfi_airflow.providers.earthbeam.operators import EarthmoverOperator, LightbeamOperator
from edu_edfi_airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator


class EarthbeamDAG:
    """
    Full Earthmover-Lightbeam DAG, with optional Python callable pre-processing.
    """
    emlb_hash_directory   : str = '/efs/emlb/prehash'
    emlb_state_directory  : str = '/efs/emlb/state'
    emlb_results_directory: str = '/efs/emlb/results'
    raw_output_directory  : str = '/efs/tmp_storage/raw'
    em_output_directory   : str = '/efs/tmp_storage/earthmover'

    params_dict = {
        "force": Param(
            default=False,
            type="boolean",
            description="If true, passes `--force` flag to Earthmover and Lightbeam"
        ),
    }

    def __init__(self,
        run_type: str,

        *,
        earthmover_path: Optional[str] = None,
        lightbeam_path: Optional[str] = None,

        pool: str = 'default_pool',
        earthmover_pool: Optional[str] = None,
        lightbeam_pool: Optional[str] = None,

        fast_cleanup: bool = False,

        **kwargs
    ):
        self.run_type = run_type

        self.earthmover_path = earthmover_path
        self.lightbeam_path = lightbeam_path

        self.pool = pool
        self.earthmover_pool = earthmover_pool or self.pool
        self.lightbeam_pool = lightbeam_pool or self.pool

        self.fast_cleanup = fast_cleanup

        self.dag = EACustomDAG(params=self.params_dict, **kwargs)


    def build_local_raw_dir(self, tenant_code: str, api_year: int, grain_update: Optional[str] = None) -> str:
        """
        Helper function to force consistency when building raw filepathing in Python operators.

        :param tenant_code:
        :param api_year:
        :param grain_update:
        :return:
        """
        raw_dir = edfi_api_client.url_join(
            self.raw_output_directory,
            tenant_code, self.run_type, api_year, grain_update,
            '{{ ds_nodash }}', '{{ ts_nodash }}'
        )
        return raw_dir
    
    
    def list_files_in_raw_dir(self, raw_dir: str, file_pattern: Optional[str] = None, **context):
        """
        List files in a raw directory. Skip if no files found
        """
        if not os.path.exists(raw_dir):
            raise AirflowSkipException(f"Directory {raw_dir} not found.")
        
        return_files = []
        unmatched_files = []
        
        for root, _, files in os.walk(raw_dir):
            for file in files:
                if file_pattern and not re.search(file_pattern, file):
                    logging.warning(f"File does not match file_pattern: {file}")
                    unmatched_files.append(os.path.join(root, file))
                    continue

                return_files.append(os.path.join(root, file))

        # Push an additional XCom if one or more files failed the file-pattern check.
        if unmatched_files:
            files_string = '\n'.join(map(lambda file: f"  - {file}", unmatched_files))
            logging.warning(
                f"One or more files did not match file pattern `{file_pattern}`:\n{files_string}"
            )
            context['ti'].xcom_push(key='failed_files', value=unmatched_files)
        
        if not return_files:
            raise AirflowSkipException(f"No files were found in directory: {raw_dir}")

        return return_files
    
    # One or more endpoints can fail total-get count. Create a second operator to track that failed status.
    # This should NOT be necessary, but we encountered a bug where a downstream "none_skipped" task skipped with "upstream_failed" status.
    @staticmethod
    def fail_if_xcom(xcom_value, **context):
        if xcom_value:
            raise AirflowFailException(f"XCom value: {xcom_value}")
        else:
            raise AirflowSkipException  # Force a skip to not mark the taskgroup as a success when all tasks skip.

    def build_python_preprocessing_operator(self,
        python_callable: Callable,
        **kwargs
    ) -> PythonOperator:
        """
        Optional Python preprocessing operator to run before Earthmover and Lightbeam.

        :param python_callable:
        :param kwargs:
        :return:
        """
        callable_name = python_callable.__name__.strip('<>')  # Remove brackets around lambdas
        task_id = f"{self.run_type}__preprocess_python_callable__{callable_name}"

        return PythonOperator(
            task_id=task_id,
            python_callable=python_callable,
            op_kwargs=kwargs,
            provide_context=True,
            pool=self.pool,
            dag=self.dag
        )

    def build_bash_preprocessing_operator(self,
        bash_command: str,
        **kwargs
    ) -> PythonOperator:
        """
        Optional Bash preprocessing operator to run before Earthmover and Lightbeam.

        :param bash_command:
        :param kwargs:
        :return:
        """
        command_name = bash_command.split(" ")[0]  # First keyword of the bash-command
        task_id = f"{self.run_type}__preprocess_bash_script__{command_name}"

        return BashOperator(
            task_id=task_id,
            bash_command=bash_command,
            **kwargs,
            pool=self.pool,
            dag=self.dag
        )


    def build_tenant_year_taskgroup(self,
        tenant_code: str,
        api_year: int,
        raw_dir: str,

        *,
        grain_update: Optional[str] = None,
        group_id: Optional[str] = None,
        prefix_group_id: bool = False,

        database_conn_id: Optional[str] = None,
        earthmover_kwargs: Optional[dict] = None,

        edfi_conn_id: Optional[str] = None,
        lightbeam_kwargs: Optional[dict] = None,

        s3_conn_id: Optional[str] = None,
        s3_filepath: Optional[str] = None,

        python_callable: Optional[Callable] = None,
        python_kwargs: Optional[dict] = None,

        snowflake_conn_id: Optional[str] = None,
        logging_table: Optional[str] = None,

        ods_version: Optional[str] = None,
        data_model_version: Optional[str] = None,
        endpoints: Optional[List[str]] = None,
        full_refresh: bool = False,

        # Mapping of input environment variables to be injected into the taskgroup.
        input_file_mapping: Optional[dict] = None,

        **kwargs
    ):
        """
        (Python) -> (S3: Raw) -> Earthmover -> (S3: EM Output) -> (Snowflake: EM Logs) +-> (Lightbeam) -> (Snowflake: LB Logs) +-> Clean-up
                                                                                       +-> (Snowflake: EM Output)

        Many steps are automatic based on arguments defined:
        * If `edfi_conn_id` is defined, use Lightbeam to post to ODS.
        * If `python_callable` is defined, run Python pre-process.
        * If `s3_conn_id` is defined, upload files raw and post-Earthmover.
        * If `snowflake_conn_id` is defined and `edfi_conn_id` is NOT defined, copy EM output into raw Snowflake tables.
        * If `logging_table` is defined, copy EM and LB logs into Snowflake table.

        :param tenant_code:
        :param api_year:
        :param raw_dir:

        :param grain_update:
        :param group_id:
        :param prefix_group_id:

        :param database_conn_id:
        :param earthmover_kwargs:

        :param edfi_conn_id:
        :param lightbeam_kwargs:

        :param s3_conn_id:
        :param s3_filepath:

        :param python_callable:
        :param python_kwargs:

        :param snowflake_conn_id:
        :param logging_table:

        :param ods_version:
        :param data_model_version:
        :param endpoints:
        :param full_refresh:

        :param input_file_mapping:

        :return:
        """
        taskgroup_grain = f"{tenant_code}_{api_year}"
        if grain_update:
            taskgroup_grain += f"_{grain_update}"

        # Group ID can be defined manually or built dynamically
        if not group_id:
            group_id = f"{taskgroup_grain}__earthmover"

            # TaskGroups have three shapes:
            if edfi_conn_id:         # Earthmover-to-Lightbeam (with optional S3)
                group_id += "_to_lightbeam"
            elif snowflake_conn_id:  # Earthmover-to-Snowflake (through S3)
                group_id += "_to_snowflake"
            elif s3_conn_id:         # Earthmover-to-S3
                group_id += "_to_s3"


        with TaskGroup(
            group_id=group_id,
            prefix_group_id=prefix_group_id,
            dag=self.dag
        ) as tenant_year_task_group:

            # Dynamically build a task-order as tasks are defined.
            task_order = []

            ### PythonOperator Preprocess
            if python_callable:
                python_preprocess = PythonOperator(
                    task_id=f"{taskgroup_grain}_preprocess_python",
                    python_callable=python_callable,
                    op_kwargs=python_kwargs or {},
                    provide_context=True,
                    pool=self.pool,
                    dag=self.dag
                )
                task_order.append(python_preprocess)
            
            # Infer input file parameters if not explicitly defined.
            if not input_file_mapping:
                input_file_mapping = {
                    key: val for key, val in earthmover_kwargs.get("parameters", {}).items()
                    if key.lower().startswith("input_file")
                    and key.lower() != "input_filetype"
                }

            em_task_group = self.build_file_to_edfi_taskgroup(
                tenant_code=tenant_code,
                api_year=api_year,
                grain_update=grain_update,

                database_conn_id=database_conn_id,
                earthmover_kwargs=earthmover_kwargs,

                edfi_conn_id=edfi_conn_id,
                lightbeam_kwargs=lightbeam_kwargs,

                s3_conn_id=s3_conn_id,
                s3_filepath=s3_filepath,

                snowflake_conn_id=snowflake_conn_id,
                logging_table=logging_table,

                ods_version=ods_version,
                data_model_version=data_model_version,
                endpoints=endpoints,
                full_refresh=full_refresh,
            )(input_file_envs=input_file_mapping.keys(), input_filepaths=input_file_mapping.values())
            task_order.append(em_task_group)

        # Chain all defined operators into task-order.
        chain(*task_order)
        return tenant_year_task_group


    ### Dynamic Earthbeam across multiple files
    def build_dynamic_tenant_year_taskgroup(self,
        tenant_code: str,
        api_year: int,
        raw_dir: str,

        *,
        grain_update: Optional[str] = None,
        group_id: Optional[str] = None,
        prefix_group_id: bool = False,  # Deprecated and unused

        database_conn_id: Optional[str] = None,
        earthmover_kwargs: Optional[dict] = None,

        edfi_conn_id: Optional[str] = None,
        lightbeam_kwargs: Optional[dict] = None,
        
        # Unique to dynamic version: fails list task if any files don't match pattern.
        file_pattern: Optional[str] = None, 

        s3_conn_id: Optional[str] = None,
        s3_filepath: Optional[str] = None,

        python_callable: Optional[Callable] = None,
        python_kwargs: Optional[dict] = None,

        snowflake_conn_id: Optional[str] = None,
        logging_table: Optional[str] = None,

        ods_version: Optional[str] = None,
        data_model_version: Optional[str] = None,
        endpoints: Optional[List[str]] = None,
        full_refresh: bool = False,

        # Allows overwrite of expected environment variable.
        input_file_var: str = "INPUT_FILE",

        **kwargs
    ):
        """
        IMPORTANT: This approach assumes a template will use 'input_file_var' as its only input parameter!
        """
        taskgroup_grain = f"{tenant_code}_{api_year}"
        if grain_update:
            taskgroup_grain += f"_{grain_update}"

        # Group ID can be defined manually or built dynamically
        if not group_id:
            group_id = f"{taskgroup_grain}__earthmover"

            # TaskGroups have three shapes:
            if edfi_conn_id:         # Earthmover-to-Lightbeam (with optional S3)
                group_id += "_to_lightbeam"
            elif snowflake_conn_id:  # Earthmover-to-Snowflake (through S3)
                group_id += "_to_snowflake"
            elif s3_conn_id:         # Earthmover-to-S3
                group_id += "_to_s3"


        with TaskGroup(
            group_id=group_id,
            prefix_group_id=True,
            dag=self.dag
        ) as dynamic_tenant_year_task_group:

            # Dynamically build a task-order as tasks are defined.
            task_order = []

            ### PythonOperator Preprocess
            if python_callable:
                python_preprocess = PythonOperator(
                    task_id=f"preprocess_python",
                    python_callable=python_callable,
                    op_kwargs=python_kwargs or {},
                    provide_context=True,
                    pool=self.pool,
                    dag=self.dag
                )

                task_order.append(python_preprocess)
                # paths_to_clean.append(airflow_util.xcom_pull_template(python_preprocess.task_id))

            ## List Raw files for dynamic task mapping
            list_files_task = PythonOperator(
                task_id = f'list_files_in_dir',
                dag=self.dag,
                python_callable=self.list_files_in_raw_dir,
                op_kwargs={
                    'raw_dir': airflow_util.xcom_pull_template(python_preprocess.task_id) if python_callable else raw_dir,
                    'file_pattern': file_pattern,
                },
                provide_context=True
            )
            task_order.append(list_files_task)

            # Optional task to fail when unexpected files were found in ShareFile.
            if file_pattern:
                failed_sentinel = PythonOperator(
                    task_id=f"failed_file_pattern",
                    python_callable=self.fail_if_xcom,
                    op_args=[airflow_util.xcom_pull_template(list_files_task, key='failed_files')],
                    trigger_rule='all_done',
                    dag=self.dag
                )
                list_files_task >> failed_sentinel

            em_task_group = self.build_file_to_edfi_taskgroup(
                tenant_code=tenant_code,
                api_year=api_year,
                grain_update=grain_update,

                database_conn_id=database_conn_id,
                earthmover_kwargs=earthmover_kwargs,

                edfi_conn_id=edfi_conn_id,
                lightbeam_kwargs=lightbeam_kwargs,

                s3_conn_id=s3_conn_id,
                s3_filepath=s3_filepath,

                snowflake_conn_id=snowflake_conn_id,
                logging_table=logging_table,

                ods_version=ods_version,
                data_model_version=data_model_version,
                endpoints=endpoints,
                full_refresh=full_refresh,
            ).partial(input_file_envs=input_file_var).expand(
                input_filepaths=list_files_task.output
            )
            task_order.append(em_task_group)

        # Chain all defined operators into task-order.
        chain(*task_order)
        return dynamic_tenant_year_task_group


    def insert_earthbeam_result_to_logging_table(self,
        snowflake_conn_id: str,
        logging_table: str,
        results_filepath: str,

        tenant_code: str,
        api_year: int,
        grain_update: Optional[str] = None,
        **kwargs
    ):
        """

        :return:
        """
        # Short-circuit fail before imports.
        if not snowflake_conn_id:
            raise Exception(
                "Snowflake connection required to copy logs into Snowflake."
            )

        from airflow.exceptions import AirflowSkipException
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        # Assume the results file is overwritten at every run.
        # If not found, raise a skip-exception instead of failing.
        try:
            with open(results_filepath, 'r') as fp:
                results = fp.read()
        except FileNotFoundError:
            raise AirflowSkipException(
                f"Results file not found: {results_filepath}\n"
                "Did Earthmover/Lightbeam run without error?"
            )

        # Retrieve the database and schema from the Snowflake hook and build the insert-query.
        database, schema = airflow_util.get_snowflake_params_from_conn(snowflake_conn_id)

        grain_update_str = f"'{grain_update}'" if grain_update else "NULL"

        qry_insert_into = f"""
            INSERT INTO {database}.{schema}.{logging_table}
                (tenant_code, api_year, grain_update, run_type, run_date, run_timestamp, result)
            SELECT
                '{tenant_code}' AS tenant_code,
                '{api_year}' AS api_year,
                {grain_update_str} AS grain_update,
                '{self.run_type}' AS run_type,
                '{kwargs['ds']}' AS run_date,
                '{kwargs['ts']}' AS run_timestamp,
                PARSE_JSON($${results}$$) AS result
        """

        # Insert each row into the table, passing the values as parameters.
        snowflake_hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
        snowflake_hook.run(
            sql=qry_insert_into
        )
    
    @staticmethod
    def inject_parameters_into_kwargs(parameters: dict, earthmover_kwargs: Optional[dict]) -> dict:
        """
        Helper for injecting parameters into kwargs passed to Earthmover in dynamic runs.
        """
        copied_kwargs = dict(earthmover_kwargs or ())

        if not 'parameters' in copied_kwargs:
            copied_kwargs['parameters'] = {}

        copied_kwargs['parameters'].update(parameters)
        return copied_kwargs
    
    @staticmethod
    def get_filename(filepath: str) -> str:
        return os.path.splitext(os.path.basename(filepath))[0]

    def build_file_to_edfi_taskgroup(self,
        *,
        tenant_code: str,
        api_year: int,
        grain_update: Optional[str] = None,

        database_conn_id: Optional[str] = None,
        earthmover_kwargs: Optional[dict] = None,

        edfi_conn_id: Optional[str] = None,
        lightbeam_kwargs: Optional[dict] = None,

        s3_conn_id: Optional[str] = None,
        s3_filepath: Optional[str] = None,

        snowflake_conn_id: Optional[str] = None,
        logging_table: Optional[str] = None,

        ods_version: Optional[str] = None,
        data_model_version: Optional[str] = None,
        endpoints: Optional[List[str]] = None,
        full_refresh: bool = False,

        **kwargs
    ):
        @task_group(prefix_group_id=True, group_id="file_to_earthbeam", dag=self.dag)
        def file_to_edfi_taskgroup(input_file_envs: Union[str, List[str]], input_filepaths: Union[str, List[str]]):

            @task
            def upload_to_s3(filepaths: Union[str, List[str]], subdirectory: str, **context):
                if not s3_filepath:
                    raise ValueError(
                        "Argument `s3_filepath` must be defined to upload transformed Earthmover files to S3."
                    )

                s3_full_filepath = edfi_api_client.url_join(
                    s3_filepath, subdirectory,
                    tenant_code, self.run_type, api_year, grain_update,
                    '{{ ds_nodash }}', '{{ ts_nodash }}'
                )
                s3_full_filepath = context['task'].render_template(s3_full_filepath, context)

                filepaths = [filepaths] if isinstance(filepaths, str) else filepaths
                for filepath in filepaths:
                    filepath = context['task'].render_template(filepath, context)

                    local_filepath_to_s3(
                        s3_conn_id=s3_conn_id,
                        s3_destination_key=s3_full_filepath,
                        local_filepath=filepath,
                        remove_local_filepath=False
                    )

                return s3_full_filepath
            
            @task
            def log_to_snowflake(results_filepath: str, **context):
                return self.insert_earthbeam_result_to_logging_table(
                    snowflake_conn_id=snowflake_conn_id,
                    logging_table=logging_table,
                    results_filepath=results_filepath,
                    tenant_code=tenant_code,
                    api_year=api_year,
                    grain_update=grain_update,
                    **context
                )
            
            @task(multiple_outputs=True)
            def run_earthmover(input_file_envs: Union[str, List[str]], input_filepaths: Union[str, List[str]], **context):
                input_file_envs = [input_file_envs] if isinstance(input_file_envs, str) else input_file_envs
                input_filepaths = [input_filepaths] if isinstance(input_filepaths, str) else input_filepaths

                file_basename = self.get_filename(input_filepaths[0])
                env_mapping = dict(zip(input_file_envs, input_filepaths))
                
                em_output_dir = edfi_api_client.url_join(
                    self.em_output_directory,
                    tenant_code, self.run_type, api_year, grain_update,
                    '{{ ds_nodash }}', '{{ ts_nodash }}',
                    file_basename
                )
                em_output_dir = context['task'].render_template(em_output_dir, context)

                em_state_file = edfi_api_client.url_join(
                    self.emlb_state_directory,
                    tenant_code, self.run_type, api_year, grain_update,
                    file_basename, 'earthmover.csv'
                )

                em_results_file = edfi_api_client.url_join(
                    self.emlb_results_directory,
                    tenant_code, self.run_type, api_year, grain_update,
                    '{{ ds_nodash }}', '{{ ts_nodash }}',
                    file_basename, 'earthmover_results.json'
                ) if logging_table else None
                em_results_file = context['task'].render_template(em_results_file, context)

                earthmover_operator = EarthmoverOperator(
                    task_id=f"run_earthmover",
                    earthmover_path=self.earthmover_path,
                    output_dir=em_output_dir,
                    state_file=em_state_file,
                    database_conn_id=database_conn_id,
                    results_file=em_results_file,
                    **self.inject_parameters_into_kwargs(env_mapping, earthmover_kwargs),
                    pool=self.earthmover_pool,
                    dag=self.dag
                )
                
                return {
                    "data_dir": earthmover_operator.execute(**context),
                    "state_file": em_state_file,
                    "results_file": em_results_file,
                }
            
            @task(multiple_outputs=True)
            def run_lightbeam(data_dir: str, **context):
                dir_basename = self.get_filename(data_dir)

                lb_state_dir = edfi_api_client.url_join(
                    self.emlb_state_directory,
                    tenant_code, self.run_type, api_year, grain_update,
                    dir_basename, 'lightbeam'
                )

                lb_results_file = edfi_api_client.url_join(
                    self.emlb_results_directory,
                    tenant_code, self.run_type, api_year, grain_update,
                    '{{ ds_nodash }}', '{{ ts_nodash }}',
                    dir_basename, 'lightbeam_results.json'
                ) if logging_table else None
                lb_results_file = context['task'].render_template(lb_results_file, context)

                lightbeam_operator = LightbeamOperator(
                    task_id=f"send_via_lightbeam",
                    lightbeam_path=self.lightbeam_path,
                    data_dir=data_dir,
                    state_dir=lb_state_dir,
                    results_file=lb_results_file ,
                    edfi_conn_id=edfi_conn_id,
                    **(lightbeam_kwargs or {}),
                    pool=self.lightbeam_pool,
                    dag=self.dag
                )
                
                return {
                    "data_dir": lightbeam_operator.execute(**context),
                    "state_dir": lb_state_dir,
                    "results_file": lb_results_file,
                }
            
            @task
            def em_to_snowflake(s3_destination_dir: str, endpoint: str, **context):
                # Snowflake tables are snake_cased; Earthmover outputs are camelCased
                snake_endpoint = edfi_api_client.camel_to_snake(endpoint)
                camel_endpoint = edfi_api_client.snake_to_camel(endpoint)

                # Descriptors have their own table
                if 'descriptor' in snake_endpoint:
                    table_name = '_descriptors'
                else:
                    table_name = snake_endpoint

                sideload_op = S3ToSnowflakeOperator(
                    task_id=f"copy_s3_to_snowflake__{camel_endpoint}",

                    tenant_code=tenant_code,
                    api_year=api_year,
                    resource=f"{snake_endpoint}__{self.run_type}",
                    table_name=table_name,

                    s3_destination_dir=s3_destination_dir,
                    s3_destination_filename=f"{camel_endpoint}.jsonl",

                    snowflake_conn_id=snowflake_conn_id,
                    ods_version=ods_version,
                    data_model_version=data_model_version,
                    full_refresh=full_refresh,

                    dag=self.dag
                )

                return sideload_op.execute(context)
        
            @task_group(prefix_group_id=True, dag=self.dag)
            def sideload_to_stadium(s3_destination_dir: str):
                if not s3_conn_id:
                    raise Exception("S3 connection required to copy into Snowflake.")

                if not (ods_version and data_model_version):
                    raise Exception("ODS-bypass requires arguments `ods_version` and `data_model_version` to be defined.")

                if not endpoints:
                    raise Exception("No endpoints defined for ODS-bypass!")

                for endpoint in endpoints:
                    em_to_snowflake.override(task_id=f"copy_s3_to_snowflake__{endpoint}")(s3_destination_dir, endpoint)

            @task(trigger_rule="all_done" if self.fast_cleanup else "all_success")
            def remove_files(filepaths):
                unnested_filepaths = []
                for filepath in filepaths:
                    if isinstance(filepath, str):
                        unnested_filepaths.append(filepath)
                    else:
                        unnested_filepaths.extend(filepath)
                
                return remove_filepaths(unnested_filepaths)


            all_tasks = []  # Track all tasks to apply cleanup at the very end
            paths_to_clean = [input_filepaths]

            # Raw to S3
            if s3_conn_id:
                upload_to_s3.override(task_id=f"upload_raw_to_s3")(input_filepaths, "raw")
                all_tasks.append(upload_to_s3)
                
            # EarthmoverOperator: Required
            earthmover_results = run_earthmover(input_file_envs, input_filepaths)
            all_tasks.append(earthmover_results)
            paths_to_clean.append(earthmover_results["data_dir"])

            # Earthmover logs to Snowflake
            if logging_table:
                log_to_snowflake.override(task_id="log_em_to_snowflake")(earthmover_results["results_file"])
                all_tasks.append(log_to_snowflake)

            # Earthmover to S3
            if s3_conn_id:
                em_s3_filepath = upload_to_s3.override(task_id="upload_em_to_s3")(earthmover_results["data_dir"], "earthmover")
                all_tasks.append(em_s3_filepath)

                # Option 1: Bypass the ODS and sideload into Stadium
                if snowflake_conn_id and not edfi_conn_id:
                    sideload_taskgroup = sideload_to_stadium(em_s3_filepath)
                    all_tasks.append(sideload_taskgroup)

            # Option 2: LightbeamOperator
            if edfi_conn_id:
                lightbeam_results = run_lightbeam(earthmover_results["data_dir"])
                all_tasks.append(lightbeam_results)

                # Lightbeam logs to Snowflake
                if logging_table:
                    log_to_snowflake.override(task_id="log_lb_to_snowflake")(lightbeam_results["results_file"])
                    all_tasks.append(log_to_snowflake)

            # Final cleanup (apply at very end of the taskgroup)
            remove_files_operator = remove_files(paths_to_clean)
            all_tasks[-1] >> remove_files_operator

        return file_to_edfi_taskgroup
