from functools import partial
from typing import Callable, List, Optional

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.helpers import chain
from airflow.utils.task_group import TaskGroup

import edfi_api_client
from ea_airflow_util import slack_callbacks

from edu_edfi_airflow.dags.callables.s3 import local_filepath_to_s3, remove_filepaths
from edu_edfi_airflow.dags.dag_util import airflow_util
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
        slack_conn_id: Optional[str] = None,

        fast_cleanup: bool = False,

        **kwargs
    ):
        self.run_type = run_type

        self.earthmover_path = earthmover_path
        self.lightbeam_path = lightbeam_path

        self.pool = pool
        self.earthmover_pool = earthmover_pool or self.pool
        self.lightbeam_pool = lightbeam_pool or self.pool
        self.slack_conn_id = slack_conn_id

        self.fast_cleanup = fast_cleanup

        self.dag = self.initialize_dag(**kwargs)


    def initialize_dag(self,
        dag_id: str,
        schedule_interval: str,
        default_args: dict,
        **kwargs
    ):
        """

        :param dag_id:
        :param schedule_interval:
        :param default_args:
        :param kwargs:
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
            **kwargs
        )


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
            provide_context=True,
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
            paths_to_clean = []

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
                paths_to_clean.append(airflow_util.xcom_pull_template(python_preprocess.task_id))


            ### Raw to S3
            if s3_conn_id:
                if not s3_filepath:
                    raise ValueError(
                        "Argument `s3_filepath` must be defined to upload raw files to S3."
                    )

                s3_raw_filepath = edfi_api_client.url_join(
                    s3_filepath, 'raw',
                    tenant_code, self.run_type, api_year, grain_update,
                    '{{ ds_nodash }}', '{{ ts_nodash }}'
                )

                raw_to_s3 = PythonOperator(
                    task_id=f"{taskgroup_grain}_upload_raw_to_s3",
                    python_callable=local_filepath_to_s3,
                    op_kwargs={
                        's3_conn_id': s3_conn_id,
                        's3_destination_key': s3_raw_filepath,
                        'local_filepath': airflow_util.xcom_pull_template(python_preprocess.task_id) if python_callable else raw_dir,
                        'remove_local_filepath': False,
                    },
                    provide_context=True,
                    pool=self.pool,
                    dag=self.dag
                )

                task_order.append(raw_to_s3)


            ### EarthmoverOperator: Required
            em_output_dir = edfi_api_client.url_join(
                self.em_output_directory,
                tenant_code, self.run_type, api_year, grain_update,
                '{{ ds_nodash }}', '{{ ts_nodash }}'
            )

            em_state_file = edfi_api_client.url_join(
                self.emlb_state_directory,
                tenant_code, self.run_type, api_year, grain_update,
                'earthmover.csv'
            )

            em_results_file = edfi_api_client.url_join(
                self.emlb_results_directory,
                tenant_code, self.run_type, api_year, grain_update,
                '{{ ds_nodash }}', '{{ ts_nodash }}',
                "earthmover_results.json"
            )

            run_earthmover = EarthmoverOperator(
                task_id=f"{taskgroup_grain}_run_earthmover",
                earthmover_path=self.earthmover_path,
                output_dir=em_output_dir,
                state_file=em_state_file,
                results_file=em_results_file if logging_table else None,
                **(earthmover_kwargs or {}),
                pool=self.earthmover_pool,
                dag=self.dag
            )

            task_order.append(run_earthmover)
            paths_to_clean.append(airflow_util.xcom_pull_template(run_earthmover.task_id))


            ### Earthmover logs to Snowflake
            if logging_table:
                if not snowflake_conn_id:
                    raise Exception(
                        "Snowflake connection required to copy Earthmover logs into Snowflake."
                    )

                log_earthmover_to_snowflake = PythonOperator(
                    task_id=f"{taskgroup_grain}_log_earthmover_to_snowflake",
                    python_callable=self.insert_earthbeam_result_to_logging_table,
                    op_kwargs={
                        'snowflake_conn_id': snowflake_conn_id,
                        'logging_table': logging_table,
                        'results_filepath': em_results_file,

                        'tenant_code': tenant_code,
                        'api_year': api_year,
                        'grain_update': grain_update,
                    },
                    provide_context=True,
                    pool=self.pool,
                    trigger_rule="all_done",
                    dag=self.dag
                )

                task_order.append(log_earthmover_to_snowflake)


            ### Earthmover to S3
            if s3_conn_id:
                if not s3_filepath:
                    raise ValueError(
                        "Argument `s3_filepath` must be defined to upload transformed Earthmover files to S3."
                    )

                s3_em_filepath = edfi_api_client.url_join(
                    s3_filepath, 'earthmover',
                    tenant_code, self.run_type, api_year, grain_update,
                    '{{ ds_nodash }}', '{{ ts_nodash }}'
                )

                em_to_s3 = PythonOperator(
                    task_id=f"{taskgroup_grain}_upload_earthmover_to_s3",
                    python_callable=local_filepath_to_s3,
                    op_kwargs={
                        's3_conn_id': s3_conn_id,
                        's3_destination_key': s3_em_filepath,
                        'local_filepath': airflow_util.xcom_pull_template(run_earthmover.task_id),
                        'remove_local_filepath': False,
                    },
                    provide_context=True,
                    pool=self.pool,
                    dag=self.dag
                )

                task_order.append(em_to_s3)


            ### LightbeamOperator
            if edfi_conn_id:
                lb_state_dir = edfi_api_client.url_join(
                    self.emlb_state_directory,
                    tenant_code, self.run_type, api_year, grain_update,
                    'lightbeam'
                )

                lb_results_file = edfi_api_client.url_join(
                    self.emlb_results_directory,
                    tenant_code, self.run_type, api_year, grain_update,
                    '{{ ds_nodash }}', '{{ ts_nodash }}',
                    "lightbeam_results.json"
                )

                run_lightbeam = LightbeamOperator(
                    task_id=f"{taskgroup_grain}_send_via_lightbeam",
                    lightbeam_path=self.lightbeam_path,
                    data_dir=airflow_util.xcom_pull_template(run_earthmover.task_id),
                    state_dir=lb_state_dir,
                    results_file=lb_results_file if logging_table else None,
                    edfi_conn_id=edfi_conn_id,
                    **(lightbeam_kwargs or {}),
                    pool=self.lightbeam_pool,
                    dag=self.dag
                )

                task_order.append(run_lightbeam)

                ### Lightbeam logs to Snowflake
                if logging_table:
                    if not snowflake_conn_id:
                        raise Exception(
                            "Snowflake connection required to copy Lightbeam logs into Snowflake."
                        )

                    log_lightbeam_to_snowflake = PythonOperator(
                        task_id=f"{taskgroup_grain}_log_lightbeam_to_snowflake",
                        python_callable=self.insert_earthbeam_result_to_logging_table,
                        op_kwargs={
                            'snowflake_conn_id': snowflake_conn_id,
                            'logging_table': logging_table,
                            'results_filepath': lb_results_file,

                            'tenant_code': tenant_code,
                            'api_year': api_year,
                            'grain_update': grain_update,
                        },
                        provide_context=True,
                        pool=self.pool,
                        trigger_rule="all_done",
                        dag=self.dag
                    )

                    task_order.append(log_lightbeam_to_snowflake)


            ### Alternate route: Bypassing the ODS directly into Snowflake
            if snowflake_conn_id and not edfi_conn_id:
                if not s3_conn_id:
                    raise Exception(
                        "S3 connection required to copy into Snowflake."
                    )

                if not (ods_version and data_model_version):
                    raise Exception(
                        "ODS-bypass requires arguments `ods_version` and `data_model_version` to be defined."
                    )

                if not endpoints:
                    raise Exception(
                        "No endpoints defined for ODS-bypass!"
                    )

                with TaskGroup(
                    group_id=f"{taskgroup_grain}_copy_s3_to_snowflake",
                    prefix_group_id=False,
                    dag=self.dag
                ) as s3_to_snowflake_task_group:

                    for endpoint in endpoints:
                        # Snowflake tables are snake_cased; Earthmover outputs are camelCased
                        snake_endpoint = edfi_api_client.camel_to_snake(endpoint)
                        camel_endpoint = edfi_api_client.snake_to_camel(endpoint)

                        endpoint_output_path = edfi_api_client.url_join(
                            airflow_util.xcom_pull_template(em_to_s3.task_id),
                            camel_endpoint + ".jsonl"
                        )

                        # Descriptors have their own table
                        if 'descriptor' in snake_endpoint:
                            table_name = '_descriptors'
                        else:
                            table_name = snake_endpoint

                        em_to_snowflake = S3ToSnowflakeOperator(
                            task_id=f"{taskgroup_grain}_copy_s3_to_snowflake__{camel_endpoint}",

                            tenant_code=tenant_code,
                            api_year=api_year,
                            resource=f"{snake_endpoint}__{self.run_type}",
                            table_name=table_name,

                            s3_destination_key=endpoint_output_path,

                            snowflake_conn_id=snowflake_conn_id,
                            ods_version=ods_version,
                            data_model_version=data_model_version,
                            full_refresh=full_refresh,

                            dag=self.dag
                        )

                task_order.append(s3_to_snowflake_task_group)


            ### Final cleanup
            cleanup_local_disk = PythonOperator(
                task_id=f"{taskgroup_grain}_cleanup_disk",
                python_callable=remove_filepaths,
                op_kwargs={
                    "filepaths": paths_to_clean,
                },
                provide_context=True,
                pool=self.pool,
                trigger_rule="all_done" if self.fast_cleanup else "all_success",
                dag=self.dag
            )

            task_order.append(cleanup_local_disk)

            # Chain all defined operators into task-order.
            chain(*task_order)

        return tenant_year_task_group


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
                PARSE_JSON('{results}') AS result
        """

        # Insert each row into the table, passing the values as parameters.
        snowflake_hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
        snowflake_hook.run(
            sql=qry_insert_into
        )
