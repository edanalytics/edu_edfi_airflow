import datetime
import os

from functools import partial
from typing import Callable, Optional

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.helpers import chain
from airflow.utils.task_group import TaskGroup

import edfi_api_client
from ea_airflow_util import slack_callbacks

from edu_edfi_airflow.dags.callables.s3 import local_filepath_to_s3
from edu_edfi_airflow.dags.dag_util import airflow_util
from edu_edfi_airflow.providers.earthbeam.operators import EarthmoverOperator, LightbeamOperator
from edu_edfi_airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator


class EarthbeamDAG:
    """
    Full Earthmover-Lightbeam DAG, with optional Python callable pre-processing.

    TODO:
    - S3-to-Snowflake COPY INTO
      * Post-Earthmover, {resource}.jsonl files are saved to OUTPUT_DIR; how do we ascertain namespace?
    - Post-run Lightbeam-to-Snowflake logging
    - Optional file hashing before initial S3
    """
    # TODO: Should these be user-definable or static?
    emlb_hash_directory : str = '/efs/emlb/prehash'
    emlb_state_directory: str = '/efs/emlb/state'
    raw_output_directory: str = '/efs/tmp_storage/raw'
    em_output_directory : str = '/efs/tmp_storage/earthmover'

    def __init__(self,
        run_type: str,

        *,
        earthmover_path: Optional[str] = None,
        lightbeam_path: Optional[str] = None,

        pool: str = 'default_pool',
        slack_conn_id: str = None,

        **kwargs
    ):
        self.run_type = run_type

        self.earthmover_path = earthmover_path
        self.lightbeam_path = lightbeam_path

        self.pool = pool
        self.slack_conn_id = slack_conn_id

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
        now = datetime.datetime.now()

        raw_dir = edfi_api_client.url_join(
            self.raw_output_directory,
            tenant_code, self.run_type, api_year, grain_update,
            now.strftime("%Y%m%d"), now.strftime("%Y%m%dT%H%M%S")
        )
        os.makedirs(raw_dir, exist_ok=True)
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

        edfi_conn_id     : Optional[str] = None,

        earthmover_kwargs: Optional[dict] = None,
        lightbeam_kwargs : Optional[dict] = None,

        s3_conn_id       : Optional[str] = None,
        s3_filepath      : Optional[str] = None,

        python_callable  : Optional[Callable] = None,
        python_kwargs    : Optional[dict] = None,

        snowflake_conn_id: Optional[str] = None,
        lightbeam_logging_table: Optional[str] = None,

        **kwargs
    ):
        """
        (Python) -> (S3: Raw) -> Earthmover -> (S3: Earthmover Output) +-> (Lightbeam) -> (Snowflake: Lightbeam Logs)
                                                                       +-> (Snowflake: Earthmover Output)

        Many steps are automatic based on arguments defined:
        * If `edfi_conn_id` is defined, use Lightbeam to post to ODS.
        * If `python_callable` is defined, run Python pre-process.
        * If `s3_conn_id` is defined, upload files raw and post-Earthmover.
        * If `snowflake_conn_id` is defined and `edfi_conn_id` is NOT defined, copy EM output into raw Snowflake tables.
        * If `lightbeam_logging_table` is defined, copy LB logs into Snowflake table.

        :param tenant_code:
        :param api_year:
        :param raw_dir:

        :param grain_update:
        :param group_id:
        :param prefix_group_id:

        :param edfi_conn_id:

        :param earthmover_kwargs:
        :param lightbeam_kwargs:

        :param s3_conn_id:
        :param s3_filepath:

        :param python_callable:
        :param python_kwargs:

        :param snowflake_conn_id:
        :param lightbeam_logging_table:
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
            else:
                python_preprocess = None


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
                        'local_filepath': airflow_util.xcom_pull_template(python_preprocess.task_id) if python_preprocess else raw_dir,
                        'remove_local_filepath': False,
                        # TODO: Include local-filepath cleanup in final logs operation.
                    },
                    provide_context=True,
                    pool=self.pool,
                    dag=self.dag
                )
            else:
                raw_to_s3 = None


            ### EarthmoverOperator
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

            run_earthmover = EarthmoverOperator(
                task_id=f"{taskgroup_grain}_run_earthmover",
                earthmover_path=self.earthmover_path,
                output_dir=em_output_dir,
                state_file=em_state_file,
                **(earthmover_kwargs or {}),
                pool=self.pool,
                dag=self.dag
            )


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
                        # TODO: Include local-filepath cleanup in final logs operation.
                    },
                    provide_context=True,
                    pool=self.pool,
                    dag=self.dag
                )
            else:
                em_to_s3 = None


            ### LightbeamOperator
            if edfi_conn_id:
                lb_state_dir = edfi_api_client.url_join(
                    self.emlb_state_directory,
                    tenant_code, self.run_type, api_year, grain_update,
                    'lightbeam'
                )

                run_lightbeam = LightbeamOperator(
                    task_id=f"{taskgroup_grain}_send_via_lightbeam",
                    lightbeam_path=self.lightbeam_path,
                    data_dir=airflow_util.xcom_pull_template(run_earthmover.task_id),
                    state_dir=lb_state_dir,
                    edfi_conn_id=edfi_conn_id,
                    **(lightbeam_kwargs or {}),
                    pool=self.pool,
                    dag=self.dag
                )
            else:
                run_lightbeam = None

            ### Lightbeam logs to Snowflake
            if lightbeam_logging_table:
                if not (edfi_conn_id and snowflake_conn_id):
                    raise Exception(
                        "Ed-Fi connection and snowflake connection required to copy Lightbeam logs into Snowflake."
                    )
            log_lightbeam_to_snowflake = None


            ### Default route: Earthmover-to-Lightbeam
            task_order = (
                python_preprocess, raw_to_s3,
                run_earthmover, em_to_s3,
                run_lightbeam, log_lightbeam_to_snowflake
            )

            chain(*filter(None, task_order))  # Chain all defined operators into task-order.


            ### Alternate route: Bypassing the ODS directly into Snowflake
            if snowflake_conn_id and not edfi_conn_id:
                if not s3_conn_id:
                    raise Exception(
                        "S3 connection required to copy into Snowflake."
                    )

                # TODO: How do we resolve the resources that need to be loaded into Snowflake?
                # for resource in resources:
                #
                #     em_to_snowflake = S3ToSnowflakeOperator(
                #         task_id=f"{taskgroup_grain}_copy_s3_to_snowflake__{resource}",
                #
                #         tenant_code=tenant_code,
                #         api_year=api_year,
                #         resource=f"{resource}__{self.run_type}",
                #         table_name=resource,
                #
                #         s3_destination_key=airflow_util.xcom_pull_template(em_to_s3.task_id),
                #
                #         snowflake_conn_id=snowflake_conn_id,
                #         ods_version="",
                #         data_model_version="",
                #     )
                #
                #     em_to_s3 >> em_to_snowflake

        return tenant_year_task_group