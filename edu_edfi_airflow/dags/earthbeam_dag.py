import os

from functools import partial
from typing import Callable, Optional

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup

from ea_airflow_util import slack_callbacks

from edu_edfi_airflow.dags.callables.s3 import local_filepath_to_s3
from edu_edfi_airflow.dags.dag_util.airflow_util import xcom_pull_template as pull_xcom
from edu_edfi_airflow.providers.earthbeam.operators import EarthmoverOperator, LightbeamOperator


class EarthbeamDAG:
    """
    Full Earthmover-Lightbeam DAG, with optional Python callable pre-processing.

    TODO:
    - S3-to-Snowflake COPY INTO
    - Post-run Lightbeam-to-Snowflake logging
    - Optional file hashing before initial S3
    """
    emlb_state_directory: str = '/efs/emlb'

    def __init__(self,
        run_type: str,

        *,
        pool: str = 'default_pool',
        slack_conn_id: str = None,

        **kwargs
    ):
        self.run_type = run_type
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


    def build_python_preprocessing_operator(self,
        python_callable: Optional[Callable] = None,
        **kwargs
    ) -> PythonOperator:
        """
        Optional Python preprocessing operator to run before Earthmover and Lightbeam.

        :param python_callable:
        :param kwargs:
        :return:
        """
        return PythonOperator(
            task_id=f"{self.run_type}__preprocess_python_callable",
            python_callable=python_callable,
            op_kwargs=kwargs or {},
            provide_context=True,
            pool=self.pool,
            dag=self.dag
        )


    def build_tenant_year_taskgroup(self,
        tenant_code: str,
        api_year: str,
        raw_dir: str,

        *,
        earthmover_kwargs: dict,

        edfi_conn_id     : Optional[str] = None,
        lightbeam_kwargs : Optional[dict] = None,

        s3_conn_id       : Optional[str] = None,
        s3_filepath      : Optional[str] = None,

        python_callable  : Optional[Callable] = None,
        python_kwargs    : Optional[dict] = None,

        snowflake_conn_id: Optional[str] = None,
        lightbeam_logging_table: Optional[str] = None
    ):
        """
        (Python) +-> Earthmover +-> (Lightbeam) -> (Snowflake: Lightbeam Logs)
                 \-> (S3: Raw)  \-> (S3: Earthmover Output)
                                \-> (Snowflake: Earthmover Output)

        Many steps are automatic based on arguments defined:
        * If `edfi_conn_id` is defined, use Lightbeam to post to ODS.
        * If `python_callable` is defined, run Python pre-process.
        * If `s3_conn_id` is defined, upload files raw and post-Earthmover.
        * If `snowflake_conn_id` is defined and `edfi_conn_id` is NOT defined, copy EM output into raw Snowflake tables.
        * If `lightbeam_logging_table` is defined, copy LB logs into Snowflake table.


        :param tenant_code:
        :param api_year:
        :param raw_dir:

        :param earthmover_kwargs:

        :param edfi_conn_id:
        :param lightbeam_kwargs:

        :param s3_conn_id:
        :param s3_filepath:

        :param python_callable:
        :param python_kwargs:

        :param snowflake_conn_id:
        :param lightbeam_logging_table:
        :return:
        """
        with TaskGroup(
            group_id=f"{tenant_code}_{api_year}__earthmover_to_lightbeam",
            prefix_group_id=False,
            dag=self.dag
        ) as tenant_year_task_group:

            ### PythonOperator Preprocess
            if python_callable:
                python_preprocess = PythonOperator(
                    task_id=f"{tenant_code}_{api_year}_preprocess_python",
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

                s3_raw_filepath = os.path.join(
                    s3_filepath, 'raw', tenant_code, self.run_type, api_year, '{{ ds_nodash }}', '{{ ts_nodash }}'
                )

                raw_to_s3 = PythonOperator(
                    task_id=f"{tenant_code}_{api_year}_upload_raw_to_s3",
                    python_callable=local_filepath_to_s3,
                    op_kwargs={
                        'local_filepath': pull_xcom(python_preprocess.task_id) if python_preprocess else raw_dir,
                        's3_destination_key': s3_raw_filepath,
                        's3_conn_id': s3_conn_id,
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
            em_output_dir = os.path.join(
                raw_dir, 'earthmover', tenant_code, self.run_type, api_year, '{{ ds_nodash }}', '{{ ts_nodash }}'
            )

            em_state_file = os.path.join(
                self.emlb_state_directory, tenant_code, self.run_type, api_year, 'earthmover.csv'
            )

            run_earthmover = EarthmoverOperator(
                task_id=f"{tenant_code}_{api_year}_run_earthmover",
                output_dir=em_output_dir,
                state_file=em_state_file,
                **earthmover_kwargs,
                pool=self.pool,
                dag=self.dag
            )


            ### LightbeamOperator
            if edfi_conn_id:
                lb_state_dir = os.path.join(
                    self.emlb_state_directory, tenant_code, self.run_type, api_year, 'lightbeam'
                )

                run_lightbeam = LightbeamOperator(
                    task_id=f"{tenant_code}_{api_year}_send_via_lightbeam",
                    data_dir=pull_xcom(run_earthmover.task_id),
                    state_dir=lb_state_dir,
                    edfi_conn_id=edfi_conn_id,
                    **lightbeam_kwargs or {},
                    pool=self.pool,
                    dag=self.dag
                )
            else:
                run_lightbeam = None


            ### Earthmover to S3
            if s3_conn_id:
                if not s3_filepath:
                    raise ValueError(
                        "Argument `s3_filepath` must be defined to upload transformed Earthmover files to S3."
                    )

                s3_em_filepath = os.path.join(
                    s3_filepath, 'earthmover', tenant_code, self.run_type, api_year, '{{ ds_nodash }}', '{{ ts_nodash }}'
                )

                em_to_s3 = PythonOperator(
                    task_id=f"{tenant_code}_{api_year}_upload_earthmover_to_s3",
                    python_callable=local_filepath_to_s3,
                    op_kwargs={
                        'local_filepath': pull_xcom(run_earthmover.task_id),
                        's3_destination_key': s3_em_filepath,
                        's3_conn_id': s3_conn_id,
                        'remove_local_filepath': False,
                        # TODO: Include local-filepath cleanup in final logs operation.
                    },
                    provide_context=True,
                    pool=self.pool,
                    dag=self.dag
                )
            else:
                em_to_s3 = None


            ### Earthmover to Snowflake
            if snowflake_conn_id and not edfi_conn_id:
                if not s3_conn_id:
                    raise Exception(
                        "S3 connection required to copy into Snowflake."
                    )
                pass
            em_to_snowflake = None


            ### Lightbeam logs to Snowflake
            if lightbeam_logging_table:
                if not (edfi_conn_id and snowflake_conn_id):
                    raise Exception(
                        "Ed-Fi connection and snowflake connection required to copy Lightbeam logs into Snowflake."
                    )
            log_lightbeam_to_snowflake = None


            ### Order TaskGroup
            python_preprocess >> [raw_to_s3, run_earthmover]
            run_earthmover >> [em_to_s3, run_lightbeam, em_to_snowflake]
            run_lightbeam >> log_lightbeam_to_snowflake

        return tenant_year_task_group


    # def build_s3_operator(self,
    #     tenant_code: str,
    #     api_year   : str,
    #     data_dir   : str,
    #     s3_conn_id : str,
    #     s3_filepath: str
    # ) -> PythonOperator:
    #     """
    #
    #     :param tenant_code:
    #     :param api_year:
    #     :param data_dir:
    #     :param s3_conn_id:
    #     :param s3_filepath:
    #     :return:
    #     """
    #     return PythonOperator(
    #         task_id=f"{tenant_code}_{api_year}_upload_to_s3",
    #         python_callable=local_filepath_to_s3,
    #         op_kwargs={
    #             'local_filepath': data_dir,
    #             's3_destination_key': s3_filepath,
    #             's3_conn_id': s3_conn_id,
    #             'remove_local_filepath': False,
    #             # TODO: Include local-filepath cleanup in final logs operation.
    #         },
    #         provide_context=True,
    #         pool=self.pool,
    #         dag=self.dag
    #     )
    #
    # def build_earthmover_operator(self,
    #     tenant_code: str,
    #     api_year   : str,
    #     raw_dir    : str,
    #     **kwargs
    # ) -> EarthmoverOperator:
    #     """
    #
    #     :param tenant_code:
    #     :param api_year:
    #     :param raw_dir:
    #     :param kwargs:
    #     :return:
    #     """
    #     output_dir = os.path.join(
    #         raw_dir, 'earthmover', tenant_code, self.run_type, api_year, '{{ ds_nodash }}', '{{ ts_nodash }}'
    #     )
    #
    #     return EarthmoverOperator(
    #         task_id=f"{tenant_code}_{api_year}_run_earthmover",
    #         output_dir=output_dir,
    #         **kwargs,
    #         pool=self.pool,
    #         dag=self.dag
    #     )
    #
    #
    # def build_lightbeam_operator(self,
    #     tenant_code : str,
    #     api_year    : str,
    #     data_dir    : str,
    #     edfi_conn_id: str,
    #     **kwargs
    # ) -> LightbeamOperator:
    #     """
    #
    #     :param tenant_code:
    #     :param api_year:
    #     :param data_dir:
    #     :param edfi_conn_id:
    #     :param kwargs:
    #     :return:
    #     """
    #     return LightbeamOperator(
    #         task_id=f"{tenant_code}_{api_year}_send_via_lightbeam",
    #         data_dir=data_dir,
    #         edfi_conn_id=edfi_conn_id,
    #         **kwargs,
    #         pool=self.pool,
    #         dag=self.dag
    #     )
    #
    #
    #
    #
    #
    # def build_earthmover_to_s3_taskgroup(self,
    #     tenant_code: str,
    #     api_year: str,
    #     raw_dir: str,
    #
    #     *,
    #     s3_conn_id : str,
    #     s3_filepath: str,
    #
    #     earthmover_kwargs: Optional[dict] = None
    # ) -> TaskGroup:
    #     """
    #
    #     :param tenant_code:
    #     :param api_year:
    #     :param raw_dir:
    #     :param s3_conn_id:
    #     :param s3_filepath:
    #     :param earthmover_kwargs:
    #     :return:
    #     """
    #     with TaskGroup(
    #         group_id=f"{tenant_code}_{api_year}__earthmover_to_s3",
    #         prefix_group_id=False,
    #         dag=self.dag
    #     ) as tg:
    #
    #         run_earthmover = self.build_earthmover_operator(
    #             tenant_code, api_year, raw_dir,
    #             **earthmover_kwargs
    #         )
    #
    #         write_to_s3 = self.build_s3_operator(
    #             tenant_code, api_year, data_dir=pull_xcom(run_earthmover.task_id),
    #             s3_conn_id=s3_conn_id, s3_filepath=s3_filepath
    #         )
    #
    #         run_earthmover >> write_to_s3
    #
    #     return tg
    #
    #
    # def build_earthbeam_taskgroup(self,
    #     tenant_code: str,
    #     api_year: str,
    #     raw_dir: str,
    #
    #     *,
    #     edfi_conn_id: str,
    #
    #     earthmover_kwargs: Optional[dict] = None,
    #     lightbeam_kwargs : Optional[dict] = None
    # ) -> TaskGroup:
    #     """
    #
    #     :param tenant_code:
    #     :param api_year:
    #     :param raw_dir:
    #     :param edfi_conn_id:
    #     :param earthmover_kwargs:
    #     :param lightbeam_kwargs:
    #     :return:
    #     """
    #     with TaskGroup(
    #         group_id=f"{tenant_code}_{api_year}__earthmover_to_lightbeam",
    #         prefix_group_id=False,
    #         dag=self.dag
    #     ) as tg:
    #
    #         run_earthmover = self.build_earthmover_operator(
    #             tenant_code, api_year, raw_dir,
    #             **earthmover_kwargs
    #         )
    #
    #         run_lightbeam = self.build_lightbeam_operator(
    #             tenant_code, api_year, data_dir=pull_xcom(run_earthmover.task_id),
    #             edfi_conn_id=edfi_conn_id,
    #             **lightbeam_kwargs
    #         )
    #
    #         run_earthmover >> run_lightbeam
    #
    #     return tg
    #
    #
    # def build_earthbeam_s3_taskgroup(self,
    #     tenant_code: str,
    #     api_year: str,
    #     raw_dir: str,
    #
    #     *,
    #     edfi_conn_id: str,
    #     s3_conn_id  : str,
    #     s3_filepath : str,
    #
    #     earthmover_kwargs: Optional[dict] = None,
    #     lightbeam_kwargs : Optional[dict] = None
    # ) -> TaskGroup:
    #     """
    #
    #     :param tenant_code:
    #     :param api_year:
    #     :param raw_dir:
    #     :param edfi_conn_id:
    #     :param s3_conn_id:
    #     :param s3_filepath:
    #     :param earthmover_kwargs:
    #     :param lightbeam_kwargs:
    #     :return:
    #     """
    #     with TaskGroup(
    #         group_id=f"{tenant_code}_{api_year}__earthmover_to_lightbeam_s3",
    #         prefix_group_id=False,
    #         dag=self.dag
    #     ) as tg:
    #
    #         run_earthmover = self.build_earthmover_operator(
    #             tenant_code, api_year, raw_dir,
    #             **earthmover_kwargs
    #         )
    #
    #         run_lightbeam = self.build_lightbeam_operator(
    #             tenant_code, api_year, data_dir=pull_xcom(run_earthmover.task_id),
    #             edfi_conn_id=edfi_conn_id,
    #             **lightbeam_kwargs
    #         )
    #
    #         write_to_s3 = self.build_s3_operator(
    #             tenant_code, api_year, data_dir=pull_xcom(run_earthmover.task_id),
    #             s3_conn_id=s3_conn_id, s3_filepath=s3_filepath
    #         )
    #
    #         run_earthmover >> [run_lightbeam, write_to_s3]
    #
    #     return tg
