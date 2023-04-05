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

    """
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

    def build_earthmover_operator(self,
        tenant_code: str,
        api_year   : str,
        tmp_dir    : str,
        **kwargs
    ) -> EarthmoverOperator:
        """

        :param tenant_code:
        :param api_year:
        :param tmp_dir:
        :param kwargs:
        :return:
        """
        output_dir = os.path.join(
            tmp_dir, 'earthmover', tenant_code, self.run_type, api_year, '{{ ds_nodash }}', '{{ ts_nodash }}'
        )

        return EarthmoverOperator(
            task_id=f"{tenant_code}_{api_year}_run_earthmover",
            output_dir=output_dir,
            **kwargs,
            pool=self.pool,
            dag=self.dag
        )


    def build_lightbeam_operator(self,
        tenant_code : str,
        api_year    : str,
        data_dir    : str,
        edfi_conn_id: str,
        **kwargs
    ) -> LightbeamOperator:
        """

        :param tenant_code:
        :param api_year:
        :param data_dir:
        :param edfi_conn_id:
        :param kwargs:
        :return:
        """
        return LightbeamOperator(
            task_id=f"{tenant_code}_{api_year}_send_via_lightbeam",
            data_dir=data_dir,
            edfi_conn_id=edfi_conn_id,
            **kwargs,
            pool=self.pool,
            dag=self.dag
        )


    def build_s3_operator(self,
        tenant_code: str,
        api_year   : str,
        data_dir   : str,
        s3_conn_id : str,
        s3_filepath: str
    ) -> PythonOperator:
        """

        :param tenant_code:
        :param api_year:
        :param data_dir:
        :param s3_conn_id:
        :param s3_filepath:
        :return:
        """
        return PythonOperator(
            task_id=f"{tenant_code}_{api_year}_upload_to_s3",
            python_callable=local_filepath_to_s3,
            op_kwargs={
                'local_filepath': data_dir,
                's3_destination_key': s3_filepath,
                's3_conn_id': s3_conn_id,
                'remove_local_filepath': False,
                # TODO: Include local-filepath cleanup in final logs operation.
            },
            provide_context=True,
            pool=self.pool,
            dag=self.dag
        )


    def build_earthmover_to_s3_taskgroup(self,
        tenant_code: str,
        api_year: str,
        tmp_dir: str,

        *,
        s3_conn_id : str,
        s3_filepath: str,

        earthmover_kwargs: Optional[dict] = None
    ) -> TaskGroup:
        """

        :param tenant_code:
        :param api_year:
        :param tmp_dir:
        :param s3_conn_id:
        :param s3_filepath:
        :param earthmover_kwargs:
        :return:
        """
        with TaskGroup(
            group_id=f"{tenant_code}_{api_year}__earthmover_to_s3",
            prefix_group_id=False,
            dag=self.dag
        ) as tg:

            run_earthmover = self.build_earthmover_operator(
                tenant_code, api_year, tmp_dir,
                **earthmover_kwargs
            )

            write_to_s3 = self.build_s3_operator(
                tenant_code, api_year, data_dir=pull_xcom(run_earthmover.task_id),
                s3_conn_id=s3_conn_id, s3_filepath=s3_filepath
            )

            run_earthmover >> write_to_s3

        return tg


    def build_earthbeam_taskgroup(self,
        tenant_code: str,
        api_year: str,
        tmp_dir: str,

        *,
        edfi_conn_id: str,

        earthmover_kwargs: Optional[dict] = None,
        lightbeam_kwargs : Optional[dict] = None
    ) -> TaskGroup:
        """

        :param tenant_code:
        :param api_year:
        :param tmp_dir:
        :param edfi_conn_id:
        :param earthmover_kwargs:
        :param lightbeam_kwargs:
        :return:
        """
        with TaskGroup(
            group_id=f"{tenant_code}_{api_year}__earthmover_to_lightbeam",
            prefix_group_id=False,
            dag=self.dag
        ) as tg:

            run_earthmover = self.build_earthmover_operator(
                tenant_code, api_year, tmp_dir,
                **earthmover_kwargs
            )

            run_lightbeam = self.build_lightbeam_operator(
                tenant_code, api_year, data_dir=pull_xcom(run_earthmover.task_id),
                edfi_conn_id=edfi_conn_id,
                **lightbeam_kwargs
            )

            run_earthmover >> run_lightbeam

        return tg


    def build_earthbeam_s3_taskgroup(self,
        tenant_code: str,
        api_year: str,
        tmp_dir: str,

        *,
        edfi_conn_id: str,
        s3_conn_id  : str,
        s3_filepath : str,

        earthmover_kwargs: Optional[dict] = None,
        lightbeam_kwargs : Optional[dict] = None
    ) -> TaskGroup:
        """

        :param tenant_code:
        :param api_year:
        :param tmp_dir:
        :param edfi_conn_id:
        :param s3_conn_id:
        :param s3_filepath:
        :param earthmover_kwargs:
        :param lightbeam_kwargs:
        :return:
        """
        with TaskGroup(
            group_id=f"{tenant_code}_{api_year}__earthmover_to_lightbeam_s3",
            prefix_group_id=False,
            dag=self.dag
        ) as tg:

            run_earthmover = self.build_earthmover_operator(
                tenant_code, api_year, tmp_dir,
                **earthmover_kwargs
            )

            run_lightbeam = self.build_lightbeam_operator(
                tenant_code, api_year, data_dir=pull_xcom(run_earthmover.task_id),
                edfi_conn_id=edfi_conn_id,
                **lightbeam_kwargs
            )

            write_to_s3 = self.build_s3_operator(
                tenant_code, api_year, data_dir=pull_xcom(run_earthmover.task_id),
                s3_conn_id=s3_conn_id, s3_filepath=s3_filepath
            )

            run_earthmover >> [run_lightbeam, write_to_s3]

        return tg
