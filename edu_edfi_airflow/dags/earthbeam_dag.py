from functools import partial
from typing import Callable, Optional

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup

from ea_airflow_util import slack_callbacks
from ea_airflow_util import EarthmoverOperator, LightbeamOperator

from edu_edfi_airflow.providers.earthbeam.operators import EarthmoverOperator, LightbeamOperator
from edu_edfi_airflow.dags.callables.s3 import local_filepath_to_s3



class EarthbeamDAG:
    """
    Full Earthmover-Lightbeam DAG, with optional Python callable pre-processing.

    """
    def __init__(self,
        *,
        run_type: str,
        api_year: int,

        pool: str,
        tmp_dir: str,

        slack_conn_id: str = None,

        **kwargs
    ):
        self.run_type = run_type
        self.api_year = api_year

        self.pool = pool
        self.tmp_dir = tmp_dir

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
        )


    def build_python_preprocessing_operator(self,
        python_callable: Optional[Callable] = None,
        **kwargs
    ):
        """
        Optional Python preprocessing operator to run before Earthmover and Lightbeam.

        :param python_callable:
        :param kwargs:
        :return:
        """
        return PythonOperator(
            task_id=f"{self.run_type}_python_callable",
            python_callable=python_callable,
            op_kwargs=kwargs or {},
            provide_context=True,
            dag=self.dag
        )


    def build_earthbeam_taskgroup(self,
        tenant_code : str,
        *,
        earthmover_kwargs: Optional[dict] = None,

        lightbeam: bool = True,
        edfi_conn_id: Optional[str] = None,
        lightbeam_kwargs : Optional[dict] = None,

        s3_conn_id : Optional[str] = None,
        s3_filepath: Optional[str] = None
    ) -> TaskGroup:
        """
        Earthmover -> Lightbeam
                   -> (AWS S3)

        :param tenant_code:
        :param earthmover_kwargs:
        :param lightbeam:
        :param edfi_conn_id:
        :param lightbeam_kwargs:
        :param s3_conn_id:
        :param s3_filepath:
        :return:
        """
        # Label the type of run in the TaskGroup group-ID
        if lightbeam and s3_filepath:
            group_id = f"earthmover_to_lightbeam_s3__{tenant_code}"
        elif lightbeam:
            group_id = f"earthmover_to_lightbeam__{tenant_code}"
        elif s3_filepath:
            group_id = f"earthmover_to_s3__{tenant_code}"
        else:
            group_id = f"earthmover__{tenant_code}"

        # Define the task group, with optional final elements as needed
        with TaskGroup(
            group_id=group_id,
            prefix_group_id=False,
            dag=self.dag
        ) as earthbeam_task_group:

            ### EARTHMOVER OPERATOR (required)
            run_earthmover = EarthmoverOperator(
                task_id=f"{self.run_type}_earthmover_{tenant_code}",
                **earthmover_kwargs,
                dag=self.dag
            )

            ### LIGHTBEAM OPERATOR (optional)
            if lightbeam:
                run_lightbeam = LightbeamOperator(
                    task_id=f"{self.run_type}_lightbeam_{tenant_code}",
                    **lightbeam_kwargs,
                    dag=self.dag
                )
                run_earthmover >> run_lightbeam

            # S3 OPERATOR (optional)
            if s3_filepath:
                if not s3_conn_id:
                    raise Exception(
                        f"AWS S3 filepath was provided, but argument `s3_conn_id` is undefined."
                    )

                push_to_s3 = PythonOperator(
                    task_id=f"{self.run_type}_to_s3",
                    python_callable=local_filepath_to_s3,
                    op_kwargs={
                        'local_filepath': '',
                        's3_destination_key': s3_filepath,
                        's3_conn_id': s3_conn_id,
                        'remove_local_filepath': not lightbeam,  # Only remove local files if not pushed to ODS
                        # TODO: Include local-filepath cleanup in final logs operation.
                    },
                    provide_context=True,
                    dag=self.dag
                )
                run_earthmover >> push_to_s3

        return earthbeam_task_group
