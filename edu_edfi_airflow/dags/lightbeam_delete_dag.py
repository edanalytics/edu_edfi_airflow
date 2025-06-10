import csv
import logging
import os
from typing import Optional

from airflow.decorators import task, task_group
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param

import edfi_api_client
from ea_airflow_util import EACustomDAG

from edu_edfi_airflow.callables.s3 import remove_filepaths
from edu_edfi_airflow.providers.earthbeam.operators import LightbeamOperator


class LightbeamDeleteDAG:
    """
    Full Earthmover-Lightbeam DAG, with optional Python callable pre-processing.
    """
    lb_output_directory   : str = '/efs/tmp_storage/lightbeam'

    ENDPOINTS_LIST = [
        "assessments",
        "objectiveAssessments",
        "studentAssessments",
    ]

    params_dict = {
        "query_parameters": Param(
            default={"assessmentIdentifier": "some_value"},
            type="object",
            description="JSON object of query parameters to define which records are to be deleted. At least one parameter is requied. For more information, see https://github.com/edanalytics/lightbeam?tab=readme-ov-file#fetch",
        ),
        "endpoints": Param(
            default=ENDPOINTS_LIST,
            type="array",
            description="Newline-separated list of endpoints to delete"
        ),
    }

    def __init__(self,
        *,
        lightbeam_path: Optional[str] = None,
        pool: str = 'default_pool',
        fast_cleanup: bool = True,

        **kwargs
    ):
        self.lightbeam_path = lightbeam_path
        self.pool = pool
        self.fast_cleanup = fast_cleanup

        self.dag = EACustomDAG(params=self.params_dict, **kwargs)


    def build_tenant_year_taskgroup(self, 
        tenant_code: str,
        api_year: int,
        edfi_conn_id: Optional[str] = None,
        lightbeam_kwargs: Optional[dict] = None,
        **kwargs
    ):
        """
        Lightbeam: fetch -> Lightbeam: delete -> Clean-up

        """

        @task_group(prefix_group_id=True, group_id=f"{tenant_code}_{api_year}", dag=self.dag)
        def tenant_year_taskgroup():

            @task(pool=self.pool, dag=self.dag)
            def run_lightbeam(command: str, **context):
                lightbeam_kwargs['query'] = context['params']['query_parameters']
                lightbeam_kwargs['selector'] = context['params']['endpoints']

                lb_output_dir = edfi_api_client.url_join(
                    self.lb_output_directory,
                    tenant_code, api_year,
                    '{{ ds_nodash }}', '{{ ts_nodash }}'
                )
                lb_output_dir = context['task'].render_template(lb_output_dir, context)

                run_lightbeam = LightbeamOperator(
                    task_id=f"run_lightbeam",
                    lightbeam_path=self.lightbeam_path,
                    data_dir=lb_output_dir,
                    edfi_conn_id=edfi_conn_id,
                    **(lightbeam_kwargs or {}),
                    command=command,
                    dag=self.dag
                )
                
                return run_lightbeam.execute(**context)

            @task(pool=self.pool, dag=self.dag)
            def check_endpoint_total_counts(data_dir: str, **context):
                """
                Checks that no resource will have all records deleted. This is a fail-safe to prevent as unintended mass deletion
                due to a typo in the key of a query parameter.
                """
                endpoints = context['params']['endpoints']
                count_results_file = edfi_api_client.url_join(data_dir, 'endpoint_counts.txt')
                
                run_lightbeam = LightbeamOperator(
                    task_id=f"run_lightbeam",
                    lightbeam_path=self.lightbeam_path,
                    data_dir=data_dir,
                    results_file=count_results_file,
                    edfi_conn_id=edfi_conn_id,
                    **(lightbeam_kwargs or {}),
                    command='count',
                    dag=self.dag
                )
                run_lightbeam.execute(**context)

                with open(count_results_file) as f:
                    rows = csv.reader(f, delimiter="\t")
                    for row in rows:
                        if row[1] in endpoints:
                            endpoint_file = edfi_api_client.url_join(data_dir, f"{row[1]}.jsonl")
                            if not os.path.exists(endpoint_file):
                                logging.info(f"No records found for the {row[1]} resource.")
                            else:
                                num_records = sum(1 for _ in open(endpoint_file))
                                if num_records == int(row[0]):
                                    raise AirflowFailException(
                                        f"All records were returned for the {row[1]} resource. Please check your query parameters."
                                    )
                                else:
                                    logging.info(f"Check passed for {row[1]}: {num_records} records to be deleted out of {row[0]} total records.")

                return 

            @task(trigger_rule="none_skipped" if self.fast_cleanup else "all_success", pool=self.pool, dag=self.dag)
            def remove_files(filepath):
                if filepath is not None:
                    return remove_filepaths(filepath)

            lightbeam_fetch = run_lightbeam.override(task_id="run_lightbeam_fetch")(command="fetch")
            lightbeam_delete = run_lightbeam.override(task_id="run_lightbeam_delete")(command="delete")

            # Validate that endpoints will not be deleted entirely
            check_counts = check_endpoint_total_counts(lightbeam_fetch)

            # Final cleanup (apply at very end of the taskgroup)
            remove_files_operator = remove_files(lightbeam_delete)

            lightbeam_fetch >> check_counts >> lightbeam_delete >> remove_files_operator

        return tenant_year_taskgroup()