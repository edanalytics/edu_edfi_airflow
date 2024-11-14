from typing import List, Optional

from airflow.decorators import task, task_group
from airflow.models.param import Param

import edfi_api_client
from ea_airflow_util import EACustomDAG

from edu_edfi_airflow.callables.s3 import remove_filepaths
from edu_edfi_airflow.callables import emlb_logging_util
from edu_edfi_airflow.providers.earthbeam.operators import LightbeamOperator


class LightbeamDeleteDAG:
    """
    Full Earthmover-Lightbeam DAG, with optional Python callable pre-processing.
    """
    emlb_state_directory  : str = '/efs/emlb/state'
    emlb_results_directory: str = '/efs/emlb/results'
    lb_output_directory   : str = '/efs/tmp_storage/lightbeam'

    ENDPOINTS_LIST = [
        "assessments",
        "objectiveAssessments",
        "studentAssessments",
    ]

    params_dict = {
        "lightbeam_fetch_query": Param(
            default=None,
            examples=f'"assessmentIdentifier": "SAT"',
            type="string",
            description="Dictionary of query parameters to define which records are to be deleted. At least one parameter is requied. For more information, see https://github.com/edanalytics/lightbeam?tab=readme-ov-file#fetch",
        ),
        "endpoints": Param(
            default=ENDPOINTS_LIST,
            examples=ENDPOINTS_LIST,
            type="array",
            description="Newline-separated list of endpoints to delete"
        ),
    }

    def __init__(self,
        *,
        lightbeam_path: Optional[str] = None,
        pool: str = 'default_pool',
        fast_cleanup: bool = False,

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
        snowflake_conn_id: Optional[str] = None,
        logging_table: Optional[str] = None,
        **kwargs
    ):
        """
        Lightbeam: fetch -> Lightbeam: delete -> (Snowflake: LB Logs) -> Clean-up

        """

        @task_group(prefix_group_id=True, group_id=f"{tenant_code}_{api_year}", dag=self.dag)
        def tenant_year_taskgroup():

            @task(multiple_outputs=True, pool=self.pool, dag=self.dag)
            def run_lightbeam(command: str, **context):
                lightbeam_kwargs['query'] = context['params']['lightbeam_fetch_query']
                lightbeam_kwargs['selector'] = context['params']['endpoints']

                lb_output_dir = edfi_api_client.url_join(
                    self.lb_output_directory,
                    tenant_code, api_year,
                    '{{ ds_nodash }}', '{{ ts_nodash }}'
                )

                lb_state_dir = edfi_api_client.url_join(
                    self.emlb_state_directory,
                    tenant_code, api_year,
                    'lightbeam'
                )

                lb_results_file = edfi_api_client.url_join(
                    self.emlb_results_directory,
                    tenant_code, api_year,
                    '{{ ds_nodash }}', '{{ ts_nodash }}',
                    f'lightbeam_{command}_results.json'
                ) if logging_table else None
                lb_results_file = context['task'].render_template(lb_results_file, context)

                run_lightbeam = LightbeamOperator(
                    task_id=f"run_lightbeam",
                    lightbeam_path=self.lightbeam_path,
                    data_dir=lb_output_dir,
                    state_dir=lb_state_dir,
                    results_file=lb_results_file,
                    edfi_conn_id=edfi_conn_id,
                    **(lightbeam_kwargs or {}),
                    command=command,
                    dag=self.dag
                )
                
                return {
                    "data_dir": run_lightbeam.execute(**context),
                    "state_dir": lb_state_dir,
                    "results_file": lb_results_file,
                }
            
            @task(pool=self.pool, dag=self.dag)
            def log_to_snowflake(results_filepath: str, **context):
                return emlb_logging_util.log_to_snowflake(
                    snowflake_conn_id=snowflake_conn_id,
                    logging_table=logging_table,
                    log_filepath=results_filepath,
                    tenant_code=tenant_code,
                    api_year=api_year,
                    **context
                )

            @task(trigger_rule="all_done" if self.fast_cleanup else "all_success", pool=self.pool, dag=self.dag)
            def remove_files(filepaths):
                unnested_filepaths = []
                for filepath in filepaths:
                    if isinstance(filepath, str):
                        unnested_filepaths.append(filepath)
                    else:
                        unnested_filepaths.extend(filepath)
                
                return remove_filepaths(unnested_filepaths)

            lightbeam_fetch = run_lightbeam(command="fetch")
            lightbeam_delete = run_lightbeam(command="delete")

            # Final cleanup (apply at very end of the taskgroup)
            remove_files_operator = remove_files(lightbeam_fetch["data_dir"])

            # Lightbeam logs to Snowflake
            if logging_table:
                log_delete_to_snowflake = log_to_snowflake.override(task_id="log_delete_to_snowflake")(lightbeam_delete["results_file"])
                lightbeam_fetch >> lightbeam_delete >> log_delete_to_snowflake >> remove_files_operator

            else:
                lightbeam_fetch >> lightbeam_delete >> remove_files_operator

        return tenant_year_taskgroup