from typing import List, Optional

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
            default={"exampleKey": "exampleValue"},
            type="object",
            description="Dictionary of query parameters to define which records are to be deleted. At least one parameter is requied. For more information, see https://github.com/edanalytics/lightbeam?tab=readme-ov-file#fetch",
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
        **kwargs
    ):
        """
        Lightbeam: fetch -> Lightbeam: delete -> Clean-up

        """

        @task_group(prefix_group_id=True, group_id=f"{tenant_code}_{api_year}", dag=self.dag)
        def tenant_year_taskgroup():

            @task(multiple_outputs=True, pool=self.pool, dag=self.dag)
            def run_lightbeam(command: str, **context):
                if 'exampleKey' in context['params']['lightbeam_fetch_query']:
                    raise AirflowFailException("No query parameters provided! At least one is required. This is a safety mechanism to prevent the deletion of entire resources.")

                lightbeam_kwargs['query'] = context['params']['lightbeam_fetch_query']
                lightbeam_kwargs['selector'] = context['params']['endpoints']

                lb_output_dir = edfi_api_client.url_join(
                    self.lb_output_directory,
                    tenant_code, api_year,
                    '{{ ds_nodash }}', '{{ ts_nodash }}'
                )
                lb_output_dir = context['task'].render_template(lb_output_dir, context)

                lb_state_dir = edfi_api_client.url_join(
                    self.emlb_state_directory,
                    tenant_code, api_year,
                    'lightbeam'
                )

                run_lightbeam = LightbeamOperator(
                    task_id=f"run_lightbeam",
                    lightbeam_path=self.lightbeam_path,
                    data_dir=lb_output_dir,
                    state_dir=lb_state_dir,
                    edfi_conn_id=edfi_conn_id,
                    **(lightbeam_kwargs or {}),
                    command=command,
                    dag=self.dag
                )
                
                return {
                    "data_dir": run_lightbeam.execute(**context),
                    "state_dir": lb_state_dir
                }

            @task(trigger_rule="all_done" if self.fast_cleanup else "all_success", pool=self.pool, dag=self.dag)
            def remove_files(filepaths):
                unnested_filepaths = []
                for filepath in filepaths:
                    if isinstance(filepath, str):
                        unnested_filepaths.append(filepath)
                    else:
                        unnested_filepaths.extend(filepath)
                
                return remove_filepaths(unnested_filepaths)

            lightbeam_fetch = run_lightbeam.override(task_id="run_lightbeam_fetch")(command="fetch")
            lightbeam_delete = run_lightbeam.override(task_id="run_lightbeam_delete")(command="delete")

            # Final cleanup (apply at very end of the taskgroup)
            remove_files_operator = remove_files(lightbeam_fetch["data_dir"])

            lightbeam_fetch >> lightbeam_delete >> remove_files_operator

        return tenant_year_taskgroup()