import csv
import logging
import os
from datetime import datetime
from typing import Optional

from airflow.decorators import task, task_group
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

import edfi_api_client
from ea_airflow_util import EACustomDAG

from edu_edfi_airflow.callables.s3 import remove_filepaths
from edu_edfi_airflow.providers.earthbeam.operators import LightbeamOperator


def get_change_version_from_date(target_date_str: str, tenant_code: str, api_year: int, endpoint: str, snowflake_conn_id: str = 'snowflake') -> int:
    """
    Query Snowflake to get the change version for a specific date.
    
    Args:
        target_date_str: Target date in ISO format (e.g., '2024-01-15T00:00:00Z')
        tenant_code: Tenant code to filter by
        api_year: API year to filter by
        endpoint: Endpoint name (e.g., 'student_assessments')
        snowflake_conn_id: Snowflake connection ID
        
    Returns:
        Change version number
        
    Raises:
        AirflowFailException: If Snowflake connection fails or no change version is found
    """
    try:
        # Parse target date
        target_date = datetime.fromisoformat(target_date_str.replace('Z', '+00:00')).date()
        
        # Query Snowflake
        snowflake_hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
        
        query = """
        SELECT max_version
        FROM raw.edfi3._meta_change_versions
        WHERE name = %s
        AND tenant_code = %s
        AND api_year = %s
        AND date <= %s
        ORDER BY date DESC
        LIMIT 1
        """
        
        result = snowflake_hook.get_first(query, parameters=(endpoint, tenant_code, api_year, target_date))
        
        if result:
            change_version = result[0]
            logging.info(f"Found change version {change_version} for date {target_date_str} (tenant: {tenant_code}, year: {api_year})")
            return change_version
        else:
            raise AirflowFailException(
                f"No change version found for date {target_date_str} (tenant: {tenant_code}, year: {api_year}, endpoint: {endpoint}). "
                f"Please check that the date is valid and that change version data exists for this tenant/year/endpoint combination."
            )
            
    except AirflowFailException:
        # Re-raise AirflowFailException as-is
        raise
    except Exception as e:
        raise AirflowFailException(
            f"Failed to query change version from Snowflake for date {target_date_str} (tenant: {tenant_code}, year: {api_year}, endpoint: {endpoint}): {e}. "
            f"Please check your Snowflake connection '{snowflake_conn_id}' and ensure the table 'raw.edfi3._meta_change_versions' exists and is accessible."
        )


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
        "minChangeVersionDate": Param(
            default=None,
            type=["string", "null"],
            description="**OPTIONAL** - Date to filter records modified since this date (ISO format, e.g., '2024-01-15T00:00:00Z'). **Note: Uses the first endpoint in the 'endpoints' list to query Snowflake change versions.**"
        ),
        "maxChangeVersionDate": Param(
            default=None,
            type=["string", "null"],
            description="**OPTIONAL** - Date to filter records modified before this date (ISO format, e.g., '2024-01-15T00:00:00Z'). **Note: Uses the first endpoint in the 'endpoints' list to query Snowflake change versions.**"
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
        snowflake_conn_id: Optional[str] = 'snowflake',
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
                
                # Add change version parameters to query if date-based parameters are provided
                query_params = context['params']['query_parameters'].copy()
                
                # Query Snowflake for change versions if date parameters are provided
                # Get the endpoint name from the selector (convert camelCase to snake_case)
                endpoints = context['params']['endpoints']
                if endpoints:
                    # Use the first endpoint and convert to snake_case for Snowflake table
                    endpoint_name = endpoints[0].lower()
                    # Convert camelCase to snake_case (e.g., studentAssessments -> student_assessments)
                    import re
                    endpoint_name = re.sub(r'(?<!^)(?=[A-Z])', '_', endpoint_name).lower()
                    
                    if context['params'].get('minChangeVersionDate'):
                        min_cv = get_change_version_from_date(
                            context['params']['minChangeVersionDate'],
                            tenant_code,
                            api_year,
                            endpoint_name,
                            snowflake_conn_id
                        )
                        query_params['minChangeVersion'] = min_cv
                    
                    if context['params'].get('maxChangeVersionDate'):
                        max_cv = get_change_version_from_date(
                            context['params']['maxChangeVersionDate'],
                            tenant_code,
                            api_year,
                            endpoint_name,
                            snowflake_conn_id
                        )
                        query_params['maxChangeVersion'] = max_cv
                
                lightbeam_kwargs['query'] = query_params

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