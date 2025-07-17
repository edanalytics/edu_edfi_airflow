from datetime import timedelta
from typing import Any
import logging

from airflow.models import BaseOperator, Variable
from airflow.triggers.temporal import TimeDeltaTrigger
from airflow.utils.state import State
from airflow.utils.context import Context

from edu_edfi_airflow.providers.edfi.hooks.edfi import EdFiHook


class EdFiTokenProviderOperator(BaseOperator):
    '''Gets and refreshes an Ed-Fi ODS bearer token until a designated terminal
    task enters a terminal state'''
    def __init__(
        self,
        *,
        edfi_conn_id: str,
        airflow_variable_name: str,
        sentinel_task_id: str = 'dag_state_sentinel',
        **kwargs
    ):
        super().__init__(**kwargs) 

        self.edfi_conn_id = edfi_conn_id
        self.airflow_variable_name = airflow_variable_name
        self.sentinel_task_id = sentinel_task_id

    # since we defer to this method, must take event as an optional kwarg
    def execute(self, context: Context, event: dict[str, Any] | None = None):
        # check if target tasks are still running via a sentinel
        sentinel_ti = context['dag_run'].get_task_instance(task_id=self.sentinel_task_id)

        if sentinel_ti.state not in State.finished:
            # instantiate an EdFi client and grab token, expiry time
            conn = EdFiHook(self.edfi_conn_id).get_conn()
            conn.session.authenticate()
            token = conn.session.access_token
            defer_seconds = conn.session.refresh_at - conn.session.authenticated_at 

            logging.info(f'Refreshed token in {self.airflow_variable_name}. Next refresh scheduled in {defer_seconds}s')
            
            # store the token in an Airflow variable
            Variable.set(self.airflow_variable_name, token)
            
            # defer til later
            self.defer(
                trigger=TimeDeltaTrigger(timedelta(seconds=defer_seconds)),
                method_name='execute'
            )

        else:
            # otherwise, clear out variable and do not defer again
            if Variable.get(self.airflow_variable_name, None):
                Variable.delete(self.airflow_variable_name)

        
        
            
        
