from typing import Optional

from airflow.hooks.base_hook import BaseHook
from airflow.operators.python import get_current_context

from edfi_api_client import EdFiClient


class EdFiHook(BaseHook):
    """
    Establish a connection to the EdFi ODS using an Airflow Connection and,
    optionally, a separate EdFiTokenProvider for shared token provision to
    reduce the number of tokens needed. If a token provider task ID is
    provided, will pull the `return_value` XCom from that task ID as an
    EdFi authentication payload. Only supports token providers in the same
    DAG.

    Default to pulling the EdFi API configs from the connection if not
    explicitly provided.
    """
    def __init__(self, 
        edfi_conn_id: str, 
        token_provider_id: Optional[str] = None, 
        **kwargs
    ) -> None:
        self.edfi_conn_id = edfi_conn_id
        self.api_conn = None
        self.token_provider_id = token_provider_id


    def get_conn(self) -> EdFiClient:
        conn = self.get_connection(self.edfi_conn_id)
        extras = conn.extra_dejson
        
        if self.token_provider_id:
            def access_token_getter():
                context = get_current_context()
                payload = context['ti'].xcom_pull(self.token_provider_id)
                
                return payload

            api_conn = EdFiClient(
                base_url=conn.host,
                access_token=access_token_getter,
                **extras
            )
        else:
            api_conn = EdFiClient(
                base_url     = conn.host,
                client_key   = conn.login,
                client_secret= conn.password,
                **extras
            )

        self.api_conn = api_conn
        return api_conn
