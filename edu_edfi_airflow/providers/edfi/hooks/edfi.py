from typing import Optional, Callable, Union

from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable

from edfi_api_client import EdFiClient


class EdFiHook(BaseHook):
    """
    Establish a connection to the EdFi ODS using an Airflow Connection.
    Default to pulling the EdFi API configs from the connection if not explicitly provided.
    """
    def __init__(self, 
        edfi_conn_id: str, 
        access_token: Optional[Union[str, Callable[[], str]]] = None, 
        **kwargs
    ) -> None:
        self.edfi_conn_id = edfi_conn_id
        self.api_conn = None
        self.access_token = access_token


    def get_conn(self) -> EdFiClient:
        conn = self.get_connection(self.edfi_conn_id)
        extras = conn.extra_dejson
        
        if self.access_token:
            api_conn = EdFiClient(
                base_url=conn.host,
                access_token=self.access_token,
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
