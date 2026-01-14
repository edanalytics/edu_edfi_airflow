from typing import Optional

from airflow.hooks.base_hook import BaseHook

from edfi_api_client import EdFiClient
from edfi_api_client.token_cache import LockfileTokenCache


class EdFiHook(BaseHook):
    """
    Establish a connection to the EdFi ODS using an Airflow Connection.
    Default to pulling the EdFi API configs from the connection if not explicitly provided.
    """
    def __init__(self, edfi_conn_id: str, use_token_cache: bool = False, **kwargs) -> None:
        self.edfi_conn_id = edfi_conn_id
        self.api_conn = None
        self.use_token_cache: bool = use_token_cache
        self.kwargs = kwargs


    def get_conn(self) -> EdFiClient:
        conn = self.get_connection(self.edfi_conn_id)
        extras = conn.extra_dejson

        if self.use_token_cache:
            token_cache = LockfileTokenCache()
        else:
            token_cache = None

        api_conn = EdFiClient(
            base_url     = conn.host,
            client_key   = conn.login,
            client_secret= conn.password,
            token_cache  = token_cache,
            **self.kwargs,
            **extras
        )

        self.api_conn = api_conn
        return api_conn
