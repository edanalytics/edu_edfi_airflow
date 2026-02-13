from airflow.hooks.base_hook import BaseHook
from runway_client import RunwayClient


class RunwayHook(BaseHook):
    def __init__(self, runway_conn_id: str) -> None:
        self.runway_conn_id = runway_conn_id
        self.runway_client = None

    def get_runway_client(self) -> RunwayClient:
        if self.runway_client is not None:
            return self.runway_client

        conn = self.get_connection(self.runway_conn_id)
        extras = conn.extra_dejson

        self.runway_client = RunwayClient(
            runway_base_url=conn.host,
            auth_base_url=extras["extra__runway__auth_base_url"],
            client_id=conn.login,
            client_secret=conn.password,
            partner_code=extras["extra__runway__partner_code"],
        )

        return self.runway_client
