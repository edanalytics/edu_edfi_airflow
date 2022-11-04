import logging

from edu_edfi_airflow.providers.edfi.hooks.edfi import EdFiHook


def get_edfi_change_version(edfi_conn_id: str, **kwargs):
    """

    :return:
    """
    ### Connect to EdFi ODS and verify EdFi3.
    edfi_conn = EdFiHook(edfi_conn_id=edfi_conn_id).get_conn()

    # Break off prematurely if change versions not supported.
    if edfi_conn.is_edfi2():
        print("Change versions are only supported in EdFi 3+!")
        return None

    # Pull current max change version from EdFi.
    max_change_version = edfi_conn.get_newest_change_version()
    logging.info(f"Current max change version is `{max_change_version}`.")

    return max_change_version
