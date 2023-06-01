from typing import Tuple

from airflow.models import Connection

from edfi_api_client import camel_to_snake


def build_display_name(resource: str, is_deletes: bool = False) -> str:
    """
    Universal helper method for building the display name of a resource.
    """
    if is_deletes:
        return f"{resource}_deletes"
    else:
        return resource

def split_display_name(display_name: str) -> (str, bool):
    """
    Universal helper method for splitting the display name of a resource into resource and deletes flag.
    """
    if display_name.endswith("_deletes"):
        resource = display_name.replace("_deletes", "")
        return resource, True
    else:
        return display_name, False


def is_full_refresh(context) -> bool:
    """

    :param context:
    :return:
    """
    return context["params"]["full_refresh"]


def is_endpoint_specified(context, endpoint: str) -> bool:
    """

    :param context:
    :param endpoint:
    :return:
    """
    endpoints_to_run = context["params"]["endpoints"]

    # If no endpoints are specified, run all.
    if not endpoints_to_run:
        return True

    else:
        # Apply camel_to_snake transform on all specified endpoints to circumvent user-input error.
        return bool(camel_to_snake(endpoint) in map(camel_to_snake, endpoints_to_run))


def xcom_pull_template(
    task_ids: str,
    key: str = 'return_value'
) -> str:
    """
    Generate a Jinja template to pull a particular xcom key from a task_id
    :param task_ids: An upstream task to pull xcom from
    :param key: The key to retrieve. Default: return_value
    :return: A formatted Jinja string for the xcom pull
    """
    xcom_string = f"ti.xcom_pull(task_ids='{task_ids}', key='{key}')"

    return '{{ ' + xcom_string + ' }}'


def get_snowflake_params_from_conn(
    snowflake_conn_id: str
) -> Tuple[str, str]:

    undefined_snowflake_error = ValueError(
        f"Snowflake `extra__snowflake__database` and `schema` must be defined within `{snowflake_conn_id}`."
    )

    try:
        snowflake_conn = Connection.get_connection_from_secrets(snowflake_conn_id)

        database = snowflake_conn.extra_dejson['extra__snowflake__database']
        schema   = snowflake_conn.schema

        if database is None or schema is None:
            raise undefined_snowflake_error

        return database, schema

    except KeyError:
        raise undefined_snowflake_error
