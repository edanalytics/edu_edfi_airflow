from typing import Tuple

from airflow.models import Connection

from edfi_api_client import camel_to_snake


def is_full_refresh(context) -> bool:
    """

    :param context:
    :return:
    """
    full_refresh = False

    if context['dag_run'].conf:
        full_refresh = context['dag_run'].conf.get('full_refresh', False)

    return full_refresh


def get_context_parameter(context, parameter: str, default: object = None) -> object:
    """
    Searches execution context for parameter.

    :param context:
    :param parameter:
    :param default:
    :return:
    """
    if context['dag_run'].conf:
        return context['dag_run'].conf.get(parameter, default)
    else:
        return default


def is_resource_specified(context, resource: str) -> bool:
    """

    :param context:
    :param resource:
    :return:
    """
    is_specified = True

    if context['dag_run'].conf:
        specified_resources = context['dag_run'].conf.get('resources')

        if specified_resources is not None:
            # Apply camel_to_snake transform on all specified resources to circumvent user-input error.
            specified_resources = list(map(camel_to_snake, specified_resources))

            if camel_to_snake(resource) not in specified_resources:
                is_specified = False

    return is_specified


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
