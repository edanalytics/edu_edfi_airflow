import inspect

from typing import List, Tuple, Union

from airflow.models import Connection


def build_display_name(resource: str, is_deletes: bool = False, is_key_changes: bool = False) -> str:
    """
    Universal helper method for building the display name of a resource.
    """
    if is_deletes:
        return f"{resource}_deletes"
    elif is_key_changes:
        return f"{resource}_key_changes"
    else:
        return resource


def get_context_variable(context, variable_name: str, default: object):
    """
    Generic method to search the DAG Run conf and params for a context variable.

    :param context:
    :param variable_name:
    :param default:
    :return:
    """
    # Check Airflow 2.6 params first
    if 'params' in context and variable_name in context['params']:
        return context['params'][variable_name]

    elif context['dag_run'].conf and variable_name in context['dag_run'].conf:
        return context['dag_run'].conf[variable_name]

    else:
        return default


def is_full_refresh(context) -> bool:
    """

    :param context:
    :return:
    """
    return get_context_variable(context, 'full_refresh', default=False)


def xcom_pull_template(
    task_ids: Union[str, List[str]],
    key: str = 'return_value',
    prefix: str = "",
    suffix: str = ""
) -> str:
    """
    Generate a Jinja template to pull a particular xcom key from a task_id
    :param task_ids: An upstream task to pull xcom from
    :param key: The key to retrieve. Default: return_value
    :return: A formatted Jinja string for the xcom pull
    """
    if not isinstance(task_ids, str):
        task_ids_string = "['{}']".format("','".join(task_ids))
    else:
        task_ids_string = "'{task_ids}'"

    xcom_string = f"{prefix} ti.xcom_pull(task_ids={task_ids_string}, key='{key}') {suffix}"

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
