import inspect
import logging

from typing import List, Tuple

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
    if dom_full_refresh_macro := context["dag"].user_defined_macros.get("is_dom_full_refresh", False):
        if dom_full_refresh_macro(**context):
            logging.info("Day-of-month full-refresh criteria met! Triggering a full-refresh run.")
            return True

    return get_context_variable(context, 'full_refresh', default=False)

def is_dom(dom: int, **context) -> bool:
    """
    Simple helper to check whether the day-of-month of the DAG-run matches a specified argument.
    """
    execution_date = context["dag_run"]["logical_date"]
    return execution_date.day == dom


def get_config_endpoints(context) -> List[str]:
    """

    :param context:
    :return:
    """
    # Apply camel_to_snake transform on all specified endpoints to circumvent user-input error.
    raw_endpoints = get_context_variable(context, 'endpoints', default=[])
    return list(map(camel_to_snake, raw_endpoints))


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


def subset_kwargs_to_class(class_: object, kwargs: dict) -> dict:
    """
    Helper function to remove unexpected arguments from kwargs,
    based on the actual arguments of the class.

    :param class_:
    :param kwargs:
    :return:
    """
    class_parameters = list(inspect.signature(class_).parameters.keys())
    return {
        arg: val for arg, val in kwargs.items()
        if arg in class_parameters
    }