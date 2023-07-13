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
    if dag_params := context.get('params'):
        if (dag_param_value := dag_params.get(variable_name)) is not None:
            return dag_param_value

    # Then check the DAG run configurations directly
    elif dag_vars := context['dag_run'].conf:
        if (dag_var_value := dag_vars.get(variable_name)) is not None:
            return dag_var_value

    else:
        return default


def is_full_refresh(context) -> bool:
    """

    :param context:
    :return:
    """
    return get_context_variable(context, 'full_refresh', default=False)


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
