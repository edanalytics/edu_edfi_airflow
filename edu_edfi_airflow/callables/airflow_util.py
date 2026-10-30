import logging
from typing import TYPE_CHECKING, Union

import airflow_client.client
import croniter
import requests
from airflow.sdk import Connection, Variable, chain
from airflow.sdk.exceptions import AirflowFailException
from airflow.sdk.types import DagRunProtocol
from airflow_client.client.rest import ApiException
from edfi_api_client import camel_to_snake

if TYPE_CHECKING:
    from airflow.sdk import BaseOperator

logger = logging.getLogger(__name__)


def get_airflow_client_config() -> str:
    url = "http://localhost:8080/auth/token"
    payload = {
        "username": "admin-api",
        "password": Variable.get("ADMIN_API"),
    }
    print("Attempting to fetch Airflow Python API access token")
    response = requests.post(
        url, json=payload, headers={"Content-Type": "application/json"}
    )

    if response.status_code != 201:
        raise RuntimeError(
            f"Failed to get access token: {response.status_code} {response.text}"
        )

    access_token = response.json()["access_token"]

    configuration = airflow_client.client.Configuration(access_token=access_token)
    return configuration


def get_context_variable(context, variable_name: str, default: object):
    """
    Generic method to search the DAG Run conf and params for a context variable.

    :param context:
    :param variable_name:
    :param default:
    :return:
    """
    # Check Airflow 2.6 params first
    if "params" in context and variable_name in context["params"]:
        return context["params"][variable_name]

    elif context["dag_run"].conf and variable_name in context["dag_run"].conf:
        return context["dag_run"].conf[variable_name]

    else:
        return default


def is_full_refresh(context) -> bool:
    """

    :param context:
    :return:
    """
    # If user_defined_macros are not defined in DAG init, their context attribute is None.
    if (
        full_refresh_macro := (context["dag"].user_defined_macros or {}).get(
            "is_scheduled_full_refresh", False
        )
    ) and full_refresh_macro(**context):
        logger.info(
            "Scheduled full-refresh criteria met! Triggering a full-refresh run."
        )
        return True

    return get_context_variable(context, "full_refresh", default=False)


def run_matches_cron(cron: str | None, **context) -> bool:
    """
    Simple helper to check whether the execution timestamp of the DAG-run matches a specified cron expression.
    Note that default cron behavior uses boolean OR logic to handle day and day-of-week entries, so we set day_or=False.

    Advanced cron examples:
        "2 4 1 * wed        ": 04:02 on every 1st day of the month if it is a Wednesday
        "0 0 * * sat#1,sun#2": 00:00 on the 1st Saturday and 2nd Sunday of the month
        "0 0 * * 5#3,L5     ": 00:00 on the 3rd and last Friday of the month
    """
    if not cron:
        return False

    if not croniter.croniter.is_valid(cron):
        raise ValueError(
            "Provided cron syntax to match against current DAG run is invalid!"
        )

    return croniter.croniter.match(cron, context["dag_run"].logical_date, day_or=False)


def get_config_endpoints(context) -> list[str]:
    """

    :param context:
    :return:
    """
    # Apply camel_to_snake transform on all specified endpoints to circumvent user-input error.
    raw_endpoints = get_context_variable(context, "endpoints", default=[])
    if raw_endpoints is not None:
        return list(map(camel_to_snake, raw_endpoints))
    return []


def xcom_pull_template(
    task_ids: Union[str, "BaseOperator", list[str], list["BaseOperator"]],
    key: str = "return_value",
    prefix: str = "",
    suffix: str = "",
) -> str:
    """
    Generate a Jinja template to pull a particular xcom key from a task_id
    :param task_ids: An upstream task to pull xcom from
    :param key: The key to retrieve. Default: return_value
    :return: A formatted Jinja string for the xcom pull
    """
    # Retrieve string representations for each XCom in a list.
    if isinstance(task_ids, (list, tuple)):
        task_ids = [
            task_id.task_id if hasattr(task_id, "task_id") else task_id
            for task_id in task_ids
        ]
        task_ids_string = "['{}']".format("','".join(task_ids))

    # Or retrieve string representation of a single XCom.
    else:
        if hasattr(task_ids, "task_id"):
            task_ids = task_ids.task_id
        task_ids_string = f"'{task_ids}'"

    xcom_string = (
        f"{prefix} ti.xcom_pull(task_ids={task_ids_string}, key='{key}') {suffix}"
    )
    return "{{ " + xcom_string + " }}"


def get_snowflake_params_from_conn(snowflake_conn_id: str) -> tuple[str, str]:

    undefined_snowflake_error = ValueError(
        f"Snowflake `extra__snowflake__database` and `schema` must be defined within `{snowflake_conn_id}`."
    )

    try:
        snowflake_conn = Connection.get(snowflake_conn_id)

        database = snowflake_conn.extra_dejson["extra__snowflake__database"]
        schema = snowflake_conn.schema

        if database is None or schema is None:
            raise undefined_snowflake_error

        return database, schema

    except KeyError:
        raise undefined_snowflake_error


def fail_if_any_task_failed(**context):
    """
    Simple Python callable that raises an AirflowFailException if any task in the DAG has failed.
    """
    drp: DagRunProtocol = context["dag_run"]
    with airflow_client.client.ApiClient(get_airflow_client_config()) as api_client:
        api_instance = airflow_client.client.TaskInstanceApi(api_client)
        try:
            response = api_instance.get_task_instances(
                drp.dag_id, drp.run_id, state=["failed"]
            )
            if len(response.task_instances) > 0:
                raise AirflowFailException("One or more tasks in the DAG failed.")
        except ApiException as e:
            print(f"Exception when calling TaskInstanceApi->get_task_instances: {e}\n")


def chain_tasks(*tasks):
    """
    Alias of airflow's built-in chain, but recursively remove Nones if present.
    """
    chain(*recursive_filter(None, tasks))


def recursive_filter(func, iterable):
    """
    Taken from here: https://www.mycompiler.io/view/CDbwWjY
    """
    results = []
    for elem in iterable:
        if isinstance(elem, (list, tuple)):
            result = recursive_filter(func, elem)
            if result:
                results.append(result)
        elif func is None:
            if elem:
                results.append(elem)
        elif func(elem):
            results.append(elem)
    return results
