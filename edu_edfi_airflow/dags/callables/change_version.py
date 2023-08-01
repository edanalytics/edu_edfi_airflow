import logging

from airflow.exceptions import AirflowSkipException, AirflowFailException
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from edu_edfi_airflow.dags.callables.snowflake import insert_into_snowflake
from edu_edfi_airflow.dags.dag_util import airflow_util
from edu_edfi_airflow.providers.edfi.hooks.edfi import EdFiHook


def get_newest_edfi_change_version(edfi_conn_id: str, **kwargs):
    """

    :return:
    """
    ### Connect to EdFi ODS and verify EdFi3.
    edfi_conn = EdFiHook(edfi_conn_id=edfi_conn_id).get_conn()

    # Break off prematurely if change versions not supported.
    if edfi_conn.is_edfi2():
        logging.warning("Change versions are only supported in EdFi 3+!")
        return None

    # Pull current max change version from EdFi.
    max_change_version = edfi_conn.get_newest_change_version()
    logging.info(f"Current max change version is `{max_change_version}`.")

    return max_change_version


def reset_change_versions(
    tenant_code : str,
    api_year    : int,

    *,
    snowflake_conn_id: str,
    change_version_table: str,

    **kwargs
) -> None:
    # Airflow-skip if run not marked for a full-refresh.
    if airflow_util.is_full_refresh(kwargs):
        logging.info(
            "Full refresh: marking previous pulls inactive."
        )
    else:
        raise AirflowSkipException(
            f"Full refresh not specified. Change version table `{change_version_table}` unchanged."
        )

    ### Prepare the SQL query.
    # Retrieve the database and schema from the Snowflake hook, and raise an exception if undefined.
    database, schema = airflow_util.get_snowflake_params_from_conn(snowflake_conn_id)

    # Reset all resources in this tenant-year.
    qry_mark_inactive = f"""
        update {database}.{schema}.{change_version_table}
            set is_active = FALSE
        where tenant_code = '{tenant_code}'
        and api_year = {api_year}
    """

    # Filter only to inactive endpoints to those specified in DAG-configs, if defined.
    if config_endpoints := airflow_util.get_config_endpoints(kwargs):
        qry_mark_inactive += "    and name in ('{}')".format("', '".join(config_endpoints))

    ### Connect to Snowflake and execute the query.
    SnowflakeHook(snowflake_conn_id).run(qry_mark_inactive)

    # No XComs are pushed, so all downstream XCom-pulls will fail and default to 0.


def get_previous_change_versions(
    tenant_code : str,
    api_year    : int,

    *,
    snowflake_conn_id: str,
    change_version_table: str,

    **kwargs
) -> None:
    """
        We separate getting the most recent EdFi change version into a separate function.
        We need pulls to be consistent by change version for each run.
        Otherwise, the raw schema will passively drift across resources around versions.

        With every pull from EdFi 3+ to Snowflake, the change-version table is updated for the resource.
        The change version documents the timestamp of the last pull.

        This operator retrieves the most recent versions found in Snowflake for all resources.
        Use this and the most recent change version from EdFi to build an ingestion window.
        At the end of the ingest, the change version table is updated with the most recent version found in EdFi.

        If a full refresh is being completed, all change versions for this tenant-year have been set to inactive.
    """
    logging.info(
        "Retrieving max previously-ingested change versions from Snowflake."
    )

    ### Prepare the SQL query.
    # Retrieve the database and schema from the Snowflake hook, and raise an exception if undefined.
    database, schema = airflow_util.get_snowflake_params_from_conn(snowflake_conn_id)

    # Retrieve the previous max change versions for this tenant-year.
    qry_prior_max = f"""
        select name, is_deletes, max(max_version) as max_version
        from {database}.{schema}.{change_version_table}
        where tenant_code = '{tenant_code}'
            and api_year = {api_year}
            and is_active
        group by 1, 2
    """

    ### Retrieve previous endpoint-level change versions and push as XComs.
    prior_change_versions = SnowflakeHook(snowflake_conn_id).get_records(qry_prior_max)
    logging.info(
        f"Collected prior change versions for {len(prior_change_versions)} endpoints."
    )

    for snake_resource, is_deletes, max_version in prior_change_versions:
        xcom_key = airflow_util.build_display_name(snake_resource, is_deletes)
        kwargs['ti'].xcom_push(key=xcom_key, value=max_version)


def update_change_versions(
    tenant_code: str,
    api_year   : int,

    *,
    snowflake_conn_id: str,
    change_version_table: str,

    edfi_change_version: int,

    **kwargs
):
    """

    :return:
    """
    rows_to_insert = []

    for task_id in kwargs['task'].upstream_task_ids:

        # Only log successful copies into Snowflake (skips will return None)
        if not (xcom_value := kwargs['ti'].xcom_pull(task_id)):
            continue
        else:
            resource, deletes = xcom_value

        rows_to_insert.append([
            tenant_code, api_year, resource, deletes,
            kwargs["ds"], kwargs["ts"],
            edfi_change_version, True
        ])

    if not rows_to_insert:
        raise AirflowSkipException(
            "There are no new change versions to update for any endpoints. All upstream tasks skipped or failed."
        )
    else:
        logging.info(
            f"Collected updated change versions for {len(rows_to_insert)} endpoints."
        )

    insert_into_snowflake(
        snowflake_conn_id=snowflake_conn_id,
        table_name=change_version_table,
        columns=[
            "tenant_code", "api_year", "name", "is_deletes",
            "pull_date", "pull_timestamp",
            "max_version", "is_active"
        ],
        values=rows_to_insert
    )

    return True
