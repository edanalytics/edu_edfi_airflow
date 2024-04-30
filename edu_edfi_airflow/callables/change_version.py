import logging

from typing import Dict, List, Tuple, Optional

from airflow.exceptions import AirflowSkipException, AirflowFailException
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from edu_edfi_airflow.callables.snowflake import insert_into_snowflake
from edu_edfi_airflow.callables import airflow_util
from edu_edfi_airflow.providers.edfi.hooks.edfi import EdFiHook


def get_newest_edfi_change_version(edfi_conn_id: str, **kwargs) -> int:
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
    if not airflow_util.is_full_refresh(kwargs):
        raise AirflowSkipException(f"Full refresh not specified. Change version table `{change_version_table}` unchanged.")

    ### Prepare the SQL query.
    # Retrieve the database and schema from the Snowflake hook, and raise an exception if undefined.
    database, schema = airflow_util.get_snowflake_params_from_conn(snowflake_conn_id)

    # Reset all resources in this tenant-year.
    qry_mark_inactive = f"""
        update {database}.{schema}.{change_version_table}
            set is_active = FALSE
        where tenant_code = '{tenant_code}'
        and api_year = {api_year}
        and is_active
    """

    # Filter only to inactive endpoints to those specified in DAG-configs, if defined.
    if config_endpoints := airflow_util.get_config_endpoints(kwargs):
        qry_mark_inactive += "    and name in ('{}')".format("', '".join(config_endpoints))

    ### Connect to Snowflake and execute the query.
    logging.info("Full refresh: marking previous pulls inactive.")
    SnowflakeHook(snowflake_conn_id).run(qry_mark_inactive)


def get_previous_change_versions(
    tenant_code: str,
    api_year: int,
    endpoints: List[Tuple[str, str]],

    *,
    snowflake_conn_id: str,
    change_version_table: str,

    edfi_conn_id: Optional[str] = None,
    max_change_version: Optional[int] = None,

    get_deletes: bool = False,
    get_key_changes: bool = False,

    **context
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

    If an Ed-Fi connection and max_change_version are specified, each endpoint is checked for deltas and only updates are returned.
    """
    # Skip deletes/key-changes if a full-refresh.
    if airflow_util.is_full_refresh(context) and (get_deletes or get_key_changes):
        raise AirflowSkipException("Skipping deletes/key-changes pull for full_refresh run.")

    logging.info("Retrieving max previously-ingested change versions from Snowflake.")

    ### Prepare the SQL query.
    # Retrieve the database and schema from the Snowflake hook, and raise an exception if undefined.
    database, schema = airflow_util.get_snowflake_params_from_conn(snowflake_conn_id)

    # Retrieve the previous max change versions for this tenant-year.
    if get_deletes:
        filter_clause = "is_deletes"
    elif get_key_changes:
        filter_clause = "is_key_changes"
    else:
        filter_clause = "TRUE"

    qry_prior_max = f"""
        select name, max(max_version) as max_version
        from {database}.{schema}.{change_version_table}
        where tenant_code = '{tenant_code}'
            and api_year = {api_year}
            and is_active
            and {filter_clause}
        group by all
    """

    prior_change_versions = dict(SnowflakeHook(snowflake_conn_id).get_records(qry_prior_max))
    logging.info(
        f"Collected prior change versions for {len(prior_change_versions)} endpoints."
    )

    ### Retrieve previous endpoint-level change versions and push as an XCom.
    return_tuples = []
    failed_total_counts = []  # Track which endpoints failed total-count gets in condition block.

    # Only initialize the Ed-Fi connection once if there is a max-change-version to compare against.
    if edfi_conn_id:
        edfi_conn = EdFiHook(edfi_conn_id=edfi_conn_id).get_conn()

    for namespace, endpoint in endpoints:
        last_max_version = prior_change_versions.get(endpoint, 0)

        # If Ed-Fi Conn is attached, only add to XCom if there are new rows to ingest.
        if edfi_conn_id and max_change_version is not None:

            try:
                resource = edfi_conn.resource(
                    endpoint, namespace=namespace,
                    get_deletes=get_deletes, get_key_changes=get_key_changes,
                    min_change_version=last_max_version,
                    max_change_version=max_change_version
                )

                if not resource.total_count():
                    continue
            
            except Exception:
                logging.warning(
                    f"Unable to retrieve record count for endpoint: {namespace}/{endpoint}"
                )
                failed_total_counts.append(endpoint)  # Still return the tuples, but mark as failed in the UI.
                continue

        return_tuples.append((endpoint, last_max_version))
        logging.info(f"{namespace}/{endpoint}: {last_max_version}")

    # Always push the xcom, but raise an AirflowFailException if any of the TotalCount gets failed.
    if failed_total_counts:
        context['ti'].xcom_push(key='return_value', value=return_tuples)
        raise AirflowFailException(
            f"Failed getting delta row count for one or more endpoints: {failed_total_counts}"
        )

    if not return_tuples:
        raise AirflowSkipException("No endpoints to process were found. Skipping downstream ingestion.")

    return return_tuples


def update_change_versions(
    tenant_code: str,
    api_year   : int,

    *,
    snowflake_conn_id: str,
    change_version_table: str,
    
    edfi_change_version: int,
    endpoints: List[str],
    get_deletes: bool,
    get_key_changes: bool,

    **kwargs
):
    """

    :return:
    """
    if not endpoints:
        raise AirflowSkipException(
            "There are no new change versions to update for any endpoints. All upstream tasks skipped or failed."
        )
    
    logging.info(f"Collected updated change versions for {len(endpoints)} endpoints.")
    rows_to_insert = []

    for endpoint in endpoints:
        rows_to_insert.append([
            tenant_code, api_year, endpoint,
            get_deletes, get_key_changes,
            kwargs["ds"], kwargs["ts"],
            edfi_change_version, True
        ])

    insert_into_snowflake(
        snowflake_conn_id=snowflake_conn_id,
        table_name=change_version_table,
        columns=[
            "tenant_code", "api_year", "name",
            "is_deletes", "is_key_changes",
            "pull_date", "pull_timestamp",
            "max_version", "is_active"
        ],
        values=rows_to_insert
    )

    return True
