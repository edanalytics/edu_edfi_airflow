import logging
from typing import Tuple

from airflow.exceptions import AirflowSkipException, AirflowFailException
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from edu_edfi_airflow.dags.dag_util.airflow_util import get_snowflake_params_from_conn
from edu_edfi_airflow.dags.dag_util.airflow_util import is_full_refresh, is_resource_specified



def get_resource_change_version(
    *,
    edfi_change_version: int,
    snowflake_conn_id: str,

    tenant_code : str,
    api_year    : int,
    resource    : str,
    deletes     : bool,

    change_version_table: str,

    full_refresh: bool = False,

    **kwargs
) -> Tuple:
    """
        We separate getting the most recent EdFi change version into a separate function.
        We need pulls to be consistent by change version for each run.
        Otherwise, the raw schema will passively drift across resources around versions.

        With every pull from EdFi 3+ to Snowflake, the change-version table is updated for the resource.
        The change version documents the timestamp of the last pull.

        This operator retrieves the most recent version found in Snowflake for a given resource.
        Use this and the most recent change version from EdFi to build an ingestion window.
        At the end of the ingest, the change version table is updated with the most recent version found in EdFi.

        If a full refresh is being completed, set all prior change versions in the table to inactive.
    """
    # If doing a resource-specific run, confirm resource is in the list.
    if not is_resource_specified(kwargs, resource):
        raise AirflowSkipException(
            "Skipping resource not specified in run context 'resources'."
        )

    # Retrieve the database and schema from the Snowflake hook.
    database, schema = get_snowflake_params_from_conn(snowflake_conn_id)

    # Build the SQL queries to be passed into `Conn.run()`.
    qry_prior_max = f"""
        select max(max_version)
        from {database}.{schema}.{change_version_table}
        where tenant_code = '{tenant_code}'
        and api_year = {api_year}
        and name = '{resource}'
        and is_deletes = {deletes}
    """

    qry_mark_inactive = f"""
        update {database}.{schema}.{change_version_table}
            set is_active = FALSE
        where tenant_code = '{tenant_code}'
        and api_year = {api_year}
        and name = '{resource}'
        and is_deletes = {deletes}
    """

    ### Connect to Snowflake and read from the change version table.
    snowflake_hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
    snowflake_conn = snowflake_hook.get_conn()

    # Pull prior max from Snowflake.
    with snowflake_conn.cursor() as cur:
        cur.execute(qry_prior_max)
        prior_change_version = cur.fetchone()

    # Collect the change version from the Cursor object.
    if prior_change_version is not None and prior_change_version[0] is not None:
        prior_change_version = prior_change_version[0]
        logging.info(
            f"Previous max change version is `{prior_change_version}`."
        )
    else:
        logging.info(
            "No previous runs detected; performing full pull."
        )
        prior_change_version = 0

    ### Update the change version table if it's a full_refresh run.
    # If marked full_refresh in context, set prior versions as inactive.
    if full_refresh or is_full_refresh(kwargs):
        logging.info(
            "Full refresh, marking previous pulls inactive."
        )
        prior_change_version = 0

        with snowflake_conn.cursor() as cur:
            cur.execute(qry_mark_inactive)

    # Run sanity checks to make sure we aren't doing something wrong.
    elif edfi_change_version == prior_change_version:
        raise AirflowSkipException(
            "ODS is unchanged since previous pull."
        )

    elif edfi_change_version < prior_change_version:
        raise AirflowFailException(
            "Apparent out-of-sequence run: current change version is smaller than previous!"
        )

    kwargs['ti'].xcom_push(key='max_change_version', value=edfi_change_version)
    kwargs['ti'].xcom_push(key='prev_change_version', value=prior_change_version)

    return edfi_change_version, prior_change_version



def update_change_version_table(
    tenant_code: str,
    api_year   : int,
    resource   : str,
    deletes    : str,

    snowflake_conn_id: str,
    change_version_table: str,

    edfi_change_version: int,

    **kwargs,
):
    """

    :return:
    """
    # Retrieve the database and schema from the Snowflake hook.
    database, schema = get_snowflake_params_from_conn(snowflake_conn_id)

    # Build the SQL queries to be passed into `Hook.run()`.
    qry_insert_into = f"""
        insert into {database}.{schema}.{change_version_table}
            (tenant_code, api_year, name, is_deletes, pull_date, pull_timestamp, max_version, is_active)
        select
            '{tenant_code}',
            '{api_year}',
            '{resource}',
            {deletes},
            to_date('{kwargs["ds_nodash"]}', 'YYYYMMDD'),
            to_timestamp('{kwargs["ts_nodash"]}', 'YYYYMMDDTHH24MISS'),
            {edfi_change_version},
            TRUE
        ;
    """

    snowflake_hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)

    cursor_log = snowflake_hook.run(
        sql=qry_insert_into
    )

    logging.info(cursor_log)
