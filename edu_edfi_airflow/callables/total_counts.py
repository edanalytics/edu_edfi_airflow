import logging

from typing import List, Tuple, Optional

from airflow.exceptions import AirflowSkipException

from edu_edfi_airflow.callables import airflow_util
from edu_edfi_airflow.interfaces.database import DatabaseInterface
from edu_edfi_airflow.providers.edfi.hooks.edfi import EdFiHook


def get_total_counts(
    endpoints: List[Tuple[str, str]],

    *,
    edfi_conn_id: str,
    max_change_version: int,
    use_edfi_token_cache: bool = False,

    **context
) -> None:

    edfi_conn = EdFiHook(edfi_conn_id=edfi_conn_id, use_token_cache=use_edfi_token_cache).get_conn()

    # Only ping the API if the endpoint is specified in the run.
    config_endpoints = airflow_util.get_config_endpoints(context)

    successful_endpoints = []
    failed_endpoints = []

    for namespace, endpoint in endpoints:

        # If a subset of endpoints have been selected, only get CV counts for these.
        if config_endpoints and endpoint not in config_endpoints:
            continue

        try:
            resource = edfi_conn.resource(
                endpoint, 
                namespace=namespace,
                min_change_version=0,
                max_change_version=max_change_version
            )

            if not (total_record_count := resource.total_count()):
                continue
            successful_endpoints.append((endpoint, total_record_count))
        
        except Exception:
            logging.warning(
                f"    Unable to retrieve record count for endpoint: {namespace}/{endpoint}"
            )
            failed_endpoints.append(endpoint)
            continue

    if failed_endpoints:
        logging.info(
            f"Failed getting delta row count for one or more endpoints: {failed_endpoints}"
        )

    if not successful_endpoints:
        raise AirflowSkipException("No endpoints were found. Skipping downstream loading.")

    return successful_endpoints


def delete_total_counts(
    tenant_code : str,
    api_year    : int,

    *,
    database_conn_id: Optional[str] = None,
    total_counts_table: str,

    **kwargs
) -> None:
    # Airflow-skip if run not marked for a full-refresh.
    if not airflow_util.is_full_refresh(kwargs):
        raise AirflowSkipException(f"Full refresh not specified. Total counts table `{total_counts_table}` unchanged.")

    ### Prepare the SQL query.
    db = DatabaseInterface(database_conn_id)

    # Retrieve the database and schema from the database connection
    qry_delete = f"""
        DELETE FROM {db.database}.{db.schema}.{total_counts_table}
        WHERE tenant_code = '{tenant_code}'
        AND api_year = '{api_year}'
    """

    ### Connect to database and execute the query.
    logging.info("Full refresh: deleting data from previous pulls.")
    db.query_database(qry_delete, **kwargs)


def insert_total_counts(
    tenant_code: str,
    api_year   : int,

    *,
    database_conn_id: Optional[str] = None,
    total_counts_table: str,
    endpoint_counts: List[Tuple[str, int]],

    **kwargs
):
    """

    :return:
    """
    
    logging.info(f"Collected total counts for {len(endpoint_counts)} endpoints.")
    
    columns = [
        "tenant_code", "api_year", "name",
        "pull_date", "pull_timestamp",
        "total_count",
    ]
    
    # Build and insert row tuples for each endpoint.
    rows_to_insert = []

    for endpoint in endpoint_counts:
        row = [
            tenant_code, api_year, endpoint[0],
            kwargs["ds"], kwargs["ts"],
            endpoint[1]
        ]
        rows_to_insert.append(row)

    DatabaseInterface(database_conn_id).insert_into_database(
        table=total_counts_table,
        columns=columns,
        values=rows_to_insert,
        **kwargs
    )

    return True
