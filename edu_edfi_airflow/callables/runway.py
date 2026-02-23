from edu_edfi_airflow.providers.runway.hooks.runway import RunwayHook


def send_to_runway(
    runway_conn_id: str,
    tenant_code: str,
    bundle_name: str,
    school_year: str,
    input_files: dict[str, str],
    bundle_params: dict[str, str] = {},
):
    hook = RunwayHook(runway_conn_id)
    client = hook.get_runway_client()

    client.load_files(
        tenant_code=tenant_code,
        bundle_name=bundle_name,
        bundle_params=bundle_params,
        input_files=input_files,
        school_year=school_year,
    )
