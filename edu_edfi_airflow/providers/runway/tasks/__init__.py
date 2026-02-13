from airflow.decorators import task

from ..hooks import RunwayHook


@task
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

    job = client.request_runway_job(
        tenant_code=tenant_code,
        bundle_name=bundle_name,
        input_files=input_files,
        bundle_params=bundle_params,
        school_year=school_year,
    )
    client.upload_to_s3(job, input_files)
    client.start_job(job)
