## Unreleased
- Add `partition_on_tenant_and_year`, a preprocessing function to shard data to parquet on disk. This is useful when a single input file contains multiple years and/or tenants.

# edu_edfi_airflow v0.3.0
## New features
- Add `/keyChanges` ingestion for resource endpoints
- Add new method for `EdFiResourceDAG` endpoint instantiation using `resource_configs` and `descriptor_configs` arguments in init
  - The prior methods `EdFiResourceDAG.{add_resource, add_descriptor, add_resource_deletes}` are deprecated in favor of this more performant approach.
- Refactor `EdFiToS3Operator` taskgroup into three options (determined by `run_type` argument):
  - "default": One `EdFiToS3Operator` task per resource/deletes/keyChanges endpoint
  - "bulk": One `BulkEdFiToS3Operator` task in which all endpoints are looped over in one callable
  - "dynamic": One dynamically-mapped `EdFiToS3Operator` task per resource with deltas to ingest

## Under the hood
- Copies from S3 to Snowflake in `EdFiResourceDAG` are now completed in a single bulk task (instead of one per endpoint)
- `EdFiResourceDAG` and `EarthbeamDAG` now inherit from `ea_airflow_util` DAG factory `EACustomDAG`
- Streamline XCom passing between tasks in `EdFiResourceDAG`
- Change-version window delta counts are made when checking change versions in Snowflake.
  - Only resources with rows-to-ingest are passed to the Ed-Fi operator.


# edu_edfi_airflow v0.2.5
## New features
- Add optional argument `schedule_interval_full_refresh` to specify a CRON syntax for full-refresh Ed-Fi DAG runs.

## Fixes
- Update Earthbeam DAG logging copy statement to prevent character-escaping issues during copy.


# edu_edfi_airflow v0.2.4
## New features
- Add alternative arguments for setting `s3_destination_key` in `S3ToSnowflakeOperator`: `s3_destination_dir` and `s3_destination_filename`.


# edu_edfi_airflow v0.2.3
## Fixes
- Fix parsing error in full-refresh runs


# edu_edfi_airflow v0.2.2
## New features
- Add optional argument `pool` to `EdFiResourceDAG.build_edfi_to_snowflake_task_group()` to override DAG-level pool when ingesting high-impact resources.

## Fixes
- Fix task-ordering in `EarthbeamDAG` to make the success of Snowflake-logging independent of downstream tasks.


# edu_edfi_airflow v0.2.1
## New features
- Add optional argument `database_conn_id` to `EarthmoverOperator` and `EarthbeamDAG` to pass database connection information without exposing it in Airflow Task Instance details.

## Fixes
- Remove `provide_context` from default kwarg arguments in `EarthbeamDAG.build_bash_preprocessing_operator()`.


# edu_edfi_airflow v0.2.0
## New features
- Add `EarthbeamDAG` for sending raw data into Ed-Fi ODS or Stadium data warehouse directly
- Add `EarthmoverOperator` and `LightbeamOperator`
- Add optional operator to `EdFiResourceDAG` to increment Airflow variable at run-completion (for selective triggering of downstream DBT DAG)

## Under the hood
- Refactor `EdFiResourceDAG` to bundle tasks that interface with the meta-change-version Snowflake table
- Refactor `EdFiResourceDAG` to streamline declaration and bundling of Ed-Fi endpoint task-groups
- Use Airflow Params for defining DAG-level configs
- Extend `S3ToSnowflakeOperator` to manually specify Ed-Fi-specific metadata in SQL copy-statement

## Fixes
- Fix bug where DAG-kwargs are not passed to `EdFiResourceDAG` init
- Fix bug where running a full-refresh on a subset of endpoints reset all endpoints in the meta-change-version Snowflake table