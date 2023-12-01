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