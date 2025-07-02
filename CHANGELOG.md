# Unreleased
- Deprecate the `pull_all_deletes` functionality (behaviour resolved in dbt)

# edu_edfi_airflow v0.4.7
## Under the hood
- Revert default of `pull_all_deletes` to false in preparation for deprecation. (Behavior resolved in dbt)

# edu_edfi_airflow v0.4.6
## New features
- Add functionality to store total counts from the ODS, for data quality checks
## Under the hood
- Consistency fix in earthbeam dag

# edu_edfi_airflow v0.4.5
## New features
- Update Resources DAG UI for compatibility with Airflow 2.9.x (backwards compatibility with prior versions is retained)


# edu_edfi_airflow v0.4.4
## Under the hood
- Allow CSV files to optionally enclose fields in quotes when copying in `EarthbeamDAG`.

## Fixes
- Wrap filepath in quotes in `EarthbeamDAG` to copy paths with spaces and escape characters.


# edu_edfi_airflow v0.4.3
## Under the hood
- Use bulk copies to Snowflake when writing deletes to circumvent statement-limit errors.

## Fixes
- Add dependency between S3 and file removal in `EarthbeamDAG` to avoid race condition where files are removed prematurely.
- 

# edu_edfi_airflow v0.4.2
## New features
- Add boolean `pull_all_deletes` argument to `EdFiResourceDAG` to re-pull all deletes for a resource when any are added (resolves deletes-skipping bug).
- Allow `SNOWFLAKE_TENANT_CODE` to be overridden in `earthmover_kwargs` in `EarthbeamDAG`.

## Under the hood
- Simplify taskgroup declaration in `EarthbeamDAG`.

## Fixes
- Fix bug where singleton filepaths in `EarthbeamDAG` were not converted to lists upon initialization.
- Add dependency between Lightbeam and file-deletion in `EarthbeamDAG`.


# edu_edfi_airflow v0.4.1
## Under the hood
- Wrap Snowflake stage with single quotes to support filepaths with special characters

## Fixes
- Fix bugs where files written to S3 could be overwritten in `EarthbeamDAG`
- Fix bug where optional files fail upload to S3



# edu_edfi_airflow v0.4.0
## New features
- Add `EarthbeamDAG.partition_on_tenant_and_year()`, a preprocessing function to shard data to parquet on disk. This is useful when a single input file contains multiple years and/or tenants.
- Add `EarthbeamDAG.build_dynamic_tenant_year_task_group()` to build dynamic Earthbeam task groups for each file to process in a source folder
- Add ID matching sub-taskgroup and arguments to `EarthbeamDAG` taskgroups, in order to retrieve an assessment's identity columns from Snowflake
- Add optional postprocess Python callable to `EarthbeamDAG` taskgroups
- Add optional Lightbeam validation to `EarthbeamDAG` taskgroups
- Add option to log Python preprocess and postprocess outputs to Snowflake

## Under the hood
- Make accessing the `Total-Count` of the Ed-Fi `/deletes` endpoints optional using argument `get_deletes_cv_with_deltas` (necessary for generic Ed-Fi 5.3 ODSes)
- Refactor `EarthbeamDAG` to use Airflow TaskFlow syntax and simplify Earthbeam task groups
- Deprecate `EarthbeamDAG.build_tenant_year_task_group()` argument `raw_dir`


# edu_edfi_airflow v0.3.1
## Fixes
- Fix bug where updates to query-parameters persisted across every `EdFiResourceDAG`
- Add logging of failed endpoints on `EdFiResourceDAG` task `failed_total_counts`


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
