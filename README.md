# Overview
`edu_edfi_airflow` provides Airflow hooks and operators for transferring data from an Ed-Fi ODS to a Snowflake data warehouse.

This package is part of Enable Data Union (EDU).
Please visit the [EDU docs site](https://docs.enabledataunion.org/) for more information.



## EdFiResourceDAG
`EdFiResourceDAG` is the main instantiation of these operators chained into an Airflow DAG.
The `EdFiToS3Operator` pulls JSON rows for a specified Ed-Fi resource and writes them locally before copying the files to S3.
The `S3ToSnowflakeOperator` copies the transferred files using a Snowflake stage.

This implementation takes advantage of Ed-Fi3 change-version logic, allowing daily ingestion of ODS deltas and incorporation of resource deletes.
Although the DAG-implementation is designed for Ed-Fi3, it will work for Ed-Fi2 ODS instances, although without incremental ingestion.

![EdFiResourceDAG](./images/EdFiResourceDAG.png)

<details>
<summary>Arguments:</summary>

| Argument             | Description                                                                                                                        |
|:---------------------|:-----------------------------------------------------------------------------------------------------------------------------------|
| tenant_code          | ODS-tenant representation to be saved in Snowflake tables                                                                          |
| api_year             | ODS API-year to be saved in Snowflake tables                                                                                       |
| edfi_conn_id         | Airflow connection with Ed-Fi ODS credentials and metadata defined for a specific tenant                                           |
| s3_conn_id           | Airflow connection with S3 bucket defined under `schema`                                                                           |
| snowflake_conn_id    | Airflow connection with Snowflake credentials, database, and schema defined                                                        |
| pool                 | Airflow pool to assign EdFi-to-S3 pulls for this DAG (designed to prevent the ODS from being overwhelmed)                          |
| tmp_dir              | Path to the temporary directory on the EC2 server where ODS data is written before their transfer to S3                            |
| use_change_version   | Boolean flag for using change versions to complete delta ingests (default `True`; turned off for Ed-Fi2)                           |
| change_version_table | Name of the table to record resource change versions on Snowflake (defaults to `'_meta_change_versions'`)                          |
| multiyear            | Boolean flag for whether the ODS has multiple years of data within one API year (defaults to `False`; dispreferred implementation) |
| full_refresh         | Boolean flag for whether to use full-refresh logic during a run (used differently in resource vs. descriptor runs)                 |
| slack_conn_id        | Optional Airflow connection with Slack webhook credentials (default None)                                                          |

Additional Airflow DAG parameters (e.g. `schedule_interval`, `default_args`, etc.) can be passed as kwargs.

-----

</details>


Default instantiation of these in our `int_edfi_project_template` come in the following YAML structure:
```yaml
edfi_resource_dags__default_args: &default_dag_args
  default_args: *default_task_args

  schedule_interval: null
  schedule_interval_resources: null    # Optional to provide differing schedule logic between resources and descriptors.
  schedule_interval_descriptors: null  # If either is unpopulated, `schedule_interval` will be used by default.

  # Airflow Connection IDs
  edfi_conn_id: ~
  s3_conn_id: 'data_lake'
  snowflake_conn_id: 'snowflake'

  # Variables for pulling from EdFi
  tmp_dir: '/home/airflow/tmp_storage'
  pool: ~

  # Variables for interacting with Snowflake
  change_version_table: '_meta_change_versions'


edfi_resource_dags:
  TENANT1:
    YEAR1:
      pool: default_pool
      edfi_conn_id: 'edfi_TENANT1_YEAR1'
      schedule_interval: null
      <<: *edfi_resource_dags__default_args
  TENANT2:
    YEAR1:
      pool: default_pool
      edfi_conn_id: 'edfi_TENANT2_YEAR1'
      schedule_interval: null
      <<: *edfi_resource_dags__default_args
```

-----


## Connections

Three types of connections must be defined in Airflow to complete a full run.
Each connection is outlined below with required fields that must be populated for a successful run.
An optional Slack connection for logging run failures has also been outlined.

### Ed-Fi Connection

Each Ed-Fi connection references one API year in one ODS (unless the ODS is multiyear).
These are passed into an `EdFiHook` to an `EdFiApiClient`.

<details>
<summary>Arguments:</summary>

| Argument        | Description                                                                                                  |
|:----------------|:-------------------------------------------------------------------------------------------------------------|
| Connection Id   | Name of connection to reference in config files and across operators                                         |
| Connection Type | `HTTP`                                                                                                       |
| Host            | Base URL for the specific Ed-Fi ODS instantiation (extra pathing must be removed)                            |
| Login           | Client Key for the ODS                                                                                       |
| Password        | Client Secret for the ODS                                                                                    |
| Extra           | JSON structure with `api_mode` (required), `api_version` (default 3), and `instance_code` (optional) defined |

If `api_version` or `api_mode` are undefined in `Extra`, these will be inferred from the ODS (Ed-Fi3 only). 

-----

</details>



### AWS S3 Connection

This connection outlines the S3 datalake bucket to which Ed-Fi data is staged before transferring to Snowflake.

<details>
<summary>Arguments:</summary>

| Argument        | Description                                                                   |
|:----------------|:------------------------------------------------------------------------------|
| Connection Id   | Name of connection to reference in config files and across operators          |
| Connection Type | `S3`                                                                          |
| Schema          | S3 bucket name used to store data transferred from the Ed-Fi ODS to Snowflake |
| Login           | [Empty]; Must be defined if EC2 IAM role is not scoped                        |
| Password        | [Empty]; Must be defined if EC2 IAM role is not scoped                        |

It is recommended to extend the EC2 server's IAM role to include S3 permissions on the datalake bucket specified in `schema`.
If done correctly, `login` and `password` can be left blank and inferred automatically. 

-----

</details>



### Snowflake Connection

This connection outlines the Snowflake account to which the S3 stage and subsequent raw tables are defined.

<details>
<summary>Arguments:</summary>

| Argument        | Description                                                                                     |
|:----------------|:------------------------------------------------------------------------------------------------|
| Connection Id   | Name of connection to reference in config files and across operators                            |
| Connection Type | `Snowflake`                                                                                     |
| Host            | Host URL for the Snowflake instance                                                             |
| Schema          | Snowflake schema destination for raw Ed-Fi data                                                 |
| Login           | Snowflake Airflow loader role                                                                   |
| Password        | Snowflake loader password                                                                       |
| Extra           | JSON structure with Snowflake-specific fields (also defined below)                              |
 | Account         | Snowflake account associated with instance (`extra__snowflake__account`)                        |
| AWS Access Key  | Access key to AWS account associated with S3 bucket (`extra__snowflake__aws_access_key_id`)     |
| AWS Secret Key  | Secret key to AWS account associated with S3 bucket (`extra__snowflake__aws_secret_access_key`) |
| Database        | Snowflake database destination for raw Ed-Fi data (`extra__snowflake__database`)                |
| Region          | (Optional) AWS region associated with S3 bucket (`extra__snowflake__region`)                    |
| Role            | Snowflake loader role (`extra__snowflake__role`)                                                |
| Warehouse       | Snowflake warehouse destination for raw Ed-Fi data (`extra__snowflake__warehouse`)              |

Snowflake-specific fields can be defined either as a JSON object in `Extra`, or in the extra fields underneath.
When editing these values, it is recommended to edit the JSON object directly.
The values in the fields will update after saving the connection.

-----

</details>



### Slack Connection

This connection is used for sending Airflow task successes and failures to a dedicated Slack channel.
Channel configurations must be specified on webhook-creation.


<details>
<summary>Arguments:</summary>

| Argument        | Description                                                          |
|:----------------|:---------------------------------------------------------------------|
| Connection Id   | Name of connection to reference in config files and across operators |
| Connection Type | `Slack Webhook`                                                      |
| Host            | `https://hooks.slack.com/services`                                   |
| Password        | The trailing ID path of the webhook URL, including the initial "/"   |

-----

</details>


-----

## Classes

### EdFiHook
This Airflow Hook connects to the Ed-Fi ODS using an `EdFiApiClient`.

<details>
<summary>Arguments:</summary>

| Argument     | Description                                                                         |
|:-------------|:------------------------------------------------------------------------------------|
| edfi_conn_id | Name of the Airflow connection where Ed-Fi ODS connection metadata has been defined |

-----

</details>



### EdFiToS3Operator
Transfers a specific resource (or resource deletes) from the Ed-Fi ODS to S3. 

<details>
<summary>Arguments:</summary>

| Argument                 | Description                                                                                             |
|:-------------------------|:--------------------------------------------------------------------------------------------------------|
| edfi_conn_id             | Name of the Airflow connection where Ed-Fi ODS connection metadata has been defined                     |
| resource                 | Name of Ed-Fi resource/descriptor to pull from the ODS                                                  |
| query_parameters         | Custom parameters to apply to the pull (default `None`)                                                 |
| min_change_version       | Minimum change version to pull for the resource (default `None`)                                        |
| max_change_version       | Maximum change version to pull for the resource (default `None`)                                        |
| change_version_step_size | Window size to apply during change-version stepping (default `50000`                                    |
| api_namespace            | Namespace under which the resource is assigned (default `"ed-fi"`)                                      |
| api_get_deletes          | Boolean flag for whether to retrieve the resource's associated deletes (default `False`)                |
| api_retries              | Number of attempts the pull should make before giving up (default `5`)                                  |
| page_size                | Number of rows to pull at each GET (default `100`)                                                      |
| tmp_dir                  | Path to the temporary directory on the EC2 server where ODS data is written before their transfer to S3 |
| s3_conn_id               | Name of the Airflow connection where S3 connection metadata has been defined                            |
| s3_destination_key       | Destination key where Ed-Fi resource data should be written on S3                                       |

`page_size` should be tuned per ODS and resource.
Contact your ODS-administrator to determine maximum-allowable page-size and recommendeded size to prevent overwhelming the ODS.

If either `min_change_version` or `max_change_version` are undefined, change version stepping does not occur.

-----

</details>



### S3ToSnowflakeOperator
Transfers a specific S3 key to the specified table in Snowflake.
First completes a `DELETE FROM` statement if `full_refresh` is defined explicitly or in the context.

<details>
<summary>Arguments:</summary>

| Argument           | Description                                                                                                |
|:-------------------|:-----------------------------------------------------------------------------------------------------------|
| edfi_conn_id       | Name of the Airflow connection where Ed-Fi ODS connection metadata has been defined                        |
| snowflake_conn_id  | Name of the Airflow connection where Snowflake connection metadata has been defined                        | 
| tenant_code        | ODS-tenant representation to be saved in Snowflake tables                                                  | 
| api_year           | ODS API-year to be saved in Snowflake tables                                                               |  
| resource           | Static name of Ed-Fi resource, placed in the `name` column of the destination table                        |
| table_name         | Name of the raw Snowflake table to copy into on Snowflake                                                  |
| s3_destination_key | Source key where JSON data is saved on S3                                                                  |
| full_refresh       | Boolean flag to explicitly mark the run as a full-refresh; used when pulling descriptors (default `False`) |

-----

</details>


-----

## Callables

### get_edfi_change_version
Retrieves the most recent change version in the ODS using `EdFiApiClient.get_newest_change_version()`
Returns `None` if an Ed-Fi2 ODS, as change versions are unimplemented.

<details>
<summary>Arguments:</summary>

| Argument     | Description                                                                         |
|:-------------|:------------------------------------------------------------------------------------|
| edfi_conn_id | Name of the Airflow connection where Ed-Fi ODS connection metadata has been defined |

-----

</details>


### get_resource_change_version
Retrieves the most recent change version for a given (tenant, year, resource) grain saved in the Snowflake `change_version_table`.
Returns `0` if Ed-Fi2 or if no records for this resource are found (signifying full-refresh).

Combining `get_edfi_change_version` with `get_resource_change_version` allows a `min_change_version` to `max_change_version` window to be defined for the pull.
This allows incremental ingests of resource deltas since the last pull, drastically improving runtimes when compared to full-refreshes.
Because Ed-Fi2 lacks change versions, all Ed-Fi2 pulls are full-refreshes.

<details>
<summary>Arguments:</summary>

| Argument             | Description                                                                                                           |
|:---------------------|:----------------------------------------------------------------------------------------------------------------------|
| edfi_change_version  | The most recent change version present in the ODS (as retrieved from `get_edfi_change_version`)                       |
| snowflake_conn_id    | Name of the Airflow connection where Snowflake connection metadata has been defined                                   |
| tenant_code          | ODS-tenant representation to be saved in Snowflake tables                                                             | 
| api_year             | ODS API-year to be saved in Snowflake tables                                                                          |
| resource             | Name of Ed-Fi resource being compared between the ODS and Snowflake                                                   |
| deletes              | Boolean flag to mark whether the change-version is associated with the specified resource's deletes (default `False`) |
| change_version_table | Name of the table to record resource change versions on Snowflake                                                     |
| full_refresh         | Boolean flag to explicitly mark the run as a full-refresh; used internally for pulling descriptors (default `False`)  |

-----

</details>


### update_change_version_table
Updates the change version table in Snowflake with the most recent change version for which data was ingested,
as specified by the (tenant, year, resource) grain.

<details>
<summary>Arguments:</summary>

| Argument             | Description                                                                                                           |
|:---------------------|:----------------------------------------------------------------------------------------------------------------------|
| tenant_code          | ODS-tenant representation to be saved in Snowflake tables                                                             | 
| api_year             | ODS API-year to be saved in Snowflake tables                                                                          |
| resource             | Name of Ed-Fi resource being compared between the ODS and Snowflake                                                   |
| deletes              | Boolean flag to mark whether the change-version is associated with the specified resource's deletes (default `False`) |
| snowflake_conn_id    | Name of the Airflow connection where Snowflake connection metadata has been defined                                   |
| change_version_table | Name of the table to record resource change versions on Snowflake                                                     |
| edfi_change_version  | The most recent change version present in the ODS (as retrieved from `get_edfi_change_version`)                       |

-----

</details>