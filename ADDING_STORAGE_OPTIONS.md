# Adding New Object Storage and Database Support

## Adding Object Storage Support

### Option 1: Current Approach (if/else in base class)

Edit `edu_edfi_airflow/providers/edfi/transfers/edfi_to_s3.py`:

1. Add new condition in `EdFiToObjectStorageOperator.get_object_storage()`:

```python
elif conn.conn_type == 'gcs':  # Google Cloud Storage
    # Get bucket from connection
    bucket = conn.schema
    if not bucket:
        raise ValueError(f"Bucket not found in connection {object_storage_conn_id}")
    
    # Build GCS URL
    storage_path = f"gs://{bucket}/{destination_key}"
    
    # Create ObjectStoragePath (adjust based on how GCS provider expects conn_id)
    obj_storage = ObjectStoragePath(storage_path, conn_id=object_storage_conn_id)
```

2. Return appropriate tuple based on whether internal/clean URLs differ.

### Option 2: Mixin Approach

Edit `edu_edfi_airflow/providers/edfi/transfers/edfi_to_s3.py`:

1. Create a new mixin after the V2 mixins section:

```python
class GCSMixinV2:
    """Google Cloud Storage mixin."""
    def __init__(self, *args, gcs_conn_id: str, **kwargs) -> None:
        super(GCSMixinV2, self).__init__(*args, object_storage_conn_id=gcs_conn_id, **kwargs)
    
    @classmethod
    def get_object_storage(cls, object_storage_conn_id, destination_key=None, 
                          destination_dir=None, destination_filename=None, **kwargs):
        # Build destination_key if needed
        if not destination_key:
            if not destination_dir or not destination_filename:
                raise ValueError("Must provide destination_key or dir+filename")
            destination_key = os.path.join(destination_dir, destination_filename)
        
        # Get connection
        conn = BaseHook.get_connection(object_storage_conn_id)
        bucket = conn.schema
        
        # Build path
        storage_path = f"gs://{bucket}/{destination_key}"
        
        return (ObjectStoragePath(storage_path, conn_id=object_storage_conn_id), storage_path)
```

2. Create operator classes:

```python
class EdFiToGCSOperator(GCSMixinV2, EdFiToObjectStorageOperator):
    pass

class BulkEdFiToGCSOperator(GCSMixinV2, BulkEdFiToObjectStorageOperator):
    pass
```

3. Uncomment and update `__new__` method in `EdFiToObjectStorageOperator` to select operators based on connection type.

---

## Adding Database Support

Edit `edu_edfi_airflow/providers/snowflake/transfers/s3_to_snowflake.py`:

1. Create a new mixin class:

```python
class BigQueryMixin:
    def __init__(self, *args, database_conn_id: str, **kwargs):
        super(BigQueryMixin, self).__init__(*args, database_conn_id=database_conn_id, **kwargs)
    
    def run_sql_queries(self, name: str, table: str, destination_key, full_refresh: bool = False):
        """Load data from object storage into BigQuery."""
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
        
        # Extract dataset and table info
        dataset, schema = airflow_util.get_database_params_from_conn(self.database_conn_id)
        
        # Parse the storage URL (destination_key is a string URL)
        from airflow.io.path import ObjectStoragePath
        if isinstance(destination_key, str):
            storage_path = ObjectStoragePath(destination_key).key
        else:
            storage_path = destination_key
        
        # Build BigQuery LOAD statement
        # ... your BigQuery-specific SQL logic here ...
        
    def run_bulk_sql_queries(self, names: List[str], table: str, destination_key, full_refresh: bool = False):
        """Bulk load implementation for BigQuery."""
        for name in names:
            self.run_sql_queries(name, table, destination_key, full_refresh)
```

2. Create operator classes:

```python
class BigQuerySingleOperator(BigQueryMixin, ObjectStorageToDatabaseOperator):
    """Single resource operator for BigQuery"""
    pass

class BigQueryBulkOperator(BigQueryMixin, BulkObjectStorageToDatabaseOperator):
    """Bulk resource operator for BigQuery"""
    pass
```

3. Update `find_ops_for_conn()` function:

```python
def find_ops_for_conn(database_conn_id: str):
    connection = Connection.get_connection_from_secrets(database_conn_id)
    if connection.conn_type == 'snowflake':
        return SnowflakeSingleOperator, SnowflakeBulkOperator
    elif connection.conn_type == 'databricks':
        return DatabricksSingleOperator, DatabricksBulkOperator
    elif connection.conn_type == 'bigquery':  # Add your database type
        return BigQuerySingleOperator, BigQueryBulkOperator
    else:
        raise ValueError(f"Unsupported database type: {connection.conn_type}")
```

4. Operators will be automatically selected via the `__new__` factory pattern based on connection type.

---

## Key Points

- **Object Storage** implementations need to handle URL formatting and authentication
- **Database** implementations need to implement SQL/loading logic for their platform
- Both use **connection type detection** to automatically select the right implementation
- The `destination_key` parameter is a **string URL** that may need parsing via `ObjectStoragePath`

