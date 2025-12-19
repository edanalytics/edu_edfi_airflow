# Adding New Object Storage and Database Support
The edfi_resource_dag currently supports loading to datalakes in S3 or Azure Data Lake Storage, and loading to databases in Snowflake or Databricks. To add other storage options, see instructions below.

## Adding Object Storage Support

The codebase uses an interface-based approach with a factory pattern for object storage implementations. New storage types are added by creating a subclass of `ObjectStorageInterface`.

### Implementation Steps

Edit `edu_edfi_airflow/interfaces/object_storage.py`:

1. **Add connection type check** in `ObjectStorageInterface.__new__()`:

```python
def __new__(cls, object_storage_conn_id: str, **kwargs):
    conn = BaseHook.get_connection(object_storage_conn_id)
    
    if conn.conn_type == 'adls':
        return object.__new__(ADLSObjectStorageInterface)
    elif conn.conn_type == 'http':
        return object.__new__(S3ObjectStorageInterface)
    elif conn.conn_type == 'gcs':  # Add your new storage type
        return object.__new__(GCSObjectStorageInterface)
    else:
        raise ValueError(f"ObjectStorageInterface type {conn.conn_type} is not defined!")
```

2. **Create interface implementation** that inherits from `ObjectStorageInterface`:

```python
class GCSObjectStorageInterface(ObjectStorageInterface):
    """
    Google Cloud Storage interface that creates ObjectStoragePath with conn_id.
    Returns tuple of (ObjectStoragePath, clean_url) for downstream operators.
    """
    def get_object_storage(self,
        destination_key: Optional[str] = None,
        destination_dir: Optional[str] = None,
        destination_filename: Optional[str] = None,
        **kwargs
    ):
        # Build destination_key if not provided
        if not destination_key:
            if not destination_dir or not destination_filename:
                raise ValueError("Must provide destination_key or both destination_dir and destination_filename")
            destination_key = os.path.join(destination_dir, destination_filename)
        
        # Get connection to extract bucket
        conn = BaseHook.get_connection(self.object_storage_conn_id)
        bucket = conn.schema
        
        if not bucket:
            raise ValueError(f"Bucket name not found in connection {self.object_storage_conn_id}")
        
        # Internal format: embed conn_id in URL so Airflow can extract it
        # Clean format: URL without conn_id for downstream operators
        storage_path = f"gs://{bucket}/{destination_key}"
        
        return (ObjectStoragePath(storage_path, conn_id=self.object_storage_conn_id), destination_key)
```

### Key Points

- **Inheritance**: Your class MUST inherit from `ObjectStorageInterface`
- **Factory Pattern**: `__new__` method automatically selects the right implementation based on connection type
- **Return Value**: Must return a tuple of `(ObjectStoragePath, clean_url_string)`
  - `ObjectStoragePath`: Internal representation with embedded conn_id for Airflow
  - `clean_url_string`: Clean URL string for downstream operators (may differ from internal path)
- **Attributes**: Automatically inherit `self.object_storage_conn_id` from parent `__init__`

---

## Adding Database Support

The codebase uses an interface-based approach with a factory pattern and context manager for database implementations. New database types are added by creating a subclass of `DatabaseInterface`.

### Implementation Steps

Edit `edu_edfi_airflow/interfaces/database.py`:

1. **Add connection type check** in `DatabaseInterface.__new__()`:

```python
def __new__(cls, database_conn_id: str, **kwargs):
    conn = BaseHook.get_connection(database_conn_id)
    
    if conn.conn_type == 'snowflake':
        return object.__new__(SnowflakeDatabaseInterface)
    elif conn.conn_type == 'databricks':
        return object.__new__(DatabricksDatabaseInterface)
    elif conn.conn_type == 'bigquery':  # Add your new database type
        return object.__new__(BigQueryDatabaseInterface)
    else:
        raise ValueError(f"DatabaseInterface type {conn.conn_type} is not defined!")
```

2. **Create interface implementation** that inherits from `DatabaseInterface`:

```python
class BigQueryDatabaseInterface(DatabaseInterface):
    
    def __enter__(self):
        """Initialize the database hook when entering context manager."""
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
        self.hook = BigQueryHook(self.database_conn_id)
        return self
    
    def __exit__(self, exc_type, exc_value, exc_traceback):
        """Execute accumulated SQL queries when exiting context manager."""
        self.hook.run(sql=self.sql, autocommit=False)
    
    def query_database(self, sql: str, **kwargs):
        """Run query on BigQuery and return results."""
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
        bigquery_hook = BigQueryHook(self.database_conn_id)
        return bigquery_hook.get_records(sql)
    
    def insert_into_database(self, table: str, columns: List[str], values: List[list], **kwargs):
        """Insert into BigQuery database."""
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
        
        # Force a single record into a list for iteration below
        if not all(isinstance(val, (list, tuple)) for val in values):
            values = [values]
        
        bigquery_hook = BigQueryHook(self.database_conn_id)
        # Implement BigQuery-specific insert logic here
        # ...
    
    def delete_from_raw(self, tenant_code: str, api_year: str, name: Union[str, List[str]], table: str) -> str:
        """Build DELETE query for BigQuery."""
        # Use an array of names to allow same method to be used in single and bulk approaches
        if isinstance(name, str):
            name = [name]
        
        names_repr = "', '".join(name)
        
        qry = f"""
            DELETE FROM {self.database}.{self.schema}.{table}
            WHERE tenant_code = '{tenant_code}'
            AND api_year = '{api_year}'
            AND name in ('{names_repr}')
        """
        self.sql.append(qry)
        return qry
    
    def copy_into_raw(self,
        tenant_code: str, api_year: str, name: str, table: str,
        ods_version: int, data_model_version: str, storage_path: str
    ) -> str:
        """Build COPY/LOAD query for BigQuery."""
        qry = f"""
            LOAD DATA INTO {self.database}.{self.schema}.{table}
            FROM FILES (
                format = 'JSON',
                uris = ['{storage_path}']
            )
            -- Add BigQuery-specific column mappings and transformations
        """
        self.sql.append(qry)
        return qry
    
    def bulk_copy_into_raw(self,
        tenant_code: str, api_year: str, name: List[str], table: str,
        ods_version: int, data_model_version: str, storage_path: str
    ) -> str:
        """Build bulk COPY/LOAD query for BigQuery."""
        # Implement bulk load logic (may reuse single copy or do directory-level load)
        # ...
        return qry
```

### Key Points

- **Inheritance**: Your class MUST inherit from `DatabaseInterface`
- **Factory Pattern**: `__new__` method automatically selects the right implementation based on connection type
- **Context Manager**: Implements `__enter__` and `__exit__` for transaction management
  - `__enter__`: Initialize the database hook
  - `__exit__`: Execute accumulated SQL queries with transaction control
- **SQL Accumulation**: Queries are built via methods like `delete_from_raw()` and `copy_into_raw()`, appended to `self.sql`, then executed on context exit
- **Attributes**: Automatically inherit from parent `__init__`:
  - `self.database_conn_id`: Connection ID
  - `self.database`: Database name (extracted from connection)
  - `self.schema`: Schema name (extracted from connection)
  - `self.hook`: Database hook (initialized in `__enter__`)
  - `self.sql`: List of SQL statements to execute (initialized as empty list)
- **Abstract Methods**: Must implement all abstract methods:
  - `__enter__` and `__exit__` for context manager protocol
  - `query_database()`: Execute query and return results
  - `insert_into_database()`: Insert rows into table
  - `delete_from_raw()`: Build DELETE statement
  - `copy_into_raw()`: Build single-file COPY/LOAD statement
  - `bulk_copy_into_raw()`: Build bulk COPY/LOAD statement

---

## How It Works

### Automatic Interface Selection

Both object storage and database interfaces use the factory pattern to automatically select the correct implementation:

```python
# In your DAG or operator code:
storage_interface = ObjectStorageInterface('my_gcs_conn')
# Automatically returns GCSObjectStorageInterface instance if connection type is 'gcs'

database_interface = DatabaseInterface('my_bigquery_conn')
# Automatically returns BigQueryDatabaseInterface instance if connection type is 'bigquery'
```

### Usage in Operators

The generic operators use these interfaces internally:

**EdFi to Object Storage:**

```python
from edu_edfi_airflow.providers.generic.transfers.edfi_to_object_storage import EdFiToObjectStorageOperator

task = EdFiToObjectStorageOperator(
    task_id='pull_data',
    edfi_conn_id='edfi_prod',
    object_storage_conn_id='my_gcs_conn',  # Interface automatically detects GCS
    resource='students',
    destination_dir='raw/students/',
    destination_filename='students.jsonl',
    tmp_dir='/tmp',
)
```

**Object Storage to Database:**

```python
from edu_edfi_airflow.providers.generic.transfers.object_storage_to_database import ObjectStorageToDatabaseOperator

task = ObjectStorageToDatabaseOperator(
    task_id='load_data',
    database_conn_id='my_bigquery_conn',  # Interface automatically detects BigQuery
    tenant_code='district1',
    api_year=2024,
    resource='students',
    table_name='raw_edfi_resources',
    destination_key='gs://my-bucket/raw/students/students.jsonl',
)
```

### Connection Configuration

Airflow connections must have the correct `conn_type` for automatic detection:

- **Object Storage:**
  - S3: `conn_type='http'` (uses AWS S3 provider)
  - ADLS: `conn_type='adls'` (uses Azure Data Lake Storage provider)
  - GCS: `conn_type='gcs'` (would use Google Cloud Storage provider)

- **Database:**
  - Snowflake: `conn_type='snowflake'`
  - Databricks: `conn_type='databricks'`
  - BigQuery: `conn_type='bigquery'`

### Key Design Principles

- **Connection type** determines which implementation is instantiated
- **Factory pattern** in `__new__` handles automatic selection
- **Abstract base classes** enforce consistent interfaces across implementations
- **No operator changes needed** - existing DAGs work with new storage/database types automatically
- **URL formats** may differ between internal (with conn_id) and clean (for downstream use)

