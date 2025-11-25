import abc
import os

from airflow.hooks.base import BaseHook
from airflow.io.path import ObjectStoragePath

from typing import Optional


class ObjectStorageMixin(abc.ABC):
    """
    Abstract class for creating generic Airflow ObjectStoragePath objects for Ed-Fi copies.
    """
    def __new__(cls, object_storage_conn_id: str, **kwargs):
        conn = BaseHook.get_connection(object_storage_conn_id)
        
        if conn.conn_type == 'adls':
            return object.__new__(ADLSObjectStorageMixin)
        elif conn.conn_type == 'http':
            return object.__new__(S3ObjectStorageMixin)
        else:
            raise ValueError(f"ObjectStorageMixin type {conn.conn_type} is not defined!")
        
    def __init__(self, object_storage_conn_id: str, **kwargs):
        self.object_storage_conn_id: str = object_storage_conn_id

    @abc.abstractmethod
    def get_object_storage(self,
        destination_key: Optional[str] = None,
        destination_dir: Optional[str] = None,
        destination_filename: Optional[str] = None,
        **kwargs
    ):
        raise NotImplementedError


class S3ObjectStorageMixin(ObjectStorageMixin):
    """
    Updated S3 mixin that creates ObjectStoragePath with conn_id.
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
        
        # S3 format: same for both internal and external use
        storage_path = f"s3://{bucket}/{destination_key}"
        
        return (ObjectStoragePath(storage_path, conn_id=self.object_storage_conn_id), storage_path)


class ADLSObjectStorageMixin:
    """
    Updated ADLS mixin that embeds conn_id in URL (Airflow pattern).
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
        
        # Get connection to extract container and storage account
        conn = BaseHook.get_connection(self.object_storage_conn_id)
        container = conn.schema
        account_name = conn.host
        
        if not container:
            raise ValueError(f"Container name not found in connection {self.object_storage_conn_id}")
        
        # Internal format: embed conn_id in URL so Airflow can extract it
        internal_storage_path = f"abfs://{self.object_storage_conn_id}@{container}/{destination_key}"
        
        # Clean format: full Azure URL for downstream operators
        clean_storage_path = f"abfs://{container}@{account_name}.dfs.core.windows.net/{destination_key}"
        
        return (ObjectStoragePath(internal_storage_path, conn_id=self.object_storage_conn_id), clean_storage_path)
