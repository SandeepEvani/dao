# s3.py
# S3-specific DataObject models for use with S3 interfaces

from typing import Optional

from dao.data_object import DataObject
from dao.data_store import DataStore


class S3DirectoryObject(DataObject):
    """Represents a collection of S3 objects with a common prefix (directory-like structure).

    Note: This is unrelated to S3 Directory Buckets - it's a logical grouping by prefix.
    """

    def __init__(
        self, name: str, data_store: DataStore, prefix: Optional[str] = None, identifier: Optional[str] = None
    ):
        super().__init__(name, data_store, identifier)
        self.prefix = prefix

    @property
    def key(self):
        return self.prefix + "/" + self.identifier if self.prefix else self.identifier


class S3FileObject(DataObject):
    """Represents a single S3 object (file)."""

    def __init__(
        self,
        name: str,
        data_store: DataStore,
        identifier: Optional[str] = None,
        prefix: Optional[str] = None,
        suffix: Optional[str] = None,
    ):
        super().__init__(name, data_store, identifier)
        self.prefix = prefix
        self.suffix = suffix

    @property
    def key(self):
        name = self.identifier + "." + self.suffix if self.suffix else self.identifier
        return self.prefix + "/" + name if self.prefix else name


class S3TableObject(S3DirectoryObject):
    """Base class for table-format storage on S3 (Delta, Hudi, Iceberg)."""

    def __init__(self, name: str, data_store: DataStore, identifier: Optional[str] = None):
        super().__init__(name, data_store, identifier)


class S3HudiObject(S3TableObject):
    """Represents an Apache Hudi table on S3."""

    def __init__(
        self,
        name: str,
        data_store: DataStore,
        identifier: Optional[str] = None,
        record_key: Optional[str] = None,
        precombine_key: Optional[str] = None,
        partition_key: Optional[str] = None,
    ):
        self.record_key = record_key
        self.precombine_key = precombine_key
        self.partition_key = partition_key
        super().__init__(name, data_store, identifier)


class S3DeltaObject(S3TableObject):
    """Represents a Delta Lake table on S3."""

    def __init__(
        self,
        name: str,
        data_store: DataStore,
        identifier: Optional[str] = None,
        partition_keys: Optional[str] = None,
        cluster_keys: Optional[str] = None,
    ):
        super().__init__(name, data_store, identifier)
        self.partition_keys = partition_keys
        self.cluster_keys = cluster_keys


class S3IcebergObject(S3TableObject):
    """Represents an Apache Iceberg table on S3."""

    def __init__(self, name: str, data_store: DataStore, identifier: Optional[str] = None):
        super().__init__(name, data_store, identifier)
