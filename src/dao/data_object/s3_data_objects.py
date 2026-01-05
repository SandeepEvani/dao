# s3_data_objects.py
# Contains all the collection of the s3 objects
from typing import Optional

from .data_object import DataObject
from ..data_store import DataStore


class S3DirectoryObject(DataObject):
    """
    Represents a collection of S3 objects with a common prefix, which is represented as a directory
    and this is in no form and shape related to the S3 Directory Bucket
    """

    # This virtual object represents a collection of s3 objects
    def __init__(
        self, name: str, data_store: DataStore, prefix: Optional[str] = None, identifier: Optional[str] = None
    ):
        super().__init__(name, data_store, identifier)

        self.prefix = prefix

    @property
    def key(self):
        return self.prefix + "/" + self.identifier if self.prefix else self.identifier


class S3FileObject(DataObject):
    """
    Represents a single S3 object, which is represented as a file
    """

    # This virtual object represents a collection of s3 objects
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
    def __init__(self, name: str, data_store: DataStore, identifier: Optional[str] = None):
        super().__init__(name, data_store, identifier)


class S3HudiObject(S3TableObject):
    def __init__(
        self,
        name: str,
        data_store: DataStore,
        identifier: Optional[str] = None,
        record_key: Optional[str] = None,
        precombine_key: Optional[str] = None,
        partition_key: Optional[str] = None,
    ):
        # Hudi table properties
        self.record_key = record_key
        self.precombine_key = precombine_key
        self.partition_key = partition_key

        super().__init__(name, data_store, identifier)


class S3DeltaObject(S3TableObject):
    def __init__(self, name: str, data_store: DataStore, identifier: Optional[str] = None):
        super().__init__(name, data_store, identifier)


class S3IcebergObject(S3TableObject):
    def __init__(self, name: str, data_store: DataStore, identifier: Optional[str] = None):
        super().__init__(name, data_store, identifier)
