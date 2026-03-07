# dao.py
# Implementation of the DAO

from typing import Any

from dao.core.accessor import data_accessor
from dao.data_object import DataObject
from dao.utils.singleton import singleton


@singleton
class DataAccessObject:
    """Data Access Object providing a unified interface for data operations.

    A singleton class that serves as the main entry point for all data access
    operations, abstracting the complexity of underlying data stores and
    providing a clean, consistent API for application code.
    """

    @data_accessor
    def read(self, data_object: DataObject, **kwargs: Any) -> None:
        """Read data from the specified data object."""

    @data_accessor
    def write(self, data: Any, data_object: DataObject, **kwargs: Any) -> None:
        """Write data to the specified data object."""

    @data_accessor
    def upsert(self, data: Any, data_object: DataObject, **kwargs: Any) -> None:
        """Insert or update data in the specified data object."""

    @data_accessor
    def delete(self, data_object: DataObject, **kwargs: Any) -> None:
        """Delete data from the specified data object."""

    @data_accessor
    def run(self, action: str, data_object: DataObject, **kwargs: Any) -> None:
        """Execute a custom action on the specified data object."""

    @data_accessor
    def list(self, data_object: DataObject, **kwargs: Any) -> None:
        """List contents or metadata of the specified data object."""

    @data_accessor
    def count(self, data_object: DataObject, **kwargs: Any) -> None:
        """Count records or items in the specified data object."""

    @data_accessor(data_object="source")
    def move(self, source: DataObject, destination: DataObject, **kwargs: Any) -> None:
        """Move data from source to the destination data object."""

    @data_accessor(data_object="source")
    def copy(self, source: DataObject, destination: DataObject, **kwargs: Any) -> None:
        """Copy data from source to the destination data object."""

    @data_accessor
    def read_stream(self, data_object: DataObject, **kwargs: Any) -> None:
        """Read data from the specified data object as a stream."""

    @data_accessor
    def write_stream(self, data_stream: Any, data_object: DataObject, **kwargs: Any) -> None:
        """Write streaming data to the specified data object."""
