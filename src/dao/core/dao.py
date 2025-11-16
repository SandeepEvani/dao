# dao.py
# Implementation of the DAO

from dao.core.accessor import data_accessor
from dao.data_object import DataObject
from dao.data_store import DataStoreRegistry
from dao.utils.singleton import singleton


@singleton
class DataAccessObject:
    """Data Access Object providing a unified interface for data operations.

    A singleton class that serves as the main entry point for all data access
    operations, abstracting the complexity of underlying data stores and
    providing a clean, consistent API for application code.
    """

    def __init__(self, data_stores: dict):
        """Initialize the Data Access Object with configured data stores.

        Args:
            data_stores: Dictionary of data store configurations where keys are
                        data store names and values are their configurations.
        """
        # Register the data stores to the registry for common access
        self.data_stores = data_stores
        DataStoreRegistry.initialize(data_stores)

    @data_accessor
    def read(self, data_object: DataObject, **kwargs):
        """Read data from the specified data object.

        Retrieves data from a data object using the appropriate interface method
        based on the provided parameters. The router automatically selects the
        best matching interface implementation.
        """

    @data_accessor
    def write(self, data, data_object: DataObject, **kwargs):
        """Write data to the specified data object.

        Persists data to a data object using the appropriate interface method.
        Supports various formats and configurations based on the target data store.
        """

    @data_accessor
    def upsert(self, data, data_object: DataObject, **kwargs):
        """Insert or update data in the specified data object.

        Performs an upsert operation - inserts new records or updates existing ones
        based on matching keys. Useful for idempotent data synchronization.
        """

    @data_accessor
    def delete(self, data_object: DataObject, **kwargs):
        """Delete data from the specified data object.

        Removes data from a data object based on filter conditions provided in kwargs.
        Use with caution as this operation is destructive.
        """

    @data_accessor
    def run(self, action: str, data_object: DataObject, **kwargs):
        """Execute a custom action on the specified data object.

        Runs arbitrary backend-specific operations or workflows on a data object.
        The action string determines the operation to perform.
        """

    @data_accessor
    def list(self, data_object: DataObject, **kwargs):
        """List contents or metadata of the specified data object.

        Returns a list of items, files, or records contained within the data object.
        Useful for discovering available data without loading it all.
        """

    @data_accessor
    def count(self, data_object: DataObject, **kwargs):
        """Count records or items in the specified data object.

        Returns the total number of records or items in a data object.
        More efficient than reading all data when only count is needed.
        """

    @data_accessor(data_object="source")
    def move(self, source: DataObject, destination: DataObject, **kwargs):
        """Move data from source to destination data object.

        Transfers data from one data object to another, typically removing it
        from the source after successful transfer.
        """

    @data_accessor(data_object="source")
    def copy(self, source: DataObject, destination: DataObject, **kwargs):
        """Copy data from source to destination data object.

        Creates a replica of data from the source data object to the destination,
        leaving the source data intact.
        """

    @data_accessor
    def read_stream(self, data_object: DataObject, **kwargs):
        """Read data from the specified data object as a stream.

        Retrieves data in streaming fashion for memory-efficient processing
        of large datasets without loading everything into memory at once.
        """

    @data_accessor
    def write_stream(self, data_stream, data_object: DataObject, **kwargs):
        """Write streaming data to the specified data object.

        Persists streaming data (generators, iterators) to a data object,
        enabling efficient handling of continuous data flows without buffering."""
