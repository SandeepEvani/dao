# dao.py
# Implementation of the DAO

from dao.core.accessor import DataAccessor
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

    # Create data accessor methods to access the respective data methods
    # DataAccessor descriptor helps you create multiple data access actions across

    read = DataAccessor(
        doc_string="""Read data from data stores.

    Provides read operations for retrieving data from configured data stores.
    """
    )

    write = DataAccessor(
        doc_string="""Write data to data stores.

    Provides write operations for storing data to configured data stores.
    """
    )

    upsert = DataAccessor(
        doc_string="""Upsert (update or insert) data in data stores.

    Provides upsert operations that update existing records or insert new ones
    if they don't exist.
    """
    )

    delete = DataAccessor(
        doc_string="""Delete data from data stores.

    Provides delete operations for removing data from configured data stores.
    """
    )

    run = DataAccessor(
        doc_string="""Execute data operations or commands.

    Provides operations for executing custom commands or procedures on data stores.
    """
    )

    list = DataAccessor(
        doc_string="""List data elements or collections.

    Provides operations for listing available data elements, collections,
    or resources in data stores.
    """
    )

    count = DataAccessor(
        doc_string="""Count data elements.

    Provides operations for counting data elements that match specific criteria.
    """
    )

    move = DataAccessor(
        doc_string="""Move data between locations or stores.

    Provides operations for moving data from one location to another within
    or between data stores.
    """
    )

    copy = DataAccessor(
        doc_string="""Copy data between locations or stores.

    Provides operations for copying data from one location to another within
    or between data stores.
    """
    )

    read_stream = DataAccessor(
        doc_string="""Read data as a stream.

    Provides streaming read operations for handling large datasets or
    continuous data flows.
    """
    )

    write_stream = DataAccessor(
        doc_string="""Write data as a stream.

    Provides streaming write operations for handling large datasets or
    continuous data flows.
    """
    )
