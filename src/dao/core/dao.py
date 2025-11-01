# dao.py
# Implementation of the DAO

from dao.core.accessor import DataAccessor
from dao.data_store import DataStoreRegistry
from dao.utils.singleton import singleton


@singleton
class DataAccessObject:
    def __init__(self, data_stores: dict):
        """Data Access Object is a convenience class, acting as an interface to access data,
        data stores and perform many other data operations, All this heavy lifting is
        abstracted from the user using the package and gives an easy and clean interface while writing
        the main application code
        """

        # Register the data stores to the registry for common access
        self.data_stores = data_stores
        DataStoreRegistry.initialize(data_stores)

    # Create data accessor methods to access the respective data methods
    read = DataAccessor()
    write = DataAccessor()
    run = DataAccessor()
