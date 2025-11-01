# dao.py
# Implementation of the DAO

from dao.core.accessor import DataAccessor
from dao.data_store import DataStoreRegistry
from dao.utils.singleton import singleton


@singleton
class DataAccessObject:
    def __init__(self, data_stores: dict):
        """ """

        DataStoreRegistry.initialize(data_stores)

    read = DataAccessor()
    write = DataAccessor()
