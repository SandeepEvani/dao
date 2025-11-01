# registry.py
# Stores data store configurations

from dao.data_store import DataStoreFactory


class DataStoreRegistry:
    _stores = {}

    @classmethod
    def initialize(cls, data_stores):
        cls._stores.clear()

        DataStoreFactory.initialize_data_stores(data_stores)
        for name, config in data_stores.items():
            cls._stores[name] = config

    @classmethod
    def get(cls, name):
        return cls._stores[name]
