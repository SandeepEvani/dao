# data_store_factory.py
# Creates instances of DataStore

from typing import Dict

from .data_store import DataStore


class DataStoreFactory:
    """Creates multiple instances of the DataStore class at the runtime."""

    _stores = {}
    _store_configs = {}

    @classmethod
    def load_configs(cls, data_store_configs: Dict[str, Dict]):
        """
        :param data_store_configs:
        :return:
        """
        cls._store_configs.clear()
        cls._store_configs.update(data_store_configs)

    @classmethod
    def _create_data_store_instance(cls, data_store: str) -> DataStore:
        """Creates the data store instance."""

        data_store_config = cls._store_configs.get(data_store)
        data_store_instance = DataStore(data_store, **(data_store_config.get("properties", {})))
        return data_store_instance

    @classmethod
    def _initialize_data_store(cls, data_store: str):
        """Initializes the data store object."""

        data_store_instance = cls._create_data_store_instance(data_store)

        data_store_config = cls._store_configs.get(data_store)
        data_store_instance.set_interface_class_lazy(
            module=data_store_config["module"],
            class_=data_store_config["class"],
            args=data_store_config.get("args"),
            primary=True,
        )

        for interface in data_store_config.get("additional_interfaces", []):
            data_store_instance.set_interface_class_lazy(
                module=interface["module"],
                class_=interface["class"],
                args=interface.get("args"),
                primary=False,
            )

        return data_store_instance

    def validate(self):
        """ """
        # TODO: Build a validation  block to check if all the
        #  modules are present at the specified locations without importing them
        return True

    @classmethod
    def get(cls, data_store: str):
        """Returns the data store object associated to the provided data_store name."""
        if data_store in cls._stores:
            return cls._stores.get(data_store)

        elif data_store in cls._store_configs:
            data_store_object = cls._initialize_data_store(data_store)
            cls._stores[data_store] = data_store_object
            return cls._stores.get(data_store)
        else:
            raise KeyError(f"Data store '{data_store}' not found.")

    @classmethod
    def check_data_store_exists(cls, data_store: str) -> bool:
        """Checks if the data store is present in the data stores."""
        return data_store in cls._store_configs
