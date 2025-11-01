# data_store_factory.py
# Creates instances of DataStore

from importlib import import_module
from typing import Optional

from .data_store import DataStore


class DataStoreFactory:
    """Creates multiple instances of the DataStore class at the runtime."""

    _stores = {}
    _classes = {}

    @classmethod
    def initialize_data_stores(cls, data_stores):
        """Initializes the data store object."""

        for identifier, properties in data_stores.items():
            if identifier in cls._stores:
                raise RuntimeError("Duplicate DataStore is detected")

            # Create the data store object
            data_store = DataStore(identifier, **(properties.get("properties", {})))

            cls._stores.update({identifier: data_store})

    @classmethod
    def initialize_data_class(cls, data_store_config: dict, data_store: str, data_class: Optional[str] = None):
        """Creates the data stores from the given datastore confs.

        :return: the identifier and the data store object
        """
        if (data_store, data_class) in cls._classes:
            return cls._classes.get((data_store, data_class))

        if data_class == data_store_config["interface_class"] or data_class is None:
            is_primary = True
            # infer the class and location from the properties
            class_name = data_store_config["interface_class"]
            class_path = data_store_config["interface_class_location"]

        else:
            is_primary = False
            for secondary_interface in data_store_config["secondary_interfaces"]:
                if data_class == secondary_interface["interface_class"]:
                    class_name = secondary_interface["interface_class"]
                    class_path = secondary_interface["interface_class_location"]
                    break
            else:
                raise KeyError(f"No class {data_class} found in data store {data_store}")

        # imports the modules from the given location
        interface_module = import_module(class_path)
        interface_class = getattr(interface_module, class_name)

        # Instantiate the interface class
        interface_object = interface_class(**data_store_config["default_configs"])

        data_store_object = cls.get_data_store(data_store) or DataStore(
            data_store, **data_store_config.get("properties", {})
        )

        if is_primary:
            data_store_object.set_primary_interface_class(interface_class)
            data_store_object.set_primary_interface_object(interface_object)
        else:
            data_store_object.set_secondary_interface_class(interface_class)
            data_store_object.set_secondary_interface_object(interface_object)

        cls._classes.update({(data_store, data_class): interface_object})

        return interface_object

    @staticmethod
    def _initialize_class(properties):
        # infer the class and location from the properties
        interface_class_name = properties["interface_class"]
        class_path = properties["interface_class_location"]

        # imports the modules from the given location
        interface_module = import_module(class_path)
        interface_class = getattr(interface_module, interface_class_name)

        # Instantiate the interface class
        interface_object = interface_class(**properties["default_configs"])

        return interface_object

    def validate(self):
        """ """
        # TODO: Build a validation  block to check if all the
        #  modules are present at the specified locations without importing them
        return True

    @classmethod
    def get_data_stores(cls):
        """Returns all the data stores initiated."""
        return cls._stores.values()

    @classmethod
    def get_data_store(cls, data_store: str):
        """Returns the data store object associated to the provided data_store name."""
        return cls._stores.get(data_store)

    @classmethod
    def check_data_store(cls, data_store: str) -> bool:
        """Checks if the data store is present in the data stores."""
        return data_store in cls._stores
