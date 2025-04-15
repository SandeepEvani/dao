# data_store_factory.py
# Creates instances of DataStore

from importlib import import_module
from json import load
from typing import Optional

from .data_store import DataStore


class DataStoreFactory:
    """Creates multiple instances of the DataStore class at the runtime."""

    def __init__(self, ds_conf_loc):
        """Initializes the data store class.

        :param ds_conf_loc: Path to the DataStore Config file
        """
        try:
            self.confs = load(open(ds_conf_loc))
            # TODO : JSON Schema validation
        except Exception as error:
            print("Error reading the config file", error)
            raise error

        self._initialize_data_stores()

    def _initialize_data_stores(self):
        """Initializes the data store object"""

        data_stores = self.confs["data_stores"]
        for identifier, properties in data_stores.items():
            # Create the data store object
            DataStore(identifier)

    def initialize_all_data_classes(self):
        """Creates the data stores from the given datastore confs.

        :return: the identifier and the data store object
        """

        # Gets the list of data stores from the confs
        data_stores = self.confs["data_stores"]
        for identifier, properties in data_stores.items():

            interface_object = self._initialize_class(properties)

            # Create the data store object
            data_store = DataStore.get_data_store(identifier)
            data_store.set_primary_interface_object(interface_object)

            for secondary_interface in properties["secondary_interfaces"]:

                interface_object = self._initialize_class(secondary_interface)
                data_store.set_secondary_interface_object(interface_object)

            yield data_store

    def initialize_data_class(self, data_store: str, data_class: Optional[str] = None):
        """Creates the data stores from the given datastore confs.

        :return: the identifier and the data store object
        """

        # Gets the list of data stores from the confs
        data_stores = self.confs["data_stores"]

        if data_store not in data_stores:
            raise KeyError("Given datastore is not found in the data stores")

        properties = data_stores[data_store]
        if data_class == properties["interface_class"] or data_class is None:
            is_primary = True
            # infer the class and location from the properties
            class_name = properties["interface_class"]
            class_path = properties["interface_class_location"]

        else:
            is_primary = False
            for secondary_interface in properties["secondary_interfaces"]:
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
        interface_object = interface_class(**properties["default_configs"])

        data_store_object = DataStore.get_data_store(data_store) or DataStore(data_store)

        if is_primary:
            data_store_object.set_primary_interface_class(interface_class)
            data_store_object.set_primary_interface_object(interface_object)
        else:
            data_store_object.set_secondary_interface_class(interface_class)
            data_store_object.set_secondary_interface_object(interface_object)

        return data_store_object

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
        """"""

        return True
