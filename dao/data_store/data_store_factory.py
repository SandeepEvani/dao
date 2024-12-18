# data_store_factory.py
# Creates instances of DataStore

from importlib import import_module
from json import load
from operator import call

from .data_store import DataStore


class DataStoreFactory:

    """
    Creates multiple instances of the DataStore class
    at the runtime.
    """

    def __init__(self, ds_conf_loc):
        """
        Initializes the data store class

        :param ds_conf_loc: Path to the DataStore Config file
        """
        try:
            self.confs = load(open(ds_conf_loc))
        except Exception as error:
            print("Error reading the config file", error)

    def create_data_stores(self):
        """
        Creates the data stores from the given datastore confs

        :return: the identifier and the data store object
        """

        # Gets the list of data stores from the confs
        data_stores = self.confs["data_stores"]
        for identifier, properties in data_stores.items():

            # infer the class and location from the properties
            interface_class_name = properties["interface_class"]
            class_path = properties["interface_class_location"]

            # imports the modules from the given location
            interface_module = import_module(class_path)
            interface_class = getattr(interface_module, interface_class_name)

            # Instantiate the interface class
            if properties == {}:
                interface_object = call(interface_class)
            else:
                interface_object = call(interface_class, **properties["default_configs"])

            # Create the data store object
            data_store = DataStore(identifier)

            # Set the interface class and objects
            data_store.set_primary_interface_class(interface_class)
            data_store.set_primary_interface_object(interface_object)

            # TODO: add secondary interface functionality

            yield identifier, data_store
