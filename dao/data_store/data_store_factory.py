# data_store_factory.py
# Creates instances of DataStore

from .data_store import DataStore
from json import load
from importlib import import_module
from operator import call


class DataStoreFactory:

    def __init__(self, ds_conf_loc):
        """

        :param ds_conf_loc: Path to the DataStore Config file
        """
        try:
            self.confs = load(open(ds_conf_loc))
        except Exception as error:
            print("Error reading the config file", error)

    def create_data_stores(self):
        """

        :return:
        """

        data_stores = self.confs["data_stores"]
        for identifier, properties in data_stores.items():
            interface_class_name = properties["interface_class"]
            class_path = properties["interface_class_location"]

            interface_module = import_module(class_path)
            interface_class = getattr(interface_module, interface_class_name)
            interface_object = call(interface_class, **properties["default_configs"])

            data_store = DataStore(identifier)

            data_store.set_primary_interface_class(interface_class)
            data_store.set_primary_interface_object(interface_object)

            yield identifier, data_store
