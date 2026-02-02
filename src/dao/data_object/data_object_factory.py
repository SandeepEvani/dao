# data_store_factory.py
# Creates instances of DataStore

from typing import Dict

from ..data_store import DataStore
from .data_object import DataObject


class DataObjectFactory:
    """Creates multiple instances of the DataStore class at the runtime."""

    _object_configs = {}
    _registered_classes = {}

    @classmethod
    def register(cls, reference_name: str, class_instance: type):
        cls._registered_classes[reference_name] = class_instance

    @classmethod
    def load_configs(cls, data_store_configs: Dict[str, Dict]):
        """
        :param data_store_configs:
        :return:
        """
        cls._object_configs.clear()
        cls._object_configs.update(data_store_configs)

    @classmethod
    def _initialize_data_object(cls, data_object: str, data_store: DataStore) -> DataObject:
        """Initializes the data store object."""
        data_object_config = cls._object_configs.get(data_object)
        data_object_type = data_object_config.get("type")

        if data_object_type is None:
            data_object_class = DataObject
        else:
            data_object_class = cls._registered_classes.get(data_object_type)

        data_object_instance = data_object_class(data_object, data_store, **(data_object_config.get("properties", {})))

        return data_object_instance

    def validate(self):
        """ """
        # TODO: Build a validation  block to check if all the
        #  modules are present at the specified locations without importing them
        return True

    @classmethod
    def get(cls, data_object: str, data_store: DataStore) -> DataObject:
        """Returns the data object associated to the provided data_object name."""

        if data_object in cls._object_configs:
            data_object_instance = cls._initialize_data_object(data_object, data_store)
            return data_object_instance
        else:
            raise KeyError(f"Data Object '{data_object}' not found.")

    @classmethod
    def check_data_store_exists(cls, data_object: str) -> bool:
        """Checks if the data object is present in the data object configs."""
        return data_object in cls._object_configs
