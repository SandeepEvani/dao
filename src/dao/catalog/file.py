# file.py
# Contains file-based catalog DAO implementation

import importlib.resources
import json
from pathlib import Path
from typing import Any, Dict

from dao.catalog.base import BaseCatalog
from dao.data_object import DataObject
from dao.data_store import DataStoreFactory


class FileCatalog(BaseCatalog):
    """File-based Catalog Data Access Object implementation.

    This class provides methods to interact with file-based data stores,
    such as local file systems or cloud storage services. It implements
    standard data access operations like read, write, upsert, and delete
    specifically for file-based catalogs.
    """

    def __init__(
        self,
        data_store_config_location: str,
        data_object_config_location: str,
        validate_on_init: bool = True,
        package: str = None,
    ):
        """Initializes the FileCatalog with the given data store configuration.

        Args:
            data_store_config_location (Dict[str, Any]): Configuration parameters for the file-based data store.
        """

        self._data_store_config = _load_json(data_store_config_location, package)
        self._data_object_config = _load_json(data_object_config_location, package)

        super().__init__(validate_on_init)

        self.factory = DataStoreFactory()
        self.factory.load_configs(self._data_store_config)

    def get_data_store_configs(self) -> Dict[str, Any]:
        """Retrieve the data store configuration.

        Returns:
            Dict[str, Any]: The data store configuration dictionary.
        """
        return self._data_store_config

    def get_data_object_configs(self) -> Dict[str, Any]:
        """Retrieve the data store configuration.

        Returns:
            Dict[str, Any]: The data object configuration dictionary.
        """
        return self._data_object_config

    def get_data_store(self, data_store) -> list:
        """Retrieve a list of data stores configured in the file catalog.

        Returns:
            list: A list of data store names or identifiers.
        """
        return self.factory.get(data_store)

    def get_data_objects(self) -> list:
        """Retrieve a list of data objects available in the file catalog.

        Returns:
            list: A list of data object identifiers (e.g., file paths).
        """
        # Implementation to list files in the catalog
        pass

    def get_data_object(self, fully_qualified_name: str) -> DataObject:
        """Retrieve a specific data object by its fully qualified name.

        Args:
            fully_qualified_name (str): The fully qualified name of the data object.

        Returns:
            Dict[str, Any]: The data object configuration.
        """

        data_store, data_object = self._parse_fully_qualified_name(fully_qualified_name)
        data_store_instance = self.factory.get(data_store)

        data_object_configs = self._data_object_config.get(data_store)
        properties = data_object_configs.get(data_object)
        return DataObject(name=data_object, data_store=data_store_instance, **properties)

    def validate(self) -> bool:
        pass


def _load_json(source: str, package: str = None) -> Any:
    """Load JSON from various sources.

    Args:
        source: File path or resource name
        package: Package name if loading from package resources

    Returns:
        Parsed JSON data
    """
    # Try as filesystem path first
    file_path = Path(source)
    if file_path.exists():
        with open(file_path, "r") as f:
            return json.load(f)

    # Try to load from package if package is specified
    if package:
        try:
            with importlib.resources.files(package).joinpath(source).open("r") as f:
                return json.load(f)
        except (AttributeError, FileNotFoundError) as e:
            FileNotFoundError(f"Error loading JSON from package: {e}")

    raise FileNotFoundError(f"Could not find JSON file: {source}")
