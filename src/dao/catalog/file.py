# file.py
# File-based catalog implementation

import importlib.resources
import json
from pathlib import Path
from typing import Any, Dict

from dao.catalog.base import BaseCatalog


class FileCatalog(BaseCatalog):
    """Catalog backed by local JSON configuration files."""

    def __init__(
        self,
        data_store_config_location: str,
        data_object_config_location: str,
        package: str = None,
    ):
        self._ds_source = data_store_config_location
        self._do_source = data_object_config_location
        self._package = package
        super().__init__()

    def _load_data_store_configs(self) -> Dict[str, Any]:
        return _load_json(self._ds_source, self._package)

    def _load_data_object_configs(self) -> Dict[str, Any]:
        return _load_json(self._do_source, self._package)

    @property
    def data_store_configs(self) -> Dict[str, Any]:
        return self._data_store_configs

    @property
    def data_object_configs(self) -> Dict[str, Any]:
        return self._data_object_configs


def _load_json(source: str, package: str = None) -> Any:
    """Load JSON from a filesystem path or package resource."""
    file_path = Path(source)
    if file_path.exists():
        with open(file_path, "r") as f:
            return json.load(f)

    if package:
        try:
            with importlib.resources.files(package).joinpath(source).open("r") as f:
                return json.load(f)
        except (AttributeError, FileNotFoundError) as e:
            raise FileNotFoundError(f"Error loading JSON from package: {e}") from e

    raise FileNotFoundError(f"Could not find JSON file: {source}")
