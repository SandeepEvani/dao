# base.py
# The base class for catalogs

import re
from abc import ABC, abstractmethod
from logging import getLogger
from typing import Any, Dict, Generator

from dao.data_object import DataObject
from dao.data_object import registry as _data_object_registry
from dao.data_store import DataStoreFactory

from .exceptions import DataObjectNotFoundError, DataStoreNotFoundError

logger = getLogger(__name__)


class BaseCatalog(ABC):
    """Abstract base for every catalog implementation.

    Subclasses supply configuration by implementing ``_load_data_store_configs``
    and ``_load_data_object_configs``.  Everything else — factory creation,
    data-object resolution, FQN parsing — lives here.

    **Object-type resolution**

    If a data-object's properties contain a ``"type"`` key (e.g.
    ``"type": "TableObject"``), :meth:`get` will look up that name in the
    :class:`~dao.data_object.registry.DataObjectRegistry` and instantiate
    the corresponding subclass instead of the base ``DataObject``.

    Custom subclasses can be registered with::

        from dao.data_object import register_data_object
        register_data_object("MyCustomObject", MyCustomObject)
    """

    def __init__(self):
        self._data_store_configs: Dict[str, Any] = self._load_data_store_configs()
        self._data_object_configs: Dict[str, Any] = self._load_data_object_configs()

        self._factory = DataStoreFactory()
        self._factory.load_configs(self._data_store_configs)

    @abstractmethod
    def _load_data_store_configs(self) -> Dict[str, Any]:
        """Return the full data-store configuration dictionary."""
        ...

    @abstractmethod
    def _load_data_object_configs(self) -> Dict[str, Any]:
        """Return the full data-object configuration dictionary."""
        ...

    def get_data_store(self, name: str):
        """Return the ``DataStore`` instance for *name*."""
        return self._factory.get(name)

    def get(self, fully_qualified_name: str) -> DataObject:
        """Resolve an exact ``"store.object"`` FQN into a ``DataObject``.

        If the resolved properties contain a ``"type"`` key the corresponding
        :class:`~dao.data_object.DataObject` subclass is looked up in the
        global :data:`~dao.data_object.registry` and used for instantiation.
        """
        data_store, data_object = self._parse_fully_qualified_name(fully_qualified_name)
        data_store_instance = self._factory.get(data_store)
        properties = self._resolve_data_object_properties(data_store, data_object)
        object_cls = _data_object_registry.get(properties.pop("type", None))
        return object_cls(name=data_object, data_store=data_store_instance, **properties)

    def search(self, pattern: str) -> Generator[DataObject, None, None]:
        """Yield every ``DataObject`` whose FQN matches the regex *pattern*."""
        compiled = re.compile(pattern)
        for store_name, objects in self._data_object_configs.items():
            for object_name in objects:
                fqn = f"{store_name}.{object_name}"
                if compiled.search(fqn):
                    yield self.get(fqn)

    def _resolve_data_object_properties(self, data_store: str, data_object: str) -> Dict[str, Any]:
        """Look up data-object properties from the loaded configs.

        Subclasses may override this for on-demand fetching (e.g. Glue API).
        """
        store_objects = self._data_object_configs.get(data_store)
        if store_objects is None:
            raise DataStoreNotFoundError(f"Data store '{data_store}' not found in catalog.")
        properties = store_objects.get(data_object)
        if properties is None:
            raise DataObjectNotFoundError(f"Data object '{data_object}' not found in store '{data_store}'.")
        return properties

    @staticmethod
    def _parse_fully_qualified_name(fully_qualified_name: str) -> tuple:
        """Parse ``'data_store.object_name'`` into a ``(data_store, object_name)`` tuple."""
        if not isinstance(fully_qualified_name, str):
            raise ValueError(f"Expected string, got {type(fully_qualified_name)}")
        if "." not in fully_qualified_name:
            raise ValueError(
                f"Invalid fully qualified name '{fully_qualified_name}'. Expected format: 'data_store.object_name'"
            )
        parts = fully_qualified_name.split(".", 1)
        if not all(parts):
            raise ValueError(
                f"Invalid fully qualified name '{fully_qualified_name}'. "
                f"Both data_store and object_name must be non-empty."
            )
        return tuple(parts)
