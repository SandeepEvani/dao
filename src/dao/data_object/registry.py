# registry.py
# Global registry for DataObject subclasses.
# Catalogs use the registered name to instantiate the right class from a config `type` field.

from typing import Dict, Type

from .data_object import DataObject


class DataObjectRegistry:
    """A registry that maps string type-names to ``DataObject`` subclasses.

    Built-in types (``DataObject``, ``TableObject``) are registered automatically
    when this module is first imported.  Users can register their own subclasses
    via :meth:`register` or the module-level :func:`register_data_object` helper.

    Example::

        from dao.data_object.registry import registry, register_data_object

        # Class-level registration
        register_data_object("FileObject", FileObject)

        # Or via the registry instance directly
        registry.register("FileObject", FileObject)

        # Lookup
        cls = registry.get("TableObject")  # → TableObject class
    """

    def __init__(self) -> None:
        self._registry: Dict[str, Type[DataObject]] = {}

    def register(self, name: str, cls: Type[DataObject]) -> None:
        """Register a ``DataObject`` subclass under *name*.

        Args:
            name: The string identifier used in catalog configs (e.g. ``"TableObject"``).
            cls:  The class itself — must be ``DataObject`` or a subclass.

        Raises:
            TypeError: If *cls* is not a subclass of ``DataObject``.
        """
        if not (isinstance(cls, type) and issubclass(cls, DataObject)):
            raise TypeError(f"'{cls}' is not a subclass of DataObject")
        self._registry[name] = cls

    def get(self, name: str) -> Type[DataObject]:
        """Return the class registered under *name*.

        Falls back to ``DataObject`` when *name* is ``None`` or not found.
        """
        if name is None:
            return DataObject
        return self._registry.get(name, DataObject)

    def unregister(self, name: str) -> None:
        """Remove a previously registered type (mainly useful in tests)."""
        self._registry.pop(name, None)

    def registered_names(self) -> list:
        """Return a sorted list of all registered type names."""
        return sorted(self._registry.keys())

    def __contains__(self, name: str) -> bool:
        return name in self._registry

    def __repr__(self) -> str:
        return f"DataObjectRegistry({self.registered_names()})"


# ── Module-level singleton ─────────────────────────────────────────────────
registry = DataObjectRegistry()


def register_data_object(name: str, cls: Type[DataObject]) -> None:
    """Convenience function — delegates to ``registry.register``."""
    registry.register(name, cls)


def _register_builtins() -> None:
    """Register the built-in DataObject types shipped with dao."""
    from .data_object import DataObject as _DataObject
    from .table_object import TableObject as _TableObject

    registry.register("DataObject", _DataObject)
    registry.register("TableObject", _TableObject)


_register_builtins()
