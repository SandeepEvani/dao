# data_object.py
# represents a physical data object present in a datastore

from typing import Any, Optional

from ..data_store import DataStore


class DataObject:
    """A Data Object is a virtual object which represents a true object in the data store.

    Every Data Object must have a name and a data store.
    """

    _name = None
    _data_store = None

    @property
    def enrichment_exclude(self) -> frozenset:
        """Attribute names that enrichment must never inject into method call args."""
        return frozenset({"name", "data_store", "identifier"})

    def __init__(self, name: str, data_store: DataStore, identifier: Optional[str] = None, **properties: Any) -> None:
        """Initializes the `Data Object` object.

        :param name: The unique identifier for a data object within a data store
        :param data_store: The actual Data Store object to which the data object belongs to
        """
        self.name = name
        self.data_store = data_store
        self.identifier = identifier or name

        # Assigns data stores parameters
        for k, v in vars(data_store).items():
            if not k.startswith("_"):
                setattr(self, k, v)

        for k, v in properties.items():
            setattr(self, k, v)

    @property
    def name(self) -> str:
        """The unique identifier for this data object within a data store."""
        return self._name

    @name.setter
    def name(self, value: str) -> None:
        if self.name is not None:
            raise ValueError("Attribute `name` is already set, cannot modify `name`")
        self._name = value

    @property
    def data_store(self) -> DataStore:
        """The data store this object belongs to."""
        return self._data_store

    @data_store.setter
    def data_store(self, value: DataStore) -> None:
        if self.data_store is not None:
            raise ValueError("Attribute `data_store` is already set, cannot modify `data_store`")
        self._data_store = value
