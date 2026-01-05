# data_object.py
# represents a physical data object present in a datastore
from typing import Optional

from ..data_store import DataStore


class DataObject:
    """A Data Object is a virtual object which represents a true object in the data
    store every Data Object must have a name and a data store."""

    _name = None
    _data_store = None

    def __init__(self, name: str, data_store: DataStore, identifier: Optional[str] = None):
        """Initializes the `Data Object` object.

        :param name: The unique identifier for a data object within a data store
        :param data_store: The actual Data Store object to which the data object belongs
            to
        """
        self.name = name
        self.data_store = data_store
        self.identifier = identifier or name

        # Assigns data stores parameters
        for k, v in vars(data_store).items():
            if not k.startswith("_"):
                setattr(self, k, v)

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        if self.name is not None:
            raise ValueError("Attribute `name` is already set, cannot modify `name`")
        self._name = value

    @property
    def data_store(self):
        return self._data_store

    @data_store.setter
    def data_store(self, value):
        if self.data_store is not None:
            raise ValueError("Attribute `data_store` is already set, cannot modify `data_store`")
        self._data_store = value
