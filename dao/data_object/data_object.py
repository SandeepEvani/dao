# data_object.py
# represents a physical data object present in a datastore

from dao.data_store import DataStore


class DataObject:
    """A Data Object is a virtual object which represents a true object in the
    data store every Data Object must have a name and a data store."""

    def __init__(self, name: str, data_store: DataStore):
        """Initializes the `Data Object` object.

        :param name: The unique identifier for a data object within a
            data store
        :param data_store: The actual Data Store object to which the
            data object belongs to
        """
        self._name = name
        self._data_store = data_store

    @property
    def name(self):
        return self._name

    @property
    def data_store(self):
        return self._data_store
