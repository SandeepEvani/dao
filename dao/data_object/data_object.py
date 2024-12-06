# data_object.py
# represents a physical data object present in a datastore


class DataObject:
    """
    A Data Object is a virtual object which represents an true object in the data store
    every Data Object must have a name and a data store
    """

    def __init__(self, name, data_store):

        self._name = name
        self._data_store = data_store

    @property
    def name(self):
        return self._name

    @property
    def data_store(self):
        return self._data_store
