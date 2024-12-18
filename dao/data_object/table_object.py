# table_object.py
# represents the table data object

from dao.data_object import DataObject


class TableObject(DataObject):

    def __init__(
        self,
        name,
        data_store,
        columns=None,
        prefix=None,
        primary_keys=None,
        partition_keys=None,
        cluster_keys=None,
        precombine_keys=None,
        index_keys=None,
        **kwargs,
    ):
        super().__init__(name, data_store)
        self.prefix = prefix
        self.columns = columns
        self.primary_keys = primary_keys
        self.partition_keys = partition_keys
        self.cluster_keys = cluster_keys
        self.precombine_keys = precombine_keys
        self.index_keys = index_keys

        for attribute, value in kwargs.items():
            setattr(self, attribute, value)

    def __repr__(self):
        return f"Table({self.data_store.name}.{self.name})"
