# table_object.py
# represents the table data object

from .data_object import DataObject


class TableObject(DataObject):
    """A Table Object is a representation of the tabular data structures stored in
    various data stores such as databases, warehouses, and data lakes."""

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
        """Initializes the Data Store class.

        :param name: Identifier of the table object
        :param data_store: The Data Store the table belongs to
        :param columns: columns the table contains
        :param prefix:
        :param primary_keys:
        :param partition_keys:
        :param cluster_keys:
        :param precombine_keys:
        :param index_keys:
        :param kwargs:
        """

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
