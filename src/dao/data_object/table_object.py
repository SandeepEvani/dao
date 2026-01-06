# table_object.py
# represents the table data object
from typing import Optional

from dao.data_store import DataStore

from .data_object import DataObject


class TableObject(DataObject):
    """A Table Object is a representation of the tabular data structures stored in
    various data stores such as databases, warehouses, and data lakes."""

    def __init__(
        self,
        name: str,
        data_store: DataStore,
        schema: Optional[str] = None,
        columns: Optional[str] = None,
        primary_keys: Optional[str] = None,
        **kwargs,
    ):
        """Initializes the Data Store class.

        :param name: Identifier of the table object
        :param data_store: The Data Store the table belongs to
        :param columns: columns the table contains
        :param schema: schema the table contains
        :param primary_keys:
        """

        super().__init__(name, data_store)
        self.schema = schema
        self.columns = columns
        self.primary_keys = primary_keys
        for attribute, value in kwargs.items():
            setattr(self, attribute, value)

    def __repr__(self):
        return f"Table({self.data_store.name}.{self.name})"
