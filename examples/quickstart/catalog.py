# catalog.py
# Creates TableObject instances from a JSON catalog config

from json import load

from dao.data_object import TableObject
from dao.data_store import DataStore


class Catalog:

    def __init__(self, confs):
        self.confs = load(open(confs))

    def get(self, table_name):
        """Retrieve a TableObject by its fully qualified name.

        Args:
            table_name: Fully qualified name in the format '<data_store>.<table>'

        Returns:
            A TableObject bound to the appropriate DataStore

        Raises:
            ValueError: If the table name is not fully qualified
            KeyError: If the data store or table is not found
        """
        if "." not in table_name:
            raise ValueError("Provide a fully qualified table name: '<data_store>.<table>'")

        data_store, table = table_name.split(".")

        if not DataStore.check_data_store(data_store) or data_store not in self.confs:
            raise KeyError(f"Data store '{data_store}' is not registered")

        data_store_object = DataStore.get_data_store(data_store)
        tables = self.confs[data_store]

        if table not in tables:
            raise KeyError(f"Table '{table}' not found in data store '{data_store}'")

        properties = tables.get(table)
        return TableObject(name=table, data_store=data_store_object, **properties)

