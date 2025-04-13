# catalog.py
# creates instances of the TableObject

from json import load

from dao.data_object import TableObject
from dao.data_store import DataStore


class Catalog:

    def __init__(self, confs):

        self.confs = load(open(confs))

    def get(self, table_name):
        """

        :param table_name: Fully qualified table name
        :return:
        """

        if "." not in table_name:
            raise Exception("Provide a fully qualified table name")
        data_store, table = table_name.split(".")

        if not (DataStore.check_data_store(data_store) and data_store in self.confs):
            raise Exception("Provided Datastore might not be registered")

        data_store_object = DataStore.get_data_store(data_store)

        tables = self.confs[data_store]

        if table not in tables:
            raise Exception("Provided table might not be registered")

        properties = tables.get(table)
        return TableObject(name=table, data_store=data_store_object, **properties)


catalog = Catalog("examples/catalog.json")
