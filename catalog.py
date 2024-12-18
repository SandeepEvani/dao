# catalog.py
# creates instances of the TableObject

from dao.data_store import DataStore
from json import load
from dao.data_object import TableObject


class Catalog:

    def __init__(self, confs):
        try:
            self.confs = load(open(confs))
        except Exception as error:
            print("Error reading the config file", error)

    def get(self, table_name):
        """

        :param table_name: Fully qualified table name
        :return:
        """

        if "." in table_name:
            data_store, table = table_name.split(".")

            if DataStore.check_data_store(data_store) and data_store in self.confs:
                data_store_object = DataStore.get_data_store(data_store)

                tables = self.confs[data_store]

                if table in tables:
                    properties = tables.get(table)
                    return TableObject(name=table, data_store=data_store_object, **properties)

        raise Exception("Some Issue")


catalog = Catalog("catalog.json")
