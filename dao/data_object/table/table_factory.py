# table_factory.py
# creates instances of the TableObject

from dao.data_store.data_store import DataStore
from json import load
from .table_object import TableObject


class TableObjectFactory:

    def __init__(self, confs):
        try:
            self.confs = load(open(confs))
            self.tables = self.confs['tables']
        except Exception as error:
            print("Error reading the config file", error)

    def get_table_object(self, table_name):
        """

        :param table_name: Fully qualified table name
        :return:
        """

        if '.' in table_name:
            data_store, table = table_name.split(".")

            if DataStore.check_data_store(data_store):
                data_store = DataStore.get_data_store(data_store)
                if table in self.tables:
                    properties = self.tables.get(table)
                    if properties.pop("data_store") == data_store.name:
                        return TableObject(name=table, data_store=data_store, **properties)

        raise Exception("Some Issue")
