# catalog.py
# maintains the catalog object

from dao.data_object.table.table_factory import TableObjectFactory

catalog = TableObjectFactory("tables.json")
