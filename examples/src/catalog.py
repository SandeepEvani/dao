from dao import DataAccessObject
from dao.catalog.file import FileCatalog

catalog = FileCatalog(
    data_object_config_location="examples2/catalog.json", data_store_config_location="examples2/data_stores.json"
)

dao = DataAccessObject()

data_object = catalog.get_data_object("raw.customer")
dao.write(data_object=data_object, data="sample data", arg1=1)
