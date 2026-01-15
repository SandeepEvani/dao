from dao.core.dao import DataAccessObject
from dao.data_object import DataObject
from dao.data_store import DataStoreFactory

data_store_config = {
    "raw": {
        "class": "S3",
        "module": "data_classes.s3",
        "args": {"bucket": "raw_zone"},
        "additional_interfaces": [
            {
                "class": "Delta",
                "module": "data_classes.delta",
            },
            {
                "class": "Hudi",
                "module": "data_classes.hudi",
                "args": {"bucket": "raw-zone"},
            },
        ],
    },
    "processed": {
        "class": "Redshift",
        "module": "data_classes.redshift",
    },
}

factory = DataStoreFactory()
factory.load_configs(data_store_config)

bronze_data_store = factory.get("raw")
custom_data_store = DataObject("custom_store", bronze_data_store)

dao = DataAccessObject()

dao.write(data_object=custom_data_store, data="", arg1=1)

# dao.write()
