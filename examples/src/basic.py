from dao.core.dao import DataAccessObject
from dao.data_object import DataObject
from dao.data_store import DataStoreFactory

dao = DataAccessObject(
    {
        "bronze": {
            "interface_class": "S3SparkInterface",
            "interface_class_location": "interface_classes.s3.spark",
            "default_configs": {"bucket": "bronze-layer"},
            "secondary_interfaces": [
                {
                    "interface_class": "S3Boto3Interface",
                    "interface_class_location": "interface_classes.s3.s3_boto3",
                    "default_configs": {"bucket": "bronze-layer"},
                },
            ],
        },
        "silver": {
            "interface_class": "S3DeltaInterface",
            "interface_class_location": "interface_classes.s3.deltalake",
            "default_configs": {"bucket": "silver-layer"},
            "secondary_interfaces": [
                {
                    "interface_class": "S3HudiInterface",
                    "interface_class_location": "interface_classes.s3.hudi",
                    "default_configs": {"bucket": "silver-layer"},
                },
            ],
        },
        "gold": {
            "interface_class": "S3DeltaInterface",
            "interface_class_location": "interface_classes.s3.deltalake",
            "default_configs": {"bucket": "gold-layer"},
        },
        "warehouse": {
            "interface_class": "RedshiftSparkInterface",
            "interface_class_location": "interface_classes.redshift.spark",
            "default_configs": {},
        },
    }
)


raw = DataStoreFactory.get_data_store("bronze")
customer_table_object = DataObject("customer", raw)

customer_df = dao.read(customer_table_object)
