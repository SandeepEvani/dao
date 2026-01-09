from dao.core.dao import DataAccessObject
from dao.data_object import DataObject
from dao.data_store import DataStore
from data_classes.delta import Delta
from data_classes.s3 import S3

bronze_data_store = DataStore("bronze")

bronze_data_store.set_interface_class(class_=S3, primary=True)
bronze_data_store.set_interface_class(class_=Delta)

customer_object = DataObject("custom_store", bronze_data_store)

dao = DataAccessObject()

dao.write(data_object=customer_object, data="", arg1=1, dao_interface_class="Delta")
