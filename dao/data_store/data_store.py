# data_store.py
# represents the data store


class DataStore:

    __data_stores = {}

    def __init__(self, name, **properties):
        if name in self.__data_stores:
            raise Exception("Duplicate DataStore is detected")
        self.name = name
        self.__data_stores.update({name: self})

    def set_primary_interface_class(self, class_):
        self.interface_class = class_

    def set_primary_interface_object(self, obj):
        self.interface_object = obj

    def get_interface_objects(self):
        return [self.interface_object]

    def get_details(self):
        for interface_class, interface_object in [(self.interface_class, self.interface_object), ]:
            yield self.name, interface_class, interface_object

    @classmethod
    def get_data_stores(cls):
        return cls.__data_stores.values()

    @classmethod
    def get_data_store(cls, data_store):
        return cls.__data_stores.get(data_store)

    @classmethod
    def check_data_store(cls, data_store: str):
        return data_store in cls.__data_stores
