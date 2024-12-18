# data_store.py
# represents the data store


class DataStore:
    """
    The Data Store class is a virtual representation of the
    Data Store present in the working environment to which we
    read and write data from. A Data store will have a primary
    interface class and optional secondary interface classes to
    access the data stored within.
    """

    __data_stores = {}

    def __init__(self, name, **properties):
        """
        Initializes the Data Store Object

        :param name: Name of the Data Store
        :param properties: Any external properties for a data store if required
        """

        if name in self.__data_stores:
            raise Exception("Duplicate DataStore is detected")

        self.name = name
        self.__data_stores.update({name: self})

    def set_primary_interface_class(self, class_) -> None:
        """
        Registers the primary interface class with the data store object
        The primary interface class/object is used when there are no
        other specified parameters

        :param class_: The class object which is used as the interface
        :return: None
        """
        self.interface_class = class_

    def set_primary_interface_object(self, obj):
        """
        Registers the primary interface object with the data store object
        The primary interface class/object is used when there are no
        other specified parameters

        :param obj: The object which is used as the primary interface
        :return: None
        """

        self.interface_object = obj

    def get_interface_objects(self):
        """
        Returns a list of interface objects which are previously registered

        :return: list: interface objects
        """
        return [self.interface_object]

    def get_details(self):
        """
        Returns an generator which returns the name, class and the object

        :return: A generator which gets the name, class and the object
        """
        for interface_class, interface_object in [
            (self.interface_class, self.interface_object),
        ]:
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
