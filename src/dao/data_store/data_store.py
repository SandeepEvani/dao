# data_store.py
# represents the data store

from importlib import import_module
from typing import Any, Dict, NamedTuple, Optional


class _DefaultInitializer(NamedTuple):
    class_: type
    args: dict

    def initialize(self):
        return self.class_(**self.args)


class _LazyInitializer(NamedTuple):
    module: str
    class_: str
    args: dict

    def initialize(self):
        interface_module = import_module(self.module)
        interface_class = getattr(interface_module, self.class_)

        return interface_class(**self.args)


class DataStore:
    """The Data Store class is a virtual representation of the Data Store present in the
    working environment to which we read and write data from.

    A Data store will have a primary interface class and optional secondary interface
    classes to access the data stored within.
    """

    _name = None

    def __init__(self, name: str, **properties):
        """Initializes the Data Store Object.

        :param name: Name of the Data Store
        :param properties: Any external properties for a data store if required
        """
        self.name = name
        self._primary_interface_class = None

        self._interface_classes = {}
        self._interface_objects = {}

        for attribute, value in properties.items():
            setattr(self, attribute, value)

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        if self._name is not None:
            raise ValueError("Attribute `name` is already set, cannot modify `name`")
        self._name = value

    def set_interface_class(self, *, class_: type, args: Optional[Dict[str, Any]] = None, primary=False) -> None:
        """Registers the primary interface class with the data store object The primary
        interface class/object is used when there are no other specified parameters.

        :param args:
        :param primary:
        :param class_: The class object which is used as the interface
        :return: None
        """
        args = args or {}

        if primary:
            if self._primary_interface_class is not None:
                raise RuntimeError("Primary interface class is already set, cannot modify")
            self._primary_interface_class = class_.__name__
        self._interface_classes[class_.__name__] = _DefaultInitializer(class_=class_, args=args)

    def set_interface_class_lazy(
        self, *, module: str, class_: str, args: Optional[Dict[str, Any]] = None, primary: Optional[bool] = False
    ) -> None:
        """Registers the primary interface class with the data store object The primary
        interface class/object is used when there are no other specified parameters.

        :param args:
        :param primary:
        :param module:
        :param class_: The class object which is used as the interface
        :return: None
        """
        args = args or {}

        if primary:
            if self._primary_interface_class is not None:
                raise RuntimeError("Primary interface class is already set, cannot modify")
            self._primary_interface_class = class_
        self._interface_classes[class_] = _LazyInitializer(module=module, class_=class_, args=args)

    def get_interface_object(self, class_: Optional[str] = None):
        """Retrieves the interface object based on the class name provided.

        :param class_: The desired interface class name
        :returns Interface object associated to the provided `class_` parameter
        :raises KeyError when the associated class is not found
        """
        class_ = self._primary_interface_class if class_ is None else class_

        if class_ in self._interface_objects:
            return self._interface_objects[class_]

        elif class_ in self._interface_classes:
            initializer = self._interface_classes[class_]
            interface_object = initializer.initialize()
            self._interface_objects[class_] = interface_object
            return interface_object

        else:
            raise KeyError(f"No interface object linked to {class_} class found")
