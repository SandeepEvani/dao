# dao_mediator.py
# mediates different classes for the dao

from sys import _getframe
from typing import Callable

from dao.core.router.router import Router
from dao.core.signature.signature_factory import SignatureFactory
from dao.data_store.data_store_factory import DataStoreFactory


class DAOMediator:
    """Mediator is used to interoperate between the DAO class and the Signature
    and Router classes providing a common hub for all the involved classes."""

    _instance = None

    def __new__(cls, *args, **kwargs):
        # Creates a singleton class of DAOMediator
        if getattr(cls, "_instance") is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, config_location: str, lazy: bool):
        """Initializes the Mediator class.

        :param config_location: path to the Data Store config files
        :param lazy: boolean flag to perform lazy initialization
        """

        self._is_lazy = lazy
        self._initialized = set()

        # Instantiates the Signature Factory and the Router objects
        self.signature_factory = SignatureFactory()
        self.dao_router = Router()
        self.data_store_factory = DataStoreFactory(config_location)
        self.operation = {}

        if lazy:
            self.data_store_factory.validate()

        else:
            # Creates a dictionary of the data stores from the config file
            self.data_stores = dict(self.data_store_factory.initialize_all_data_classes())
            self.dao_router.create_routes_from_data_stores(self.data_stores)

    def register_signature(self, method):
        """Registers the signature of the method passed against the name of the
        method.

        :param method: The Method whose signature is to be registered
        :return:
        """

        from inspect import signature

        self.operation[method.__name__] = signature(method)

        return True

    def mediate(self, method_args, confs) -> Callable:
        """choose_route method is used to choose the required data access
        method based on the method args provided to the operator function.

        :param confs:
        :param method_args:
        :return:
        """

        # Get the caller function's name
        caller = _getframe(2).f_code.co_name
        signature = self.operation.get(caller)

        if caller not in ("read", "write"):
            raise ValueError("Invalid caller function")

        # Create a signature based on the arguments
        argument_signature = self.signature_factory.create_argument_signature(method_args, signature)

        # Extract the name of the data store
        data_object = method_args.get("data_object")
        data_store_object = data_object.data_store
        data_store = data_store_object.name

        data_class = confs.get("dao_interface_class")

        if self._is_lazy:
            if not (data_store, data_class) in self._initialized:
                self.data_store_factory.initialize_data_class(data_store, data_class)

                self.dao_router.create_routes_from_data_object(data_store, data_store_object.get_interface_object(data_class))

                self._initialized.add((data_store, data_class))
            # If not, create routes for the specific data store

            ...

        # Choose the required method from the router
        route = self.dao_router.choose_route(argument_signature, data_store, caller, confs)

        return route["method"]
