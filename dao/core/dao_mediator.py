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

        # Instantiates the Signature Factory and the Router objects
        self.signature_factory = SignatureFactory()
        self.dao_router = Router()
        self.data_store_factory = DataStoreFactory(config_location)

        if lazy:
            self.data_store_factory.validate()

        else:
            # Creates a dictionary of the data stores from the config file
            self.data_stores = dict(self.data_store_factory.create_data_stores())
            self.operation = {}

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

        # Create a signature based on the arguments
        argument_signature = self.signature_factory.create_argument_signature(
            method_args, signature
        )

        # Extract the name of the data store
        data_object = method_args.get("data_object")
        data_store = data_object.data_store.name

        if self._is_lazy:
            # Check if the data store is previously initialized

            # If Initialised move ahead to route_selection

            # If not, create routes for the specific data store

            # Move to route_selection

            ...


        # Choose the required method from the router
        route = self.dao_router.choose_route(argument_signature, data_store, confs)

        return route["method"]
