# dao_mediator.py
# mediates different classes for the dao

from sys import _getframe
from typing import Callable

from dao.core.router.router import Router
from dao.core.signature.signature_factory import SignatureFactory
from dao.data_store.data_store_factory import DataStoreFactory


class DAOMediator:

    _instance = None

    def __new__(cls, *args, **kwargs):

        # Creates a singleton class of DAOMediator
        if not hasattr(cls, "_instance") or getattr(cls, "_instance") is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, config_location: str):
        """

        :param config_location:
        """
        self.data_stores = dict(DataStoreFactory(config_location).create_data_stores())

        # self.dao_config = Config(config_location)
        self.signature_factory = SignatureFactory()
        self.dao_router = Router(self.data_stores)

        self.operation = {}
        self.datastore = {}

    def register_signature(self, method, parameter):
        """

        :param method:
        :param parameter:
        :return:
        """

        from inspect import signature

        self.operation[method.__name__] = signature(method)
        self.datastore[method.__name__] = parameter

        return True

    def mediate(self, method_args, confs) -> Callable:
        """
        choose_route method is used to choose the required data access method
        based on the method args provided to the operator function

        :param confs:
        :param method_args:
        :return:
        """

        # get the caller function's name
        caller = _getframe(1).f_code.co_name
        signature = self.operation.get(caller)

        argument_signature = self.signature_factory.create_argument_signature(
            method_args, signature
        )
        table = method_args.get("data_object")

        data_store = table.data_store.name

        route = self.dao_router.choose_route(argument_signature, data_store, confs)

        return route["method"]
