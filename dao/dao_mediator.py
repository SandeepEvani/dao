# dao_mediator.py
# mediates different classes for the dao

########################################################

from typing import Callable
from sys import _getframe

from .router.router import Router
from .config.config import Config
from .signature.signature_factory import SignatureFactory
from .utils import segregate_args
from pandas import DataFrame

########################################################


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

        self.dao_config = Config(config_location)
        self.signature_factory = SignatureFactory()
        self.dao_router = Router(self.dao_config)

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

    def mediate(self, method_args, confs, data_store) -> Callable:
        """
        choose_route method is used to choose the required data access method
        based on the method args provided to the operator function

        :param data_store:
        :param confs:
        :param method_args:
        :return:
        """

        # get the caller function's name
        caller = _getframe(1).f_code.co_name
        signature = self.operation.get(caller)

        argument_signature = self.signature_factory.create_argument_signature(method_args, signature)

        route = self.dao_router.choose_route(argument_signature, data_store, confs)

        return route['method']

    def router(self, signature, data_store, conf_args):
        """

        :param signature:
        :param data_store:
        :param conf_args:
        :return:
        """

        ...
