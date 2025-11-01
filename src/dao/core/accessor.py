# accessor.py
# Defines the data access logic

from inspect import Parameter
from inspect import signature as signature_generator
from typing import Optional

from dao.core.router import Router
from dao.core.signature import SignatureFactory
from dao.data_store import DataStoreFactory, DataStoreRegistry


class DataAccessor:
    """ """

    _initialized = set()

    def __init__(self, signature: Optional[dict] = None, doc_string: Optional[str] = None):
        """ """
        if signature is None:
            self.signature = signature_generator(self.accessor_function)
        self.doc_string = doc_string

        self.signature_factory = SignatureFactory()
        self.data_store_factory = DataStoreFactory()

    def __get__(self, instance, owner):
        """ """
        return self.accessor_function

    def __set_name__(self, owner: type, name: str):
        """Get the name and owner of the accessor method"""
        self.owner = owner
        self.action = name
        self.router = Router(name)

    def create_accessor_function(self): ...

    def accessor_function(self, data_object, **kwargs):
        """

        :param data_object:
        :param kwargs:
        :return:
        """

        # calls the data access logic
        result = self.data_access_logic(locals().copy())

        return result

    def data_access_logic(self, provided_args):
        """The Data access logic used by the read and write methods in the DAO.

        :param provided_args: The arg set provided to the DAO
        :return
        """
        method_args, conf_args = self._preprocess_args(provided_args)

        argument_signature = self.signature_factory.create_argument_signature(method_args, self.signature)

        # Extract the name of the data store
        data_object = method_args.get("data_object")
        data_store_object = data_object.data_store
        data_store = data_store_object.name

        data_class = conf_args.get("dao_interface_class")

        if (data_store, data_class, self.action) not in self._initialized:
            data_store_config = DataStoreRegistry.get(data_store)

            interface_object = self.data_store_factory.initialize_data_class(data_store_config, data_store, data_class)
            self.router.create_routes_from_interface_object(data_store, interface_object)
            self._initialized.add((data_store, data_class, self.action))

        # If the dao is not lazy, we can safely assume all the data stores
        # are initiated, and we can proceed to choose routes
        route = self.router.choose_route(argument_signature, data_store, conf_args)

        # choosing the required method by using the router
        method = route["method"]

        return method(**method_args)

    def _preprocess_args(self, provided_args):
        """Filters, flattens (only the kwarg) and segregates the args.

        :param provided_args: Args provided through the DAO
        :return:
        """

        # Filters the args from unwanted variables
        self._filter_args(provided_args, self.signature)

        # Derives the name of the keyword argument
        kwarg_name: str = [
            key for key, value in self.signature.parameters.items() if value.kind == Parameter.VAR_KEYWORD
        ][0]

        # Updating the flattened KWArg to the provided args
        kwarg_value = provided_args.pop(kwarg_name)
        provided_args.update(kwarg_value)

        # Separate the config args from method args
        method_args, conf_args = self._segregate_args(provided_args)

        return method_args, conf_args

    @staticmethod
    def _filter_args(args, signature):
        """Filters the local variable set to only the required args.

        :param args: The local variables set
        :param signature: The Signature of the respective method (read/write)
        :return:
        """
        keys = list(args.keys())
        for arg_name in keys:
            if arg_name not in signature.parameters:
                args.pop(arg_name)

    @staticmethod
    def _segregate_args(args, segregation_prefix="dao_"):
        """Segregates the conf args and method args.

        :param args: set of provided args to the DAO
        :param segregation_prefix: The prefix on which the argument segregation happens
        :return: Tuple: method args and config args
        """

        confs = {}
        method_args = {}

        # Segregates the args based on the `segregation_prefix`
        for arg_key in args:
            if arg_key.startswith(segregation_prefix):
                confs.update({arg_key: args[arg_key]})
            else:
                method_args.update({arg_key: args[arg_key]})

        return method_args, confs
