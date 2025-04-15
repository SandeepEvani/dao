# dao.py
# Implementation of the DAO

import inspect
from sys import _getframe
from typing import Any

from src.dao.core.dao_interface import IDAO
from src.dao.core.dao_mediator import DAOMediator


class DAO(IDAO):
    """This is the Data Access Object class, this is used to create data access object
    to read/write data from different sources and is extendable to multiple classes.

    The DAO object pre-registers all the methods required for reading and writing data,
    creates a route table out of different method's acceptable signatures and based upon
    the input of the user arguments during the runtime the appropriate method is chosen
    and is used to execute the read/write operation. All this heavy lifting is
    abstracted from the user using the package and gives an easy interface while writing
    the main application code.
    """

    __INTERFACE_IDENTIFIER = "dao_interface_class"
    _instance = None

    def __new__(cls, *args, **kwargs):
        # Creates a singleton class of DAO
        if getattr(cls, "_instance") is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        """Initialize the DAO class."""
        self.__mediator = None
        self.__lazy = None

    def init(self, data_stores: str, lazy=False) -> None:
        """Initialize the DAO Class where ever/ when ever the user needs.

        :param data_stores: location to the data stores config file
        :param lazy: Flag to perform lazy initiation of data classes
        :return: None
        """

        self.__lazy = lazy

        # Checks for the private attribute, if the init function is run the second time,
        # The init block errors the process
        if getattr(self, self.__get_private_attr_name("__mediator"), False):
            raise RuntimeError("Cannot re-initialize the DAO object")

        # Initialize the DAOMediator
        self.__mediator = DAOMediator(data_stores, lazy=lazy)

        # register the read and write signatures
        self.__mediator.register_signature(self.write)
        self.__mediator.register_signature(self.read)

    def write(self, data, data_object, **kwargs) -> Any:
        """Triggers the appropriate writer method across all the registered methods
        based on the input parameters received.

        :param data: The data that has to be written to the data object
        :param data_object: The corresponding data object to which the data has to be
            written
        :param kwargs: Any other keyword args that are either used as
            configurations/settings for deciding the appropriate write method or a part
            of the method arguments that are passed on to the writer methods. Any
            Keyword argument starting with 'dao_' will be considered as a setting for
            the DAO class but not as an argument to the writer methods, hence all the
            arguments with the prefix 'dao_' are filtered before creating the signature
            object
        :return: Any. the return of the method is the value returned after executing the
            respective writer method based on the input arguments
        """

        # Take a snapshot of local args. i.e. the provided args
        provided_args = locals().copy()

        # calls the data access logic
        result = self.__data_access_logic(provided_args)

        return result

    def read(self, data_object, **kwargs) -> Any:
        """Triggers the appropriate reader method across all the registered methods
        based on the input parameters received.

        :param data_object: The corresponding data object from which the data has to be
            read.
        :param kwargs: Any other keyword args that are either used as
            configurations/settings for deciding the appropriate read method or a part
            of the method arguments that are passed on to the reader methods. Any
            Keyword argument starting with 'dao_' will be considered as a setting for
            the DAO class but not as an argument to the reader methods, hence all the
            arguments with the prefix 'dao_' are filtered before creating the signature
            object
        :return: Any. the return of the method is the value returned after executing the
            respective reader method based on the input arguments
        """

        # Take a snapshot of local args. i.e. the provided args
        provided_args = locals().copy()

        result = self.__data_access_logic(provided_args)

        return result

    def __data_access_logic(self, provided_args):
        """The Data access logic used by the read and write methods in the DAO.

        :param provided_args: The arg set provided to the DAO :return
        """
        method_args, conf_args = self.__preprocess_args(provided_args)

        # choosing the required method by using the router
        method = self.__mediator.mediate(method_args, conf_args)

        return method(**method_args)

    def __preprocess_args(self, provided_args):
        """Filters, flattens (only the kwarg) and segregates the args.

        :param provided_args: Args provided through the DAO
        :return:
        """

        # Gets the caller frame i.e, read or write and the respective signature
        caller = _getframe(2).f_code.co_name
        signature = self.__mediator.operation.get(caller)

        provided_args = provided_args.copy()

        # Filters the args from unwanted variables
        self.__filter_args(provided_args, signature)

        # Derives the name of the keyword argument
        kwarg_name: str = [
            key for key, value in signature.parameters.items() if value.kind == inspect.Parameter.VAR_KEYWORD
        ][0]

        # Updating the flattened KWArg to the provided args
        kwarg_value = provided_args.pop(kwarg_name)
        provided_args.update(kwarg_value)

        # Separate the config args from method args
        method_args, conf_args = self.__segregate_args(provided_args)

        return method_args, conf_args

    @staticmethod
    def __filter_args(args, signature):
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
    def __segregate_args(args, segregation_prefix="dao_"):
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

    def __get_private_attr_name(self, attr):
        """Dynamically generate the mangled name for the private attribute."""

        return f"_{self.__class__.__name__}{attr}"
