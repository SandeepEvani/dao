# File : dao.py
# Description : Implementation of the DAO


########################################################

from typing import Any
from operator import call
from sys import _getframe

from .dao_interface import IDAO
from .dao_mediator import DAOMediator
from .utils import flatten_kwargs, get_uri_prefix, segregate_args

########################################################


class DAO(IDAO):
    """
    This is the Data Access Object class, this is used to create data access object to read/write data from different
    sources and is extendable to multiple classes. The DAO object pre-registers all the methods required for reading
    and writing data, creates a route table out of different method's acceptable signatures and based upon the input
    of the user arguments during the runtime the appropriate method is chosen and is used to execute the read/write
    operation. All this heavy lifting is abstracted from the user using the package and gives an easy interface while
    writing the main application code.

    # >>> from dao import DAO
    # >>> dao = DAO("/path/to/config.json")
    # >>> # writing data to a s3 bucket
    # >>> dao.write(data="Write this data", destination='s3://my-bucket/data.txt')

    """

    __INTERFACE_IDENTIFIER = "dao_interface_identifier"

    def __new__(cls, *args, **kwargs):
        # Creates a singleton class of DAO
        if not hasattr(cls, "_instance"):
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, config_location: str):
        """
        Initializes the DAO Class
        :param config_location: str
        """
        self.__mediator = DAOMediator(config_location)

        self.__mediator.register_signature(self.write, "destination")
        self.__mediator.register_signature(self.read, "source")

    def write(self, data, destination, **kwargs) -> Any:
        """
        Triggers the appropriate writer method across all the
        registered methods based on the input parameters received

        :param data:    The data object that has to be written to a destination
        :param destination: The destination is a structured URI to where the data has to written to.
                            common URI structure :: {prefix}://{upper_level_location}/{lower_level_location}
                            some examples include :-
                            - s3://my-sample-bucket/my_sample_file.txt
                            - rs://sales/customer_sales
                            - sqs://my-fifo-queue.fifo/

        :param kwargs:  Any other keyword args that are either used as configurations/settings for deciding the appropriate write method
                        or a part of the method arguments that are passed on to the writer methods.

                        Any Keyword argument starting with 'dao_' will be considered as a setting for the DAO class
                        but not as an argument to the writer methods, hence all the arguments with the prefix 'dao_'
                        are filtered before creating the signature object

        :return: Any.   the return of the method is the value returned after executing the respective writer
                        method based on the input arguments
        """

        # Take a snapshot of local args. i.e. the provided args
        provided_args = locals().copy()

        # flatten the kwargs
        flattened_args = self.__flatten_args(provided_args)

        # separate both the method args and the conf args
        method_args, dao_conf_args = segregate_args(flattened_args)

        data_store = self.__resolve_data_store(flattened_args, destination)

        # Replace the argument values with the type using the __create_arg_set
        modified_method_args = self.__create_arg_set(method_args)

        # choosing the required method by using the router
        method = self.__mediator.mediate(modified_method_args, dao_conf_args, data_store)

        result = call(method, **method_args)

        return

    def read(self, source, **kwargs) -> Any: ...

    def __create_arg_set(self, args):
        """
        Returns a new arg dictionary by replacing the value of the arg
        with the type of the arg

        :param args:
        :return:
        """

        caller = _getframe(1).f_code.co_name
        signature = self.__mediator.operation.get(caller)

        return {arg_name: type(arg_value) for arg_name, arg_value in args.items()
                if arg_name in signature.parameters}

    def __resolve_data_store(self, args,  data_store_uri):
        """
        Get the data store mentioned in the 'datastore_uri'
        resolve_data_store first looks for the explicit declaration of the identifier through the
        'dao_interface_identifier' parameter and then looks at the data_store_uri
        if the interface identifier is not provided

        :param data_store_uri:
        :return:
        """
        if self.__INTERFACE_IDENTIFIER in args:
            return args.get(self.__INTERFACE_IDENTIFIER)

        elif (datastore := get_uri_prefix(data_store_uri)) is not None:
            return datastore

        else:
            raise Exception("No Data Store Identifier found.")

    def __flatten_args(self, args):
        """

        :return:
        """
        import inspect

        caller = _getframe(1).f_code.co_name
        signature = self.__mediator.operation.get(caller)

        kwarg_name: str = [key for key, value in signature.parameters.items()
                           if value.kind == inspect.Parameter.VAR_KEYWORD][0]

        # Flatten the keyword argument first
        return flatten_kwargs(args, kwarg_name)

