# router.py
# routes to appropriate data access method

from inspect import getmembers, ismethod, signature
from itertools import chain

from pandas import DataFrame, Series

from dao.core.signature.signature_factory import SignatureFactory


class Router:

    """
    Router class stores all the appropriate methods from
    all the interface classes and their respective
    function signatures and manages the route table.
    """

    read_methods_predicate: str = "read"
    write_methods_predicate: str = "write"

    def __init__(self, data_stores):
        """
        Initializes the Router class

        :param data_stores: a collection of data store objects
        """

        # Creates the route table
        self.routes = self.create_routes(data_stores)

        # variable to hold method signatures
        self.__signatures = {}

    def register_signature(self, method):
        """
        Registers the signature of different methods

        :param method: Takes the callable and registers the signature
        :return: None
        """

        self.__signatures[method.__name__] = signature(method)

    def choose_route(self, signature, data_store, confs) -> Series:
        """
        choose_route method is used to choose the required data access method
        based on the method args provided to the operator function

        :param signature: The argument signature created on the passed arguments
        :param data_store: The data store to which the data is being accessed
        :param confs: extra configuration options for Router
        :return: A suitable method based on the input signature
        """

        # Filters the route table for unwanted kwargs
        search_space: DataFrame = self.filter_routes(
            data_store, signature.len_all_args, confs
        )

        route = self.get_route(search_space, signature, confs)

        return route

    def filter_routes(self, datastore, length, confs) -> DataFrame:
        """
        Filters the routes based on the data store class and the length

        :param datastore: The Data Store identifier
        :param length: The number of args that are passed to the DAO
        :param confs: extra configuration options for the Router
        :return:
        """

        return self.routes.loc[
            (self.routes["identifier"] == datastore)
            & (self.routes["length_non_var_args"] <= length)
        ]

    def _list_methods_with_predicate(self, interface_object):
        """
        For a DAO class as an input, this function returns an iterable of
        functions that have a prefix <Router.read_methods_predicate> or
        <Router.write_methods_predicate>

        :param interface_object: The Interface object created from the data store confs
        :return: A list of all the methods that are filtered by the router
        """

        predicated_methods = filter(
            lambda x: x[0].startswith(self.write_methods_predicate)
            or x[0].startswith(self.read_methods_predicate)
            or (
                getattr(x[1], "__dao_register__", False) is True
                and getattr(x[1], "__dao_register_params__", None) is not None
            ),
            getmembers(interface_object, predicate=ismethod),
        )

        method_pointers = list(map(lambda x: x[1], predicated_methods))

        return method_pointers

    @staticmethod
    def _get_method_signatures(methods):
        """
        This method takes different functions and returns
        possible combinations of signatures

        :param methods: Iterable consisting required methods to build signatures for.
        :returns: Mapping object of different signatures
        """

        return SignatureFactory().create_method_signature(methods)

    @staticmethod
    def get_route(search_space, argument_signature, confs):
        """

        :param search_space: The filtered section of the route table
        :param argument_signature: The Argument Signature created from the args passed to the DAO class
        :param confs: Extra configurations for the router object

        :return: Returns the route from the search space
        """

        # Iterates over the search space and returns the first match
        for _, route in search_space.iterrows():
            if argument_signature.is_compatible(route["signature"]):
                return route
        else:
            raise Exception("No Compatible Method Found")

    def create_routes(self, data_stores):
        """
        Creates the route table from the data stores

        :param data_stores: collection of data stores

        :return: None
        """

        # Gets the details from each data store
        dao_objects = chain.from_iterable(
            (data_store.get_details() for data_store in data_stores.values())
        )

        # Creates the route table from the respective details
        route_table = DataFrame(
            dao_objects, columns=["identifier", "interface_class", "interface_object"]
               )

        route_table["method"] = route_table["interface_object"].apply(
            self._list_methods_with_predicate
        )
        route_table = route_table.explode("method")

        route_table["signature"] = route_table["method"].apply(
            SignatureFactory().create_method_signature
        )

        route_table["method_type"] = route_table["method"].apply(self._get_method_type)
        route_table["preference"] = route_table["method"].apply(
            self._get_method_preference
        )

        route_table = route_table.explode("signature")

        route_table["length_non_var_args"] = route_table["signature"].apply(
            lambda x: x.len_non_var_args
        )
        route_table["length_all_args"] = route_table["signature"].apply(
            lambda x: x.len_all_args
        )

        return route_table.sort_values(
            ["identifier", "length_non_var_args", "preference", "length_all_args"],
            ascending=[True, False, True, True],
        ).reset_index(drop=True)

    @staticmethod
    def _get_method_type(function):
        """

        """

        if hasattr(function, "__dao_register_params__"):
            return getattr(function, "__dao_register_params__")[0]

        elif function.__name__.startswith("read"):
            return "read"

        elif function.__name__.startswith("write"):
            return "write"

        else:
            raise Exception("Cannot get the method type")

    @staticmethod
    def _get_method_preference(function):
        """

        """
        if hasattr(function, "__dao_register_params__"):
            return getattr(function, "__dao_register_params__")[1]

        else:
            return 0
