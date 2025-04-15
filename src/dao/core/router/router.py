# router.py
# routes to appropriate data access method

from inspect import getmembers, ismethod, signature
from itertools import chain

from pandas import DataFrame, Series, concat

from src.dao.core.signature.signature_factory import SignatureFactory


class Router:
    """Router class stores all the appropriate methods from all the interface classes
    and their respective function signatures and manages the route table."""

    read_methods_predicate: str = "read"
    write_methods_predicate: str = "write"

    def __init__(self):
        """Initializes the Router class."""

        # Creates the route table
        self.routes = DataFrame()

        # variable to hold method signatures
        self.__signatures = {}

    def register_signature(self, method) -> None:
        """Registers the signature of different DAO methods.

        :param method: The callable of which the signature is to be registered
        """

        self.__signatures[method.__name__] = signature(method)

    def choose_route(self, arg_signature, data_store: str, method_type: str, confs) -> Series:
        """choose_route method is used to choose the required data access method based
        on the method args provided to the operator function.

        :param method_type:
        :param arg_signature: The argument signature created on the passed arguments
        :param data_store: The data store to which the data is being accessed
        :param confs: extra configuration options for Router
        :return: A suitable method based on the input signature
        """

        # Filters the route table for unwanted routes
        search_space: DataFrame = self.filter_routes(data_store, arg_signature.len_all_args, method_type, confs)

        route = self.get_route(search_space, arg_signature, confs)

        return route

    def filter_routes(self, datastore, length, method_type, confs) -> DataFrame:
        """Filters the routes based on the datastore class, length and the method/caller
        type.

        :param method_type:
        :param datastore: The Data Store identifier
        :param length: The number of args that are passed to the DAO
        :param confs: extra configuration options for the Router
        :return:
        """

        return self.routes.loc[
            (self.routes["identifier"] == datastore)
            & (self.routes["method_type"] == method_type)
            & (self.routes["length_non_var_args"] <= length)
        ]

    def _list_methods_with_predicate(self, interface_object):
        """For an interface object, this function returns an iterable of functions that
        have a prefix <Router.read_methods_predicate> or
        <Router.write_methods_predicate> or were registered by the `dao.register`
        function.

        :param interface_object: The Interface object created from the data store confs
        :return: A list of all the methods that are filtered by the router
        """

        # filter methods on the given conditions
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
        """This method takes different functions and returns possible combinations of
        signatures.

        :param methods: Iterable consisting required methods to build signatures for.
        :returns: Mapping object of different signatures
        """

        return SignatureFactory().create_method_signature(methods)

    @staticmethod
    def get_route(search_space, argument_signature, confs):
        """Loops over the search_space to get the compatible route based on the argument
        signature.

        :param search_space: The filtered section of the route table
        :param argument_signature: The Argument Signature created from the args passed
            to the DAO class
        :param confs: Extra configurations for the router object
        :return: Returns the route from the search space
        """

        # Iterates over the search space and returns the first match
        for _, route in search_space.iterrows():
            if argument_signature.is_compatible(route["signature"]):
                return route
        else:
            raise RuntimeError("No Compatible Method Found")

    def create_routes_from_data_stores(self, data_stores) -> None:
        """Creates the route table from the data stores.

        :param data_stores: collection of data stores
        :return: None
        """

        # Gets the details from each data store
        dao_objects = chain.from_iterable(
            (
                [(data_object, data_store.name) for data_object in data_store.get_interface_objects()]
                for data_store in data_stores
            )
        )

        # Creates the route table from the respective details
        base_table = DataFrame(dao_objects, columns=["interface_object", "identifier"])

        route_table = self._create_route_table(base_table)
        self.routes = concat((self.routes, route_table))

        return

    def create_routes_from_data_object(self, data_store: str, interface_object) -> None:
        """Creates the route table from the data stores.

        :param data_store: collection of data stores
        :param interface_object: collection of data stores
        :return: None
        """

        # Creates the route table from the respective details
        base_table = DataFrame([(data_store, interface_object)], columns=["identifier", "interface_object"])

        route_table = self._create_route_table(base_table)
        self.routes = concat((self.routes, route_table))

    def _create_route_table(self, base_table: DataFrame) -> DataFrame:
        """Creates the route table from the data stores.

        :param base_table: collection of data stores
        :return: None
        """

        base_table["interface_class"] = base_table["interface_object"].apply(lambda x: x.__class__.__name__)

        base_table["method"] = base_table["interface_object"].apply(self._list_methods_with_predicate)
        base_table = base_table.explode("method")

        base_table["signature"] = base_table["method"].apply(SignatureFactory().create_method_signature)

        base_table["method_type"] = base_table["method"].apply(self._get_method_type)
        base_table["preference"] = base_table["method"].apply(self._get_method_preference)

        base_table = base_table.explode("signature")

        base_table["length_non_var_args"] = base_table["signature"].apply(lambda x: x.len_non_var_args)
        base_table["length_all_args"] = base_table["signature"].apply(lambda x: x.len_all_args)

        route_table = base_table.sort_values(
            ["identifier", "length_non_var_args", "preference", "length_all_args"],
            ascending=[True, False, True, True],
        ).reset_index(drop=True)

        return route_table

    @staticmethod
    def _get_method_type(function):
        """Get the method type for the given method."""

        if hasattr(function, "__dao_register_params__"):
            return getattr(function, "__dao_register_params__")[0]

        elif function.__name__.startswith("read"):
            return "read"

        elif function.__name__.startswith("write"):
            return "write"

        else:
            raise ValueError("Cannot get the method type")

    @staticmethod
    def _get_method_preference(function):
        """Get the method preference for the given method."""

        if hasattr(function, "__dao_register_params__"):
            return getattr(function, "__dao_register_params__")[1]

        else:
            return 0
