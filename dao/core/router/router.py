# router.py
# routes to appropriate data access method

from inspect import getmembers, ismethod
from itertools import chain

from pandas import DataFrame, Series

from dao.core.signature.signature_factory import SignatureFactory


class Router:

    read_methods_predicate: str = "read"
    write_methods_predicate: str = "write"

    def __init__(self, data_stores):
        """

        :param data_stores:
        """

        self.routes = self.create_routes(data_stores)

        self.__operation = {}
        self.__datastore = {}

    def register_signature(self, method, parameter):
        """

        :param method:
        :param parameter:
        :return:
        """

        from inspect import signature

        self.__operation[method.__name__] = signature(method)
        self.__datastore[method.__name__] = parameter

        return True

    def choose_route(self, signature, data_store, confs) -> Series:
        """
        choose_route method is used to choose the required data access method
        based on the method args provided to the operator function

        :param confs:
        :param data_store:
        :param signature:
        :return:
        """

        search_space: DataFrame = self.filter_routes(
            data_store, signature.len_all_args, confs
        )

        route = self.get_route(search_space, signature, confs)

        return route

    def filter_routes(self, datastore_class, length, confs) -> DataFrame:
        """

        :return:
        """

        return self.routes.loc[
            (self.routes["identifier"] == datastore_class)
            & (self.routes["length_non_var_args"] <= length)
        ]

    def _list_methods_with_predicate(self, dao_object):
        """
        For a DAO class as an input, this function returns an iterable of
        functions that have a prefix <Router.read_methods_predicate> or
        <Router.write_methods_predicate>

        :param dao_object:
        :return:

        """

        predicated_methods = filter(
            lambda x: x[0].startswith(self.write_methods_predicate)
            or x[0].startswith(self.read_methods_predicate)
            or (
                getattr(x[1], "__dao_register__", False) is True
                and getattr(x[1], "__dao_register_params__", None) is not None
            ),
            getmembers(dao_object, predicate=ismethod),
        )

        method_pointers = list(map(lambda x: x[1], predicated_methods))

        return method_pointers

    def _get_method_signatures(self, methods):
        """
        This method takes different functions and returns
        possible combinations of signatures

        :param methods: Iterable consisting required methods to build signatures for.
        :returns: Mapping object of different signatures

        """

        return SignatureFactory().create_method_signature(methods)

    def get_route(self, search_space, derived_signature, confs):
        """

        :param confs:
        :param search_space:
        :param derived_signature:
        :return:
        """

        for _, route in search_space.iterrows():
            if derived_signature.is_compatible(route["signature"]):
                return route

        else:
            raise Exception("No Compatible Method Found")

    def create_routes(self, data_stores):
        """

        :return:
        """
        dao_objects = chain.from_iterable(
            (data_store.get_details() for data_store in data_stores.values())
        )
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

    def _get_method_type(self, function):
        if hasattr(function, "__dao_register_params__"):
            return getattr(function, "__dao_register_params__")[0]

        elif function.__name__.startswith("read"):
            return "read"

        elif function.__name__.startswith("write"):
            return "write"

        else:
            raise Exception("Cannot get the method type")

    def _get_method_preference(self, function):

        if hasattr(function, "__dao_register_params__"):
            return getattr(function, "__dao_register_params__")[1]

        else:
            return 0
