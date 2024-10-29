# router.py
# routes to appropriate data access method

######################################################################

from inspect import getmembers, ismethod
from itertools import chain

from pandas import DataFrame, Series

from dao.signature.signature_factory import SignatureFactory

######################################################################


class Router:

    read_methods_predicate: str = "read"
    write_methods_predicate: str = "write"

    def __init__(self, confs):
        """

        :param confs:
        """

        dao_objects = confs.get_dao_objects

        methods = chain.from_iterable(
            [
                self._list_methods_with_predicate(dao_object)
                for dao_object in dao_objects.values()
            ]
        )

        self._routes = self._get_method_signatures(methods)

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

        return self._routes.loc[
            (self._routes["class"] == datastore_class)
            & (self._routes["length_non_var"] <= length)]

    def _list_methods_with_predicate(self, dao_object):
        """
        For a DAO class as an input, this function returns a iterable of
        functions that have a prefix <Router.read_methods_predicate> or
        <Router.write_methods_predicate>

        :param dao_object:
        :return:

        """

        predicated_methods = filter(
            lambda x: x[0].startswith(self.write_methods_predicate)
            or x[0].startswith(self.read_methods_predicate),
            getmembers(dao_object["interface_object"], predicate=ismethod),
        )

        method_pointers = map(lambda x: x[1], predicated_methods)

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
