# router.py
# routes to appropriate data access method

from inspect import getmembers, ismethod
from itertools import chain

from pandas import DataFrame

from dao.config.config import Config
from dao.signature.signature_factory import SignatureFactory


class Router:
    read_methods_predicate: str = "read"
    write_methods_predicate: str = "write"

    def __init__(self, confs: str):

        confs = Config(confs)
        self._register_prefixes(confs)

        dao_objects = confs.get_dao_objects
        methods = chain.from_iterable(
            [
                self._list_methods_with_predicate(dao_object)
                for dao_object in dao_objects.values()
            ]
        )

        self._routes = self._get_method_signatures(methods)

    def choose_method(self, local_args, signature, prefix):
        """

        :param local_args:
        :param signature:
        :param prefix:
        :return:
        """

        derived_signature = SignatureFactory().create_argument_signature(
            local_args, signature
        )

        search_space: DataFrame = self._routes.loc[
            (self._routes["length_non_var"] <= derived_signature.len_all_args)
            & (self._routes["class"] == self._prefixes[prefix]),
            :,
        ]

        for _, route in search_space.iterrows():
            if derived_signature.is_compatible(route["signature"]):
                return route

        else:
            raise Exception("No Compatible Method Found")

    def routes(self): ...

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
            getmembers(dao_object, predicate=ismethod),
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

    def _register_prefixes(self, confs):
        """

        :return:
        """
        self._prefixes = {}
        for dao_class_name, dao_class_pointer in confs.get_dao_classes():
            if hasattr(dao_class_pointer, "registration_prefix"):
                registration_prefix = dao_class_pointer.registration_prefix
            else:
                registration_prefix = dao_class_name.lower()
            self._prefixes.update({registration_prefix: dao_class_name})

    def _register_method_signatures(self):
        # method_signatures = {}
        #
        # for target in self.targets.values():
        #     name = target.__class__.__name__
        #     method_signatures[name] = method_signatures.get(name) or {}
        #
        #     methods = list(filter(lambda x: x[0].startswith('write') or x[0].startswith('read'),
        #                           getmembers_static(target, predicate=isfunction)))
        #
        #     for method in methods:
        #
        #         sign = signature(method[-1])
        #
        #         non_defaults = []
        #         default = []
        #         for a in sign.parameters.values():
        #             if a.default == Parameter.empty:
        #                 non_defaults.append(a)
        #             else:
        #                 default.append(a)
        #
        #         possible_signatures = self.powerset(default, non_defaults)
        #
        #         for each_sign in possible_signatures:
        #             method_signatures[name].update({each_sign: {"method_name": method[0],
        #                                                         "method_function": method[1],
        #                                                         "object": target}})
        #
        # return method_signatures

        ...
