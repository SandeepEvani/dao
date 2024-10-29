# signature_factory.py
# creates signature objects

######################################################################

import inspect
from itertools import chain, combinations

from pandas import DataFrame

from .argument_signature import ArgumentSignature
from .method_signatures import MethodSignature

######################################################################


class SignatureFactory:
    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, "_instance"):
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self): ...

    def create_method_signature(self, methods) -> DataFrame:
        signatures = []

        for method in methods:
            method_signature = inspect.signature(method)
            possible_method_signatures = self._get_all_combinations(method_signature)

            for generated_method_signature in possible_method_signatures:
                new_signature = MethodSignature(generated_method_signature)
                signatures.append(
                    {
                        "class": self._class_name(method),
                        "method": method,
                        "signature": new_signature,
                        "length_non_var": new_signature.len_non_var_args,
                        "length_all_vars": new_signature.len_all_args,
                    }
                )

        return DataFrame(signatures).sort_values(
            ["length_non_var", "length_all_vars"], ascending=False
        )

    def create_argument_signature(self, args, signature):
        """

        :param args:
        :param signature:
        :return:
        """

        parameters = []

        for arg_key, arg_type in args.items():
            if arg_key in signature.parameters:

                parameters.append(
                    self._parameter(
                        arg_key,
                        inspect.Parameter.POSITIONAL_OR_KEYWORD,
                        arg_type,
                    )
                )
            else:
                parameters.append(
                    self._parameter(
                        arg_key,
                        inspect.Parameter.VAR_KEYWORD,
                        arg_type,
                    )
                )

        return ArgumentSignature(inspect.Signature(parameters))

    @staticmethod
    def _get_all_combinations(signature):
        """
        Returns all combinations of signatures for a
        specific method based on the defaults assigned

        :param signature:
        :return:
        """

        defaulted_params = []
        required_params = []
        for param in signature.parameters.values():
            if param.default == inspect.Parameter.empty and param.kind not in (
                inspect.Parameter.VAR_KEYWORD,
                inspect.Parameter.VAR_POSITIONAL,
            ):
                required_params.append(param)
            else:
                defaulted_params.append(param)

        chain_object = chain.from_iterable(
            combinations(defaulted_params, length)
            for length in range(len(defaulted_params) + 1)
        )

        return [inspect.Signature(required_params + list(obj)) for obj in chain_object]

    @staticmethod
    def _class_name(method_):
        return method_.__self__.__class__.__name__

    @staticmethod
    def _parameter(name, kind, type_):
        """

        :param name:
        :param kind:
        :param type_:
        :return:
        """

        return inspect.Parameter(name=name, kind=kind, annotation=type_)

    @staticmethod
    def _flatten_kwargs(local_args: dict, signature) -> None:
        """
        loops through the parameters in the signature
        object and adds the flattened VAR_KEYWORD
        argument to the local_args

        WARN: local_args is a mutable argument and is mutated but not returned

        :param signature:
        :param local_args:
        :return: None
        """

        [
            local_args.update(local_args.pop(k))
            for k, v in signature.parameters.items()
            if v.kind == inspect.Parameter.VAR_KEYWORD
        ]

    @staticmethod
    def _filter_unwanted_args(local_args: dict, signature) -> None:
        """
        loops through the parameters in the signature
        object and adds the flattened VAR_KEYWORD
        argument to the local_args

        WARN: local_args is a mutable argument and is mutated but not returned

        :param signature:
        :param local_args:
        :return: None
        """

        filtered_args = {
            key: value
            for key, value in local_args.items()
            if key in signature.parameters.keys()
        }

        return filtered_args
