# signature_factory.py
# creates signature objects

######################################################################

import inspect
from itertools import chain, combinations

from .argument_signature import ArgumentSignature
from .method_signatures import MethodSignature

######################################################################


class SignatureFactory:
    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, "_instance"):
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self): ...

    def create_method_signature(self, method):

        method_signature = inspect.signature(method)
        possible_method_signatures = self._get_all_combinations(method_signature)

        return [
            MethodSignature(generated_method_signature)
            for generated_method_signature in possible_method_signatures
        ]

    def create_argument_signature(self, args, signature):
        """

        :param args:
        :param signature:
        :return:
        """

        parameters = []

        for arg_key, arg_value in args.items():
            if arg_key in signature.parameters:

                parameters.append(
                    self._parameter(
                        arg_key,
                        inspect.Parameter.POSITIONAL_OR_KEYWORD,
                        type(arg_value),
                    )
                )
            else:
                parameters.append(
                    self._parameter(
                        arg_key,
                        inspect.Parameter.VAR_KEYWORD,
                        type(arg_value),
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
    def _parameter(name, kind, type_):
        """

        :param name:
        :param kind:
        :param type_:
        :return:
        """

        return inspect.Parameter(name=name, kind=kind, annotation=type_)
