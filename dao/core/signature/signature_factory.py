# signature_factory.py
# creates signature objects

import inspect
from itertools import chain, combinations

from .argument_signature import ArgumentSignature
from .method_signatures import MethodSignature


class SignatureFactory:

    """
    Signature factory is used to create both the method signature
    and argument signature at the runtime
    """

    _instance = None

    def __new__(cls, *args, **kwargs):
        if getattr(cls, "_instance") is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def create_method_signature(self, method):
        """
        Creates the method signature for the given method with all possible combinatorics

        :param method: Callable from the DAO interface class
        :return List: All acceptable parameter-signature combination for the given callable
        """

        # Create the signature using the inspect module
        method_signature = inspect.signature(method)

        # Get all signature combinations
        possible_method_signatures = self._get_all_combinations(method_signature)

        return [
            MethodSignature(generated_method_signature)
            for generated_method_signature in possible_method_signatures
        ]

    def create_argument_signature(self, args, signature):
        """
        Creates the argument signature for the given set of args

        :param args: The given args for the DAO
        :param signature: Signature of the access method (Read or Write)
        :return: An Argument signature object
        """

        parameters = []

        for arg_key, arg_value in args.items():

            # Decides the type of the parameter when the signature is being created
            if arg_key in signature.parameters:
                # If the parameter is present in the signature,
                # It is considered as a positional or keyword argument
                parameters.append(
                    self._parameter(
                        arg_key,
                        inspect.Parameter.POSITIONAL_OR_KEYWORD,
                        type(arg_value),
                    )
                )
            else:
                # If the parameter is not present in the signature,
                # it's considered as a variable keyword argument
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

        # Looping over the parameters to classify between required and default arguments
        for param in signature.parameters.values():
            if param.default == inspect.Parameter.empty and param.kind not in (
                inspect.Parameter.VAR_KEYWORD,
                inspect.Parameter.VAR_POSITIONAL,
            ):
                required_params.append(param)
            else:
                defaulted_params.append(param)

        # Creating a chain object over the different combinations of the defaulted parameters
        chain_object = chain.from_iterable(
            combinations(defaulted_params, length)
            for length in range(len(defaulted_params) + 1)
        )

        # Returns the list of signatures with the non-default parameters
        return [inspect.Signature(required_params + list(obj)) for obj in chain_object]

    @staticmethod
    def _parameter(name, kind, type_):
        """
        Creates the parameter object which is used to create the signature object

        :param name: The Name of the parameter
        :param kind: The Type of the parameter (Positional, keyword, variable positional or variable keyword)
        :param type_: The Type of the parameter value that is needed (int, str. float)
        :return: The parameter object with the given parameters
        """

        return inspect.Parameter(name=name, kind=kind, annotation=type_)
