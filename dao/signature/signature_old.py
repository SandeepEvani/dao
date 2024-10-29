# signature_old.py
# defines the signature object for methods and method parameters

import inspect
from itertools import chain, combinations

from pandas import DataFrame


class Signature:
    def __init__(self): ...

    @staticmethod
    def build_method_signatures(methods) -> DataFrame:
        """

        :param methods:
        :return: list of signatures
        """

        signatures = []

        for method in methods:
            method_signature = inspect.signature(method)
            possible_method_signatures = Signature._get_all_combinations(
                method_signature
            )

            for p_m_s in possible_method_signatures:
                signatures.append(
                    {
                        "class": Signature._class_name(method),
                        "method": method,
                        "signature": p_m_s,
                        "length": len(p_m_s.parameters.keys()),
                    }
                )

        return DataFrame(signatures)

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
            if param.default == inspect.Parameter.empty or param.kind not in (
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

    # @staticmethod
    # def create_input_signature(local_args, signature):
    #     """
    #
    #     :param local_args:
    #     :param signature:
    #     :return:
    #     """
    #
    #     Signature._flatten_kwargs(local_args, signature)
    #     parameters = [
    #         Signature._parameter(arg_name, inspect.Parameter.POSITIONAL_OR_KEYWORD, type(arg_value))
    #         for arg_name, arg_value in local_args.items()
    #         if arg_name in signature.parameters
    #     ]
    #
    #     return inspect.Signature(parameters)

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
    def _flatten_kwargs(local_args, signature) -> None:
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
    def check_signature_compatibility(derived_signature, registered_signature):
        """

        :return:
        """
        ...

    @staticmethod
    def create_input_signature(local_vars, signature):
        """

        :param local_vars:
        :param signature:
        :return:
        """

        parameters = []

        for param_key, param_value in signature.parameters.items():
            if param_value.kind == inspect.Parameter.VAR_KEYWORD:
                for kw_param_key, kw_param_value in local_vars[param_key].items():
                    parameters.append(
                        Signature._parameter(
                            kw_param_key,
                            inspect.Parameter.VAR_KEYWORD,
                            type(kw_param_value),
                        )
                    )
            else:
                parameters.append(
                    Signature._parameter(
                        param_key,
                        inspect.Parameter.POSITIONAL_OR_KEYWORD,
                        type(local_vars[param_key]),
                    )
                )
        return inspect.Signature(parameters)
