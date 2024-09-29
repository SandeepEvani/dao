# signature2
# version2 of signature.py

from __future__ import annotations

import inspect
import types
import typing
from itertools import chain, combinations

from pandas import DataFrame


class MutatedSignature:

    def __init__(self, signature: inspect.Signature):
        """

        :parameter signature
        """

        self.has_var_args = False
        self.var_pos_args = None
        self.var_key_args = None
        self.non_var_args = []
        self.all_args = list(signature.parameters.keys())
        self.len_all_args = len(self.all_args)
        self.signature = signature

        self._analyse_args()

    @staticmethod
    def build_method_signatures(methods) -> DataFrame:
        """

        :param methods:
        :return: list of signatures
        """

        signatures = []

        for method in methods:
            method_signature = inspect.signature(method)
            possible_method_signatures = MutatedSignature._get_all_combinations(
                method_signature
            )

            for generated_method_signature in possible_method_signatures:

                new_signature = MutatedSignature(generated_method_signature)
                signatures.append(
                    {
                        "class": MutatedSignature._class_name(method),
                        "method": method,
                        "signature": new_signature,
                        "length": new_signature.len_non_var_args,
                    }
                )

        return DataFrame(signatures).sort_values("length", ascending=False)

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
        keys = list(local_args.keys())
        [local_args.pop(key) for key in keys if key not in signature.parameters.keys()]

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
                        MutatedSignature._parameter(
                            kw_param_key,
                            inspect.Parameter.VAR_KEYWORD,
                            type(kw_param_value),
                        )
                    )
            else:
                parameters.append(
                    MutatedSignature._parameter(
                        param_key,
                        inspect.Parameter.POSITIONAL_OR_KEYWORD,
                        type(local_vars[param_key]),
                    )
                )

        MutatedSignature._filter_unwanted_args(local_vars, signature)
        MutatedSignature._flatten_kwargs(local_vars, signature)

        return MutatedSignature(inspect.Signature(parameters))

    @property
    def len_non_var_args(self):
        """

        :return:
        """
        return len(self.non_var_args)

    def _analyse_args(self):
        """

        :return:
        """
        for parameter_key, parameter_value in self.signature.parameters.items():
            if parameter_value.kind not in (
                inspect.Parameter.VAR_KEYWORD,
                inspect.Parameter.VAR_POSITIONAL,
            ):
                self.non_var_args.append(parameter_key)
            else:
                if not self.has_var_args:
                    self.has_var_args = True

            if parameter_value.kind == inspect.Parameter.VAR_KEYWORD:
                self.var_key_args = parameter_key

            elif parameter_value.kind == inspect.Parameter.VAR_POSITIONAL:
                self.var_pos_args = parameter_key

    def is_compatible(self, other: MutatedSignature) -> bool:
        """
        Input is verified against registered
        :param other:
        :return:
        """

        # check for the non variable args
        if not other.has_var_args and other.len_non_var_args != self.len_all_args:
            return False

        # check if all the required arguments are present
        # in all the args of the registered signature
        if set(other.non_var_args).issubset(set(self.all_args)):
            return self.check_type_compatibility(other)

        return False

    def check_type_compatibility(self, other: MutatedSignature) -> bool:
        """

        :param other:
        :return:
        """
        for parameter in other.signature.parameters.values():
            if parameter.annotation == inspect.Parameter.empty:
                continue

            if isinstance(parameter.annotation, types.UnionType):
                if self.signature.parameters[
                    parameter.name
                ].annotation not in typing.get_args(parameter.annotation):
                    return False

            elif (
                self.signature.parameters[parameter.name].annotation
                != parameter.annotation
            ):
                return False

        return True
