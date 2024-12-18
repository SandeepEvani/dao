# argument_signature
# creates argument signatures for the given args

from __future__ import annotations

import inspect
import types
import typing

from .method_signatures import MethodSignature
from .signature import Signature


class ArgumentSignature(Signature):
    def is_compatible(self, other: MethodSignature) -> bool:
        """
        Input is verified against registered
        :param other:
        :return:
        """

        if type(other) is not MethodSignature:
            raise NotImplementedError(
                f"Cannot check compatibility of type {type(self)} with type {type(other)}"
            )

        # check for the non variable args
        if not other.has_var_args and other.len_non_var_args != self.len_all_args:
            return False

        # check if all the required arguments are present
        # in all the args of the registered signature
        if set(other.non_var_args).issubset(set(self.all_args)):
            return self.check_type_compatibility(other)

        return False

    def check_type_compatibility(self, other: MethodSignature) -> bool:
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
