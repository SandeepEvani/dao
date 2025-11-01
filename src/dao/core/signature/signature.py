# signature
# defines the signature class

from __future__ import annotations

import inspect


class Signature:
    """Base signature class for creating and maintaining different method and argument
    signatures, uses the signature class from inspect module and some other metadata
    useful for comparison."""

    def __init__(self, signature: inspect.Signature):
        """Initialized the signature class, with the signature object created using the
        `inspect.Signature` class and analyzes the signature to create and store
        metadata with respect to the signature.

        :parameter signature: The signature object of type inspect.Signature
        """

        self.has_var_args = False

        self.all_args = []
        self.var_pos_args = []
        self.var_key_args = []
        self.non_var_args = []
        self.signature = signature

        self._analyse_args(signature)

        self.len_all_args = len(self.all_args)
        self.len_non_var_args = len(self.non_var_args)

    def _analyse_args(self, signature):
        """Loops over the signature parameters and their types and creates a metadata
        dictionary based on the parameters."""

        self.all_args = list(signature.parameters.keys())

        # Loops over different params in the signature class
        for parameter_key, parameter_value in signature.parameters.items():
            # Checks if any variable args are present,
            # if present it adds them into the non var args list
            # if not, flips the has_var_args flag

            if parameter_value.kind not in (
                inspect.Parameter.VAR_KEYWORD,
                inspect.Parameter.VAR_POSITIONAL,
            ):
                self.non_var_args.append(parameter_key)
            else:
                if not self.has_var_args:
                    self.has_var_args = True

            # Checks for variable keyword args and variable position args
            if parameter_value.kind == inspect.Parameter.VAR_KEYWORD:
                self.var_key_args = [parameter_key]
            elif parameter_value.kind == inspect.Parameter.VAR_POSITIONAL:
                self.var_pos_args = [parameter_key]

    def __repr__(self):
        return self.signature.__repr__()
