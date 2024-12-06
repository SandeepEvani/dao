# signature
# defines the signature class

######################################################################

from __future__ import annotations

import inspect

######################################################################


class Signature:
    def __init__(self, signature: inspect.Signature):
        """

        :parameter signature
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
        """

        :return:
        """

        self.all_args = list(signature.parameters.keys())

        for parameter_key, parameter_value in signature.parameters.items():
            if parameter_value.kind not in (
                inspect.Parameter.VAR_KEYWORD,
                inspect.Parameter.VAR_POSITIONAL,
            ):
                self.non_var_args.append(parameter_key)
            else:
                if not self.has_var_args:
                    self.has_var_args = True

            if parameter_value.kind == inspect.Parameter.VAR_KEYWORD:
                self.var_key_args = [parameter_key]

            elif parameter_value.kind == inspect.Parameter.VAR_POSITIONAL:
                self.var_pos_args = [parameter_key]

    def __repr__(self):

        return self.signature.__repr__()
