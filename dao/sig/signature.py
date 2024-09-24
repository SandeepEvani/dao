# signature.py
# defines the signature object for methods and method parameters

import inspect
from itertools import chain, combinations


class Signature:

    def __init__(self): ...

    @staticmethod
    def build_method_signatures(methods) -> list:
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

        return signatures

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

    # def
