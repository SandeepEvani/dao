# signature_factory.py
# creates signature objects

import inspect
from itertools import chain, combinations

from dao.utils.singleton import singleton

from .argument_signature import ArgumentSignature
from .method_signatures import MethodSignature


@singleton
class SignatureFactory:
    """Factory for creating method and argument signatures at runtime.

    Handles the generation of signature combinations for method parameters,
    particularly for optional/default parameters, and creates argument signatures
    from provided DAO arguments.
    """

    _instance = None

    def create_method_signature(self, method):
        """Create all valid method signatures for a callable.

        Generates all possible signature combinations by including/excluding
        optional parameters (with defaults). This allows the router to match
        calls with fewer arguments.

        Args:
            method: A callable (method/function) from an interface class.

        Returns:
            list[MethodSignature]: All valid parameter combinations for the method.

        Example:
            For method `read(path: str, columns: List[str] = None)`, generates:
            - Signature with just `path` (columns excluded)
            - Signature with both `path` and `columns` (both included)
        """
        method_signature = inspect.signature(method)
        possible_signatures = self._get_all_combinations(method_signature)
        return [MethodSignature(sig) for sig in possible_signatures]

    def create_argument_signature(self, args, method_signature):
        """Create argument signature from provided DAO arguments.

        Args:
            args: Dictionary mapping argument names to values (from DAO call).
            method_signature: The expected method signature (from interface).

        Returns:
            ArgumentSignature: Analysis of provided arguments with type annotations.
        """
        parameters = [
            self._create_parameter(arg_name, arg_value, arg_name in method_signature.parameters)
            for arg_name, arg_value in args.items()
        ]
        return ArgumentSignature(inspect.Signature(parameters))

    @staticmethod
    def _create_parameter(name: str, value, is_defined_in_signature: bool) -> inspect.Parameter:
        """Create an inspect.Parameter with appropriate kind and type annotation.

        Args:
            name: Parameter name.
            value: Parameter value (used to infer type).
            is_defined_in_signature: Whether this parameter is defined in the method signature.

        Returns:
            inspect.Parameter with appropriate kind (POSITIONAL_OR_KEYWORD or VAR_KEYWORD).
        """
        # Parameters in the signature are regular args; others are **kwargs
        kind = inspect.Parameter.POSITIONAL_OR_KEYWORD if is_defined_in_signature else inspect.Parameter.VAR_KEYWORD
        return inspect.Parameter(name=name, kind=kind, annotation=type(value))

    @staticmethod
    def _get_all_combinations(signature: inspect.Signature) -> list[inspect.Signature]:
        """Generate all valid signature combinations for optional parameters.

        Separates parameters into required (no defaults) and optional (with defaults),
        then generates all combinations by including/excluding optional parameters.
        This enables matching calls with varying argument counts.

        Args:
            signature: The method signature to analyze.

        Returns:
            list[inspect.Signature]: All valid signature variants.

        Example:
            For `foo(x, y=1, z=2)`, generates:
            - `foo(x)`
            - `foo(x, y)`
            - `foo(x, y, z)`
        """
        # Separate parameters into required and optional
        required = [
            param
            for param in signature.parameters.values()
            if param.default is inspect.Parameter.empty
            and param.kind not in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD)
        ]
        optional = [
            param
            for param in signature.parameters.values()
            if param.default is not inspect.Parameter.empty
            or param.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD)
        ]

        # Generate all combinations of optional parameters
        combinations_list = chain.from_iterable(combinations(optional, r) for r in range(len(optional) + 1))

        # Create signatures: required params + each combination of optional params
        return [inspect.Signature(required + list(combo)) for combo in combinations_list]
