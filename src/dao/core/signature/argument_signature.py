# argument_signature.py
# Represents argument signatures from DAO calls

import inspect

from .method_signatures import MethodSignature
from .signature import Signature
from .type_compatibility import TypeChecker


class ArgumentSignature(Signature):
    """Signature of arguments provided in a DAO method call.

    Analyzes provided arguments and validates compatibility against expected
    method signatures through structural matching and type checking.

    Uses composition: delegates type checking to TypeChecker for separation of concerns.
    """

    def is_compatible(self, target: MethodSignature) -> bool:
        """Check if these arguments are compatible with a target method signature.

        Performs two checks:
        1. Structural: All required parameters are present
        2. Type: Provided types match target type annotations

        Args:
            target: The target method signature to check against.

        Returns:
            bool: True if arguments are compatible with the method.

        Raises:
            TypeError: If target is not a MethodSignature.
        """
        if not isinstance(target, MethodSignature):
            raise TypeError(f"Expected MethodSignature, got {type(target).__name__}")

        # Check structural compatibility
        if not self._check_structure(target):
            return False

        # Check type compatibility
        return self._check_types(target)

    def _check_structure(self, target: MethodSignature) -> bool:
        """Verify that all required parameters are present.

        Args:
            target: The target method signature.

        Returns:
            bool: True if all required parameters are provided.
        """
        # If target doesn't have varargs, argument count must match exactly
        if not target.has_var_args and target.len_all_args != self.len_all_args:
            return False

        # Check that all required arguments are present
        return set(target.non_var_args).issubset(set(self.all_args))

    def _check_types(self, target: MethodSignature) -> bool:
        """Verify that provided argument types match target type annotations.

        Args:
            target: The target method signature.

        Returns:
            bool: True if all types are compatible.
        """
        for param_name, target_param in target.signature.parameters.items():
            # Skip parameters without type annotations
            if target_param.annotation == inspect.Parameter.empty:
                continue

            # Skip if parameter not in provided arguments
            if param_name not in self.signature.parameters:
                return False

            provided_param = self.signature.parameters[param_name]
            provided_annotation = provided_param.annotation

            # Skip if provided annotation not specified
            if provided_annotation == inspect.Parameter.empty:
                continue

            # Check type compatibility using TypeChecker
            if not TypeChecker.is_compatible(provided_annotation, target_param.annotation):
                return False

        return True
