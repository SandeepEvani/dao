# type_compatibility.py
# Handles type compatibility checking for method signatures

import typing
from collections.abc import Iterable, Mapping, Sequence


class TypeChecker:
    """Validates type compatibility between provided and target types.

    Handles modern and legacy typing syntax, Union/Optional types, abstract base
    classes, and generics with recursive type argument matching.

    Separated from signature analysis for single responsibility and testability.
    """

    # Mapping of typing module types to runtime equivalents
    _TYPE_EQUIVALENCE = {
        typing.List: list,
        typing.Dict: dict,
        typing.Set: set,
        typing.Tuple: tuple,
        typing.FrozenSet: frozenset,
        typing.Sequence: Sequence,
        typing.Mapping: Mapping,
        typing.Iterable: Iterable,
    }

    @classmethod
    def is_compatible(cls, provided_type, target_type) -> bool:
        """Check if provided_type is compatible with target_type.

        Supports:
        - Basic types (int, str, float, bool, etc.)
        - Typing module types (List, Dict, Optional, Union, etc.)
        - Union types (int | str, Union[int, str])
        - Optional types (Optional[int] = Union[int, None])
        - Generic types (List[int], Dict[str, int])
        - Abstract base classes (Sequence, Mapping, Iterable)
        - Mixed modern and legacy typing syntax (list[int] vs List[int])
        - Nested generics (List[Dict[str, int]])

        Args:
            provided_type: The type provided in the DAO method arguments.
            target_type: The type annotation in the interface method signature.

        Returns:
            bool: True if types are compatible, False otherwise.

        Examples:
            >>> TypeChecker.is_compatible(list, Sequence[int])
            True
            >>> TypeChecker.is_compatible(int, typing.Optional[int])
            True
            >>> TypeChecker.is_compatible(list[int], typing.List[int])
            True
        """
        # Direct match
        if provided_type is target_type:
            return True

        # Extract origin types and arguments
        provided_origin = typing.get_origin(provided_type)
        target_origin = typing.get_origin(target_type)
        provided_args = typing.get_args(provided_type)
        target_args = typing.get_args(target_type)

        # Handle Union types (including Optional)
        if target_origin is typing.Union:
            return any(cls.is_compatible(provided_type, member) for member in target_args)

        if provided_origin is typing.Union:
            return all(cls.is_compatible(member, target_type) for member in provided_args)

        # If both have origins (e.g., List[int] vs Sequence[int])
        if provided_origin is not None and target_origin is not None:
            if not cls._origins_compatible(provided_origin, target_origin):
                return False

            # Check generic type arguments if both have them
            if provided_args and target_args and len(provided_args) == len(target_args):
                return all(cls.is_compatible(p_arg, t_arg) for p_arg, t_arg in zip(provided_args, target_args))

            return True

        # Handle one side having origin, other doesn't
        if provided_origin is not None:
            return cls._check_origin_vs_bare(provided_origin, target_type)

        if target_origin is not None:
            return cls._check_bare_vs_origin(provided_type, target_origin)

        # Handle bare types: check subclass relationship
        return cls._check_bare_types(provided_type, target_type)

    @classmethod
    def _origins_compatible(cls, provided_origin, target_origin) -> bool:
        """Check if two origin types are compatible.

        Handles normalization of typing module equivalences and subclass checking.
        """
        provided_normalized = cls._normalize_type(provided_origin)
        target_normalized = cls._normalize_type(target_origin)

        if provided_normalized == target_normalized:
            return True

        # Check subclass relationship for abstract base classes
        try:
            if isinstance(provided_normalized, type) and isinstance(target_normalized, type):
                return issubclass(provided_normalized, target_normalized)
        except TypeError:
            pass

        return False

    @classmethod
    def _check_origin_vs_bare(cls, provided_origin, target_type) -> bool:
        """Handle case where provided has origin but target is bare type."""
        provided_normalized = cls._normalize_type(provided_origin)
        target_normalized = cls._normalize_type(target_type)

        if provided_normalized is target_normalized:
            return True

        try:
            if isinstance(provided_normalized, type) and isinstance(target_normalized, type):
                return issubclass(provided_normalized, target_normalized)
        except TypeError:
            pass

        return False

    @classmethod
    def _check_bare_vs_origin(cls, provided_type, target_origin) -> bool:
        """Handle case where target has origin but provided is bare type."""
        target_normalized = cls._normalize_type(target_origin)
        provided_normalized = cls._normalize_type(provided_type)

        if provided_normalized is target_normalized:
            return True

        try:
            if isinstance(provided_normalized, type) and isinstance(target_normalized, type):
                return issubclass(provided_normalized, target_normalized)
        except TypeError:
            pass

        return False

    @classmethod
    def _check_bare_types(cls, provided_type, target_type) -> bool:
        """Handle case where both types are bare (no origins)."""
        try:
            if isinstance(provided_type, type) and isinstance(target_type, type):
                return issubclass(provided_type, target_type)
        except TypeError:
            pass

        return False

    @classmethod
    def _normalize_type(cls, type_obj):
        """Normalize type objects to handle modern and legacy typing syntax equivalence.

        Maps typing module types to their runtime equivalents:
        - typing.List → list
        - typing.Dict → dict
        - typing.Set → set
        - typing.Tuple → tuple
        - typing.FrozenSet → frozenset
        - typing.Sequence → collections.abc.Sequence
        - typing.Mapping → collections.abc.Mapping
        - typing.Iterable → collections.abc.Iterable

        Args:
            type_obj: The type object to normalize.

        Returns:
            The normalized type object.

        Example:
            >>> TypeChecker._normalize_type(typing.List)
            <class 'list'>
            >>> TypeChecker._normalize_type(list)
            <class 'list'>
        """
        return cls._TYPE_EQUIVALENCE.get(type_obj, type_obj)
