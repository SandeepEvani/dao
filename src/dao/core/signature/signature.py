# signature.py
# Core signature classes for method and argument analysis

from __future__ import annotations

import inspect
from inspect import Parameter
from types import MappingProxyType

from .parameter_analyzer import ParameterAnalyzer


class Signature:
    """Base class for analyzing and comparing method/argument signatures.

    Wraps inspect.Signature and provides metadata analysis through composition.
    Clean separation: Signature manages the signature object, ParameterAnalyzer
    handles parameter metadata extraction.
    """

    def __init__(self, signature: inspect.Signature):
        """Initialize signature analysis.

        Args:
            signature: The signature object from inspect.signature() or inspect.Signature()
        """
        self._signature = signature
        self.metadata = ParameterAnalyzer.analyze(signature)

    @property
    def all_args(self) -> list[str]:
        """All parameter names in order."""
        return self.metadata.all_args

    @property
    def non_var_args(self) -> list[str]:
        """Regular parameters (not *args or **kwargs)."""
        return self.metadata.non_var_args

    @property
    def has_var_args(self) -> bool:
        """Whether signature accepts *args or **kwargs."""
        return self.metadata.has_var_args

    @property
    def len_all_args(self) -> int:
        """Total parameter count."""
        return self.metadata.len_all_args

    @property
    def len_non_var_args(self) -> int:
        """Non-variadic parameter count."""
        return self.metadata.len_non_var_args

    @property
    def parameters(self) -> MappingProxyType[str, Parameter]:
        """Return the parameters of the signature as a dictionary."""
        return self._signature.parameters

    def __repr__(self) -> str:
        return self._signature.__repr__()
