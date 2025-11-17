# parameter_analyzer.py
# Analyzes method/argument parameters and extracts metadata

import inspect
from dataclasses import dataclass
from typing import List


@dataclass
class ParameterMetadata:
    """Metadata about a method's parameters."""

    all_args: List[str]  # All parameter names in order
    non_var_args: List[str]  # Regular parameters (not *args/**kwargs)
    has_var_args: bool  # Whether signature accepts *args or **kwargs
    len_all_args: int  # Total parameter count
    len_non_var_args: int  # Non-variadic parameter count


class ParameterAnalyzer:
    """Analyzes inspect.Signature objects to extract parameter metadata.

    Provides a focused responsibility: analyzing parameter structure without
    dealing with type checking or signature generation.
    """

    @staticmethod
    def analyze(signature: inspect.Signature) -> ParameterMetadata:
        """Analyze a signature and extract parameter metadata.

        Args:
            signature: The signature to analyze.

        Returns:
            ParameterMetadata with extracted information about parameters.

        Example:
            >>> sig = inspect.signature(lambda x, y=1, *args: None)
            >>> metadata = ParameterAnalyzer.analyze(sig)
            >>> metadata.all_args
            ['x', 'y', 'args']
            >>> metadata.non_var_args
            ['x', 'y']
            >>> metadata.has_var_args
            True
        """
        all_args = list(signature.parameters.keys())

        non_var_args = [
            name
            for name, param in signature.parameters.items()
            if param.kind not in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD)
        ]

        has_var_args = any(
            param.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD)
            for param in signature.parameters.values()
        )

        return ParameterMetadata(
            all_args=all_args,
            non_var_args=non_var_args,
            has_var_args=has_var_args,
            len_all_args=len(all_args),
            len_non_var_args=len(non_var_args),
        )
