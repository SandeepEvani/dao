# decorators.py
# All DAO decorators for annotating interface methods.

from typing import Callable


def register(action: str) -> Callable:
    """Registers a method with the DAO router under the given action.

    Use this when the method name does not follow the ``<action>_`` naming
    convention and you still want the router to pick it up.

    Example::

        @register("read")
        def fetch(self, path: str) -> DataFrame: ...

    Args:
        action: The action name (e.g. ``"read"``, ``"write"``) this method
            should be registered under.
    """

    def internal_wrapper(method):
        setattr(method, "__dao_register_action__", action)
        return method

    return internal_wrapper


def when(conditions: dict) -> Callable:
    """Marks a method for conditional dispatch based on argument values.

    When the DAO routes a call it will only select this method if every
    key/value pair in *conditions* matches the corresponding argument
    supplied by the caller.

    Example::

        @when({"format": "parquet"})
        def read_parquet(self, path: str) -> DataFrame: ...

    Args:
        conditions: A mapping of parameter name → expected value that
            must all be satisfied for this method to be chosen.
    """

    def internal_wrapper(method):
        setattr(method, "__dao_when__", conditions)
        return method

    return internal_wrapper
