# accessor.py
# Decorator that wires a DAO method to the routing + execution pipeline.

from __future__ import annotations

from functools import partial
from inspect import Parameter, signature
from types import MethodType
from typing import Any, Callable, Optional

from makefun import create_function

from dao.core.router import Router


class DataAccessor:
    """Decorator that turns a DAO method stub into a fully routed data-access call.

    Each decorated method gets its own ``DataAccessor`` instance with an
    independent router and initialized-interface registry — no shared class-level
    state between different actions or DAO classes.

    Usage::

        @data_accessor
        def read(self, data_object: DataObject, **kwargs): ...

        @data_accessor(data_object="source")
        def move(self, source: DataObject, destination: DataObject, **kwargs): ...
    """

    def __init__(
        self,
        function: Callable,
        *,
        name: Optional[str] = None,
        doc: Optional[str] = None,
        data_object: Optional[str] = None,
    ) -> None:
        self.function = function
        self.action = name or function.__name__
        self.doc = doc or function.__doc__
        self.data_object = data_object or "data_object"

        self.router = Router(action=self.action, data_object_identifier=self.data_object)
        self._initialized: set = set()
        self._callable = self._build_callable()

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """Delegate to the signature-stamped callable."""
        return self._callable(*args, **kwargs)

    def _build_callable(self) -> Callable:
        """Build a callable that mirrors the original signature but routes through the pipeline."""
        bound = MethodType(self.function, self)
        func_sig = signature(bound)
        impl: Callable[..., Any] = self._execute
        return create_function(
            func_signature=func_sig,
            func_impl=impl,
            name=self.action,
            qualname=self.function.__qualname__,
            doc=self.doc,
        )

    def _execute(self, **kwargs: Any) -> Any:
        """Route the call and invoke the matched interface method."""
        method_args, conf_args = _segregate_args(kwargs)

        data_object = method_args.get(self.data_object)
        if not data_object:
            raise ValueError(f"Required parameter '{self.data_object}' not found in call arguments.")

        data_store = data_object.data_store
        interface_class = conf_args.get("dao_interface_class")
        self._ensure_routes(data_store, interface_class)

        route = self.router.choose_route(method_args.copy(), data_store.name, conf_args)

        # Enrich call args with DataObject properties for declared optional params
        call_args = _enrich_from_data_object(method_args, route, data_object)

        # Strip @when keys that were routing hints (not declared on the method)
        routing_hints = set(route.get("when", {}).keys()) - set(route["signature"].parameters.keys())
        call_args = {k: v for k, v in call_args.items() if k not in routing_hints}

        return route["method"](**call_args)

    def _ensure_routes(self, data_store: Any, interface_class: Optional[str]) -> None:
        """Register routes for this data_store + interface_class pair if not yet done."""
        interface = data_store.get_interface_object(interface_class)
        self.router.create_routes_from_interface_object(data_store.name, interface)


def _segregate_args(args: dict, prefix: str = "dao_") -> tuple[dict, dict]:
    """Split args into method args and ``dao_``-prefixed config args."""
    method_args, conf_args = {}, {}
    for k, v in args.items():
        (conf_args if k.startswith(prefix) else method_args)[k] = v
    return method_args, conf_args


def _enrich_from_data_object(method_args: dict, route: dict, data_object: Any) -> dict:
    """Fill declared optional params from DataObject properties.

    Loops over the route's signature parameters and injects any that:
    - have a default (are optional),
    - were not supplied by the caller,
    - are not private or listed in ``data_object.enrichment_exclude``, and
    - exist as a non-None attribute on the DataObject.
    """
    call_args = dict(method_args)
    if data_object is None:
        return call_args

    exclude = data_object.enrichment_exclude

    for param_name, param in route["signature"].parameters.items():
        if param_name in call_args:
            continue
        if param.kind in (Parameter.VAR_POSITIONAL, Parameter.VAR_KEYWORD):
            continue
        if param.default is Parameter.empty:
            continue
        if param_name.startswith("_") or param_name in exclude:
            continue
        value = getattr(data_object, param_name, None)
        if value is not None:
            call_args[param_name] = value

    return call_args


def data_accessor(
    function: Callable = None,
    *,
    name: Optional[str] = None,
    doc: Optional[str] = None,
    data_object: Optional[str] = None,
) -> DataAccessor | partial:
    """Decorator that turns a DAO method into a routed data-access call.

    Can be used bare or with keyword arguments::

        @data_accessor
        def read(self, data_object: DataObject, **kwargs): ...

        @data_accessor(data_object="source")
        def move(self, source: DataObject, destination: DataObject, **kwargs): ...

    Args:
        function: Function to decorate.
        name: Override the action name (defaults to the function name).
        doc: Override the docstring.
        data_object: Name of the parameter that holds the DataObject
            (defaults to ``"data_object"``).
    """
    if function is None:
        return partial(DataAccessor, name=name, doc=doc, data_object=data_object)
    return DataAccessor(function, name=name, doc=doc, data_object=data_object)
