# test_router_choose_route.py
# Tests focused on Router.choose_route – end-to-end dispatch and type-checking.

import typing
from collections.abc import Iterable, Mapping, Sequence
from typing import Dict, FrozenSet, List, Optional, Set, Tuple, Union

import pytest

from dao.core.router import Router
from dao.registrar import register

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _router_for(interface) -> Router:
    """Create a read Router pre-loaded with routes from *interface*."""
    router = Router("read")
    router.create_routes_from_interface_object("s3", interface)
    return router


def _route_for(interface, args: dict):
    """End-to-end helper: build arg dict and call choose_route."""
    router = _router_for(interface)
    return router.choose_route(args, "s3", {})


# ---------------------------------------------------------------------------
# Shared interface stubs
# ---------------------------------------------------------------------------


class ReadInterface:
    def read_csv(self, path: str) -> None: ...
    def read_parquet(self, path: str, columns: List[str] = None) -> None: ...
    def read_with_kwargs(self, path: str, **kwargs) -> None: ...


class WriteInterface:
    def write_csv(self, path: str, data: list) -> None: ...


class OverloadedInterface:
    def read_simple(self, path: str) -> None: ...
    def read_detailed(self, path: str, columns: List[str], limit: int) -> None: ...


class RegisteredInterface:
    @register("read")
    def fetch_data(self, path: str) -> None: ...

    @register("write")
    def persist_data(self, path: str, data: list) -> None: ...

    def not_registered(self) -> None: ...


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def reset_signature_factory_singleton():
    """Reset the SignatureFactory singleton before every test."""
    import dao.core.signature.signature_factory as sf_module

    sf_module.SignatureFactory._instance = None
    yield


# ---------------------------------------------------------------------------
# 1. choose_route – core dispatch behaviour
# ---------------------------------------------------------------------------


class TestChooseRoute:
    def test_choose_route_returns_matching_method(self):
        router = Router("read")
        interface = ReadInterface()
        router.create_routes_from_interface_object("s3", interface)
        args = {"path": "s3://bucket/file.csv"}
        route = router.choose_route(args, "s3", {})
        assert route["method_name"] == "read_csv"

    def test_choose_route_matches_most_specific_first(self):
        """With more args provided, the more specific method should win."""
        router = Router("read")
        interface = OverloadedInterface()
        router.create_routes_from_interface_object("s3", interface)
        args = {"path": "s3://bucket/file", "columns": ["col1"], "limit": 10}
        route = router.choose_route(args, "s3", {})
        assert route["method_name"] == "read_detailed"

    def test_choose_route_raises_for_unknown_data_store(self):
        router = Router("read")
        router.create_routes_from_interface_object("s3", ReadInterface())
        with pytest.raises(RuntimeError, match="No methods available"):
            router.choose_route({"path": "s3://bucket/file.csv"}, "nonexistent_store", {})

    def test_choose_route_respects_data_store_isolation(self):
        """Routes registered for 's3' must not be returned when querying 'gcs'."""
        router = Router("read")

        class S3Interface:
            def read_s3(self, path: str) -> None: ...

        class GCSInterface:
            def read_gcs(self, path: str) -> None: ...

        router.create_routes_from_interface_object("s3", S3Interface())
        router.create_routes_from_interface_object("gcs", GCSInterface())
        route = router.choose_route({"path": "gs://bucket/file"}, "gcs", {})
        assert route["method_name"] == "read_gcs"

    def test_choose_route_write_action(self):
        router = Router("write")
        interface = WriteInterface()
        router.create_routes_from_interface_object("db", interface)
        route = router.choose_route({"path": "/tmp/out.csv", "data": [1, 2, 3]}, "db", {})
        assert route["method_name"] == "write_csv"

    def test_choose_route_uses_registered_decorator(self):
        router = Router("read")
        interface = RegisteredInterface()
        router.create_routes_from_interface_object("db", interface)
        route = router.choose_route({"path": "/data"}, "db", {})
        assert route["method_name"] == "fetch_data"

    def test_choose_route_type_mismatch_raises(self):
        """Passing args with wrong types should not match any route."""
        router = Router("read")

        class TypedInterface:
            def read_typed(self, path: str) -> None: ...

        router.create_routes_from_interface_object("s3", TypedInterface())
        with pytest.raises(RuntimeError):
            router.choose_route({"path": 123}, "s3", {})

    def test_no_routes_no_methods_available_error(self):
        router = Router("read")
        with pytest.raises(RuntimeError, match="No methods available"):
            router.choose_route({"path": "s3://bucket"}, "s3", {})


# ---------------------------------------------------------------------------
# 2. Type checking – routes selected / rejected based on argument types
# ---------------------------------------------------------------------------


class TestTypeCheckingViaRouter:
    """Verify that the Router dispatches to the correct method (or raises) based
    on the *types* of the provided arguments, exercising all TypeChecker paths."""

    # ------------------------------------------------------------------
    # 2.1  Basic / bare types
    # ------------------------------------------------------------------

    def test_exact_type_match_str(self):
        class Iface:
            def read_str(self, path: str) -> None: ...

        route = _route_for(Iface(), {"path": "s3://bucket"})
        assert route["method_name"] == "read_str"

    def test_exact_type_match_int(self):
        class Iface:
            def read_int(self, count: int) -> None: ...

        route = _route_for(Iface(), {"count": 42})
        assert route["method_name"] == "read_int"

    def test_subclass_is_compatible(self):
        """bool is a subclass of int; int-annotated param should accept bool."""

        class Iface:
            def read_flag(self, flag: int) -> None: ...

        route = _route_for(Iface(), {"flag": True})
        assert route["method_name"] == "read_flag"

    def test_incompatible_bare_types_raises(self):
        """str value passed where int is expected should not match."""

        class Iface:
            def read_int(self, count: int) -> None: ...

        router = _router_for(Iface())
        with pytest.raises(RuntimeError):
            router.choose_route({"count": "not_an_int"}, "s3", {})

    def test_float_not_compatible_with_str(self):
        class Iface:
            def read_path(self, path: str) -> None: ...

        router = _router_for(Iface())
        with pytest.raises(RuntimeError):
            router.choose_route({"path": 3.14}, "s3", {})

    # ------------------------------------------------------------------
    # 2.2  Optional / Union types
    # ------------------------------------------------------------------

    def test_optional_param_accepts_correct_type(self):
        """Optional[str] = Union[str, None]; passing str should match."""

        class Iface:
            def read_opt(self, path: str, schema: Optional[str] = None) -> None: ...

        route = _route_for(Iface(), {"path": "s3://bucket", "schema": "my_schema"})
        assert route["method_name"] == "read_opt"

    def test_optional_param_accepts_none_type(self):
        """Optional[str] should also match when None is passed."""

        class Iface:
            def read_opt(self, path: str, schema: Optional[str] = None) -> None: ...

        route = _route_for(Iface(), {"path": "s3://bucket", "schema": None})
        assert route["method_name"] == "read_opt"

    def test_union_type_first_member(self):
        class Iface:
            def read_union(self, value: Union[int, str]) -> None: ...

        route = _route_for(Iface(), {"value": 42})
        assert route["method_name"] == "read_union"

    def test_union_type_second_member(self):
        class Iface:
            def read_union(self, value: Union[int, str]) -> None: ...

        route = _route_for(Iface(), {"value": "hello"})
        assert route["method_name"] == "read_union"

    def test_union_incompatible_type_raises(self):
        """Passing a list where Union[int, str] is expected should fail."""

        class Iface:
            def read_union(self, value: Union[int, str]) -> None: ...

        router = _router_for(Iface())
        with pytest.raises(RuntimeError):
            router.choose_route({"value": [1, 2]}, "s3", {})

    # ------------------------------------------------------------------
    # 2.3  Generic collection types (modern syntax)
    # ------------------------------------------------------------------

    def test_list_int_compatible_with_list_int(self):
        class Iface:
            def read_list(self, items: list[int]) -> None: ...

        route = _route_for(Iface(), {"items": [1, 2, 3]})
        assert route["method_name"] == "read_list"

    def test_dict_compatible_with_dict(self):
        class Iface:
            def read_dict(self, config: dict) -> None: ...

        route = _route_for(Iface(), {"config": {"a": 1}})
        assert route["method_name"] == "read_dict"

    def test_set_compatible_with_set(self):
        class Iface:
            def read_set(self, tags: set) -> None: ...

        route = _route_for(Iface(), {"tags": {1, 2}})
        assert route["method_name"] == "read_set"

    # ------------------------------------------------------------------
    # 2.4  Legacy typing module generics (typing.List, typing.Dict, …)
    # ------------------------------------------------------------------

    def test_list_compatible_with_typing_List(self):
        class Iface:
            def read_typed_list(self, items: List[str]) -> None: ...

        route = _route_for(Iface(), {"items": ["a", "b"]})
        assert route["method_name"] == "read_typed_list"

    def test_dict_compatible_with_typing_Dict(self):
        class Iface:
            def read_typed_dict(self, config: Dict[str, int]) -> None: ...

        route = _route_for(Iface(), {"config": {"key": 1}})
        assert route["method_name"] == "read_typed_dict"

    def test_set_compatible_with_typing_Set(self):
        class Iface:
            def read_typed_set(self, tags: Set[str]) -> None: ...

        route = _route_for(Iface(), {"tags": {"a", "b"}})
        assert route["method_name"] == "read_typed_set"

    def test_tuple_compatible_with_typing_Tuple(self):
        class Iface:
            def read_typed_tuple(self, pair: Tuple[str, int]) -> None: ...

        route = _route_for(Iface(), {"pair": ("s3://bucket", 10)})
        assert route["method_name"] == "read_typed_tuple"

    def test_frozenset_compatible_with_typing_FrozenSet(self):
        class Iface:
            def read_frozen(self, ids: FrozenSet[int]) -> None: ...

        route = _route_for(Iface(), {"ids": frozenset({1, 2})})
        assert route["method_name"] == "read_frozen"

    # ------------------------------------------------------------------
    # 2.5  Abstract base class (ABC) types
    # ------------------------------------------------------------------

    def test_list_compatible_with_Sequence(self):
        class Iface:
            def read_seq(self, items: Sequence) -> None: ...

        route = _route_for(Iface(), {"items": [1, 2, 3]})
        assert route["method_name"] == "read_seq"

    def test_list_compatible_with_Iterable(self):
        class Iface:
            def read_iter(self, items: Iterable) -> None: ...

        route = _route_for(Iface(), {"items": [1, 2, 3]})
        assert route["method_name"] == "read_iter"

    def test_dict_compatible_with_Mapping(self):
        class Iface:
            def read_mapping(self, config: Mapping) -> None: ...

        route = _route_for(Iface(), {"config": {"key": "val"}})
        assert route["method_name"] == "read_mapping"

    def test_list_compatible_with_typing_Sequence(self):
        class Iface:
            def read_tseq(self, items: typing.Sequence) -> None: ...

        route = _route_for(Iface(), {"items": ["a"]})
        assert route["method_name"] == "read_tseq"

    def test_dict_compatible_with_typing_Mapping(self):
        class Iface:
            def read_tmap(self, config: typing.Mapping) -> None: ...

        route = _route_for(Iface(), {"config": {}})
        assert route["method_name"] == "read_tmap"

    def test_list_compatible_with_typing_Iterable(self):
        class Iface:
            def read_titer(self, items: typing.Iterable) -> None: ...

        route = _route_for(Iface(), {"items": []})
        assert route["method_name"] == "read_titer"

    # ------------------------------------------------------------------
    # 2.6  Mixed modern vs. legacy syntax equivalence
    # ------------------------------------------------------------------

    def test_modern_list_compatible_with_legacy_List(self):
        class Iface:
            def read_legacy_list(self, items: List[str]) -> None: ...

        route = _route_for(Iface(), {"items": ["x", "y"]})
        assert route["method_name"] == "read_legacy_list"

    def test_modern_dict_compatible_with_legacy_Dict(self):
        class Iface:
            def read_legacy_dict(self, config: Dict[str, int]) -> None: ...

        route = _route_for(Iface(), {"config": {"a": 1}})
        assert route["method_name"] == "read_legacy_dict"

    # ------------------------------------------------------------------
    # 2.7  Overloaded interface – type-based disambiguation
    # ------------------------------------------------------------------

    def test_type_disambiguates_overloaded_methods(self):
        """When two methods share the same parameter name but different types,
        the router must select the one matching the provided type."""

        class Iface:
            def read_str_path(self, path: str) -> None: ...
            def read_int_path(self, path: int) -> None: ...

        router = Router("read")
        router.create_routes_from_interface_object("s3", Iface())

        route_int = router.choose_route({"path": 42}, "s3", {})
        assert route_int["method_name"] == "read_int_path"

        route_str = router.choose_route({"path": "s3://bucket"}, "s3", {})
        assert route_str["method_name"] == "read_str_path"

    # ------------------------------------------------------------------
    # 2.8  Unannotated parameters – permissive matching
    # ------------------------------------------------------------------

    def test_unannotated_param_accepts_any_type(self):
        """If a method parameter carries no type annotation, any value type
        should still be compatible (TypeChecker skips the check)."""

        class Iface:
            def read_any(self, data) -> None: ...

        route = _route_for(Iface(), {"data": {"nested": [1, 2]}})
        assert route["method_name"] == "read_any"

    # ------------------------------------------------------------------
    # 2.9  Nested generics
    # ------------------------------------------------------------------

    def test_nested_generic_list_of_list(self):
        class Iface:
            def read_nested(self, matrix: List[List[int]]) -> None: ...

        route = _route_for(Iface(), {"matrix": [[1, 2], [3, 4]]})
        assert route["method_name"] == "read_nested"

    def test_nested_generic_dict_of_list(self):
        class Iface:
            def read_nested_dict(self, lookup: Dict[str, List[int]]) -> None: ...

        route = _route_for(Iface(), {"lookup": {"a": [1, 2]}})
        assert route["method_name"] == "read_nested_dict"
