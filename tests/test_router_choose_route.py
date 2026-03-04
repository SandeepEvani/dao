# test_router_choose_route.py
# Tests focused on Router.choose_route – end-to-end dispatch and type-checking.

import typing
from collections.abc import Iterable, Mapping, Sequence
from inspect import Parameter
from typing import Dict, FrozenSet, List, Literal, Optional, Set, Tuple, Union

import pytest

from dao.core.router import Router
from dao.data_object import DataObject
from dao.decorators import register

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

    # ------------------------------------------------------------------
    # 2.10  Literal types
    # ------------------------------------------------------------------

    def test_literal_str_exact_match(self):
        """Exact Literal value should match."""

        class Iface:
            def read_parquet(self, path: str, format: Literal["parquet"]) -> None: ...

        route = _route_for(Iface(), {"path": "s3://bucket/file", "format": "parquet"})
        assert route["method_name"] == "read_parquet"

    def test_literal_str_wrong_value_raises(self):
        """A string that is not in the Literal should not match."""

        class Iface:
            def read_parquet(self, path: str, format: Literal["parquet"]) -> None: ...

        router = _router_for(Iface())
        with pytest.raises(RuntimeError):
            router.choose_route({"path": "s3://bucket/file", "format": "csv"}, "s3", {})

    def test_literal_int_match(self):
        """Literal works with integer values too."""

        class Iface:
            def read_version(self, path: str, version: Literal[1]) -> None: ...

        route = _route_for(Iface(), {"path": "s3://bucket", "version": 1})
        assert route["method_name"] == "read_version"

    def test_literal_int_wrong_value_raises(self):
        class Iface:
            def read_version(self, path: str, version: Literal[1]) -> None: ...

        router = _router_for(Iface())
        with pytest.raises(RuntimeError):
            router.choose_route({"path": "s3://bucket", "version": 2}, "s3", {})

    def test_literal_multi_value_first(self):
        """Literal with multiple allowed values accepts any of them."""

        class Iface:
            def read_fmt(self, path: str, format: Literal["parquet", "csv", "json"]) -> None: ...

        route = _route_for(Iface(), {"path": "s3://bucket", "format": "parquet"})
        assert route["method_name"] == "read_fmt"

    def test_literal_multi_value_last(self):
        class Iface:
            def read_fmt(self, path: str, format: Literal["parquet", "csv", "json"]) -> None: ...

        route = _route_for(Iface(), {"path": "s3://bucket", "format": "json"})
        assert route["method_name"] == "read_fmt"

    def test_literal_multi_value_not_in_set_raises(self):
        class Iface:
            def read_fmt(self, path: str, format: Literal["parquet", "csv", "json"]) -> None: ...

        router = _router_for(Iface())
        with pytest.raises(RuntimeError):
            router.choose_route({"path": "s3://bucket", "format": "avro"}, "s3", {})

    def test_literal_disambiguates_overloaded_methods(self):
        """Two methods with the same param name but different Literal values
        should dispatch to the correct one based on the supplied value."""

        class Iface:
            def read_parquet(self, path: str, format: Literal["parquet"]) -> None: ...
            def read_csv(self, path: str, format: Literal["csv"]) -> None: ...

        router = Router("read")
        router.create_routes_from_interface_object("s3", Iface())

        route = router.choose_route({"path": "s3://bucket", "format": "parquet"}, "s3", {})
        assert route["method_name"] == "read_parquet"

        route = router.choose_route({"path": "s3://bucket", "format": "csv"}, "s3", {})
        assert route["method_name"] == "read_csv"


# ---------------------------------------------------------------------------
# 3. Inheritance-based type dispatch – subclass specificity
# ---------------------------------------------------------------------------


class _BaseTableObject:
    """Generic table object (parent)."""

    pass


class _HudiObject(_BaseTableObject):
    """Hudi-specific table object (child of _BaseTableObject)."""

    pass


class _DeltaObject(_BaseTableObject):
    """Delta-specific table object (child of _BaseTableObject)."""

    pass


class _IcebergObject(_BaseTableObject):
    """Iceberg-specific table object (child of _BaseTableObject)."""

    pass


class TestInheritanceTypeDispatch:
    """Verify that the Router prefers the most specific (subclass) type-annotated
    method over a more general (parent class) method, regardless of method
    definition order in the interface class."""

    # ------------------------------------------------------------------
    # 3.1  Subclass object routes to subclass-specific method
    # ------------------------------------------------------------------

    def test_hudi_object_routes_to_hudi_method(self):
        """An _HudiObject should match read_hudi (typed _HudiObject),
        not read_table (typed _BaseTableObject)."""

        class Iface:
            def read_table(self, data_object: _BaseTableObject) -> None: ...
            def read_hudi(self, data_object: _HudiObject) -> None: ...

        route = _route_for(Iface(), {"data_object": _HudiObject()})
        assert route["method_name"] == "read_hudi"

    def test_delta_object_routes_to_delta_method(self):
        """An _DeltaObject should match read_delta, not read_table."""

        class Iface:
            def read_table(self, data_object: _BaseTableObject) -> None: ...
            def read_delta(self, data_object: _DeltaObject) -> None: ...

        route = _route_for(Iface(), {"data_object": _DeltaObject()})
        assert route["method_name"] == "read_delta"

    def test_base_object_routes_to_generic_method(self):
        """A _BaseTableObject should match read_table, not read_hudi or read_delta."""

        class Iface:
            def read_table(self, data_object: _BaseTableObject) -> None: ...
            def read_hudi(self, data_object: _HudiObject) -> None: ...
            def read_delta(self, data_object: _DeltaObject) -> None: ...

        route = _route_for(Iface(), {"data_object": _BaseTableObject()})
        assert route["method_name"] == "read_table"

    # ------------------------------------------------------------------
    # 3.2  Definition order independence
    # ------------------------------------------------------------------

    def test_subclass_method_selected_when_generic_defined_first(self):
        """Even when the generic (parent-typed) method is defined first,
        the subclass-specific method should be selected for a subclass arg."""

        class IfaceGenericFirst:
            def read_a_generic(self, data_object: _BaseTableObject) -> None: ...
            def read_hudi(self, data_object: _HudiObject) -> None: ...

        route = _route_for(IfaceGenericFirst(), {"data_object": _HudiObject()})
        assert route["method_name"] == "read_hudi"

    def test_subclass_method_selected_when_generic_defined_last(self):
        """When the generic method is defined last, subclass method still wins."""

        class IfaceGenericLast:
            def read_hudi(self, data_object: _HudiObject) -> None: ...
            def read_z_generic(self, data_object: _BaseTableObject) -> None: ...

        route = _route_for(IfaceGenericLast(), {"data_object": _HudiObject()})
        assert route["method_name"] == "read_hudi"

    def test_all_three_subclass_methods_dispatch_correctly(self):
        """With three subclass methods + one generic, each subclass object
        should route to its specific method."""

        class Iface:
            def read_table(self, data_object: _BaseTableObject) -> None: ...
            def read_hudi(self, data_object: _HudiObject) -> None: ...
            def read_delta(self, data_object: _DeltaObject) -> None: ...
            def read_iceberg(self, data_object: _IcebergObject) -> None: ...

        router = _router_for(Iface())

        route_hudi = router.choose_route({"data_object": _HudiObject()}, "s3", {})
        assert route_hudi["method_name"] == "read_hudi"

        route_delta = router.choose_route({"data_object": _DeltaObject()}, "s3", {})
        assert route_delta["method_name"] == "read_delta"

        route_iceberg = router.choose_route({"data_object": _IcebergObject()}, "s3", {})
        assert route_iceberg["method_name"] == "read_iceberg"

        route_generic = router.choose_route({"data_object": _BaseTableObject()}, "s3", {})
        assert route_generic["method_name"] == "read_table"

    # ------------------------------------------------------------------
    # 3.3  Method name does not influence specificity
    # ------------------------------------------------------------------

    def test_generic_method_name_sorts_first_but_specific_still_wins(self):
        """If the generic method's name alphabetically precedes the specific
        one, the specific method must still be chosen for a subclass arg."""

        class Iface:
            def read_aaa_base(self, data_object: _BaseTableObject) -> None: ...
            def read_zzz_hudi(self, data_object: _HudiObject) -> None: ...

        route = _route_for(Iface(), {"data_object": _HudiObject()})
        assert route["method_name"] == "read_zzz_hudi"

    def test_specific_method_name_sorts_first_still_works(self):
        """If the specific method's name alphabetically precedes the generic,
        it should still be correctly selected."""

        class Iface:
            def read_aaa_hudi(self, data_object: _HudiObject) -> None: ...
            def read_zzz_base(self, data_object: _BaseTableObject) -> None: ...

        route = _route_for(Iface(), {"data_object": _HudiObject()})
        assert route["method_name"] == "read_aaa_hudi"

    # ------------------------------------------------------------------
    # 3.4  Subclass object does not match sibling class method
    # ------------------------------------------------------------------

    def test_hudi_object_does_not_match_delta_method(self):
        """_HudiObject should NOT match a method typed for _DeltaObject
        (sibling in the hierarchy)."""

        class Iface:
            def read_delta(self, data_object: _DeltaObject) -> None: ...

        router = _router_for(Iface())
        with pytest.raises(RuntimeError):
            router.choose_route({"data_object": _HudiObject()}, "s3", {})

    def test_delta_object_does_not_match_hudi_method(self):
        """_DeltaObject should NOT match a method typed for _HudiObject."""

        class Iface:
            def read_hudi(self, data_object: _HudiObject) -> None: ...

        router = _router_for(Iface())
        with pytest.raises(RuntimeError):
            router.choose_route({"data_object": _DeltaObject()}, "s3", {})

    # ------------------------------------------------------------------
    # 3.5  Fallback to generic when no specific method exists
    # ------------------------------------------------------------------

    def test_hudi_falls_back_to_generic_when_no_specific_method(self):
        """If only a generic _BaseTableObject method exists, _HudiObject
        should still match it (subclass is-a parent)."""

        class Iface:
            def read_table(self, data_object: _BaseTableObject) -> None: ...

        route = _route_for(Iface(), {"data_object": _HudiObject()})
        assert route["method_name"] == "read_table"

        def test_delta_falls_back_to_generic_when_no_specific_method(self):
            """_DeltaObject should fall back to _BaseTableObject method."""

            class Iface:
                def read_table(self, data_object: _BaseTableObject) -> None: ...

            route = _route_for(Iface(), {"data_object": _DeltaObject()})
            assert route["method_name"] == "read_table"

    # ------------------------------------------------------------------
    # 3.6  Mixed typed and untyped parameters alongside data_object
    # ------------------------------------------------------------------

    def test_specificity_with_additional_params(self):
        """Subclass dispatch still works when methods have extra parameters."""

        class Iface:
            def read_table(self, data_object: _BaseTableObject, limit: int) -> None: ...
            def read_hudi(self, data_object: _HudiObject, limit: int) -> None: ...

        router = _router_for(Iface())

        route = router.choose_route({"data_object": _HudiObject(), "limit": 100}, "s3", {})
        assert route["method_name"] == "read_hudi"

        route = router.choose_route({"data_object": _BaseTableObject(), "limit": 100}, "s3", {})
        assert route["method_name"] == "read_table"

    # ------------------------------------------------------------------
    # 3.7  Optional / Union annotations with inheritance
    # ------------------------------------------------------------------

    def test_optional_subclass_preferred_over_bare_parent(self):
        """Optional[_HudiObject] should beat bare _BaseTableObject for a
        _HudiObject value, even though Optional is Union[_HudiObject, None]."""

        class Iface:
            def read_table(self, data_object: _BaseTableObject) -> None: ...
            def read_hudi(self, data_object: Optional[_HudiObject]) -> None: ...

        route = _route_for(Iface(), {"data_object": _HudiObject()})
        assert route["method_name"] == "read_hudi"

    def test_optional_subclass_none_value_routes_to_optional_method(self):
        """Passing None should route to the Optional-annotated method when
        the bare-parent method cannot accept None."""

        class Iface:
            def read_hudi(self, data_object: Optional[_HudiObject]) -> None: ...

        route = _route_for(Iface(), {"data_object": None})
        assert route["method_name"] == "read_hudi"

    def test_union_subclass_preferred_over_bare_parent(self):
        """Union[_HudiObject, str] should beat _BaseTableObject for a
        _HudiObject value — the Union contains a more specific member."""

        class Iface:
            def read_table(self, data_object: _BaseTableObject) -> None: ...
            def read_hudi(self, data_object: Union[_HudiObject, str]) -> None: ...

        route = _route_for(Iface(), {"data_object": _HudiObject()})
        assert route["method_name"] == "read_hudi"

    def test_union_parent_vs_bare_subclass(self):
        """A bare _HudiObject annotation should beat Union[_BaseTableObject, str]
        for a _HudiObject value because the bare type is distance 0."""

        class Iface:
            def read_generic(self, data_object: Union[_BaseTableObject, str]) -> None: ...
            def read_hudi(self, data_object: _HudiObject) -> None: ...

        route = _route_for(Iface(), {"data_object": _HudiObject()})
        assert route["method_name"] == "read_hudi"

    def test_union_parent_falls_back_for_base_object(self):
        """A _BaseTableObject value should still route to Union[_BaseTableObject, str]
        when no exact-type method exists."""

        class Iface:
            def read_generic(self, data_object: Union[_BaseTableObject, str]) -> None: ...
            def read_hudi(self, data_object: _HudiObject) -> None: ...

        route = _route_for(Iface(), {"data_object": _BaseTableObject()})
        assert route["method_name"] == "read_generic"

    def test_optional_parent_vs_bare_subclass(self):
        """Bare _HudiObject should beat Optional[_BaseTableObject] for a
        _HudiObject value."""

        class Iface:
            def read_table(self, data_object: Optional[_BaseTableObject]) -> None: ...
            def read_hudi(self, data_object: _HudiObject) -> None: ...

        route = _route_for(Iface(), {"data_object": _HudiObject()})
        assert route["method_name"] == "read_hudi"

    def test_optional_subclass_vs_optional_parent(self):
        """Optional[_HudiObject] should beat Optional[_BaseTableObject] for a
        _HudiObject value — both are Unions but the former has a closer member."""

        class Iface:
            def read_table(self, data_object: Optional[_BaseTableObject]) -> None: ...
            def read_hudi(self, data_object: Optional[_HudiObject]) -> None: ...

        route = _route_for(Iface(), {"data_object": _HudiObject()})
        assert route["method_name"] == "read_hudi"

    # ------------------------------------------------------------------
    # 3.8  Union/Optional with method-name ordering traps
    # ------------------------------------------------------------------

    def test_optional_subclass_wins_when_generic_name_sorts_first(self):
        """Alphabetically-first generic Optional[_BaseTableObject] must not
        shadow Optional[_HudiObject] for a _HudiObject arg."""

        class Iface:
            def read_aaa_table(self, data_object: Optional[_BaseTableObject]) -> None: ...
            def read_zzz_hudi(self, data_object: Optional[_HudiObject]) -> None: ...

        route = _route_for(Iface(), {"data_object": _HudiObject()})
        assert route["method_name"] == "read_zzz_hudi"

    def test_union_subclass_wins_when_generic_name_sorts_first(self):
        """Alphabetically-first Union[_BaseTableObject, str] must not shadow
        bare _HudiObject for a _HudiObject arg."""

        class Iface:
            def read_aaa_generic(self, data_object: Union[_BaseTableObject, str]) -> None: ...
            def read_zzz_hudi(self, data_object: _HudiObject) -> None: ...

        route = _route_for(Iface(), {"data_object": _HudiObject()})
        assert route["method_name"] == "read_zzz_hudi"

    def test_three_way_optional_dispatch(self):
        """Three Optional methods at different hierarchy levels should each
        pick the tightest match."""

        class Iface:
            def read_table(self, data_object: Optional[_BaseTableObject]) -> None: ...
            def read_hudi(self, data_object: Optional[_HudiObject]) -> None: ...
            def read_delta(self, data_object: Optional[_DeltaObject]) -> None: ...

        router = _router_for(Iface())

        route = router.choose_route({"data_object": _HudiObject()}, "s3", {})
        assert route["method_name"] == "read_hudi"

        route = router.choose_route({"data_object": _DeltaObject()}, "s3", {})
        assert route["method_name"] == "read_delta"

        route = router.choose_route({"data_object": _BaseTableObject()}, "s3", {})
        assert route["method_name"] == "read_table"

    # ------------------------------------------------------------------
    # 3.9  Edge cases – deep hierarchy, diamond, no data_object param
    # ------------------------------------------------------------------

    def test_three_level_deep_hierarchy(self):
        """A → B → C: C should beat B which should beat A."""

        class _A:
            pass

        class _B(_A):
            pass

        class _C(_B):
            pass

        class Iface:
            def read_a(self, data_object: _A) -> None: ...
            def read_b(self, data_object: _B) -> None: ...
            def read_c(self, data_object: _C) -> None: ...

        router = _router_for(Iface())

        route = router.choose_route({"data_object": _C()}, "s3", {})
        assert route["method_name"] == "read_c"

        route = router.choose_route({"data_object": _B()}, "s3", {})
        assert route["method_name"] == "read_b"

        route = router.choose_route({"data_object": _A()}, "s3", {})
        assert route["method_name"] == "read_a"

    def test_three_level_deep_fallback_to_grandparent(self):
        """C (grandchild) should fall back to A (grandparent) when no B or C method exists."""

        class _A:
            pass

        class _B(_A):
            pass

        class _C(_B):
            pass

        class Iface:
            def read_a(self, data_object: _A) -> None: ...

        route = _route_for(Iface(), {"data_object": _C()})
        assert route["method_name"] == "read_a"

    def test_diamond_inheritance_routes_correctly(self):
        """Diamond: D inherits from B and C which both inherit from A.
        D should route to the most specific method available."""

        class _A:
            pass

        class _B(_A):
            pass

        class _C(_A):
            pass

        class _D(_B, _C):
            pass

        class Iface:
            def read_a(self, data_object: _A) -> None: ...
            def read_d(self, data_object: _D) -> None: ...

        router = _router_for(Iface())

        route = router.choose_route({"data_object": _D()}, "s3", {})
        assert route["method_name"] == "read_d"

        route = router.choose_route({"data_object": _A()}, "s3", {})
        assert route["method_name"] == "read_a"

    def test_method_without_data_object_param_still_works(self):
        """Methods that don't have a `data_object` parameter should still
        route correctly — they get DataObject as default type."""

        class Iface:
            def read_csv(self, path: str) -> None: ...

        route = _route_for(Iface(), {"path": "s3://bucket/file.csv"})
        assert route["method_name"] == "read_csv"

    def test_custom_data_object_identifier(self):
        """Router with custom data_object_identifier should extract types
        from the correctly named parameter."""

        class Iface:
            def read_table(self, obj: _BaseTableObject) -> None: ...
            def read_hudi(self, obj: _HudiObject) -> None: ...

        router = Router("read", data_object_identifier="obj")
        router.create_routes_from_interface_object("s3", Iface())

        route = router.choose_route({"obj": _HudiObject()}, "s3", {})
        assert route["method_name"] == "read_hudi"

        route = router.choose_route({"obj": _BaseTableObject()}, "s3", {})
        assert route["method_name"] == "read_table"

    def test_mixed_data_object_and_non_data_object_methods(self):
        """An interface with both typed data_object methods and plain
        string-param methods should route both correctly."""

        class Iface:
            def read_table(self, data_object: _BaseTableObject) -> None: ...
            def read_hudi(self, data_object: _HudiObject) -> None: ...
            def read_csv(self, path: str) -> None: ...

        router = _router_for(Iface())

        route = router.choose_route({"data_object": _HudiObject()}, "s3", {})
        assert route["method_name"] == "read_hudi"

        route = router.choose_route({"path": "s3://bucket/file.csv"}, "s3", {})
        assert route["method_name"] == "read_csv"


# ---------------------------------------------------------------------------
# 4. MRO sort mechanics – unit tests for the sort infrastructure
# ---------------------------------------------------------------------------


class TestResolveConcreteTypes:
    """Exhaustive unit tests for Router._resolve_concrete_types.

    Code paths under test:
    1. ``None`` / ``Parameter.empty`` → ``[DataObject]``
    2. ``Union`` / ``Optional``       → recurse into members
    3. Bare ``type``                  → ``[annotation]``
    4. Parameterised generic          → ``[annotation]`` (kept as-is)
    5. Fallback                       → ``[DataObject]``
    """

    # ------------------------------------------------------------------
    # Path 1: None / Parameter.empty → [DataObject]
    # ------------------------------------------------------------------

    def test_none_returns_data_object(self):
        assert Router._resolve_concrete_types(None) == [DataObject]

    def test_parameter_empty_returns_data_object(self):
        assert Router._resolve_concrete_types(Parameter.empty) == [DataObject]

    # ------------------------------------------------------------------
    # Path 3: Bare types → [annotation]
    # ------------------------------------------------------------------

    def test_bare_builtin_int(self):
        assert Router._resolve_concrete_types(int) == [int]

    def test_bare_builtin_str(self):
        assert Router._resolve_concrete_types(str) == [str]

    def test_bare_builtin_float(self):
        assert Router._resolve_concrete_types(float) == [float]

    def test_bare_builtin_bool(self):
        assert Router._resolve_concrete_types(bool) == [bool]

    def test_bare_nonetype(self):
        assert Router._resolve_concrete_types(type(None)) == [type(None)]

    def test_bare_object(self):
        assert Router._resolve_concrete_types(object) == [object]

    def test_bare_custom_class(self):
        assert Router._resolve_concrete_types(_BaseTableObject) == [_BaseTableObject]

    def test_bare_subclass(self):
        assert Router._resolve_concrete_types(_HudiObject) == [_HudiObject]

    def test_bare_deep_subclass(self):
        class _A:
            pass

        class _B(_A):
            pass

        class _C(_B):
            pass

        assert Router._resolve_concrete_types(_C) == [_C]

    def test_bare_data_object_class_itself(self):
        """DataObject as an annotation should be treated as a bare type,
        not conflated with the fallback."""
        assert Router._resolve_concrete_types(DataObject) == [DataObject]

    def test_bare_abc_sequence(self):
        """collections.abc.Sequence is a concrete type (isinstance check passes)."""
        from collections.abc import Sequence

        assert Router._resolve_concrete_types(Sequence) == [Sequence]

    def test_bare_abc_mapping(self):
        from collections.abc import Mapping

        assert Router._resolve_concrete_types(Mapping) == [Mapping]

    def test_bare_abc_iterable(self):
        from collections.abc import Iterable

        assert Router._resolve_concrete_types(Iterable) == [Iterable]

    # ------------------------------------------------------------------
    # Path 2: Union / Optional → recurse into members
    # ------------------------------------------------------------------

    def test_optional_int(self):
        result = Router._resolve_concrete_types(Optional[int])
        assert set(result) == {int, type(None)}
        assert len(result) == 2

    def test_optional_custom_class(self):
        result = Router._resolve_concrete_types(Optional[_HudiObject])
        assert set(result) == {_HudiObject, type(None)}
        assert len(result) == 2

    def test_optional_str(self):
        result = Router._resolve_concrete_types(Optional[str])
        assert set(result) == {str, type(None)}

    def test_union_two_bare_types(self):
        result = Router._resolve_concrete_types(Union[int, str])
        assert set(result) == {int, str}
        assert len(result) == 2

    def test_union_three_bare_types(self):
        result = Router._resolve_concrete_types(Union[int, str, float])
        assert set(result) == {int, str, float}
        assert len(result) == 3

    def test_union_with_none_is_optional(self):
        """Union[A, None] is semantically identical to Optional[A]."""
        result = Router._resolve_concrete_types(Union[_BaseTableObject, None])
        assert set(result) == {_BaseTableObject, type(None)}

    def test_union_siblings_in_hierarchy(self):
        result = Router._resolve_concrete_types(Union[_HudiObject, _DeltaObject])
        assert set(result) == {_HudiObject, _DeltaObject}
        assert len(result) == 2

    def test_union_parent_and_children(self):
        result = Router._resolve_concrete_types(Union[_BaseTableObject, _HudiObject, _DeltaObject])
        assert set(result) == {_BaseTableObject, _HudiObject, _DeltaObject}
        assert len(result) == 3

    def test_union_preserves_order(self):
        """Members should appear in the order they are declared in the Union."""
        result = Router._resolve_concrete_types(Union[_HudiObject, _DeltaObject, str])
        assert result == [_HudiObject, _DeltaObject, str]

    # ------------------------------------------------------------------
    # Path 2 (nested): Union nesting / flattening
    # ------------------------------------------------------------------

    def test_nested_union_flattened(self):
        """Union[Union[int, str], float] is auto-flattened by Python to
        Union[int, str, float], so we get 3 members."""
        result = Router._resolve_concrete_types(Union[Union[int, str], float])
        assert set(result) == {int, str, float}
        assert len(result) == 3

    def test_union_of_optional(self):
        """Union[Optional[A], B] → Union[A, None, B] after Python flattening."""
        result = Router._resolve_concrete_types(Union[Optional[_HudiObject], _DeltaObject])
        assert _HudiObject in result
        assert _DeltaObject in result
        assert type(None) in result
        assert len(result) == 3

    def test_optional_of_union_like_type(self):
        """Optional[Union[int, str]] → Union[int, str, None]."""
        result = Router._resolve_concrete_types(Optional[Union[int, str]])
        assert set(result) == {int, str, type(None)}
        assert len(result) == 3

    # ------------------------------------------------------------------
    # Path 2 (edge): Union that collapses
    # ------------------------------------------------------------------

    def test_union_duplicate_types_collapsed_by_python(self):
        """Union[int, int] is collapsed by Python to int (bare type)."""
        result = Router._resolve_concrete_types(Union[int, int])
        # Python collapses this to just `int`, so it's a bare type
        assert result == [int]

    def test_optional_nonetype_collapsed_by_python(self):
        """Optional[NoneType] → just NoneType (Python collapses it)."""
        result = Router._resolve_concrete_types(Optional[type(None)])
        assert result == [type(None)]

    # ------------------------------------------------------------------
    # Path 2 (with generics inside Union)
    # ------------------------------------------------------------------

    def test_union_containing_parameterised_generic(self):
        """Union[List[int], str] should produce [List[int], str]."""
        result = Router._resolve_concrete_types(Union[List[int], str])
        assert len(result) == 2
        assert str in result
        # The first element should be the parameterised List[int]
        non_str = [r for r in result if r is not str]
        assert len(non_str) == 1

    def test_optional_parameterised_generic(self):
        """Optional[List[int]] → [List[int], NoneType]."""
        result = Router._resolve_concrete_types(Optional[List[int]])
        assert type(None) in result
        assert len(result) == 2

    def test_union_with_literal_member(self):
        """Union[Literal['csv'], int] → each member resolved independently."""
        result = Router._resolve_concrete_types(Union[Literal["csv"], int])
        assert int in result
        assert len(result) == 2

    # ------------------------------------------------------------------
    # Path 4: Parameterised generics → [annotation] (kept as-is)
    # ------------------------------------------------------------------

    def test_typing_list_int(self):
        result = Router._resolve_concrete_types(List[int])
        assert len(result) == 1

    def test_typing_dict_str_int(self):
        result = Router._resolve_concrete_types(Dict[str, int])
        assert len(result) == 1

    def test_typing_set_str(self):
        result = Router._resolve_concrete_types(Set[str])
        assert len(result) == 1

    def test_typing_tuple_int_str(self):
        result = Router._resolve_concrete_types(Tuple[int, str])
        assert len(result) == 1

    def test_typing_frozenset_int(self):
        result = Router._resolve_concrete_types(FrozenSet[int])
        assert len(result) == 1

    def test_builtin_list_int(self):
        result = Router._resolve_concrete_types(list[int])
        assert len(result) == 1

    def test_builtin_dict_str_int(self):
        result = Router._resolve_concrete_types(dict[str, int])
        assert len(result) == 1

    def test_builtin_tuple_int_ellipsis(self):
        result = Router._resolve_concrete_types(tuple[int, ...])
        assert len(result) == 1

    def test_literal_string(self):
        result = Router._resolve_concrete_types(Literal["csv"])
        assert len(result) == 1

    def test_literal_int(self):
        result = Router._resolve_concrete_types(Literal[1])
        assert len(result) == 1

    def test_literal_multi_value(self):
        result = Router._resolve_concrete_types(Literal["a", "b", "c"])
        assert len(result) == 1

    def test_typing_sequence_parameterised(self):
        result = Router._resolve_concrete_types(typing.Sequence[int])
        assert len(result) == 1

    def test_typing_mapping_parameterised(self):
        result = Router._resolve_concrete_types(typing.Mapping[str, int])
        assert len(result) == 1

    # ------------------------------------------------------------------
    # Path 4 / 5 boundary: typing module special forms
    # ------------------------------------------------------------------

    def test_typing_any(self):
        """typing.Any has a non-None get_origin in some Python versions,
        but regardless, it should produce a single-element list."""
        result = Router._resolve_concrete_types(typing.Any)
        assert len(result) == 1

    # ------------------------------------------------------------------
    # Return type guarantees
    # ------------------------------------------------------------------

    def test_always_returns_list(self):
        """No matter the input, result is always a list."""
        inputs = [None, Parameter.empty, int, Optional[int], Union[int, str], List[int], Literal["x"], _HudiObject]
        for ann in inputs:
            result = Router._resolve_concrete_types(ann)
            assert isinstance(result, list), f"Expected list for {ann}, got {type(result)}"

    def test_never_returns_empty_list(self):
        """The function should never return an empty list — the fallback
        guarantees at least [DataObject]."""
        inputs = [
            None,
            Parameter.empty,
            int,
            Optional[int],
            Union[int, str],
            List[int],
            Literal["x"],
            _HudiObject,
            object,
            type(None),
        ]
        for ann in inputs:
            result = Router._resolve_concrete_types(ann)
            assert len(result) > 0, f"Empty list returned for {ann}"


class TestMroDepth:
    """Unit tests for Router._mro_depth."""

    def test_bare_class(self):
        assert Router._mro_depth(_BaseTableObject) == 2  # _BaseTableObject -> object

    def test_subclass(self):
        assert Router._mro_depth(_HudiObject) == 3  # _HudiObject -> _BaseTableObject -> object

    def test_none_returns_zero(self):
        assert Router._mro_depth(None) == 0

    def test_nonetype_has_mro(self):
        assert Router._mro_depth(type(None)) == 2  # NoneType -> object

    def test_parameterised_generic_has_origin_mro(self):
        """List[int].mro() delegates to list.mro() → depth 2 in Python 3.11+."""
        assert Router._mro_depth(List[int]) == 2

    def test_diamond_has_correct_depth(self):
        class _A:
            pass

        class _B(_A):
            pass

        class _C(_A):
            pass

        class _D(_B, _C):
            pass

        # D -> B -> C -> A -> object = 5
        assert Router._mro_depth(_D) == 5


class TestExtractParamTypes:
    """Unit tests for Router._extract_param_types."""

    def test_extracts_from_named_parameter(self):
        import inspect

        sig = inspect.Signature(
            [inspect.Parameter("data_object", inspect.Parameter.POSITIONAL_OR_KEYWORD, annotation=_HudiObject)]
        )
        from dao.core.signature import Signature

        signature = Signature(sig)
        result = Router._extract_param_types(signature, "data_object")
        assert result == [_HudiObject]

    def test_missing_parameter_returns_data_object(self):
        import inspect

        sig = inspect.Signature([inspect.Parameter("path", inspect.Parameter.POSITIONAL_OR_KEYWORD, annotation=str)])
        from dao.core.signature import Signature

        signature = Signature(sig)
        result = Router._extract_param_types(signature, "data_object")
        assert result == [DataObject]

    def test_unannotated_parameter_returns_data_object(self):
        import inspect

        sig = inspect.Signature([inspect.Parameter("data_object", inspect.Parameter.POSITIONAL_OR_KEYWORD)])
        from dao.core.signature import Signature

        signature = Signature(sig)
        result = Router._extract_param_types(signature, "data_object")
        assert result == [DataObject]


class TestMroSortOrder:
    """Verify that _sort_routes orders routes by MRO depth descending
    within the same structural bucket."""

    def test_subclass_routes_sorted_before_parent(self):
        """Routes for _HudiObject (MRO=3) must appear before
        _BaseTableObject (MRO=2) after sorting."""

        class Iface:
            def read_table(self, data_object: _BaseTableObject) -> None: ...
            def read_hudi(self, data_object: _HudiObject) -> None: ...

        router = _router_for(Iface())
        # All routes have 1 non-var arg, so they're in the same structural bucket
        one_arg_routes = [r for r in router.routes if r["length_non_var_args"] == 1]
        types = [r["data_object_type"] for r in one_arg_routes]

        # _HudiObject (MRO=3) should come before _BaseTableObject (MRO=2)
        hudi_idx = next(i for i, t in enumerate(types) if t is _HudiObject)
        base_idx = next(i for i, t in enumerate(types) if t is _BaseTableObject)
        assert hudi_idx < base_idx

    def test_three_level_sort_order(self):
        """C (MRO=4) before B (MRO=3) before A (MRO=2)."""

        class _A:
            pass

        class _B(_A):
            pass

        class _C(_B):
            pass

        class Iface:
            def read_a(self, data_object: _A) -> None: ...
            def read_b(self, data_object: _B) -> None: ...
            def read_c(self, data_object: _C) -> None: ...

        router = _router_for(Iface())
        types = [r["data_object_type"] for r in router.routes]
        assert types.index(_C) < types.index(_B) < types.index(_A)

    def test_optional_creates_multiple_route_entries(self):
        """Optional[_HudiObject] should produce two routes: one for
        _HudiObject and one for NoneType."""

        class Iface:
            def read_hudi(self, data_object: Optional[_HudiObject]) -> None: ...

        router = _router_for(Iface())
        hudi_routes = [r for r in router.routes if r["method_name"] == "read_hudi"]
        types = {r["data_object_type"] for r in hudi_routes}
        assert _HudiObject in types
        assert type(None) in types
        assert len(hudi_routes) == 2

    def test_union_creates_route_per_member(self):
        """Union[_HudiObject, _DeltaObject] should produce two routes."""

        class Iface:
            def read_either(self, data_object: Union[_HudiObject, _DeltaObject]) -> None: ...

        router = _router_for(Iface())
        routes = [r for r in router.routes if r["method_name"] == "read_either"]
        types = {r["data_object_type"] for r in routes}
        assert _HudiObject in types
        assert _DeltaObject in types
        assert len(routes) == 2

    def test_data_object_type_key_present_in_all_routes(self):
        """Every route created via create_routes_from_interface_object should
        have a 'data_object_type' key."""

        class Iface:
            def read_table(self, data_object: _BaseTableObject) -> None: ...
            def read_csv(self, path: str) -> None: ...

        router = _router_for(Iface())
        for route in router.routes:
            assert "data_object_type" in route, f"Route {route['method_name']} missing 'data_object_type'"

    def test_method_without_data_object_gets_data_object_type(self):
        """Methods without a data_object parameter should get DataObject
        as their data_object_type."""

        class Iface:
            def read_csv(self, path: str) -> None: ...

        router = _router_for(Iface())
        csv_routes = [r for r in router.routes if r["method_name"] == "read_csv"]
        assert all(r["data_object_type"] is DataObject for r in csv_routes)

    def test_sort_does_not_break_with_missing_data_object_type(self):
        """Manually injected routes without data_object_type should not
        cause _sort_routes to crash."""
        router = Router("read")
        router.routes = [
            {"identifier": "s3", "length_non_var_args": 1, "length_all_args": 1},
            {"identifier": "s3", "length_non_var_args": 2, "length_all_args": 2},
        ]
        router._sort_routes()  # Should not raise
        lengths = [r["length_non_var_args"] for r in router.routes]
        assert lengths == [2, 1]

    def test_unrelated_hierarchies_do_not_cross_dispatch(self):
        """Two unrelated hierarchies: a deep unrelated type should not
        steal routes from a shallower lineage."""

        class _FileBase:
            pass

        class _SpecialFile(_FileBase):
            pass

        class _VerySpecialFile(_SpecialFile):
            pass  # MRO=4, deeper than _HudiObject's 3

        class Iface:
            def read_table(self, data_object: _BaseTableObject) -> None: ...
            def read_hudi(self, data_object: _HudiObject) -> None: ...
            def read_file(self, data_object: _VerySpecialFile) -> None: ...

        router = _router_for(Iface())

        # _VerySpecialFile should route to read_file, not read_table or read_hudi
        route = router.choose_route({"data_object": _VerySpecialFile()}, "s3", {})
        assert route["method_name"] == "read_file"

        # _HudiObject should still route to read_hudi
        route = router.choose_route({"data_object": _HudiObject()}, "s3", {})
        assert route["method_name"] == "read_hudi"

        # _BaseTableObject should route to read_table
        route = router.choose_route({"data_object": _BaseTableObject()}, "s3", {})
        assert route["method_name"] == "read_table"

    def test_optional_signature_variants_multiply_with_union(self):
        """A method with Optional[_HudiObject] and an optional param should
        produce route entries = (union members) × (signature variants)."""

        class Iface:
            def read_hudi(self, data_object: Optional[_HudiObject], limit: int = 10) -> None: ...

        router = _router_for(Iface())
        hudi_routes = [r for r in router.routes if r["method_name"] == "read_hudi"]
        # Optional[_HudiObject] = 2 types × 2 signature variants (with/without limit) = 4
        assert len(hudi_routes) == 4
