# test_router.py
# Stringent tests for the Router class – init, method discovery, route creation,
# filtering, get_route, sort, and edge cases.
# choose_route tests live in test_router_choose_route.py

from typing import List, Optional, Union
from unittest.mock import MagicMock

import pytest

from dao.core.router import Router
from dao.data_object import DataObject
from dao.decorators import register

# ---------------------------------------------------------------------------
# Helpers – lightweight interface classes used across tests
# ---------------------------------------------------------------------------


class ReadInterface:
    """Interface with several read methods of increasing specificity."""

    def read_csv(self, path: str) -> None: ...

    def read_parquet(self, path: str, columns: List[str] = None) -> None: ...

    def read_with_kwargs(self, path: str, **kwargs) -> None: ...


class WriteInterface:
    """Interface with a write method."""

    def write_csv(self, path: str, data: list) -> None: ...


class MultiActionInterface:
    """Interface mixing read and write methods."""

    def read_table(self, path: str) -> None: ...

    def write_table(self, path: str, data: list) -> None: ...

    def helper_method(self) -> None:
        """Should NOT be picked up by read or write routers."""


class RegisteredInterface:
    """Interface using @register decorator instead of naming convention."""

    @register("read")
    def fetch_data(self, path: str) -> None: ...

    @register("write")
    def persist_data(self, path: str, data: list) -> None: ...

    def not_registered(self) -> None: ...


class OverloadedInterface:
    """Two read methods, one more specific (more required args) than the other."""

    def read_simple(self, path: str) -> None: ...

    def read_detailed(self, path: str, columns: List[str], limit: int) -> None: ...


class EmptyInterface:
    """Interface with no applicable methods."""

    def unrelated(self) -> None: ...


class MultiStoreInterfaces:
    """Two separate interfaces for two data stores."""

    class StoreA:
        def read_data(self, path: str) -> None: ...

    class StoreB:
        def read_data(self, path: str, schema: dict) -> None: ...


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def reset_signature_factory_singleton():
    """Reset the SignatureFactory singleton before every test so caching
    from singleton decorator does not bleed between tests."""
    import dao.core.signature.signature_factory as sf_module

    sf_module.SignatureFactory._instance = None
    yield


# ---------------------------------------------------------------------------
# 1. Initialisation
# ---------------------------------------------------------------------------


class TestRouterInit:
    def test_action_stored(self):
        router = Router("read")
        assert router.action == "read"

    def test_routes_empty_on_init(self):
        router = Router("write")
        assert router.routes == []

    def test_different_actions_are_independent(self):
        r1 = Router("read")
        r2 = Router("write")
        r1.routes.append({"identifier": "s3"})
        assert r2.routes == [], "Routes should not be shared between Router instances"

    def test_default_data_object_identifier(self):
        router = Router("read")
        assert router.data_object_identifier == "data_object"

    def test_custom_data_object_identifier(self):
        router = Router("read", data_object_identifier="obj")
        assert router.data_object_identifier == "obj"


# ---------------------------------------------------------------------------
# 2. _list_methods_with_predicate
# ---------------------------------------------------------------------------


class TestListMethodsWithPredicate:
    def test_picks_up_naming_convention_methods(self):
        router = Router("read")
        interface = ReadInterface()
        methods = router._list_methods_with_predicate(interface)
        names = [m.__name__ for m in methods]
        assert "read_csv" in names
        assert "read_parquet" in names
        assert "read_with_kwargs" in names

    def test_ignores_wrong_action_methods(self):
        router = Router("read")
        interface = WriteInterface()
        methods = router._list_methods_with_predicate(interface)
        assert methods == []

    def test_picks_up_registered_methods(self):
        router = Router("read")
        interface = RegisteredInterface()
        methods = router._list_methods_with_predicate(interface)
        names = [m.__name__ for m in methods]
        assert "fetch_data" in names
        assert "not_registered" not in names

    def test_registered_write_not_picked_by_read_router(self):
        router = Router("read")
        interface = RegisteredInterface()
        methods = router._list_methods_with_predicate(interface)
        names = [m.__name__ for m in methods]
        assert "persist_data" not in names

    def test_mixed_naming_and_register(self):
        """Methods matching by naming convention AND by @register both appear."""
        router = Router("read")

        class MixedInterface:
            def read_something(self, path: str) -> None: ...

            @register("read")
            def custom_reader(self, path: str) -> None: ...

        interface = MixedInterface()
        methods = router._list_methods_with_predicate(interface)
        names = [m.__name__ for m in methods]
        assert "read_something" in names
        assert "custom_reader" in names

    def test_helper_methods_excluded(self):
        router = Router("read")
        interface = MultiActionInterface()
        methods = router._list_methods_with_predicate(interface)
        names = [m.__name__ for m in methods]
        assert "helper_method" not in names
        assert "write_table" not in names


# ---------------------------------------------------------------------------
# 3. create_routes_from_interface_object
# ---------------------------------------------------------------------------


class TestCreateRoutesFromInterfaceObject:
    def test_routes_created_for_read_interface(self):
        router = Router("read")
        router.create_routes_from_interface_object("s3", ReadInterface())
        assert len(router.routes) > 0

    def test_all_routes_have_required_keys(self):
        router = Router("read")
        router.create_routes_from_interface_object("s3", ReadInterface())
        required_keys = {
            "identifier",
            "interface_class",
            "method",
            "method_name",
            "signature",
            "length_non_var_args",
            "length_all_args",
            "data_object_type",
        }
        for route in router.routes:
            assert required_keys.issubset(route.keys()), f"Missing keys in route: {route}"

    def test_identifier_set_correctly(self):
        router = Router("read")
        router.create_routes_from_interface_object("my_store", ReadInterface())
        for route in router.routes:
            assert route["identifier"] == "my_store"

    def test_interface_class_name_set_correctly(self):
        router = Router("read")
        router.create_routes_from_interface_object("s3", ReadInterface())
        for route in router.routes:
            assert route["interface_class"] == "ReadInterface"

    def test_empty_interface_creates_no_routes(self):
        router = Router("read")
        router.create_routes_from_interface_object("s3", EmptyInterface())
        assert router.routes == []

    def test_multiple_interfaces_accumulate_routes(self):
        router = Router("read")
        router.create_routes_from_interface_object("s3", ReadInterface())
        count_after_first = len(router.routes)
        router.create_routes_from_interface_object("gcs", ReadInterface())
        assert len(router.routes) > count_after_first

    def test_routes_from_registered_interface(self):
        router = Router("read")
        router.create_routes_from_interface_object("db", RegisteredInterface())
        assert len(router.routes) > 0
        names = [r["method_name"] for r in router.routes]
        assert "fetch_data" in names

    def test_optional_params_produce_multiple_signature_variants(self):
        """read_parquet(path, columns=None) should yield 2 route entries (with/without columns)."""
        router = Router("read")
        router.create_routes_from_interface_object("s3", ReadInterface())
        parquet_routes = [r for r in router.routes if r["method_name"] == "read_parquet"]
        assert len(parquet_routes) >= 2, "Expected signature variants for optional parameters"

    def test_routes_sorted_after_creation(self):
        """Routes must be sorted descending by length_non_var_args (more specific first)."""
        router = Router("read")
        router.create_routes_from_interface_object("s3", OverloadedInterface())
        non_var_lengths = [r["length_non_var_args"] for r in router.routes]
        assert non_var_lengths == sorted(non_var_lengths, reverse=True), (
            "Routes must be sorted descending by non_var_args length"
        )

    def test_data_object_type_set_for_bare_annotation(self):
        """A bare type annotation on data_object should set data_object_type directly."""

        class _Obj:
            pass

        class Iface:
            def read_obj(self, data_object: _Obj) -> None: ...

        router = Router("read")
        router.create_routes_from_interface_object("s3", Iface())
        assert all(r["data_object_type"] is _Obj for r in router.routes)

    def test_data_object_type_defaults_to_data_object_when_param_missing(self):
        """Methods without a data_object param get DataObject as the type."""
        router = Router("read")
        router.create_routes_from_interface_object("s3", ReadInterface())
        csv_routes = [r for r in router.routes if r["method_name"] == "read_csv"]
        assert all(r["data_object_type"] is DataObject for r in csv_routes)

    def test_optional_annotation_creates_two_routes_per_variant(self):
        """Optional[X] = Union[X, None] should create 2 route entries per
        signature variant (one for X, one for NoneType)."""

        class _Obj:
            pass

        class Iface:
            def read_opt(self, data_object: Optional[_Obj]) -> None: ...

        router = Router("read")
        router.create_routes_from_interface_object("s3", Iface())
        routes = [r for r in router.routes if r["method_name"] == "read_opt"]
        types = {r["data_object_type"] for r in routes}
        assert _Obj in types
        assert type(None) in types
        assert len(routes) == 2

    def test_union_annotation_creates_route_per_member(self):
        """Union[A, B] should create 2 route entries."""

        class _A:
            pass

        class _B:
            pass

        class Iface:
            def read_either(self, data_object: Union[_A, _B]) -> None: ...

        router = Router("read")
        router.create_routes_from_interface_object("s3", Iface())
        routes = [r for r in router.routes if r["method_name"] == "read_either"]
        types = {r["data_object_type"] for r in routes}
        assert types == {_A, _B}

    def test_union_with_optional_param_route_count(self):
        """Union[A, B] × 2 signature variants (with/without limit) = 4 routes."""

        class _A:
            pass

        class _B:
            pass

        class Iface:
            def read_either(self, data_object: Union[_A, _B], limit: int = 10) -> None: ...

        router = Router("read")
        router.create_routes_from_interface_object("s3", Iface())
        routes = [r for r in router.routes if r["method_name"] == "read_either"]
        assert len(routes) == 4

    def test_custom_data_object_identifier_extracts_from_correct_param(self):
        """Router with data_object_identifier='obj' should extract types from 'obj'."""

        class _Obj:
            pass

        class Iface:
            def read_obj(self, obj: _Obj) -> None: ...

        router = Router("read", data_object_identifier="obj")
        router.create_routes_from_interface_object("s3", Iface())
        assert all(r["data_object_type"] is _Obj for r in router.routes)

    def test_custom_identifier_falls_back_when_param_not_named_data_object(self):
        """When data_object_identifier='obj' but method has 'data_object', it
        should fall back to DataObject (the 'obj' param doesn't exist)."""

        class _Obj:
            pass

        class Iface:
            def read_obj(self, data_object: _Obj) -> None: ...

        router = Router("read", data_object_identifier="obj")
        router.create_routes_from_interface_object("s3", Iface())
        assert all(r["data_object_type"] is DataObject for r in router.routes)


# ---------------------------------------------------------------------------
# 4. filter_routes
# ---------------------------------------------------------------------------


class TestFilterRoutes:
    def _populated_router(self):
        router = Router("read")
        router.create_routes_from_interface_object("s3", ReadInterface())
        router.create_routes_from_interface_object("gcs", ReadInterface())
        return router

    def test_filters_by_identifier(self):
        router = self._populated_router()
        s3_routes = router.filter_routes("s3", 10)
        for route in s3_routes:
            assert route["identifier"] == "s3"

    def test_unknown_identifier_returns_empty(self):
        router = self._populated_router()
        result = router.filter_routes("unknown_store", 10)
        assert result == []

    def test_filters_by_arg_length_minimum(self):
        """Routes require length_non_var_args <= arg_length."""
        router = Router("read")
        router.create_routes_from_interface_object("s3", OverloadedInterface())
        # read_detailed needs 3 args; providing only 1 should exclude it
        filtered = router.filter_routes("s3", 1)
        for route in filtered:
            assert route["length_non_var_args"] <= 1

    def test_zero_arg_length_returns_no_routes_for_non_trivial_interface(self):
        router = Router("read")
        router.create_routes_from_interface_object("s3", ReadInterface())
        result = router.filter_routes("s3", 0)
        # All read methods need at least 'path', so zero args → nothing matches
        assert result == []

    def test_large_arg_length_returns_all_identifier_routes(self):
        router = Router("read")
        router.create_routes_from_interface_object("s3", ReadInterface())
        all_s3 = router.filter_routes("s3", 100)
        assert len(all_s3) == len([r for r in router.routes if r["identifier"] == "s3"])


# ---------------------------------------------------------------------------
# 5. get_route
# ---------------------------------------------------------------------------


class TestGetRoute:
    def test_raises_runtime_error_on_empty_search_space(self):
        router = Router("read")
        with pytest.raises(RuntimeError, match="No methods available"):
            router.get_route([], {}, {})

    def test_raises_runtime_error_when_no_compatible_route(self):
        router = Router("read")
        router.create_routes_from_interface_object("s3", ReadInterface())
        # Provide args whose types don't match any signature (int where str expected)
        search_space = router.filter_routes("s3", 1)
        with pytest.raises(RuntimeError, match="No compatible method found"):
            router.get_route(search_space, {"path": 999}, {})

    def test_returns_first_compatible_route(self):
        router = Router("read")
        router.create_routes_from_interface_object("s3", ReadInterface())
        search_space = router.filter_routes("s3", 1)
        route = router.get_route(search_space, {"path": "s3://bucket/file.csv"}, {})
        assert route in search_space

    def test_returns_dict_with_method_key(self):
        router = Router("read")
        router.create_routes_from_interface_object("s3", ReadInterface())
        search_space = router.filter_routes("s3", 1)
        route = router.get_route(search_space, {"path": "s3://bucket/file.csv"}, {})
        assert "method" in route
        assert callable(route["method"])

    def test_skips_incompatible_routes_before_match(self):
        """A more-specific route that needs 3 args is skipped when only 1 is provided,
        and the less-specific route that needs 1 arg is returned."""
        router = Router("read")
        router.create_routes_from_interface_object("s3", OverloadedInterface())
        # Filter with enough args to include all routes
        search_space = router.filter_routes("s3", 3)
        assert len(search_space) >= 2, "Need at least 2 routes for this test"

        # read_detailed (3 required args) comes first due to sort order.
        # Supplying only path (str) should fail the detailed route and match read_simple.
        route = router.get_route(search_space, {"path": "s3://bucket"}, {})
        assert route["method_name"] == "read_simple"


# ---------------------------------------------------------------------------
# 6. _sort_routes
# ---------------------------------------------------------------------------


class TestSortRoutes:
    def test_routes_sorted_descending_non_var_args(self):
        router = Router("read")
        router.create_routes_from_interface_object("s3", OverloadedInterface())
        lengths = [r["length_non_var_args"] for r in router.routes]
        assert lengths == sorted(lengths, reverse=True)

    def test_sort_stable_across_multiple_stores(self):
        router = Router("read")
        router.create_routes_from_interface_object("s3", OverloadedInterface())
        router.create_routes_from_interface_object("gcs", OverloadedInterface())
        # Within each store, routes must remain ordered correctly
        for store in ("s3", "gcs"):
            store_routes = [r for r in router.routes if r["identifier"] == store]
            lengths = [r["length_non_var_args"] for r in store_routes]
            assert lengths == sorted(lengths, reverse=True), f"Sort broken for store '{store}'"

    def test_manual_unsorted_routes_get_sorted(self):
        router = Router("read")
        # Inject pre-built unsorted routes manually
        router.routes = [
            {"identifier": "s3", "length_non_var_args": 1, "length_all_args": 2},
            {"identifier": "s3", "length_non_var_args": 3, "length_all_args": 3},
            {"identifier": "s3", "length_non_var_args": 2, "length_all_args": 2},
        ]
        router._sort_routes()
        lengths = [r["length_non_var_args"] for r in router.routes]
        assert lengths == [3, 2, 1]

    def test_mro_depth_sorts_subclass_before_parent(self):
        """Within the same arg-count bucket, subclass (deeper MRO) sorts first."""

        class _Parent:
            pass

        class _Child(_Parent):
            pass

        class Iface:
            def read_parent(self, data_object: _Parent) -> None: ...
            def read_child(self, data_object: _Child) -> None: ...

        router = Router("read")
        router.create_routes_from_interface_object("s3", Iface())

        one_arg = [r for r in router.routes if r["length_non_var_args"] == 1]
        names = [r["method_name"] for r in one_arg]
        # read_child routes should appear before read_parent routes
        first_child = next(i for i, n in enumerate(names) if n == "read_child")
        last_parent = max(i for i, n in enumerate(names) if n == "read_parent")
        assert first_child < last_parent

    def test_mro_depth_three_levels(self):
        """A → B → C: routes should be sorted C, B, A within same bucket."""

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

        router = Router("read")
        router.create_routes_from_interface_object("s3", Iface())
        types = [r["data_object_type"] for r in router.routes]
        assert types.index(_C) < types.index(_B) < types.index(_A)

    def test_mro_sort_does_not_break_structural_sort(self):
        """MRO sort must not override the primary structural sort (non_var_args).
        A 3-arg route with shallow MRO must still come before a 1-arg route
        with deep MRO."""

        class _Parent:
            pass

        class _Child(_Parent):
            pass

        class _GrandChild(_Child):
            pass

        class Iface:
            def read_deep(self, data_object: _GrandChild) -> None: ...
            def read_many(self, data_object: _Parent, x: int, y: str) -> None: ...

        router = Router("read")
        router.create_routes_from_interface_object("s3", Iface())

        # read_many has 3 non_var_args → should sort before read_deep (1 non_var_arg)
        three_arg = [r for r in router.routes if r["length_non_var_args"] == 3]
        one_arg = [r for r in router.routes if r["length_non_var_args"] == 1]
        assert three_arg  # sanity
        assert one_arg
        # All 3-arg routes should come before all 1-arg routes
        last_three = max(router.routes.index(r) for r in three_arg)
        first_one = min(router.routes.index(r) for r in one_arg)
        assert last_three < first_one

    def test_mro_sort_with_optional_expansion(self):
        """Optional[Child] routes (MRO=3 for Child, MRO=2 for NoneType) should
        sort correctly among bare Parent routes (MRO=2)."""

        class _Parent:
            pass

        class _Child(_Parent):
            pass

        class Iface:
            def read_parent(self, data_object: _Parent) -> None: ...
            def read_child(self, data_object: Optional[_Child]) -> None: ...

        router = Router("read")
        router.create_routes_from_interface_object("s3", Iface())

        one_arg = [r for r in router.routes if r["length_non_var_args"] == 1]
        # The _Child route (MRO=3) must come before the _Parent route (MRO=2)
        child_idx = next(i for i, r in enumerate(one_arg) if r["data_object_type"] is _Child)
        parent_idx = next(i for i, r in enumerate(one_arg) if r["data_object_type"] is _Parent)
        assert child_idx < parent_idx

    def test_manual_routes_without_data_object_type_dont_crash(self):
        """Manually injected routes missing data_object_type should sort
        by the remaining keys without raising."""
        router = Router("read")
        router.routes = [
            {"identifier": "s3", "length_non_var_args": 1, "length_all_args": 1},
            {"identifier": "s3", "length_non_var_args": 2, "length_all_args": 2},
        ]
        router._sort_routes()  # Must not raise
        lengths = [r["length_non_var_args"] for r in router.routes]
        assert lengths == [2, 1]


# ---------------------------------------------------------------------------
# 8. Edge cases and error message quality
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_router_with_varargs_interface(self):
        """A method with **kwargs should still produce a valid route."""
        router = Router("read")
        router.create_routes_from_interface_object("s3", ReadInterface())
        kwargs_routes = [r for r in router.routes if r["method_name"] == "read_with_kwargs"]
        assert len(kwargs_routes) >= 1

    def test_error_message_contains_action_name(self):
        router = Router("custom_action")
        router.routes = [
            {
                "identifier": "s3",
                "length_non_var_args": 1,
                "length_all_args": 1,
                "signature": MagicMock(
                    **{
                        "has_var_args": False,
                        "len_all_args": 1,
                        "non_var_args": ["path"],
                        "parameters": {},
                    }
                ),
                "method": MagicMock(),
                "method_name": "custom_action_do",
                "interface_class": "X",
            }
        ]
        # arg_length=1 lets the route pass filter_routes; empty args then fail _check_structure
        search_space = router.filter_routes("s3", 1)
        with pytest.raises(RuntimeError, match="custom_action"):
            router.get_route(search_space, {}, {})

    def test_adding_same_interface_twice_doubles_routes(self):
        router = Router("read")
        interface = ReadInterface()
        router.create_routes_from_interface_object("s3", interface)
        count_once = len(router.routes)
        router.create_routes_from_interface_object("s3", interface)
        assert len(router.routes) == count_once * 2

    def test_routes_are_independent_between_router_instances(self):
        r1 = Router("read")
        r2 = Router("read")
        r1.create_routes_from_interface_object("s3", ReadInterface())
        assert r2.routes == [], "Routes from r1 must not pollute r2"

    def test_write_router_ignores_read_methods(self):
        router = Router("write")
        router.create_routes_from_interface_object("s3", ReadInterface())
        assert router.routes == []

    def test_filter_routes_does_not_mutate_route_table(self):
        router = Router("read")
        router.create_routes_from_interface_object("s3", ReadInterface())
        original_count = len(router.routes)
        router.filter_routes("s3", 10)
        assert len(router.routes) == original_count, "filter_routes must not mutate route table"
