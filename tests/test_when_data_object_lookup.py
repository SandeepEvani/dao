# tests/test_when_data_object_lookup.py
# Tests for the two-phase @when lookup:
#   Phase 1 — caller-supplied args (existing behavior, unchanged)
#   Phase 2 — DataObject attribute fallback (new)
#
# These tests exercise the Router directly, not via the full DataAccessor stack.

import pytest

from dao.core.router import Router
from dao.data_object import DataObject
from dao.decorators import when

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _router_for(interface) -> Router:
    router = Router("read")
    router.create_routes_from_interface_object("s3", interface)
    return router


def _route_for(interface, args: dict):
    return _router_for(interface).choose_route(args, "s3", {})


class _FakeDataStore:
    """Minimal stand-in so DataObject.__init__ can succeed."""

    name = "s3"

    def __init__(self):
        pass


def _make_data_object(**properties) -> DataObject:
    """Create a DataObject with arbitrary properties."""
    ds = _FakeDataStore()
    return DataObject(name="test", data_store=ds, **properties)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def reset_signature_factory_singleton():
    import dao.core.signature.signature_factory as sf_module

    sf_module.SignatureFactory._instance = None
    yield


# ---------------------------------------------------------------------------
# Phase 2 — DataObject attribute fallback
# ---------------------------------------------------------------------------


class TestWhenDataObjectFallback:
    """@when condition keys resolved from DataObject attributes when absent from caller args."""

    def test_condition_matched_from_data_object(self):
        """classification not in caller args → falls back to data_object.classification."""

        class Iface:
            @when({"classification": "parquet"})
            def read_parquet(self, data_object: DataObject, **kwargs) -> None: ...

        obj = _make_data_object(classification="parquet")
        route = _route_for(Iface(), {"data_object": obj})
        assert route["method_name"] == "read_parquet"

    def test_condition_not_matched_from_data_object(self):
        """data_object.classification doesn't match → route skipped."""

        class Iface:
            @when({"classification": "parquet"})
            def read_parquet(self, data_object: DataObject, **kwargs) -> None: ...

        obj = _make_data_object(classification="csv")
        router = _router_for(Iface())
        with pytest.raises(RuntimeError):
            router.choose_route({"data_object": obj}, "s3", {})

    def test_condition_key_missing_from_both_sources(self):
        """Key not in caller args AND not on DataObject → condition fails."""

        class Iface:
            @when({"classification": "parquet"})
            def read_parquet(self, data_object: DataObject, **kwargs) -> None: ...

        obj = _make_data_object()  # no classification attr
        router = _router_for(Iface())
        with pytest.raises(RuntimeError):
            router.choose_route({"data_object": obj}, "s3", {})

    def test_false_attribute_value_matches_false_condition(self):
        """data_object.compressed = False should match @when({"compressed": False})."""

        class Iface:
            @when({"compressed": False})
            def read_uncompressed(self, data_object: DataObject, **kwargs) -> None: ...

        obj = _make_data_object(compressed=False)
        route = _route_for(Iface(), {"data_object": obj})
        assert route["method_name"] == "read_uncompressed"


# ---------------------------------------------------------------------------
# Caller args always win (Phase 1 precedence)
# ---------------------------------------------------------------------------


class TestCallerArgsAlwaysWin:
    """When a @when key is present in both caller args AND DataObject, caller wins."""

    def test_caller_arg_matches_overrides_data_object(self):
        """Caller passes classification="parquet" explicitly — DataObject irrelevant."""

        class Iface:
            @when({"classification": "parquet"})
            def read_parquet(self, data_object: DataObject, classification: str, **kwargs) -> None: ...

        obj = _make_data_object(classification="csv")  # DataObject says csv
        route = _route_for(Iface(), {"data_object": obj, "classification": "parquet"})
        assert route["method_name"] == "read_parquet"

    def test_caller_arg_overrides_data_object_mismatch(self):
        """Caller passes classification="csv" — even though DataObject says parquet,
        caller wins and the parquet route is rejected."""

        class Iface:
            @when({"classification": "parquet"})
            def read_parquet(self, data_object: DataObject, classification: str, **kwargs) -> None: ...

            @when({"classification": "csv"})
            def read_csv(self, data_object: DataObject, classification: str, **kwargs) -> None: ...

        obj = _make_data_object(classification="parquet")
        route = _route_for(Iface(), {"data_object": obj, "classification": "csv"})
        assert route["method_name"] == "read_csv"


# ---------------------------------------------------------------------------
# Mixed conditions — some from caller, some from DataObject
# ---------------------------------------------------------------------------


class TestMixedConditions:
    """@when with multiple keys where some resolve from caller args and others
    from DataObject attributes."""

    def test_mixed_all_match(self):
        class Iface:
            @when({"mode": "snapshot", "classification": "delta"})
            def read_delta_snapshot(self, data_object: DataObject, mode: str, **kwargs) -> None: ...

        obj = _make_data_object(classification="delta")
        route = _route_for(Iface(), {"data_object": obj, "mode": "snapshot"})
        assert route["method_name"] == "read_delta_snapshot"

    def test_mixed_caller_matches_object_fails(self):
        class Iface:
            @when({"mode": "snapshot", "classification": "delta"})
            def read_delta_snapshot(self, data_object: DataObject, mode: str, **kwargs) -> None: ...

        obj = _make_data_object(classification="parquet")  # wrong
        router = _router_for(Iface())
        with pytest.raises(RuntimeError):
            router.choose_route({"data_object": obj, "mode": "snapshot"}, "s3", {})

    def test_mixed_object_matches_caller_fails(self):
        class Iface:
            @when({"mode": "snapshot", "classification": "delta"})
            def read_delta_snapshot(self, data_object: DataObject, mode: str, **kwargs) -> None: ...

        obj = _make_data_object(classification="delta")
        router = _router_for(Iface())
        with pytest.raises(RuntimeError):
            router.choose_route({"data_object": obj, "mode": "cdf"}, "s3", {})

    def test_mixed_three_conditions(self):
        class Iface:
            @when({"mode": "batch", "classification": "parquet", "compressed": True})
            def read_batch_parquet_gz(self, data_object: DataObject, mode: str, **kwargs) -> None: ...

        obj = _make_data_object(classification="parquet", compressed=True)
        route = _route_for(Iface(), {"data_object": obj, "mode": "batch"})
        assert route["method_name"] == "read_batch_parquet_gz"


# ---------------------------------------------------------------------------
# Disambiguation via DataObject properties
# ---------------------------------------------------------------------------


class TestDisambiguationViaDataObject:
    """Multiple overloads where the DataObject property is the differentiator."""

    def test_two_overloads_differentiated_by_object_prop(self):
        class Iface:
            @when({"classification": "parquet"})
            def read_parquet(self, data_object: DataObject, **kwargs) -> None: ...

            @when({"classification": "csv"})
            def read_csv(self, data_object: DataObject, **kwargs) -> None: ...

        parquet_obj = _make_data_object(classification="parquet")
        csv_obj = _make_data_object(classification="csv")

        assert _route_for(Iface(), {"data_object": parquet_obj})["method_name"] == "read_parquet"
        assert _route_for(Iface(), {"data_object": csv_obj})["method_name"] == "read_csv"

    def test_fallback_method_without_when(self):
        """If no @when matches, a plain method (no @when) should still be found."""

        class Iface:
            @when({"classification": "parquet"})
            def read_parquet(self, data_object: DataObject, **kwargs) -> None: ...

            def read_fallback(self, data_object: DataObject, **kwargs) -> None: ...

        obj = _make_data_object(classification="json")
        route = _route_for(Iface(), {"data_object": obj})
        assert route["method_name"] == "read_fallback"


# ---------------------------------------------------------------------------
# No DataObject in args (edge case)
# ---------------------------------------------------------------------------


class TestNoDataObjectInArgs:
    """When data_object is not in args and condition isn't in caller args either."""

    def test_condition_fails_when_no_data_object_and_no_caller_arg(self):
        class Iface:
            @when({"classification": "parquet"})
            def read_parquet(self, path: str, **kwargs) -> None: ...

        router = _router_for(Iface())
        with pytest.raises(RuntimeError):
            router.choose_route({"path": "s3://bucket"}, "s3", {})


# ---------------------------------------------------------------------------
# Hint stripping with DataObject-resolved conditions
# ---------------------------------------------------------------------------


class TestHintStrippingWithDataObjectConditions:
    """@when keys resolved from DataObject were never in method_args, so the
    existing hint-stripping logic naturally leaves them alone."""

    def test_object_resolved_key_not_in_call_args(self):
        """classification resolved from DataObject — should not appear in call_args
        regardless of stripping, because it was never in method_args."""
        received = {}

        class Iface:
            @when({"classification": "parquet"})
            def read_parquet(self, data_object: DataObject, **kwargs) -> None:
                received["data_object"] = data_object

        obj = _make_data_object(classification="parquet")
        route = _route_for(Iface(), {"data_object": obj})

        # Simulate accessor hint stripping
        method_args = {"data_object": obj}
        method_params = set(__import__("inspect").signature(route["method"]).parameters.keys())
        routing_hints = set(route.get("when", {}).keys()) - method_params
        call_args = {k: v for k, v in method_args.items() if k not in routing_hints}

        route["method"](**call_args)
        assert "classification" not in call_args
        assert received["data_object"] is obj

    def test_caller_provided_hint_still_stripped(self):
        """classification passed by caller as a routing hint (not declared on method)
        — should still be stripped as before."""
        received = {}

        class Iface:
            @when({"classification": "parquet"})
            def read_parquet(self, data_object: DataObject, **kwargs) -> None:
                received["data_object"] = data_object

        obj = _make_data_object()
        method_args = {"data_object": obj, "classification": "parquet"}
        route = _route_for(Iface(), method_args)

        method_params = set(__import__("inspect").signature(route["method"]).parameters.keys())
        routing_hints = set(route.get("when", {}).keys()) - method_params
        call_args = {k: v for k, v in method_args.items() if k not in routing_hints}

        route["method"](**call_args)
        assert "classification" not in call_args
