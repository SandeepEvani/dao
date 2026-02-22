# test_when_decorator.py
# Tests for the @when decorator and its interaction with the Router.
#
# Coverage:
#   1. Decorator behaviour  – the dunder attribute is set correctly
#   2. Single condition     – route is selected / rejected on exact value match
#   3. Multiple conditions  – all conditions must be satisfied
#   4. @when + type check   – both gates must pass independently
#   5. Disambiguation       – @when picks the right overload when types are identical
#   6. No @when             – methods without the decorator are never blocked
#   7. @when + @register    – decorators compose correctly

import pytest

from dao.core.router import Router
from dao.decorators import register, when

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _router_for(interface) -> Router:
    router = Router("read")
    router.create_routes_from_interface_object("s3", interface)
    return router


def _route_for(interface, args: dict):
    return _router_for(interface).choose_route(args, "s3", {})


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def reset_signature_factory_singleton():
    import dao.core.signature.signature_factory as sf_module

    sf_module.SignatureFactory._instance = None
    yield


# ---------------------------------------------------------------------------
# 1. Decorator behaviour
# ---------------------------------------------------------------------------


class TestWhenDecoratorAttribute:
    def test_sets_dunder_attribute(self):
        @when({"format": "parquet"})
        def read_parquet(self, path: str) -> None: ...

        assert hasattr(read_parquet, "__dao_when__")
        assert read_parquet.__dao_when__ == {"format": "parquet"}

    def test_attribute_is_exact_dict(self):
        conditions = {"format": "csv", "compressed": True}

        @when(conditions)
        def read_csv(self, path: str) -> None: ...

        assert read_csv.__dao_when__ == conditions

    def test_empty_conditions_sets_empty_dict(self):
        @when({})
        def read_any(self, path: str) -> None: ...

        assert read_any.__dao_when__ == {}

    def test_decorator_preserves_method_name(self):
        @when({"format": "parquet"})
        def read_parquet(self, path: str) -> None: ...

        assert read_parquet.__name__ == "read_parquet"

    def test_no_when_decorator_has_no_attribute(self):
        def read_plain(self, path: str) -> None: ...

        assert not hasattr(read_plain, "__dao_when__")


# ---------------------------------------------------------------------------
# 2. Single condition
# ---------------------------------------------------------------------------


class TestWhenSingleCondition:
    def test_matching_value_selects_route(self):
        class Iface:
            @when({"format": "parquet"})
            def read_parquet(self, path: str, format: str) -> None: ...

        route = _route_for(Iface(), {"path": "s3://bucket/file", "format": "parquet"})
        assert route["method_name"] == "read_parquet"

    def test_non_matching_value_raises(self):
        class Iface:
            @when({"format": "parquet"})
            def read_parquet(self, path: str, format: str) -> None: ...

        router = _router_for(Iface())
        with pytest.raises(RuntimeError):
            router.choose_route({"path": "s3://bucket/file", "format": "csv"}, "s3", {})

    def test_missing_condition_key_raises(self):
        """If the arg the condition references is absent from the call, it should not match."""

        class Iface:
            @when({"format": "parquet"})
            def read_parquet(self, path: str) -> None: ...

        router = _router_for(Iface())
        with pytest.raises(RuntimeError):
            router.choose_route({"path": "s3://bucket/file"}, "s3", {})

    def test_integer_condition_matches(self):
        class Iface:
            @when({"version": 2})
            def read_v2(self, path: str, version: int) -> None: ...

        route = _route_for(Iface(), {"path": "s3://bucket", "version": 2})
        assert route["method_name"] == "read_v2"

    def test_integer_condition_wrong_value_raises(self):
        class Iface:
            @when({"version": 2})
            def read_v2(self, path: str, version: int) -> None: ...

        router = _router_for(Iface())
        with pytest.raises(RuntimeError):
            router.choose_route({"path": "s3://bucket", "version": 3}, "s3", {})

    def test_boolean_condition_matches(self):
        class Iface:
            @when({"compressed": True})
            def read_compressed(self, path: str, compressed: bool) -> None: ...

        route = _route_for(Iface(), {"path": "s3://bucket", "compressed": True})
        assert route["method_name"] == "read_compressed"

    def test_boolean_condition_false_does_not_match_true(self):
        class Iface:
            @when({"compressed": True})
            def read_compressed(self, path: str, compressed: bool) -> None: ...

        router = _router_for(Iface())
        with pytest.raises(RuntimeError):
            router.choose_route({"path": "s3://bucket", "compressed": False}, "s3", {})


# ---------------------------------------------------------------------------
# 3. Multiple conditions – all must be satisfied
# ---------------------------------------------------------------------------


class TestWhenMultipleConditions:
    # ------------------------------------------------------------------
    # 3.1  Single method with multiple conditions
    # ------------------------------------------------------------------

    def test_all_conditions_satisfied_selects_route(self):
        class Iface:
            @when({"format": "parquet", "compressed": True})
            def read_compressed_parquet(self, path: str, format: str, compressed: bool) -> None: ...

        route = _route_for(Iface(), {"path": "s3://bucket", "format": "parquet", "compressed": True})
        assert route["method_name"] == "read_compressed_parquet"

    def test_one_condition_fails_raises(self):
        class Iface:
            @when({"format": "parquet", "compressed": True})
            def read_compressed_parquet(self, path: str, format: str, compressed: bool) -> None: ...

        router = _router_for(Iface())
        with pytest.raises(RuntimeError):
            router.choose_route({"path": "s3://bucket", "format": "parquet", "compressed": False}, "s3", {})

    def test_all_conditions_fail_raises(self):
        class Iface:
            @when({"format": "parquet", "compressed": True})
            def read_compressed_parquet(self, path: str, format: str, compressed: bool) -> None: ...

        router = _router_for(Iface())
        with pytest.raises(RuntimeError):
            router.choose_route({"path": "s3://bucket", "format": "csv", "compressed": False}, "s3", {})

    def test_three_conditions_all_match(self):
        class Iface:
            @when({"format": "parquet", "engine": "spark", "compressed": True})
            def read_spark_parquet(self, path: str, format: str, engine: str, compressed: bool) -> None: ...

        route = _route_for(
            Iface(),
            {"path": "s3://bucket", "format": "parquet", "engine": "spark", "compressed": True},
        )
        assert route["method_name"] == "read_spark_parquet"

    def test_three_conditions_one_fails_raises(self):
        class Iface:
            @when({"format": "parquet", "engine": "spark", "compressed": True})
            def read_spark_parquet(self, path: str, format: str, engine: str, compressed: bool) -> None: ...

        router = _router_for(Iface())
        with pytest.raises(RuntimeError):
            # engine is "pandas" instead of "spark"
            router.choose_route(
                {"path": "s3://bucket", "format": "parquet", "engine": "pandas", "compressed": True},
                "s3",
                {},
            )

    # ------------------------------------------------------------------
    # 3.2  Multiple overloads disambiguated by multiple conditions
    # ------------------------------------------------------------------

    def test_two_overloads_differ_on_two_conditions(self):
        """format=parquet+engine=spark vs format=csv+engine=pandas – fully independent pairs."""

        class Iface:
            @when({"format": "parquet", "engine": "spark"})
            def read_spark_parquet(self, path: str, format: str, engine: str) -> None: ...

            @when({"format": "csv", "engine": "pandas"})
            def read_pandas_csv(self, path: str, format: str, engine: str) -> None: ...

        router = Router("read")
        router.create_routes_from_interface_object("s3", Iface())

        route = router.choose_route({"path": "s3://bucket", "format": "parquet", "engine": "spark"}, "s3", {})
        assert route["method_name"] == "read_spark_parquet"

        route = router.choose_route({"path": "s3://bucket", "format": "csv", "engine": "pandas"}, "s3", {})
        assert route["method_name"] == "read_pandas_csv"

    def test_shared_condition_disambiguated_by_second(self):
        """Both methods share format=parquet; engine alone separates them."""

        class Iface:
            @when({"format": "parquet", "engine": "spark"})
            def read_spark(self, path: str, format: str, engine: str) -> None: ...

            @when({"format": "parquet", "engine": "pandas"})
            def read_pandas(self, path: str, format: str, engine: str) -> None: ...

        router = Router("read")
        router.create_routes_from_interface_object("s3", Iface())

        route = router.choose_route({"path": "s3://bucket", "format": "parquet", "engine": "spark"}, "s3", {})
        assert route["method_name"] == "read_spark"

        route = router.choose_route({"path": "s3://bucket", "format": "parquet", "engine": "pandas"}, "s3", {})
        assert route["method_name"] == "read_pandas"

    def test_no_overload_matches_all_conditions_raises(self):
        """Supplying a combination that matches neither overload should raise."""

        class Iface:
            @when({"format": "parquet", "engine": "spark"})
            def read_spark_parquet(self, path: str, format: str, engine: str) -> None: ...

            @when({"format": "csv", "engine": "pandas"})
            def read_pandas_csv(self, path: str, format: str, engine: str) -> None: ...

        router = Router("read")
        router.create_routes_from_interface_object("s3", Iface())

        with pytest.raises(RuntimeError):
            # format matches first overload but engine does not, and vice-versa
            router.choose_route({"path": "s3://bucket", "format": "parquet", "engine": "pandas"}, "s3", {})

    def test_four_way_disambiguation_two_conditions(self):
        """Four overloads, each uniquely identified by (format, engine)."""

        class Iface:
            @when({"format": "parquet", "engine": "spark"})
            def read_spark_parquet(self, path: str, format: str, engine: str) -> None: ...

            @when({"format": "parquet", "engine": "pandas"})
            def read_pandas_parquet(self, path: str, format: str, engine: str) -> None: ...

            @when({"format": "csv", "engine": "spark"})
            def read_spark_csv(self, path: str, format: str, engine: str) -> None: ...

            @when({"format": "csv", "engine": "pandas"})
            def read_pandas_csv(self, path: str, format: str, engine: str) -> None: ...

        router = Router("read")
        router.create_routes_from_interface_object("s3", Iface())

        cases = [
            ({"format": "parquet", "engine": "spark"}, "read_spark_parquet"),
            ({"format": "parquet", "engine": "pandas"}, "read_pandas_parquet"),
            ({"format": "csv", "engine": "spark"}, "read_spark_csv"),
            ({"format": "csv", "engine": "pandas"}, "read_pandas_csv"),
        ]
        for extra, expected in cases:
            route = router.choose_route({"path": "s3://bucket", **extra}, "s3", {})
            assert route["method_name"] == expected


# ---------------------------------------------------------------------------
# 4. @when + type checking – both gates must pass independently
# ---------------------------------------------------------------------------


class TestWhenWithTypeCheck:
    def test_correct_type_and_condition_matches(self):
        """Both the type check (str) and the @when condition must pass."""

        class Iface:
            @when({"format": "parquet"})
            def read_parquet(self, path: str, format: str) -> None: ...

        route = _route_for(Iface(), {"path": "s3://bucket", "format": "parquet"})
        assert route["method_name"] == "read_parquet"

    def test_wrong_type_even_with_matching_condition_raises(self):
        """Type mismatch should prevent the route even if @when value matches."""

        class Iface:
            @when({"version": 1})
            def read_v1(self, path: str, version: int) -> None: ...

        router = _router_for(Iface())
        with pytest.raises(RuntimeError):
            # version is a str, not int — type check fails before @when even applies
            router.choose_route({"path": "s3://bucket", "version": "1"}, "s3", {})

    def test_correct_type_wrong_condition_raises(self):
        """Type check passes but @when condition is not met."""

        class Iface:
            @when({"format": "parquet"})
            def read_parquet(self, path: str, format: str) -> None: ...

        router = _router_for(Iface())
        with pytest.raises(RuntimeError):
            router.choose_route({"path": "s3://bucket", "format": "csv"}, "s3", {})


# ---------------------------------------------------------------------------
# 5. Disambiguation – @when picks the right overload when types are identical
# ---------------------------------------------------------------------------


class TestWhenDisambiguatesOverloads:
    def test_routes_to_parquet_method(self):
        class Iface:
            @when({"format": "parquet"})
            def read_parquet(self, path: str, format: str) -> None: ...

            @when({"format": "csv"})
            def read_csv(self, path: str, format: str) -> None: ...

        router = Router("read")
        router.create_routes_from_interface_object("s3", Iface())

        route = router.choose_route({"path": "s3://bucket", "format": "parquet"}, "s3", {})
        assert route["method_name"] == "read_parquet"

    def test_routes_to_csv_method(self):
        class Iface:
            @when({"format": "parquet"})
            def read_parquet(self, path: str, format: str) -> None: ...

            @when({"format": "csv"})
            def read_csv(self, path: str, format: str) -> None: ...

        router = Router("read")
        router.create_routes_from_interface_object("s3", Iface())

        route = router.choose_route({"path": "s3://bucket", "format": "csv"}, "s3", {})
        assert route["method_name"] == "read_csv"

    def test_unknown_format_raises(self):
        class Iface:
            @when({"format": "parquet"})
            def read_parquet(self, path: str, format: str) -> None: ...

            @when({"format": "csv"})
            def read_csv(self, path: str, format: str) -> None: ...

        router = Router("read")
        router.create_routes_from_interface_object("s3", Iface())

        with pytest.raises(RuntimeError):
            router.choose_route({"path": "s3://bucket", "format": "avro"}, "s3", {})

    def test_three_way_disambiguation(self):
        class Iface:
            @when({"format": "parquet"})
            def read_parquet(self, path: str, format: str) -> None: ...

            @when({"format": "csv"})
            def read_csv(self, path: str, format: str) -> None: ...

            @when({"format": "json"})
            def read_json(self, path: str, format: str) -> None: ...

        router = Router("read")
        router.create_routes_from_interface_object("s3", Iface())

        for fmt, expected in [("parquet", "read_parquet"), ("csv", "read_csv"), ("json", "read_json")]:
            route = router.choose_route({"path": "s3://bucket", "format": fmt}, "s3", {})
            assert route["method_name"] == expected


# ---------------------------------------------------------------------------
# 6. No @when – methods without the decorator are never blocked
# ---------------------------------------------------------------------------


class TestNoWhenDecorator:
    def test_method_without_when_always_matches(self):
        """A method with no @when should behave exactly as before."""

        class Iface:
            def read_plain(self, path: str) -> None: ...

        route = _route_for(Iface(), {"path": "s3://bucket"})
        assert route["method_name"] == "read_plain"

    def test_when_method_and_plain_method_coexist(self):
        """@when method should not block the plain fallback method."""

        class Iface:
            @when({"format": "parquet"})
            def read_parquet(self, path: str, format: str) -> None: ...

            def read_plain(self, path: str) -> None: ...

        route = _route_for(Iface(), {"path": "s3://bucket"})
        assert route["method_name"] == "read_plain"


# ---------------------------------------------------------------------------
# 7. @when + @register – decorators compose correctly
# ---------------------------------------------------------------------------


class TestWhenWithRegister:
    def test_register_and_when_both_applied(self):
        class Iface:
            @register("read")
            @when({"format": "parquet"})
            def fetch_parquet(self, path: str, format: str) -> None: ...

        route = _route_for(Iface(), {"path": "s3://bucket", "format": "parquet"})
        assert route["method_name"] == "fetch_parquet"

    def test_register_and_when_wrong_condition_raises(self):
        class Iface:
            @when({"format": "parquet"})
            @register("read")
            def fetch_parquet(self, path: str, format: str) -> None: ...

        router = _router_for(Iface())
        with pytest.raises(RuntimeError):
            router.choose_route({"path": "s3://bucket", "format": "csv"}, "s3", {})


# ---------------------------------------------------------------------------
# 8. Routing-hint stripping – @when keys removed before method call
#    when absent from the method signature, passed through when present
# ---------------------------------------------------------------------------


class TestWhenRoutingHintStripping:
    def test_hint_absent_from_signature_is_stripped(self):
        """format is a routing hint only — not declared on the method.
        The method must be called without it, otherwise it would blow up
        with an unexpected keyword argument."""
        received = {}

        class Iface:
            @when({"format": "parquet"})
            def read_parquet(self, path: str) -> None:
                received.update({"path": path})

        route = _route_for(Iface(), {"path": "s3://bucket", "format": "parquet"})
        # Call the method directly the same way the accessor does
        method_params = set(__import__("inspect").signature(route["method"]).parameters.keys())
        routing_hints = set(route.get("when", {}).keys()) - method_params
        call_args = {k: v for k, v in {"path": "s3://bucket", "format": "parquet"}.items() if k not in routing_hints}
        # format must have been stripped — calling with it would raise TypeError
        route["method"](**call_args)
        assert "format" not in call_args
        assert received == {"path": "s3://bucket"}

    def test_hint_present_in_signature_is_passed_through(self):
        """format is declared on the method — it must NOT be stripped even
        though it also appears in the @when conditions."""
        received = {}

        class Iface:
            @when({"format": "parquet"})
            def read_parquet(self, path: str, format: str) -> None:
                received.update({"path": path, "format": format})

        route = _route_for(Iface(), {"path": "s3://bucket", "format": "parquet"})
        method_params = set(__import__("inspect").signature(route["method"]).parameters.keys())
        routing_hints = set(route.get("when", {}).keys()) - method_params
        call_args = {k: v for k, v in {"path": "s3://bucket", "format": "parquet"}.items() if k not in routing_hints}
        # format is declared on the method, so it stays in call_args
        route["method"](**call_args)
        assert received == {"path": "s3://bucket", "format": "parquet"}

    def test_multiple_hints_absent_all_stripped(self):
        """Multiple @when conditions that are all absent from the signature
        must all be stripped."""
        received = {}

        class Iface:
            @when({"format": "parquet", "engine": "spark"})
            def read_parquet(self, path: str) -> None:
                received.update({"path": path})

        route = _route_for(Iface(), {"path": "s3://bucket", "format": "parquet", "engine": "spark"})
        method_params = set(__import__("inspect").signature(route["method"]).parameters.keys())
        routing_hints = set(route.get("when", {}).keys()) - method_params
        call_args = {
            k: v
            for k, v in {"path": "s3://bucket", "format": "parquet", "engine": "spark"}.items()
            if k not in routing_hints
        }
        route["method"](**call_args)
        assert "format" not in call_args
        assert "engine" not in call_args
        assert received == {"path": "s3://bucket"}

    def test_mixed_hints_one_absent_one_declared(self):
        """engine is a routing hint (absent from signature), format is declared.
        Only engine must be stripped; format must be passed through."""
        received = {}

        class Iface:
            @when({"format": "parquet", "engine": "spark"})
            def read_parquet(self, path: str, format: str) -> None:
                received.update({"path": path, "format": format})

        route = _route_for(Iface(), {"path": "s3://bucket", "format": "parquet", "engine": "spark"})
        method_params = set(__import__("inspect").signature(route["method"]).parameters.keys())
        routing_hints = set(route.get("when", {}).keys()) - method_params
        call_args = {
            k: v
            for k, v in {"path": "s3://bucket", "format": "parquet", "engine": "spark"}.items()
            if k not in routing_hints
        }
        route["method"](**call_args)
        assert "engine" not in call_args
        assert "format" in call_args
        assert received == {"path": "s3://bucket", "format": "parquet"}
