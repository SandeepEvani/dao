# tests/test_enrich_from_data_object.py
# Tests for DataAccessor._enrich_from_data_object — post-route enrichment.
#
# This exercises the static method directly with mock route dicts, so there
# is no need for a full DataAccessor / DataStore / interface stack.

import inspect

from dao.core.accessor import _enrich_from_data_object
from dao.core.signature import Signature
from dao.data_object import DataObject

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _FakeDataStore:
    name = "test_store"

    def __init__(self):
        pass


def _make_data_object(**properties) -> DataObject:
    return DataObject(name="test", data_store=_FakeDataStore(), **properties)


def _make_route(method) -> dict:
    """Build a route dict with 'method' and 'signature' matching what the router stores."""
    return {"method": method, "signature": Signature(inspect.signature(method))}


# ---------------------------------------------------------------------------
# Basic enrichment
# ---------------------------------------------------------------------------


class TestBasicEnrichment:
    def test_optional_param_filled_from_data_object(self):
        """schema has a default → should be enriched from DataObject."""

        def read_table(self, data_object, schema: str = None):
            pass

        obj = _make_data_object(schema="public")
        route = _make_route(read_table)
        result = _enrich_from_data_object({"data_object": obj}, route, obj)
        assert result["schema"] == "public"

    def test_multiple_optional_params_filled(self):
        def read_table(self, data_object, schema: str = None, limit: int = 100):
            pass

        obj = _make_data_object(schema="analytics", limit=50)
        route = _make_route(read_table)
        result = _enrich_from_data_object({"data_object": obj}, route, obj)
        assert result["schema"] == "analytics"
        assert result["limit"] == 50

    def test_no_enrichment_when_data_object_lacks_attr(self):
        """DataObject doesn't have the attribute → param stays absent."""

        def read_table(self, data_object, schema: str = None):
            pass

        obj = _make_data_object()  # no schema attr
        route = _make_route(read_table)
        result = _enrich_from_data_object({"data_object": obj}, route, obj)
        assert "schema" not in result


# ---------------------------------------------------------------------------
# Caller args always win
# ---------------------------------------------------------------------------


class TestCallerArgsWin:
    def test_caller_provided_value_not_overridden(self):
        """Caller passes schema="other" — DataObject's schema must be ignored."""

        def read_table(self, data_object, schema: str = None):
            pass

        obj = _make_data_object(schema="public")
        route = _make_route(read_table)
        result = _enrich_from_data_object({"data_object": obj, "schema": "other"}, route, obj)
        assert result["schema"] == "other"

    def test_caller_provided_none_not_overridden(self):
        """Caller explicitly passes schema=None — should NOT be replaced by DataObject."""

        def read_table(self, data_object, schema: str = "default"):
            pass

        obj = _make_data_object(schema="public")
        route = _make_route(read_table)
        result = _enrich_from_data_object({"data_object": obj, "schema": None}, route, obj)
        assert result["schema"] is None


# ---------------------------------------------------------------------------
# Required params never enriched
# ---------------------------------------------------------------------------


class TestRequiredParamsNotEnriched:
    def test_required_param_not_injected(self):
        """path is required (no default) — must come from caller, not DataObject."""

        def read_table(self, data_object, path: str):
            pass

        obj = _make_data_object(path="s3://bucket/data")
        route = _make_route(read_table)
        result = _enrich_from_data_object({"data_object": obj}, route, obj)
        assert "path" not in result

    def test_mix_required_and_optional(self):
        """path is required (skipped), schema is optional (enriched)."""

        def read_table(self, data_object, path: str, schema: str = None):
            pass

        obj = _make_data_object(path="s3://bucket", schema="public")
        route = _make_route(read_table)
        result = _enrich_from_data_object({"data_object": obj, "path": "s3://other"}, route, obj)
        assert result["path"] == "s3://other"  # caller's value
        assert result["schema"] == "public"  # enriched


# ---------------------------------------------------------------------------
# Framework / private attrs excluded
# ---------------------------------------------------------------------------


class TestFrameworkAttrsExcluded:
    def test_name_not_injected(self):
        def read_table(self, data_object, name: str = None):
            pass

        obj = _make_data_object()
        route = _make_route(read_table)
        result = _enrich_from_data_object({"data_object": obj}, route, obj)
        assert "name" not in result

    def test_data_store_not_injected(self):
        def read_table(self, data_object, data_store=None):
            pass

        obj = _make_data_object()
        route = _make_route(read_table)
        result = _enrich_from_data_object({"data_object": obj}, route, obj)
        assert "data_store" not in result

    def test_identifier_not_injected(self):
        def read_table(self, data_object, identifier: str = None):
            pass

        obj = _make_data_object()
        route = _make_route(read_table)
        result = _enrich_from_data_object({"data_object": obj}, route, obj)
        assert "identifier" not in result

    def test_private_attr_not_injected(self):
        def read_table(self, data_object, _secret: str = None):
            pass

        obj = _make_data_object(_secret="hunter2")
        route = _make_route(read_table)
        result = _enrich_from_data_object({"data_object": obj}, route, obj)
        assert "_secret" not in result

    def test_subclass_can_extend_exclusions(self):
        """A DataObject subclass can override enrichment_exclude to add exclusions."""

        class CustomObject(DataObject):
            @property
            def enrichment_exclude(self) -> frozenset:
                return super().enrichment_exclude | {"internal_id"}

        def read_table(self, data_object, internal_id: str = None, region: str = None):
            pass

        ds = _FakeDataStore()
        obj = CustomObject(name="test", data_store=ds, internal_id="abc", region="us-east-1")
        route = _make_route(read_table)
        result = _enrich_from_data_object({"data_object": obj}, route, obj)
        # internal_id is excluded by the subclass
        assert "internal_id" not in result
        # region is NOT excluded — should be enriched
        assert result["region"] == "us-east-1"

    def test_subclass_can_override_property_dynamically(self):
        """A DataObject subclass can use dynamic logic in enrichment_exclude."""

        class DynamicObject(DataObject):
            @property
            def enrichment_exclude(self) -> frozenset:
                return super().enrichment_exclude | {"dynamic_field"}

        def read_table(self, data_object, dynamic_field: str = None, region: str = None):
            pass

        ds = _FakeDataStore()
        obj = DynamicObject(name="test", data_store=ds, dynamic_field="x", region="us-east-1")
        route = _make_route(read_table)
        result = _enrich_from_data_object({"data_object": obj}, route, obj)
        assert "dynamic_field" not in result
        assert result["region"] == "us-east-1"

    def test_base_data_object_excludes_default_attrs(self):
        """Verify the base DataObject.enrichment_exclude has the expected defaults."""
        obj = _make_data_object()
        assert "name" in obj.enrichment_exclude
        assert "data_store" in obj.enrichment_exclude
        assert "identifier" in obj.enrichment_exclude


# ---------------------------------------------------------------------------
# VAR_POSITIONAL / VAR_KEYWORD skipped
# ---------------------------------------------------------------------------


class TestVarArgsSkipped:
    def test_kwargs_not_injected(self):
        """DataObject props should NOT be auto-injected into **kwargs."""

        def read_table(self, data_object, **kwargs):
            pass

        obj = _make_data_object(schema="public", location="s3://bucket")
        route = _make_route(read_table)
        result = _enrich_from_data_object({"data_object": obj}, route, obj)
        # Only data_object should be in result — nothing from DataObject via **kwargs
        assert result == {"data_object": obj}


# ---------------------------------------------------------------------------
# None DataObject
# ---------------------------------------------------------------------------


class TestNoneDataObject:
    def test_none_data_object_returns_method_args_unchanged(self):
        def read_table(self, data_object, schema: str = None):
            pass

        route = _make_route(read_table)
        result = _enrich_from_data_object({"data_object": None}, route, None)
        assert result == {"data_object": None}


# ---------------------------------------------------------------------------
# Enrichment with None attribute value
# ---------------------------------------------------------------------------


class TestNoneAttributeValue:
    def test_none_attribute_value_is_not_injected(self):
        """DataObject.tag = None means 'absent' — should NOT be injected."""

        def read_table(self, data_object, tag: str = "default"):
            pass

        obj = _make_data_object(tag=None)
        route = _make_route(read_table)
        result = _enrich_from_data_object({"data_object": obj}, route, obj)
        assert "tag" not in result

    def test_false_attribute_value_is_injected(self):
        """DataObject.compressed = False should be injected."""

        def read_table(self, data_object, compressed: bool = True):
            pass

        obj = _make_data_object(compressed=False)
        route = _make_route(read_table)
        result = _enrich_from_data_object({"data_object": obj}, route, obj)
        assert result["compressed"] is False
