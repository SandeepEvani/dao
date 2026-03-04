# tests/test_data_object_registry.py
"""Tests for dao.data_object.registry — the DataObject type registry."""

import pytest

from dao.data_object import DataObject, TableObject
from dao.data_object.registry import DataObjectRegistry, register_data_object, registry

# ── Fixtures ──────────────────────────────────────────────────────────────


class _CustomObject(DataObject):
    """A dummy subclass used only in tests."""

    pass


class _NotADataObject:
    """Not a DataObject subclass — should be rejected."""

    pass


# ── Built-in registration ────────────────────────────────────────────────


class TestBuiltins:
    def test_data_object_registered(self):
        assert registry.get("DataObject") is DataObject

    def test_table_object_registered(self):
        assert registry.get("TableObject") is TableObject

    def test_none_returns_data_object(self):
        assert registry.get(None) is DataObject

    def test_unknown_returns_data_object(self):
        assert registry.get("NoSuchType") is DataObject

    def test_contains(self):
        assert "DataObject" in registry
        assert "TableObject" in registry
        assert "FakeType" not in registry

    def test_registered_names_includes_builtins(self):
        names = registry.registered_names()
        assert "DataObject" in names
        assert "TableObject" in names


# ── Custom registration ──────────────────────────────────────────────────


class TestCustomRegistration:
    def test_register_and_get(self):
        r = DataObjectRegistry()
        r.register("Custom", _CustomObject)
        assert r.get("Custom") is _CustomObject

    def test_register_rejects_non_subclass(self):
        r = DataObjectRegistry()
        with pytest.raises(TypeError, match="not a subclass"):
            r.register("Bad", _NotADataObject)

    def test_unregister(self):
        r = DataObjectRegistry()
        r.register("Custom", _CustomObject)
        assert "Custom" in r
        r.unregister("Custom")
        assert "Custom" not in r

    def test_unregister_missing_is_noop(self):
        r = DataObjectRegistry()
        r.unregister("nope")  # should not raise

    def test_convenience_function_uses_global_registry(self):
        register_data_object("_TestCustom", _CustomObject)
        try:
            assert registry.get("_TestCustom") is _CustomObject
        finally:
            registry.unregister("_TestCustom")

    def test_repr(self):
        r = DataObjectRegistry()
        r.register("A", _CustomObject)
        assert "A" in repr(r)
