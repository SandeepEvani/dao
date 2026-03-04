# tests/test_catalog_type_resolution.py
"""Tests for BaseCatalog type resolution via the DataObject registry.

Uses FileCatalog as the concrete implementation since it requires no AWS
credentials and exercises the shared BaseCatalog.get() code path.
"""

import json
from pathlib import Path

import pytest

from dao.catalog.file import FileCatalog
from dao.data_object import DataObject, TableObject, register_data_object, registry

# ── Custom subclass for tests ─────────────────────────────────────────────


class FileObject(DataObject):
    """A custom DataObject subclass used in tests."""

    def __repr__(self):
        return f"FileObject({self.name})"


# ── Fixture helpers ───────────────────────────────────────────────────────


def _write_json(path: Path, data: dict):
    path.write_text(json.dumps(data))


@pytest.fixture()
def catalog_dir(tmp_path):
    """Create a temporary directory with store + object config files."""
    ds_path = tmp_path / "stores.json"
    do_path = tmp_path / "objects.json"

    _write_json(
        ds_path,
        {
            "bronze": {
                "class": "MockInterface",
                "module": "tests.test_catalog_type_resolution",
                "properties": {"layer": "raw"},
            },
        },
    )

    _write_json(
        do_path,
        {
            "bronze": {
                "orders": {
                    "key": "orders/2025.csv",
                    "type": "TableObject",
                    "schema": "public",
                    "columns": "order_id,amount",
                },
                "events": {
                    "key": "events/stream.json",
                    # no "type" — should default to DataObject
                },
                "logs": {
                    "key": "logs/app.log",
                    "type": "FileObject",
                },
                "unknown_type": {
                    "key": "misc/data.bin",
                    "type": "NoSuchType",
                },
            },
        },
    )

    return ds_path, do_path


# Minimal mock interface so DataStoreFactory can initialise
class MockInterface:
    def __init__(self, **kwargs):
        pass


# ── Tests ─────────────────────────────────────────────────────────────────


class TestTypeResolution:
    def test_table_object_resolved(self, catalog_dir):
        ds_path, do_path = catalog_dir
        catalog = FileCatalog(str(ds_path), str(do_path))
        obj = catalog.get("bronze.orders")
        assert isinstance(obj, TableObject)
        assert obj.name == "orders"
        # TableObject-specific attrs set via kwargs
        assert obj.schema == "public"
        assert obj.columns == "order_id,amount"

    def test_default_data_object_when_no_type(self, catalog_dir):
        ds_path, do_path = catalog_dir
        catalog = FileCatalog(str(ds_path), str(do_path))
        obj = catalog.get("bronze.events")
        assert type(obj) is DataObject
        assert obj.key == "events/stream.json"

    def test_custom_registered_type(self, catalog_dir):
        register_data_object("FileObject", FileObject)
        try:
            ds_path, do_path = catalog_dir
            catalog = FileCatalog(str(ds_path), str(do_path))
            obj = catalog.get("bronze.logs")
            assert isinstance(obj, FileObject)
            assert obj.key == "logs/app.log"
        finally:
            registry.unregister("FileObject")

    def test_unknown_type_falls_back_to_data_object(self, catalog_dir):
        ds_path, do_path = catalog_dir
        catalog = FileCatalog(str(ds_path), str(do_path))
        obj = catalog.get("bronze.unknown_type")
        assert type(obj) is DataObject

    def test_type_key_not_on_object(self, catalog_dir):
        """The ``type`` key should be consumed during resolution, not leaked onto the object."""
        ds_path, do_path = catalog_dir
        catalog = FileCatalog(str(ds_path), str(do_path))
        obj = catalog.get("bronze.orders")
        assert not hasattr(obj, "type")
