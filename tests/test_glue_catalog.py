# tests/test_glue_catalog.py
"""Integration tests for GlueCatalog — mocks boto3, no AWS credentials needed."""

import json
from unittest.mock import MagicMock, patch

import pytest

from dao.catalog.glue import GlueCatalog
from dao.catalog.glue.extractors import ObjectTypeExtractor, lambda_extractor
from dao.catalog.glue.translator import GlueTableTranslator
from dao.data_object import DataObject, TableObject

# ── Shared fixtures ───────────────────────────────────────────────────────

SAMPLE_TABLE_RESPONSE = {
    "Table": {
        "Name": "orders",
        "DatabaseName": "bronze",
        "TableType": "EXTERNAL_TABLE",
        "StorageDescriptor": {
            "Columns": [
                {"Name": "order_id", "Type": "bigint"},
                {"Name": "amount", "Type": "double"},
            ],
            "Location": "s3://bronze-bucket/orders/",
            "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                "Parameters": {"field.delim": ","},
            },
            "Compressed": False,
        },
        "PartitionKeys": [{"Name": "year", "Type": "string"}],
        "Parameters": {
            "classification": "csv",
            "owner": "data-eng",
        },
    }
}

DATA_STORE_CONFIG = {
    "class": "MockInterface",
    "module": "tests.test_glue_catalog",
    "properties": {"layer": "raw"},
}

DATABASE_LIST_RESPONSE = {
    "DatabaseList": [
        {
            "Name": "bronze",
            "Parameters": {
                "data_store_config": json.dumps(DATA_STORE_CONFIG, sort_keys=True),
            },
        },
        {
            "Name": "no_config_db",
            "Parameters": {},
        },
    ]
}


class MockInterface:
    """Minimal interface so DataStoreFactory can initialize."""

    def __init__(self, **kwargs):
        pass


def _make_catalog(translator=None):
    """Create a GlueCatalog with a mocked boto3 client."""
    with patch("dao.catalog.glue.catalog.boto3") as mock_boto3:
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client
        mock_client.get_databases.return_value = DATABASE_LIST_RESPONSE
        mock_client.get_table.return_value = SAMPLE_TABLE_RESPONSE

        catalog = GlueCatalog(
            aws_access_key_id="fake",
            aws_secret_access_key="fake",
            aws_region="us-east-1",
            translator=translator,
        )
        # Store the mock so tests can inspect calls
        catalog._mock_client = mock_client
        return catalog


# ── Tests: default translator ────────────────────────────────────────────


class TestGlueCatalogDefaults:
    def test_get_extracts_default_fields(self):
        catalog = _make_catalog()
        obj = catalog.get("bronze.orders")
        assert obj.name == "orders"
        assert obj.location == "s3://bronze-bucket/orders/"
        assert obj.classification == "csv"
        assert len(obj.columns) == 2
        assert obj.partition_keys == [{"Name": "year", "Type": "string"}]

    def test_default_returns_data_object(self):
        """Without ObjectTypeExtractor, type is DataObject."""
        catalog = _make_catalog()
        obj = catalog.get("bronze.orders")
        assert type(obj) is DataObject


# ── Tests: translator with ObjectTypeExtractor ───────────────────────────


class TestGlueCatalogWithTypeExtractor:
    def test_external_table_becomes_table_object(self):
        translator = GlueTableTranslator().include(ObjectTypeExtractor())
        catalog = _make_catalog(translator=translator)
        obj = catalog.get("bronze.orders")
        assert isinstance(obj, TableObject)

    def test_full_preset_resolves_type(self):
        catalog = _make_catalog(translator=GlueTableTranslator.full())
        obj = catalog.get("bronze.orders")
        assert isinstance(obj, TableObject)
        assert obj.table_type == "EXTERNAL_TABLE"
        assert obj.compressed is False


# ── Tests: user overlay (data_object_config) ─────────────────────────────


class TestGlueCatalogUserOverlay:
    def test_user_config_overrides_translator(self):
        """data_object_config in Parameters takes precedence."""
        overlay_response = {
            "Table": {
                **SAMPLE_TABLE_RESPONSE["Table"],
                "Parameters": {
                    **SAMPLE_TABLE_RESPONSE["Table"]["Parameters"],
                    "data_object_config": json.dumps({"location": "s3://override/"}),
                },
            }
        }
        catalog = _make_catalog()
        catalog._glue.get_table.return_value = overlay_response
        obj = catalog.get("bronze.orders")
        assert obj.location == "s3://override/"

    def test_user_config_adds_extra_fields(self):
        overlay_response = {
            "Table": {
                **SAMPLE_TABLE_RESPONSE["Table"],
                "Parameters": {
                    **SAMPLE_TABLE_RESPONSE["Table"]["Parameters"],
                    "data_object_config": json.dumps({"custom_tag": "important"}),
                },
            }
        }
        catalog = _make_catalog()
        catalog._glue.get_table.return_value = overlay_response
        obj = catalog.get("bronze.orders")
        assert obj.custom_tag == "important"
        # Original translated fields still present
        assert obj.classification == "csv"


# ── Tests: custom translator ─────────────────────────────────────────────


class TestGlueCatalogCustomTranslator:
    def test_minimal_preset(self):
        catalog = _make_catalog(translator=GlueTableTranslator.minimal())
        obj = catalog.get("bronze.orders")
        assert obj.location == "s3://bronze-bucket/orders/"
        assert obj.classification == "csv"
        assert not hasattr(obj, "columns")

    def test_lambda_extractor(self):
        translator = GlueTableTranslator().include(
            lambda_extractor("owner", lambda t: t.get("Parameters", {}).get("owner"))
        )
        catalog = _make_catalog(translator=translator)
        obj = catalog.get("bronze.orders")
        assert obj.owner == "data-eng"


# ── Tests: caching ───────────────────────────────────────────────────────


class TestGlueCatalogCaching:
    def test_second_get_uses_cache(self):
        catalog = _make_catalog()
        catalog.get("bronze.orders")
        catalog.get("bronze.orders")
        # get_table should only be called once — second hit is cached
        assert catalog._mock_client.get_table.call_count == 1

    def test_refresh_single_entry_busts_cache(self):
        catalog = _make_catalog()
        catalog.get("bronze.orders")
        catalog.refresh("bronze.orders")
        catalog.get("bronze.orders")
        assert catalog._mock_client.get_table.call_count == 2

    def test_refresh_all_busts_cache(self):
        catalog = _make_catalog()
        catalog.get("bronze.orders")
        catalog.refresh()
        catalog.get("bronze.orders")
        assert catalog._mock_client.get_table.call_count == 2

    def test_refresh_missing_key_is_noop(self):
        catalog = _make_catalog()
        catalog.refresh("bronze.nonexistent")  # should not raise

    def test_cache_returns_copy(self):
        """Cached dict should be a copy so mutations don't corrupt the cache."""
        catalog = _make_catalog()
        obj1 = catalog.get("bronze.orders")
        obj2 = catalog.get("bronze.orders")
        # Both resolve fine, but are separate DataObject instances
        assert obj1 is not obj2
        assert obj1.location == obj2.location


# ── Tests: auth validation ───────────────────────────────────────────────


class TestGlueCatalogAuth:
    def test_ambiguous_auth_raises(self):
        with pytest.raises(ValueError, match="Ambiguous"):
            with patch("dao.catalog.glue.catalog.boto3"):
                GlueCatalog(aws_profile="dev", aws_access_key_id="key")
