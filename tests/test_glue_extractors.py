# tests/test_glue_extractors.py
"""Tests for dao.catalog.glue.extractors — individual field extractors."""

import pytest

from dao.catalog.glue.extractors import (
    ClassificationExtractor,
    ColumnsExtractor,
    CompressedExtractor,
    InputFormatExtractor,
    LocationExtractor,
    ObjectTypeExtractor,
    OutputFormatExtractor,
    PartitionKeysExtractor,
    RawParametersExtractor,
    SerDeExtractor,
    TableTypeExtractor,
    lambda_extractor,
)

# ── Fixture: a realistic Glue GetTable["Table"] dict ────────────────────

SAMPLE_TABLE = {
    "Name": "orders",
    "DatabaseName": "bronze",
    "TableType": "EXTERNAL_TABLE",
    "StorageDescriptor": {
        "Columns": [
            {"Name": "order_id", "Type": "bigint"},
            {"Name": "customer_id", "Type": "string"},
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
        "StoredAsSubDirectories": False,
    },
    "PartitionKeys": [{"Name": "year", "Type": "string"}],
    "Parameters": {
        "classification": "csv",
        "compressionType": "none",
        "typeOfData": "file",
        "EXTERNAL": "TRUE",
        "owner": "data-eng",
    },
}

EMPTY_TABLE: dict = {"Name": "empty"}


# ── LocationExtractor ────────────────────────────────────────────────────


class TestLocationExtractor:
    def test_extracts_location(self):
        assert LocationExtractor().extract(SAMPLE_TABLE) == "s3://bronze-bucket/orders/"

    def test_none_when_no_storage_descriptor(self):
        assert LocationExtractor().extract(EMPTY_TABLE) is None


# ── ColumnsExtractor ─────────────────────────────────────────────────────


class TestColumnsExtractor:
    def test_extracts_columns(self):
        cols = ColumnsExtractor().extract(SAMPLE_TABLE)
        assert len(cols) == 3
        assert cols[0]["Name"] == "order_id"

    def test_none_when_no_columns(self):
        assert ColumnsExtractor().extract(EMPTY_TABLE) is None

    def test_none_when_columns_key_absent(self):
        table = {"StorageDescriptor": {}}
        assert ColumnsExtractor().extract(table) is None


# ── PartitionKeysExtractor ───────────────────────────────────────────────


class TestPartitionKeysExtractor:
    def test_extracts_partition_keys(self):
        keys = PartitionKeysExtractor().extract(SAMPLE_TABLE)
        assert keys == [{"Name": "year", "Type": "string"}]

    def test_none_when_no_keys(self):
        assert PartitionKeysExtractor().extract(EMPTY_TABLE) is None

    def test_none_when_empty_list(self):
        assert PartitionKeysExtractor().extract({"PartitionKeys": []}) is None


# ── ClassificationExtractor ──────────────────────────────────────────────


class TestClassificationExtractor:
    def test_extracts_classification(self):
        assert ClassificationExtractor().extract(SAMPLE_TABLE) == "csv"

    def test_none_when_absent(self):
        assert ClassificationExtractor().extract(EMPTY_TABLE) is None


# ── InputFormatExtractor ─────────────────────────────────────────────────


class TestInputFormatExtractor:
    def test_extracts_input_format(self):
        assert InputFormatExtractor().extract(SAMPLE_TABLE) == "org.apache.hadoop.mapred.TextInputFormat"

    def test_none_when_no_sd(self):
        assert InputFormatExtractor().extract(EMPTY_TABLE) is None


# ── OutputFormatExtractor ────────────────────────────────────────────────


class TestOutputFormatExtractor:
    def test_extracts_output_format(self):
        assert (
            OutputFormatExtractor().extract(SAMPLE_TABLE)
            == "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
        )

    def test_none_when_no_sd(self):
        assert OutputFormatExtractor().extract(EMPTY_TABLE) is None


# ── SerDeExtractor ───────────────────────────────────────────────────────


class TestSerDeExtractor:
    def test_extracts_serde(self):
        serde = SerDeExtractor().extract(SAMPLE_TABLE)
        assert "SerializationLibrary" in serde

    def test_none_when_absent(self):
        assert SerDeExtractor().extract(EMPTY_TABLE) is None


# ── TableTypeExtractor ───────────────────────────────────────────────────


class TestTableTypeExtractor:
    def test_extracts_table_type(self):
        assert TableTypeExtractor().extract(SAMPLE_TABLE) == "EXTERNAL_TABLE"

    def test_none_when_absent(self):
        assert TableTypeExtractor().extract(EMPTY_TABLE) is None


# ── CompressedExtractor ──────────────────────────────────────────────────


class TestCompressedExtractor:
    def test_extracts_false(self):
        assert CompressedExtractor().extract(SAMPLE_TABLE) is False

    def test_none_when_absent(self):
        assert CompressedExtractor().extract(EMPTY_TABLE) is None


# ── RawParametersExtractor ───────────────────────────────────────────────


class TestRawParametersExtractor:
    def test_extracts_parameters_as_copy(self):
        params = RawParametersExtractor().extract(SAMPLE_TABLE)
        assert params["classification"] == "csv"
        # Must be a copy, not the same reference
        assert params is not SAMPLE_TABLE["Parameters"]

    def test_none_when_absent(self):
        assert RawParametersExtractor().extract(EMPTY_TABLE) is None


# ── ObjectTypeExtractor ──────────────────────────────────────────────────


class TestObjectTypeExtractor:
    @pytest.mark.parametrize(
        "table_type, expected",
        [
            ("EXTERNAL_TABLE", "TableObject"),
            ("MANAGED_TABLE", "TableObject"),
            ("VIRTUAL_VIEW", "DataObject"),
            ("GOVERNED", "TableObject"),
            ("SOMETHING_NEW", "DataObject"),
        ],
    )
    def test_mapping(self, table_type, expected):
        assert ObjectTypeExtractor().extract({"TableType": table_type}) == expected

    def test_default_when_absent(self):
        assert ObjectTypeExtractor().extract({}) == "DataObject"


# ── lambda_extractor ─────────────────────────────────────────────────────


class TestLambdaExtractor:
    def test_basic(self):
        ext = lambda_extractor("owner", lambda t: t.get("Parameters", {}).get("owner"))
        assert ext.name == "owner"
        assert ext.extract(SAMPLE_TABLE) == "data-eng"

    def test_returns_none(self):
        ext = lambda_extractor("nope", lambda t: None)
        assert ext.extract(SAMPLE_TABLE) is None

    def test_repr(self):
        ext = lambda_extractor("foo", lambda t: t)
        assert "foo" in repr(ext)
