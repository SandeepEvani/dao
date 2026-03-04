# tests/test_glue_translator.py
"""Tests for dao.catalog.glue.translator — GlueTableTranslator."""

from dao.catalog.glue.extractors import (
    InputFormatExtractor,
    LocationExtractor,
    OutputFormatExtractor,
    lambda_extractor,
)
from dao.catalog.glue.translator import GlueTableTranslator

# ── Shared fixture ────────────────────────────────────────────────────────

SAMPLE_TABLE = {
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


# ── Default translator ───────────────────────────────────────────────────


class TestDefaultTranslator:
    def test_default_keys(self):
        t = GlueTableTranslator()
        assert sorted(t.extractor_names) == sorted(["location", "columns", "partition_keys", "classification"])

    def test_translate_defaults(self):
        props = GlueTableTranslator().translate(SAMPLE_TABLE)
        assert props["location"] == "s3://bronze-bucket/orders/"
        assert len(props["columns"]) == 2
        assert props["partition_keys"] == [{"Name": "year", "Type": "string"}]
        assert props["classification"] == "csv"
        # Stuff NOT in defaults should be absent
        assert "serde_info" not in props
        assert "table_type" not in props


# ── include / exclude / only ─────────────────────────────────────────────


class TestMutators:
    def test_include_adds_extractor(self):
        t = GlueTableTranslator().include(InputFormatExtractor())
        assert "input_format" in t.extractor_names

    def test_include_replaces_extractor(self):
        custom_loc = lambda_extractor("location", lambda table: "custom")
        t = GlueTableTranslator().include(custom_loc)
        props = t.translate(SAMPLE_TABLE)
        assert props["location"] == "custom"

    def test_exclude_removes(self):
        t = GlueTableTranslator().exclude("columns", "partition_keys")
        assert "columns" not in t.extractor_names
        assert "partition_keys" not in t.extractor_names
        # The rest survive
        assert "location" in t.extractor_names

    def test_exclude_missing_is_noop(self):
        t = GlueTableTranslator().exclude("does_not_exist")
        assert len(t.extractor_names) == 4  # defaults unchanged

    def test_only_keeps_subset(self):
        t = GlueTableTranslator().only("location")
        assert t.extractor_names == ["location"]

    def test_chaining(self):
        t = GlueTableTranslator().include(InputFormatExtractor()).exclude("columns").only("location", "input_format")
        assert sorted(t.extractor_names) == ["input_format", "location"]


# ── InputFormatExtractor / OutputFormatExtractor ─────────────────────────


class TestFormatExtractors:
    def test_input_format_extracted(self):
        t = GlueTableTranslator().include(InputFormatExtractor())
        props = t.translate(SAMPLE_TABLE)
        assert props["input_format"] == "org.apache.hadoop.mapred.TextInputFormat"

    def test_output_format_extracted(self):
        t = GlueTableTranslator().include(OutputFormatExtractor())
        props = t.translate(SAMPLE_TABLE)
        assert props["output_format"] == "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    def test_both_formats_extracted(self):
        t = GlueTableTranslator().include(InputFormatExtractor(), OutputFormatExtractor())
        props = t.translate(SAMPLE_TABLE)
        assert "input_format" in props
        assert "output_format" in props


# ── None handling ────────────────────────────────────────────────────────


class TestNoneHandling:
    def test_none_fields_omitted(self):
        empty_table = {"Name": "empty"}
        props = GlueTableTranslator().translate(empty_table)
        # All default extractors should return None for an empty table
        assert props == {}


# ── Presets ──────────────────────────────────────────────────────────────


class TestPresets:
    def test_minimal(self):
        t = GlueTableTranslator.minimal()
        assert sorted(t.extractor_names) == ["classification", "location"]

    def test_schema_aware(self):
        t = GlueTableTranslator.schema_aware()
        assert "input_format" in t.extractor_names
        assert "output_format" in t.extractor_names
        assert "serde_info" in t.extractor_names
        # defaults are still present
        assert "location" in t.extractor_names

    def test_full(self):
        t = GlueTableTranslator.full()
        expected = sorted(
            [
                "location",
                "columns",
                "partition_keys",
                "classification",
                "input_format",
                "output_format",
                "serde_info",
                "table_type",
                "compressed",
                "parameters",
                "type",
            ]
        )
        assert t.extractor_names == expected

    def test_full_translate(self):
        props = GlueTableTranslator.full().translate(SAMPLE_TABLE)
        assert props["location"] == "s3://bronze-bucket/orders/"
        assert props["type"] == "TableObject"
        assert props["table_type"] == "EXTERNAL_TABLE"
        assert props["compressed"] is False
        assert props["input_format"] == "org.apache.hadoop.mapred.TextInputFormat"
        assert props["output_format"] == "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"


# ── Custom extractors list at init ───────────────────────────────────────


class TestCustomInit:
    def test_explicit_extractors_replace_defaults(self):
        t = GlueTableTranslator(extractors=[LocationExtractor()])
        assert t.extractor_names == ["location"]

    def test_empty_extractors_list(self):
        t = GlueTableTranslator(extractors=[])
        assert t.extractor_names == []
        assert t.translate(SAMPLE_TABLE) == {}
