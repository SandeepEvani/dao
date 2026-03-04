# RFC-001: Glue Catalog Metadata Translation & DataObject Type Registry

| Field       | Value                                              |
|-------------|----------------------------------------------------|
| **Title**   | Flexible Glue Catalog Metadata Translation & DataObject Type Registry |
| **Status**  | Draft                                              |
| **Created** | 2026-03-04                                         |
| **Author**  | Sandeep Evani                                      |

---

## 1. Problem Statement

### 1a. Glue Metadata Translation

AWS Glue Crawlers discover external data and populate the Glue Data Catalog with rich metadata — column schemas, storage formats (Parquet, CSV, JSON, ORC, Avro, Delta, Iceberg …), S3 locations, SerDe parameters, partition keys, table types (EXTERNAL_TABLE, VIRTUAL_VIEW), and more. As crawlers evolve and new sources are added, **the set of metadata fields is not fixed**.

The current `GlueCatalog._resolve_data_object_properties()` reads a single user-managed JSON blob from `Table.Parameters["data_object_config"]`. This:

1. **Ignores** everything the crawler already discovered (columns, location, format, SerDe, partition keys, classification).
2. **Requires users to duplicate** metadata they shouldn't have to manage.
3. **Cannot adapt** when Glue adds new fields — the code must be changed every time.

We need a mechanism that:

- Translates the rich `GetTable` response into a `DataObject` (or `TableObject`) **automatically**.
- Lets users **control which pieces** of Glue metadata land on the DataObject — without requiring them to write a full custom parser.
- Stays **open for extension** as Glue adds new response fields in the future.
- Offers **multiple approaches** at different levels of convenience vs. control.

### 1b. DataObject Type Resolution

All catalog implementations (`FileCatalog`, `GlueCatalog`, and any future catalogs) always return a bare `DataObject` regardless of the underlying data's nature. There is no way to declare that a catalog entry should produce a `TableObject`, a user-defined `FileObject`, or any other `DataObject` subclass.

We need:

- A **global registry** mapping string type-names to `DataObject` subclasses.
- A **`type` field** in data-object configuration that all catalog implementations honour.
- The ability for users to **register custom subclasses** without modifying catalog code.
- This to work **uniformly across every catalog** (File, Glue, and future ones) via `BaseCatalog.get()`.

---

## 2. Background: Anatomy of a Glue `GetTable` Response

```jsonc
{
  "Table": {
    "Name": "iris",
    "DatabaseName": "bronze",
    "TableType": "EXTERNAL_TABLE",          // or VIRTUAL_VIEW, GOVERNED, etc.
    "StorageDescriptor": {
      "Columns": [
        {"Name": "sepal_length", "Type": "double"},
        {"Name": "sepal_width",  "Type": "double"}
      ],
      "Location": "s3://bronze-bucket/iris/",
      "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
      "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
      "SerdeInfo": {
        "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
        "Parameters": {"field.delim": ","}
      },
      "Compressed": false,
      "StoredAsSubDirectories": false
    },
    "PartitionKeys": [
      {"Name": "year", "Type": "string"}
    ],
    "Parameters": {
      "classification": "csv",
      "compressionType": "none",
      "typeOfData": "file",
      "data_object_config": "{...}",       // user-managed (current approach)
      "EXTERNAL": "TRUE"
    }
  }
}
```

Different users need different subsets of this output:

| Use-case | Needs |
|----------|-------|
| Simple S3 reader | `location` only |
| Schema validation pipeline | `columns`, `partition_keys` |
| Format-aware writer | `classification`, `input_format`, `output_format`, `serde_info` |
| Full governance | Everything above + `table_type`, `parameters`, `compressed` |

---

## 3. Proposed Design

### 3.1 Core Idea — `GlueTableTranslator` (Extractor + Field Registry)

Introduce a **`GlueTableTranslator`** class that sits between the raw Glue API response and the `DataObject` constructor. It owns a registry of **field extractors** — small callables that know how to pull one logical piece of metadata from the `GetTable` response dict.

```
┌─────────────┐      GetTable         ┌──────────────────────┐      props dict      ┌────────────┐
│  Glue API   │ ───────────────────►  │  GlueTableTranslator │ ──────────────────►  │ DataObject │
└─────────────┘   (raw response)      │  ┌────────────────┐  │   (flat key→value)   └────────────┘
                                      │  │ Field Registry  │  │
                                      │  │  - location     │  │
                                      │  │  - columns      │  │
                                      │  │  - format       │  │
                                      │  │  - serde        │  │
                                      │  │  - partitions   │  │
                                      │  │  - (user's own) │  │
                                      │  └────────────────┘  │
                                      └──────────────────────┘
```

### 3.2 The `FieldExtractor` Protocol

```python
from typing import Any, Dict, Protocol, runtime_checkable

@runtime_checkable
class FieldExtractor(Protocol):
    """Extracts a single logical field from a Glue GetTable response."""

    @property
    def name(self) -> str:
        """The key that will appear on the DataObject (e.g. 'location')."""
        ...

    def extract(self, table: Dict[str, Any]) -> Any:
        """Return the value, or MISSING sentinel to skip."""
        ...
```

### 3.3 Built-in Extractors (shipped with `dao`)

| Extractor class | `name` on DataObject | Source path in response |
|-----------------|---------------------|------------------------|
| `LocationExtractor` | `location` | `StorageDescriptor.Location` |
| `ColumnsExtractor` | `columns` | `StorageDescriptor.Columns` |
| `PartitionKeysExtractor` | `partition_keys` | `PartitionKeys` |
| `ClassificationExtractor` | `classification` | `Parameters.classification` |
| `FormatExtractor` | `input_format`, `output_format` | `StorageDescriptor.InputFormat / OutputFormat` |
| `SerDeExtractor` | `serde_info` | `StorageDescriptor.SerdeInfo` |
| `TableTypeExtractor` | `table_type` | `TableType` |
| `CompressedExtractor` | `compressed` | `StorageDescriptor.Compressed` |
| `RawParametersExtractor` | `parameters` | `Parameters` (full dict) |

### 3.4 `GlueTableTranslator` API

```python
from typing import Any, Callable, Dict, List, Optional, Set

MISSING = object()  # sentinel — field is absent, skip it


class GlueTableTranslator:
    """Translates a Glue GetTable response into DataObject properties.

    Ships with sensible defaults; users can add/remove/replace extractors.
    """

    def __init__(self, extractors: Optional[List[FieldExtractor]] = None):
        if extractors is not None:
            self._extractors = {e.name: e for e in extractors}
        else:
            self._extractors = self._default_extractors()

    # ── mutators (builder-style) ───────────────────────────────

    def include(self, *extractors: FieldExtractor) -> "GlueTableTranslator":
        """Add or replace extractors. Returns self for chaining."""
        for e in extractors:
            self._extractors[e.name] = e
        return self

    def exclude(self, *names: str) -> "GlueTableTranslator":
        """Remove extractors by name. Returns self for chaining."""
        for n in names:
            self._extractors.pop(n, None)
        return self

    def only(self, *names: str) -> "GlueTableTranslator":
        """Keep only the named extractors. Returns self for chaining."""
        self._extractors = {
            n: e for n, e in self._extractors.items() if n in names
        }
        return self

    # ── core translation ───────────────────────────────────────

    def translate(self, table: Dict[str, Any]) -> Dict[str, Any]:
        """Run every registered extractor and return a flat props dict."""
        props: Dict[str, Any] = {}
        for extractor in self._extractors.values():
            value = extractor.extract(table)
            if value is not MISSING:
                props[extractor.name] = value
        return props

    # ── defaults ───────────────────────────────────────────────

    @staticmethod
    def _default_extractors() -> Dict[str, FieldExtractor]:
        return {e.name: e for e in [
            LocationExtractor(),
            ColumnsExtractor(),
            PartitionKeysExtractor(),
            ClassificationExtractor(),
        ]}
```

### 3.5 Convenience: `lambda_extractor` Factory

For one-off fields without writing a whole class:

```python
def lambda_extractor(name: str, func: Callable[[Dict], Any]) -> FieldExtractor:
    """Create a FieldExtractor from a name + callable."""
    ...
```

Usage:

```python
translator = GlueTableTranslator().include(
    lambda_extractor("owner", lambda t: t.get("Parameters", {}).get("owner", MISSING)),
)
```

---

## 4. Integration with `GlueCatalog`

### 4.1 Updated `GlueCatalog.__init__`

```python
class GlueCatalog(BaseCatalog):
    def __init__(
        self,
        *,
        translator: Optional[GlueTableTranslator] = None,
        # ... existing auth params ...
    ):
        self._translator = translator or GlueTableTranslator()
        # ... boto3 setup ...
        super().__init__()
```

### 4.2 Updated `_resolve_data_object_properties`

```python
def _resolve_data_object_properties(self, data_store: str, data_object: str) -> Dict[str, Any]:
    response = self._glue.get_table(DatabaseName=data_store, Name=data_object)
    table = response["Table"]

    # Phase 1 — translator extracts crawler-discovered metadata
    props = self._translator.translate(table)

    # Phase 2 — overlay explicit user config (always wins)
    user_config = table.get("Parameters", {}).get("data_object_config")
    if user_config:
        props.update(_deserialize_config(user_config))

    return props
```

This gives a **two-layer** model:

1. **Automatic** — translator pulls what the crawler found.
2. **Override** — anything the user set in `data_object_config` takes precedence.

### 4.3 Object Type Resolution via `DataObjectRegistry`

The translator can include an `ObjectTypeExtractor` that maps the Glue
`TableType` to a registry name (e.g. `"TableObject"`).  `BaseCatalog.get()`
pops the `"type"` key from the resolved properties and looks it up in the
global `DataObjectRegistry` to decide which class to instantiate.  See
**Section 5** for the full registry design.

```python
class ObjectTypeExtractor:
    """Maps Glue TableType to a dao DataObject subclass name."""

    name = "type"                       # stored as the "type" key in props

    MAPPING = {
        "EXTERNAL_TABLE": "TableObject",
        "MANAGED_TABLE":  "TableObject",
        "VIRTUAL_VIEW":   "DataObject",
    }

    def extract(self, table):
        table_type = table.get("TableType", "")
        return self.MAPPING.get(table_type, "DataObject")
```

---

## 5. DataObject Type Registry

### 5.1 Motivation

Every catalog — File, Glue, and any future implementation — calls
`BaseCatalog.get()` to construct a `DataObject`.  Today this always returns
the base `DataObject` class.  We want a single, catalog-agnostic mechanism
so that a config entry like `"type": "TableObject"` (in a JSON file **or**
extracted from Glue) causes the catalog to instantiate the right subclass.

### 5.2 `DataObjectRegistry`

A module-level singleton registry that maps string names → `DataObject`
subclasses.

```python
# src/dao/data_object/registry.py

class DataObjectRegistry:
    """Maps string type-names to DataObject subclasses."""

    def register(self, name: str, cls: Type[DataObject]) -> None: ...
    def get(self, name: str) -> Type[DataObject]: ...
    def unregister(self, name: str) -> None: ...
    def registered_names(self) -> list: ...

# Module-level singleton
registry = DataObjectRegistry()

# Convenience function
def register_data_object(name: str, cls: Type[DataObject]) -> None:
    registry.register(name, cls)
```

**Built-in registrations** happen at import time:

| Name | Class |
|------|-------|
| `"DataObject"` | `DataObject` |
| `"TableObject"` | `TableObject` |

**Fallback behaviour**: `registry.get(None)` and `registry.get("UnknownName")`
both return `DataObject` — unknown types never crash, they degrade gracefully.

### 5.3 Updated `BaseCatalog.get()`

The change lives in `BaseCatalog` so **every** catalog gets it for free:

```python
# base.py
from dao.data_object import registry as _data_object_registry

class BaseCatalog(ABC):

    def get(self, fully_qualified_name: str) -> DataObject:
        data_store, data_object = self._parse_fully_qualified_name(fully_qualified_name)
        data_store_instance = self._factory.get(data_store)
        properties = self._resolve_data_object_properties(data_store, data_object)

        # Pop "type" so it doesn't leak onto the object as an attribute
        object_cls = _data_object_registry.get(properties.pop("type", None))
        return object_cls(name=data_object, data_store=data_store_instance, **properties)
```

### 5.4 How It Works Across Catalogs

**FileCatalog** — users add `"type"` in the JSON config:

```json
{
  "bronze": {
    "orders": {
      "type": "TableObject",
      "schema": "public",
      "key": "orders/2025.csv"
    },
    "logs": {
      "type": "FileObject",
      "key": "logs/app.log"
    }
  }
}
```

**GlueCatalog** — the `ObjectTypeExtractor` writes `"type"` into the
translated properties automatically, or users set it in `data_object_config`.

**Custom catalogs** — any `BaseCatalog` subclass that returns `"type"` in
`_resolve_data_object_properties()` gets subclass instantiation for free.

### 5.5 Registering Custom Subclasses

```python
from dao.data_object import DataObject, register_data_object

class FileObject(DataObject):
    """Represents a raw file (S3 key, local path, etc.)."""
    pass

# Register once at startup
register_data_object("FileObject", FileObject)

# Now any catalog config with "type": "FileObject" will instantiate this class
catalog = FileCatalog(...)
obj = catalog.get("bronze.logs")   # → FileObject instance
```

### 5.6 File Layout

```
src/dao/data_object/
├── __init__.py          # re-exports registry, register_data_object
├── data_object.py       # DataObject (unchanged)
├── table_object.py      # TableObject (unchanged)
└── registry.py          # DataObjectRegistry + built-in registrations
```

---

## 6. Four Methods — From Zero-Config to Full Control

### Method A: Zero-Config (Sensible Defaults)

```python
catalog = GlueCatalog(aws_profile="dev")
obj = catalog.get("bronze.iris")
# obj.location  → "s3://bronze-bucket/iris/"
# obj.columns   → [{"Name": "sepal_length", "Type": "double"}, ...]
# obj.partition_keys → [{"Name": "year", "Type": "string"}]
# obj.classification → "csv"
```

**User effort:** None. The default translator ships `location`, `columns`, `partition_keys`, `classification`.

### Method B: Select Only What You Need

```python
translator = GlueTableTranslator().only("location", "classification")
catalog = GlueCatalog(aws_profile="dev", translator=translator)

obj = catalog.get("bronze.iris")
# obj.location       → present ✓
# obj.classification → present ✓
# obj.columns        → absent (not extracted)
```

**User effort:** One line to narrow the scope.

### Method C: Add Extra Built-in Extractors

```python
from dao.catalog.glue.extractors import FormatExtractor, SerDeExtractor

translator = GlueTableTranslator().include(FormatExtractor(), SerDeExtractor())
catalog = GlueCatalog(aws_profile="dev", translator=translator)

obj = catalog.get("bronze.iris")
# obj.input_format → "org.apache.hadoop.mapred.TextInputFormat"
# obj.serde_info   → {"SerializationLibrary": "...", ...}
```

**User effort:** Import and `.include()` the extras.

### Method D: Fully Custom Extractor

```python
from dao.catalog.glue.translator import lambda_extractor, MISSING

custom = lambda_extractor(
    "row_count",
    lambda t: int(t.get("Parameters", {}).get("recordCount", -1))
)

translator = GlueTableTranslator().include(custom)
catalog = GlueCatalog(aws_profile="dev", translator=translator)

obj = catalog.get("bronze.iris")
# obj.row_count → 150  (if crawler set recordCount)
```

Or for complex cases, implement the `FieldExtractor` protocol directly:

```python
class IcebergSnapshotExtractor:
    name = "iceberg_snapshot"

    def extract(self, table):
        params = table.get("Parameters", {})
        if params.get("table_type") != "ICEBERG":
            return MISSING
        return {
            "current_snapshot_id": params.get("current-snapshot-id"),
            "snapshot_count": params.get("snapshot-count"),
        }
```

**User effort:** Write a small class or lambda; plug it in.

---

## 7. Presets — Opinionated Bundles

For common personas, provide pre-built translator configs:

```python
class GlueTableTranslator:

    @classmethod
    def minimal(cls) -> "GlueTableTranslator":
        """Only location + classification."""
        return cls().only("location", "classification")

    @classmethod
    def schema_aware(cls) -> "GlueTableTranslator":
        """Defaults + format + serde — for schema-validation pipelines."""
        return cls().include(FormatExtractor(), SerDeExtractor())

    @classmethod
    def full(cls) -> "GlueTableTranslator":
        """Every built-in extractor enabled."""
        return cls(extractors=[
            LocationExtractor(), ColumnsExtractor(), PartitionKeysExtractor(),
            ClassificationExtractor(), FormatExtractor(), SerDeExtractor(),
            TableTypeExtractor(), CompressedExtractor(), RawParametersExtractor(),
        ])
```

Usage:

```python
catalog = GlueCatalog(translator=GlueTableTranslator.full(), aws_profile="prod")
```

---

## 8. Caching

Glue API calls are expensive. The translator layer does **not** cache — that responsibility stays with `GlueCatalog`:

```python
class GlueCatalog(BaseCatalog):
    def __init__(self, *, cache_ttl: int = 300, **kwargs):
        self._cache: Dict[str, Tuple[float, Dict]] = {}
        self._cache_ttl = cache_ttl
        ...

    def _resolve_data_object_properties(self, data_store, data_object):
        key = f"{data_store}.{data_object}"
        now = time.time()
        if key in self._cache:
            ts, props = self._cache[key]
            if now - ts < self._cache_ttl:
                return props

        response = self._glue.get_table(DatabaseName=data_store, Name=data_object)
        table = response["Table"]
        props = self._translator.translate(table)

        user_cfg = table.get("Parameters", {}).get("data_object_config")
        if user_cfg:
            props.update(_deserialize_config(user_cfg))

        self._cache[key] = (now, props)
        return props
```

---

## 9. Proposed File Layout

```
src/dao/data_object/
├── __init__.py          # re-exports registry, register_data_object
├── data_object.py       # DataObject (unchanged)
├── table_object.py      # TableObject (unchanged)
└── registry.py          # DataObjectRegistry + built-in registrations

src/dao/catalog/
├── __init__.py
├── base.py              # updated — get() uses DataObjectRegistry
├── exceptions.py        # add GlueTranslationError
├── file.py              # unchanged
└── glue/
    ├── __init__.py      # re-exports GlueCatalog
    ├── catalog.py       # GlueCatalog class
    ├── translator.py    # GlueTableTranslator, MISSING, lambda_extractor
    └── extractors.py    # All built-in FieldExtractor implementations
```

The existing `src/dao/catalog/glue.py` (single-file) would be **replaced** by the `glue/` package.

---

## 10. Migration Path

| Step | Change | Breaking? |
|------|--------|-----------|
| 1 | Add `glue/` package alongside `glue.py` | No |
| 2 | Deprecate `dao.catalog.glue.GlueCatalog`, alias to new package | No |
| 3 | `GlueCatalog()` with no translator gives same behavior as today (reads `data_object_config`) | No |
| 4 | Default translator adds `location`, `columns`, `partition_keys`, `classification` automatically | **Additive** — new attrs appear on DataObject |
| 5 | Remove old `glue.py` in next minor release | Yes (import path) |

For existing users who only use `data_object_config`, behavior is identical — the user-config overlay always runs and the new extracted fields are simply additional attributes.

---

## 11. Testing Strategy

| Layer | Approach |
|-------|----------|
| **DataObjectRegistry** | Unit tests for register/get/unregister, built-in types, custom types, fallback for unknown names. |
| **Type resolution (BaseCatalog)** | FileCatalog integration tests with `"type"` in JSON configs — assert correct subclass, `type` key not leaked onto object. |
| **Extractors** | Pure-function unit tests with fixture JSON dicts (no AWS calls). |
| **Translator** | Test `translate()` with mock table dicts; assert correct inclusion/exclusion. |
| **GlueCatalog** | Mock `boto3` glue client (`get_table`, `get_databases`); assert DataObject attributes and subclass resolution. |
| **Presets** | Snapshot tests — each preset produces expected extractor names. |
| **Caching** | Cache hit/miss tests; `refresh()` eviction; assert cached dict is a copy. |

---

## 12. Example: End-to-End

```python
from dao import DataAccessObject
from dao.catalog.glue import GlueCatalog
from dao.catalog.glue.translator import GlueTableTranslator, lambda_extractor
from dao.catalog.glue.extractors import FormatExtractor

# Build a translator that gets the basics + format + a custom field
translator = (
    GlueTableTranslator()
    .include(FormatExtractor())
    .include(lambda_extractor("owner", lambda t: t.get("Parameters", {}).get("owner")))
)

catalog = GlueCatalog(aws_profile="analytics", translator=translator)

# Resolve a table — no manual config needed if the crawler populated it
orders = catalog.get("bronze.orders")

print(orders.location)       # s3://bronze-bucket/orders/
print(orders.columns)        # [{"Name": "order_id", "Type": "bigint"}, ...]
print(orders.classification) # parquet
print(orders.input_format)   # org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat
print(orders.owner)          # data-engineering-team

dao = DataAccessObject()
data = dao.read(data_object=orders)
```

---

## 13. Open Questions

1. **Column type normalization** — Should `ColumnsExtractor` convert Glue/Hive types (`string`, `bigint`) to Python types? Or keep raw strings and let consumers decide?
2. **Partition handling** — Should `partition_keys` be merged into `columns` or kept separate (current proposal)?
3. **Multi-value extractors** — `FormatExtractor` sets two keys (`input_format`, `output_format`). Should every extractor be strictly one-key, or allow multi-key extractors?
4. **Schema object** — Should we introduce a first-class `Schema` / `Column` dataclass rather than passing raw dicts?
5. **Refresh API** — Should `GlueCatalog` expose a `refresh("bronze.orders")` method to bust the cache on demand?

---

## 14. Summary of Approaches

| Method | User Effort | Flexibility | When to Use |
|--------|-------------|-------------|-------------|
| **A — Zero-Config** | None | Medium | "Just give me the table" |
| **B — `.only()`** | 1 line | Medium | "I only need location + format" |
| **C — `.include()` built-ins** | 1–2 lines | High | "I need serde / format details too" |
| **D — Custom extractor** | 5–20 lines | Full | "I need project-specific metadata" |
| **Presets** | 1 line | Curated | "Give me a known-good bundle" |

All methods compose — you can start with a preset and `.include()` / `.exclude()` from there.

---

## 15. Decision

*Pending review.*














