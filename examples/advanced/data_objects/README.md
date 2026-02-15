# Data Object Models

Specialized DataObject classes that extend the base `DataObject` with storage-specific properties and computed attributes.

## Why Specialized Data Objects?

The base `DataObject` is generic - it works with any storage. But production interfaces often need:
- **Computed properties** (e.g., S3 keys from prefix + identifier)
- **Storage-specific metadata** (e.g., partition keys, record keys)
- **Type safety** for interface methods

## S3 Data Objects (`s3.py`)

| Class | Use Case | Key Properties |
|-------|----------|----------------|
| `S3FileObject` | Single S3 object | `prefix`, `suffix`, `key` |
| `S3DirectoryObject` | Collection by prefix | `prefix`, `key` |
| `S3DeltaObject` | Delta Lake table | `partition_keys`, `cluster_keys` |
| `S3HudiObject` | Apache Hudi table | `record_key`, `precombine_key`, `partition_key` |
| `S3IcebergObject` | Apache Iceberg table | (inherits from S3TableObject) |

## Hierarchy

```
DataObject (base)
├── S3FileObject          # Single file
└── S3DirectoryObject     # Directory/prefix
    └── S3TableObject     # Table format base
        ├── S3DeltaObject
        ├── S3HudiObject
        └── S3IcebergObject
```

## Usage Example

```python
from dao.data_store import DataStore
from examples.advanced.data_objects import S3FileObject, S3DeltaObject

# S3 file with computed key
store = DataStore("raw")
config = S3FileObject(
    name="app_config",
    data_store=store,
    prefix="config",
    identifier="settings",
    suffix="json"
)
print(config.key)  # "config/settings.json"

# Delta table with partitioning
events = S3DeltaObject(
    name="events",
    data_store=store,
    identifier="events_table",
    partition_keys="date,region",
    cluster_keys="customer_id"
)
```

## Pairing with Interfaces

Each data object type is designed for specific interfaces:

| Data Object | Recommended Interface |
|-------------|----------------------|
| `S3FileObject` | `S3Boto3Interface` |
| `S3DirectoryObject` | `S3Boto3Interface`, `S3SparkInterface` |
| `S3DeltaObject` | `S3DeltaInterface` |
| `S3HudiObject` | `S3HudiInterface` |
| `S3IcebergObject` | `S3IcebergInterface` |

See [advanced examples](../) for complete usage patterns.
