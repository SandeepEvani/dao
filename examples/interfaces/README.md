# Production Interface Classes

This directory contains **production-ready Redshift interface implementations**.

> **Note**: These require external dependencies. Install with: `uv sync --group example`

## Directory Structure

```
interfaces/
└── redshift/            # Redshift interface implementations
```

## Redshift Interfaces (`redshift/`)

| Interface | Description | Dependencies |
|-----------|-------------|--------------|
| `RedshiftConnector` | Direct Redshift via ODBC | redshift-connector |
| `RedshiftSparkInterface` | Redshift via Spark JDBC | pyspark |

## Usage Example

```python
from dao import DataAccessObject
from dao.data_store import DataStore

# Production interface
from examples.interfaces.redshift.odbc import RedshiftConnector

store = DataStore("warehouse")
store.set_interface_class(
    class_=RedshiftConnector,
    args={
        "host": "my-cluster.region.redshift.amazonaws.com",
        "database": "analytics",
        "user": "admin",
        "password": "secret"
    },
    primary=True,
)

dao = DataAccessObject()
# Use with your DataObjects
```

## Interface Method Naming

DAO discovers interface methods by naming convention:

| Action | Method Prefix | Example |
|--------|---------------|---------|
| read | `read_` | `read_data()`, `read_dataframe()` |
| write | `write_` | `write_data()`, `write_dataframe()` |
| delete | `delete_` | `delete_data()`, `delete_table()` |
| list | `list_` | `list_objects()`, `list_tables()` |
| upsert | `upsert_` | `upsert_table()` |

Or use the `@register` decorator:

```python
from dao import register

class MyInterface:
    @register(mode="read")
    def fetch_records(self, data_object, **kwargs):
        # Custom method name, registered as 'read' action
        pass
```

## Creating Custom Interfaces

Use these production interfaces as templates for your own:

1. **Copy** the closest matching interface
2. **Implement** methods with appropriate prefixes (`read_`, `write_`, etc.)
3. **Register** with DataStore using `set_interface_class()`

See the [DAO documentation](../../README.md) for more details.

## S3 and Specialized Data Objects

For S3 interfaces and specialized data objects (S3FileObject, S3DeltaObject, etc.), 
see the [advanced examples](../advanced/) directory.
