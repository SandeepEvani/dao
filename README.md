# DAO

**Data Access Object (DAO)** is a Python module that provides a unified interface for accessing data across multiple data stores. Register your data stores once, and DAO automatically routes `read`, `write`, and `run` operations to the correct interface class based on the arguments you provide — no need to call store-specific methods directly.

## Features

- **Unified API** — A single `dao.read()`, `dao.write()`, and `dao.run()` interface for all your data stores.
- **Automatic Routing** — DAO inspects method signatures and routes calls to the correct interface method based on the arguments provided.
- **Multiple Interface Support** — Each data store can have a primary interface and optional secondary interfaces (e.g., Redshift as primary, with Delta or Hudi as secondary).
- **Lazy Initialization** — Interface classes can be initialized on-demand to save startup time.
- **Extensible** — Add your own interface classes with `read_*`, `write_*`, or `run_*` methods, or use the `@register` decorator.

## Installation

```bash
pip install dao
```

## Quick Start

### 1. Create an Interface Class

An interface class contains the actual `read_*` / `write_*` / `run_*` methods that interact with your data store. The DAO discovers methods by their name prefix.

```python
# data_classes/redshift.py

class Redshift:

    def __init__(self, **kwargs):
        # Initialize connection, configs, etc.
        ...

    def write_dataframe(self, data, destination, **kwargs):
        """Write a DataFrame to a Redshift table."""
        print(f"Writing to Redshift table: {destination}")
        # ... your Redshift write logic here ...
        return True

    def read_table(self, data_object, **kwargs):
        """Read data from a Redshift table."""
        print(f"Reading from Redshift table: {data_object.name}")
        # ... your Redshift read logic here ...
        return {"data": "sample"}

    def run_query(self, data_object, query: str, **kwargs):
        """Execute a SQL query on Redshift."""
        print(f"Running query: {query}")
        # ... your Redshift query logic here ...
        return []
```

### 2. Configure Your Data Stores

Create a JSON configuration file that tells DAO about your data stores and their interface classes.

```json
{
  "data_stores": {
    "warehouse": {
      "interface_class": "Redshift",
      "interface_class_location": "data_classes.redshift",
      "default_configs": {}
    }
  }
}
```

| Field                      | Description                                                        |
|----------------------------|--------------------------------------------------------------------|
| `interface_class`          | The class name of the interface                                    |
| `interface_class_location` | The Python module path where the class can be imported from        |
| `default_configs`          | A dict of keyword arguments passed to the interface class `__init__` |

### 3. Define a Catalog (Data Objects)

Data Objects represent individual tables, files, or objects within a data store. Create a catalog config and a helper class to build `TableObject` instances.

**catalog.json**
```json
{
  "warehouse": {
    "customers": {
      "primary_keys": "customer_id"
    },
    "orders": {
      "primary_keys": "order_id",
      "partition_keys": "year,month"
    }
  }
}
```

**catalog.py**
```python
from json import load
from dao.data_object import TableObject
from dao.data_store import DataStore


class Catalog:

    def __init__(self, confs):
        self.confs = load(open(confs))

    def get(self, table_name):
        """Retrieve a TableObject by its fully qualified name (e.g., 'warehouse.customers')."""
        if "." not in table_name:
            raise ValueError("Provide a fully qualified table name: '<data_store>.<table>'")

        data_store, table = table_name.split(".")

        if not DataStore.check_data_store(data_store) or data_store not in self.confs:
            raise KeyError(f"Data store '{data_store}' is not registered")

        data_store_object = DataStore.get_data_store(data_store)
        tables = self.confs[data_store]

        if table not in tables:
            raise KeyError(f"Table '{table}' not found in data store '{data_store}'")

        properties = tables.get(table)
        return TableObject(name=table, data_store=data_store_object, **properties)


catalog = Catalog("catalog.json")
```

### 4. Initialize and Use the DAO

```python
from catalog import catalog
from dao import dao

# Initialize the DAO with the data stores config
dao.init("data_stores.json")

# Get a table object from the catalog
customers = catalog.get("warehouse.customers")

# Write data — DAO routes to Redshift.write_dataframe automatically
dao.write(data=my_dataframe, data_object=customers)

# Read data — DAO routes to Redshift.read_table automatically
result = dao.read(data_object=customers)

# Run a query — DAO routes to Redshift.run_query automatically
rows = dao.run(data_object=customers, query="SELECT * FROM customers LIMIT 10")
```

## Design

The DAO is built around two core entities:

### Data Stores

A **Data Store** is a virtual representation of a storage layer — a database schema, an S3 prefix, a file system directory, etc. Each data store is associated with one or more **interface classes** that contain the methods for reading, writing, and executing operations.

### Data Objects

A **Data Object** represents a specific item within a data store — a database table, a file, an object. Each Data Object is bound to a Data Store, and the DAO uses the associated interface class to perform operations on it.

### How Routing Works

1. You call `dao.read()`, `dao.write()`, or `dao.run()` with a `data_object` and any additional arguments.
2. The DAO identifies the data store from the `data_object`.
3. The **Signature Factory** generates an argument signature from the provided args.
4. The **Router** compares the argument signature against all registered method signatures in the route table.
5. The best-matching method is selected and executed.

```
dao.write(data, data_object, **kwargs)
        │
        ▼
   ┌─────────┐     ┌───────────────────┐     ┌────────┐
   │   DAO   │────▶│  Signature Match   │────▶│ Router │
   └─────────┘     └───────────────────┘     └────────┘
                                                  │
                                                  ▼
                                          ┌──────────────┐
                                          │ Interface     │
                                          │ Class Method  │
                                          └──────────────┘
```

### The `@register` Decorator

By default, the DAO discovers methods by name prefix (`read_*`, `write_*`, `run_*`). You can also explicitly register a method using the `@register` decorator:

```python
from dao.core.dao_registrar import register

class MyInterface:

    @register(mode="write", preference=1)
    def custom_upsert(self, data, data_object, **kwargs):
        """This method is registered as a 'write' method with higher preference."""
        ...
```

The `preference` parameter controls method priority — lower values have higher priority (default is `0`).

### Lazy Initialization

Pass `lazy=True` to defer the initialization of interface classes until they are first needed:

```python
dao.init("data_stores.json", lazy=True)
```

This is useful when you have many data stores but only access a subset during each run.

## Advanced Examples

### Multiple Interface Classes per Data Store

A data store can have a primary interface and one or more secondary interfaces. For example, an S3-based data lake might support writes through S3 directly, or through Delta Lake or Apache Hudi formats.

**data_classes/s3.py**
```python
class S3:

    def __init__(self, bucket=None, **kwargs):
        self.bucket = bucket

    def write_object(self, data, data_object, **kwargs):
        print(f"Writing to S3: s3://{self.bucket}/{data_object.name}")
        return True

    def read_object(self, data_object, **kwargs):
        print(f"Reading from S3: s3://{self.bucket}/{data_object.name}")
        return data_object.name
```

**data_classes/delta.py**
```python
class Delta:

    def __init__(self, **kwargs): ...

    def write_delta_table(self, data, data_object, **kwargs):
        print(f"Writing Delta table: {data_object.name}")
        return True

    def read_delta_table(self, data_object, **kwargs):
        print(f"Reading Delta table: {data_object.name}")
        return data_object.name
```

**data_classes/hudi.py**
```python
class Hudi:

    def __init__(self, bucket=None, **kwargs):
        self.bucket = bucket

    def write_hudi_table(self, data, data_object, **kwargs):
        print(f"Writing Hudi table: {data_object.name}")
        return True

    def read_hudi_table(self, data_object, **kwargs):
        print(f"Reading Hudi table: {data_object.name}")
        return data_object.name
```

**data_stores.json**
```json
{
  "data_stores": {
    "raw": {
      "interface_class": "S3",
      "interface_class_location": "data_classes.s3",
      "default_configs": { "bucket": "raw-zone" },
      "secondary_interfaces": [
        {
          "interface_class": "Delta",
          "interface_class_location": "data_classes.delta",
          "default_configs": {}
        },
        {
          "interface_class": "Hudi",
          "interface_class_location": "data_classes.hudi",
          "default_configs": { "bucket": "raw-zone" }
        }
      ]
    }
  }
}
```

**Usage — switching between interfaces at runtime:**
```python
from catalog import catalog
from dao import dao

dao.init("data_stores.json")

table = catalog.get("raw.customer")

# Uses the primary interface (S3) by default
dao.write(data="payload", data_object=table)

# Use a secondary interface by passing dao_interface_class
dao.write(data="payload", data_object=table, dao_interface_class="Hudi")
dao.write(data="payload", data_object=table, dao_interface_class="Delta")
```

> **Note:** Any keyword argument prefixed with `dao_` is treated as a DAO configuration option and is **not** passed to the interface method. Use `dao_interface_class` to target a specific secondary interface.

### Custom Data Objects

The `DataObject` class is the base for all data objects. `TableObject` extends it with table-specific metadata:

```python
from dao.data_object import TableObject
from dao.data_store import DataStore

data_store = DataStore.get_data_store("warehouse")

# Create a TableObject with metadata
orders = TableObject(
    name="orders",
    data_store=data_store,
    columns=["order_id", "customer_id", "amount", "order_date"],
    primary_keys="order_id",
    partition_keys="year,month",
)

# Properties from the data store are inherited
print(orders)  # Table(warehouse.orders)
```

You can also create your own data object subclasses:

```python
from dao.data_object import DataObject

class FileObject(DataObject):
    def __init__(self, name, data_store, file_format="parquet", compression="snappy"):
        super().__init__(name, data_store)
        self.file_format = file_format
        self.compression = compression
```

## Project Structure

```
src/dao/
├── __init__.py              # Exports the singleton `dao` instance
├── core/
│   ├── dao.py               # Main DAO class (singleton) with read/write/run
│   ├── dao_interface.py     # Abstract interface (IDAO)
│   ├── dao_mediator.py      # Mediates between DAO, Router, and SignatureFactory
│   ├── dao_registrar.py     # @register decorator for custom method registration
│   ├── router/
│   │   └── router.py        # Route table management and method selection
│   └── signature/
│       ├── signature.py           # Base Signature class
│       ├── method_signatures.py   # MethodSignature (from interface classes)
│       ├── argument_signature.py  # ArgumentSignature (from user-provided args)
│       └── signature_factory.py   # Creates and compares signatures
├── data_object/
│   ├── data_object.py       # Base DataObject class
│   └── table_object.py      # TableObject for tabular data
├── data_store/
│   ├── data_store.py        # DataStore class
│   └── data_store_factory.py # Creates DataStore instances from config
└── utils/
    └── singleton.py          # Singleton decorator
```

## Requirements

- Python >= 3.9
- pandas >= 2.2.3

## License

See [LICENSE](LICENSE) for details.

## Feedback

If you have any feedback, please reach out at evani.sandeep.sh@gmail.com

