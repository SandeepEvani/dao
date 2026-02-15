# DAO - Data Access Object

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)

A unified, interface-agnostic data access layer for Python that abstracts the complexity of working with multiple data stores and provides intelligent method routing based on signatures.

## Overview

DAO (Data Access Object) is a convenience module that simplifies data access across different storage backends. By registering data stores with DAO, you can perform read, write, and other operations without explicitly managing the underlying interface implementations.

The framework automatically routes your method calls to the appropriate backend implementation based on:
- The data store being accessed
- The interface class configured for that store
- The method signature and parameters provided

## Features

- **Unified API**: Single `DataAccessObject` interface for all data operations (read, write, upsert, delete, copy, move, etc.)
- **Intelligent Routing**: Automatically selects the best matching interface method based on provided arguments
- **Lazy Interface Loading**: Interface classes are loaded on-demand, improving startup performance
- **Multiple Interfaces per Store**: Support for primary and additional interface classes per data store
- **Catalog Support**: File-based configuration for data stores and data objects
- **Extensible**: Easy to add custom interface classes and data object types
- **Type-Safe**: Includes `py.typed` marker for mypy support

## Installation

This project uses [UV](https://github.com/astral-sh/uv) as the project manager.

```bash
# Clone the repository
git clone https://github.com/SandeepEvani/dao.git
cd dao

# Install with UV
uv sync

# Install with development dependencies
uv sync --group dev

# Install with example dependencies (boto3, pyspark, delta-spark, etc.)
uv sync --group example
```

## Quick Start

### Basic Usage

```python
from dao import DataAccessObject
from dao.data_object import DataObject
from dao.data_store import DataStore

# 1. Create a data store
bronze_store = DataStore("bronze")

# 2. Set up interface classes
bronze_store.set_interface_class(class_=S3Interface, primary=True)
bronze_store.set_interface_class(class_=DeltaInterface)

# 3. Create a data object
customer_data = DataObject("customers", bronze_store)

# 4. Use the DAO singleton for operations
dao = DataAccessObject()
dao.write(data=my_data, data_object=customer_data)
dao.read(data_object=customer_data)
```

### Using the DataStoreFactory

For configuration-driven setup:

```python
from dao import DataAccessObject
from dao.data_object import DataObject
from dao.data_store import DataStoreFactory

# Define data store configurations
config = {
    "raw": {
        "class": "S3Interface",
        "module": "my_interfaces.s3",
        "args": {"bucket": "raw-zone"},
        "additional_interfaces": [
            {"class": "DeltaInterface", "module": "my_interfaces.delta"},
            {"class": "HudiInterface", "module": "my_interfaces.hudi", "args": {"bucket": "raw-zone"}}
        ]
    },
    "processed": {
        "class": "RedshiftInterface",
        "module": "my_interfaces.redshift"
    }
}

# Load configurations
factory = DataStoreFactory()
factory.load_configs(config)

# Get data store and create data object
raw_store = factory.get("raw")
sales_data = DataObject("sales", raw_store)

# Use DAO
dao = DataAccessObject()
dao.write(data=my_data, data_object=sales_data)
```

### Using the File Catalog

For JSON-based configuration management:

```python
from dao import DataAccessObject
from dao.catalog.file import FileCatalog

# Initialize catalog with config file paths
catalog = FileCatalog(
    data_store_config_location="config/data_stores.json",
    data_object_config_location="config/data_objects.json"
)

# Get data object using fully qualified name
customer_data = catalog.get_data_object("raw.customer")

# Use DAO
dao = DataAccessObject()
dao.write(data=my_data, data_object=customer_data)
```

**data_stores.json:**
```json
{
    "raw": {
        "class": "S3Interface",
        "module": "my_interfaces.s3",
        "args": {"bucket": "raw-zone"},
        "additional_interfaces": [
            {"class": "DeltaInterface", "module": "my_interfaces.delta"}
        ]
    }
}
```

**data_objects.json:**
```json
{
    "raw": {
        "customer": {},
        "sales": {"partition_key": "date"}
    }
}
```

## Core Concepts

### Data Store

A **Data Store** represents a storage layer (or segment thereof) such as:
- A specific schema in a database
- A prefix in object storage (S3, GCS, Azure Blob)
- A directory in a file system
- A partition of block storage

Each data store has:
- A **primary interface class** for default operations
- Optional **additional interface classes** for specialized operations

```python
from dao.data_store import DataStore

store = DataStore("my_store", bucket="my-bucket", region="us-east-1")
store.set_interface_class(class_=PrimaryInterface, primary=True)
store.set_interface_class(class_=SecondaryInterface)
```

### Data Object

A **Data Object** represents an actual data entity within a data store:
- Database tables
- Files or objects
- Directories/prefixes

Built-in data object types:
- `DataObject` - Base class for all data objects
- `TableObject` - For tabular data with schema, columns, and primary keys

```python
from dao.data_object import DataObject, TableObject

# Simple data object
data = DataObject("my_data", data_store)

# Table with schema
table = TableObject("users", data_store, schema="public", primary_keys=["id"])
```

### Interface Classes

Interface classes contain the actual implementation for data operations. Methods are discovered by:
1. **Name convention**: Methods starting with the action name (e.g., `read_`, `write_`)
2. **Registration decorator**: Using `@register` to mark methods

```python
from dao import register

class MyS3Interface:
    def __init__(self, bucket: str):
        self.bucket = bucket
    
    # Discovered by name convention
    def read_json(self, data_object, **kwargs):
        """Read JSON data from S3."""
        pass
    
    def write_parquet(self, data, data_object, compression="snappy"):
        """Write Parquet data to S3."""
        pass
    
    # Discovered by @register decorator
    @register(mode="read")
    def fetch_data(self, data_object, format: str):
        """Custom read operation."""
        pass
```

### DataAccessObject

The singleton `DataAccessObject` is the main entry point for all data operations:

```python
from dao import DataAccessObject

dao = DataAccessObject()

# Available operations
dao.read(data_object=obj)                          # Read data
dao.write(data=data, data_object=obj)              # Write data
dao.upsert(data=data, data_object=obj)             # Insert or update
dao.delete(data_object=obj)                        # Delete data
dao.list(data_object=obj)                          # List contents
dao.count(data_object=obj)                         # Count records
dao.copy(source=src_obj, destination=dest_obj)     # Copy data
dao.move(source=src_obj, destination=dest_obj)     # Move data
dao.read_stream(data_object=obj)                   # Stream read
dao.write_stream(data_stream=stream, data_object=obj)  # Stream write
dao.run(action="custom", data_object=obj)          # Custom action
```

### Intelligent Routing

DAO automatically routes method calls to the best matching interface implementation based on:
- Argument names and types
- Number of arguments
- Data store and interface class

You can specify a particular interface class using the `dao_interface_class` configuration parameter:

```python
# Use the primary interface (default)
dao.write(data=my_data, data_object=obj)

# Use a specific interface class
dao.write(data=my_data, data_object=obj, dao_interface_class="DeltaInterface")
```

## Project Structure

```
dao/
├── src/dao/
│   ├── __init__.py           # Main exports: DataAccessObject, register
│   ├── registrar.py          # @register decorator for interface methods
│   ├── core/
│   │   ├── dao.py            # DataAccessObject singleton
│   │   ├── accessor.py       # DataAccessor descriptor and routing logic
│   │   ├── router.py         # Method routing based on signatures
│   │   └── signature/        # Signature analysis utilities
│   ├── data_store/
│   │   ├── data_store.py     # DataStore class
│   │   └── data_store_factory.py  # Factory for creating data stores
│   ├── data_object/
│   │   ├── data_object.py    # Base DataObject class
│   │   └── table_object.py   # TableObject for tabular data
│   ├── catalog/
│   │   ├── base.py           # Abstract base catalog
│   │   └── file.py           # File-based catalog implementation
│   └── utils/
│       └── singleton.py      # Singleton decorator utility
├── examples/
│   ├── quickstart/           # ⭐ Getting started (5 min)
│   ├── patterns/             # ⭐⭐ Production patterns
│   ├── interfaces/           # ⭐⭐⭐ Production-ready interfaces
│   └── shared/               # Mock interfaces for examples
├── pyproject.toml
└── uv.lock
```

## Examples

See the [examples/](examples/) directory for runnable code:

| Level | Example | Description |
|-------|---------|-------------|
| ⭐ | [quickstart/01_basic_usage.py](examples/quickstart/01_basic_usage.py) | Basic DataStore and DataObject setup |
| ⭐ | [quickstart/02_multiple_interfaces.py](examples/quickstart/02_multiple_interfaces.py) | Using multiple interfaces per store |
| ⭐⭐ | [patterns/01_factory_pattern.py](examples/patterns/01_factory_pattern.py) | Config-driven DataStore creation |
| ⭐⭐ | [patterns/02_catalog_pattern.py](examples/patterns/02_catalog_pattern.py) | Data discovery with FileCatalog |
| ⭐⭐ | [patterns/03_medallion_pipeline.py](examples/patterns/03_medallion_pipeline.py) | Bronze → Silver → Gold pipeline |
| ⭐⭐⭐ | [interfaces/](examples/interfaces/) | Production interfaces (S3, Delta, Redshift) |

## Dependencies

**Core:**
- Python >= 3.11
- [makefun](https://pypi.org/project/makefun/) >= 1.16.0 - Dynamic function creation

**Development:**
- black - Code formatting
- isort - Import sorting
- ruff - Linting
- pre-commit - Git hooks

**Examples** (optional):
- boto3 - AWS SDK
- pyspark - Apache Spark
- delta-spark - Delta Lake
- redshift-connector - Amazon Redshift

## Development

```bash
# Install development dependencies
uv sync --group dev

# Run linting
uv run ruff check .

# Format code
uv run black .
uv run isort .
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Author

Sandeep Evani - [sandeepevani01@gmail.com](mailto:sandeepevani01@gmail.com)

## Feedback

If you have any feedback, please reach out at [evani.sandeep.sh@gmail.com](mailto:evani.sandeep.sh@gmail.com)
