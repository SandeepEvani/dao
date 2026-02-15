# DAO Examples

Learn how to use the DAO library through practical, runnable examples.

## Quick Links

| Level | Folder | Description | Time |
|-------|--------|-------------|------|
| ⭐ | [quickstart/](quickstart/) | Get started in 5 minutes | 5 min |
| ⭐⭐ | [patterns/](patterns/) | Factory, Catalog, Medallion patterns | 15-30 min |
| ⭐⭐⭐ | [advanced/](advanced/) | Specialized DataObjects, production pipelines | 30+ min |
| 📚 | [interfaces/](interfaces/) | Production-ready interfaces & data objects | Reference |

## Running Examples

```bash
# Install dependencies
uv sync

# Run quickstart examples (no external deps)
python examples/quickstart/01_basic_usage.py
python examples/quickstart/02_multiple_interfaces.py

# Run pattern examples (no external deps)
python examples/patterns/01_factory_pattern.py
python examples/patterns/02_catalog_pattern.py

# Run advanced examples (requires AWS/Spark)
uv sync --group example
python examples/advanced/01_s3_file_operations.py
```

## Example Overview

### Quickstart (5 minutes)

Learn the three building blocks with mock interfaces:

```python
from dao import DataAccessObject
from dao.data_store import DataStore
from dao.data_object import DataObject

store = DataStore("my_store")
store.set_interface_class(class_=MyInterface, primary=True)
data = DataObject("my_data", store)

dao = DataAccessObject()
dao.read(data_object=data)
dao.write(data=my_data, data_object=data)
```

### Patterns (Production-Ready)

Scalable approaches for real-world systems:

| Pattern | Use Case |
|---------|----------|
| **Factory** | Config-driven DataStore creation |
| **Catalog** | Data discovery & governance |
| **Medallion** | Bronze → Silver → Gold pipelines |

### Advanced (Production)

Specialized DataObjects with production interfaces:

| Example | DataObject | Interface |
|---------|------------|-----------|
| S3 Files | `S3FileObject` | `S3Boto3Interface` |
| Delta Tables | `S3DeltaObject` | `S3DeltaInterface` |
| Multi-Format | Multiple | Multiple |

## Directory Structure

```
examples/
├── README.md
├── quickstart/               # ⭐ Getting started
│   ├── 01_basic_usage.py
│   └── 02_multiple_interfaces.py
├── patterns/                 # ⭐⭐ Production patterns
│   ├── 01_factory_pattern.py
│   ├── 02_catalog_pattern.py
│   ├── 03_medallion_pipeline.py
│   └── config/
├── advanced/                 # ⭐⭐⭐ Specialized data objects
│   ├── 01_s3_file_operations.py
│   ├── 02_delta_lake_tables.py
│   └── 03_multi_format_pipeline.py
├── interfaces/               # 📚 Production interfaces
│   ├── s3/
│   ├── redshift/
│   └── data_objects/         # Specialized DataObject models
└── shared/                   # Mock interfaces
    └── mock_interfaces.py
```

## Prerequisites

- Python 3.11+
- `uv sync` for quickstart/patterns
- `uv sync --group example` for advanced examples
