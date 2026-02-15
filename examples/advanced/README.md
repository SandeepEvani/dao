# Advanced Examples

Production-ready patterns using specialized DataObjects, S3 interfaces, and real storage backends.

## Directory Structure

```
advanced/
├── s3/                  # S3 interface implementations
├── data_objects/        # Specialized DataObject models
├── 01_s3_file_operations.py
├── 02_delta_lake_tables.py
└── 03_multi_format_pipeline.py
```

## Examples

| File | Description | Prerequisites |
|------|-------------|---------------|
| [01_s3_file_operations.py](01_s3_file_operations.py) | S3 file upload/download with S3FileObject | boto3, AWS credentials |
| [02_delta_lake_tables.py](02_delta_lake_tables.py) | Delta Lake CRUD with S3DeltaObject | pyspark, delta-spark |
| [03_multi_format_pipeline.py](03_multi_format_pipeline.py) | Pipeline using multiple table formats | pyspark, delta-spark |

## S3 Interfaces (`s3/`)

| Interface | Description | Dependencies |
|-----------|-------------|--------------|
| `S3Boto3Interface` | Direct S3 operations via boto3 | boto3 |
| `S3SparkInterface` | S3 operations via PySpark | pyspark |
| `S3DeltaInterface` | Delta Lake tables on S3 | delta-spark, pyspark |
| `S3HudiInterface` | Apache Hudi tables on S3 | pyspark |
| `S3IcebergInterface` | Apache Iceberg tables on S3 | pyspark |

## Specialized Data Objects (`data_objects/`)

Extend the base `DataObject` with storage-specific properties:

| Data Object | Use Case | Key Properties |
|-------------|----------|----------------|
| `S3FileObject` | Single S3 file | `prefix`, `suffix` → computed `key` |
| `S3DirectoryObject` | S3 prefix/directory | `prefix` → computed `key` |
| `S3DeltaObject` | Delta Lake table | `partition_keys`, `cluster_keys` |
| `S3HudiObject` | Apache Hudi table | `record_key`, `precombine_key` |
| `S3IcebergObject` | Apache Iceberg table | (inherits from S3TableObject) |

## Key Concepts

### Specialized Data Objects

Instead of generic `DataObject`, use storage-specific models:

```python
# Generic (works but no computed properties)
from dao.data_object import DataObject
obj = DataObject("myfile", store, key="path/to/file.json")

# Specialized (computed key, type hints for interfaces)
from examples.advanced.data_objects import S3FileObject
obj = S3FileObject("myfile", store, prefix="path/to", identifier="file", suffix="json")
# obj.key → "path/to/file.json" (computed automatically)
```

### Interface + DataObject Pairing

| Data Object | Interface | Use Case |
|-------------|-----------|----------|
| `S3FileObject` | `S3Boto3Interface` | Single file operations |
| `S3DirectoryObject` | `S3SparkInterface` | Batch processing |
| `S3DeltaObject` | `S3DeltaInterface` | ACID transactions |
| `S3HudiObject` | `S3HudiInterface` | Incremental upserts |

## Running Examples

```bash
# Install production dependencies
uv sync --group example

# Set AWS credentials (for S3 examples)
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret

# Run examples
python examples/advanced/01_s3_file_operations.py
python examples/advanced/02_delta_lake_tables.py
```

## Prerequisites

- `uv sync --group example` for dependencies
- AWS credentials configured
- For Spark examples: Java 11+ installed
