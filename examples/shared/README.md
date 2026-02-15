# Shared Utilities

Mock interfaces that allow examples to run without external dependencies.

## Mock Interfaces

| Interface | Simulates | Production Replacement |
|-----------|-----------|----------------------|
| `MockStorageInterface` | Simple object storage | [S3Boto3Interface](../interfaces/s3/s3_boto3.py) |
| `MockSparkInterface` | Distributed processing | [S3SparkInterface](../interfaces/s3/spark.py) |
| `MockDeltaInterface` | ACID table operations | [S3DeltaInterface](../interfaces/s3/deltalake.py) |

## Why Mocks?

- **No credentials required** - Run examples without AWS/GCP setup
- **Fast execution** - No network calls, everything in-memory
- **Learning focused** - Understand DAO patterns without infrastructure

## Replacing with Production Interfaces

```python
# Development
from examples.shared.mock_interfaces import MockStorageInterface

# Production
from examples.interfaces.s3.s3_boto3 import S3Boto3Interface
```

See [interfaces/](../interfaces/) for production-ready implementations.
