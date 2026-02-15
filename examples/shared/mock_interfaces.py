"""
Mock Interfaces for Examples
============================

These mock interfaces allow examples to run without external dependencies
like AWS credentials, Spark clusters, or database connections.

For production-ready interfaces, see examples/interfaces/
"""

from typing import Any, Dict, Optional


class MockStorageInterface:
    """Mock storage interface simulating simple object storage (like S3 with boto3)."""

    _storage: Dict[str, Any] = {}

    def __init__(self, bucket: str, region: str = "us-east-1"):
        self.bucket = bucket
        self.region = region

    def read_data(self, data_object, **kwargs) -> Any:
        """Read data from mock storage."""
        key = f"{self.bucket}/{getattr(data_object, 'key', data_object.name)}"
        return self._storage.get(key, {"message": "No data found"})

    def write_data(self, data, data_object, **kwargs) -> bool:
        """Write data to mock storage."""
        key = f"{self.bucket}/{getattr(data_object, 'key', data_object.name)}"
        self._storage[key] = data
        return True

    def list_objects(self, data_object, **kwargs) -> list:
        """List objects in mock storage."""
        prefix = f"{self.bucket}/{getattr(data_object, 'key', data_object.name)}"
        return [k for k in self._storage.keys() if k.startswith(prefix)]

    def delete_data(self, data_object, **kwargs) -> bool:
        """Delete data from mock storage."""
        key = f"{self.bucket}/{getattr(data_object, 'key', data_object.name)}"
        if key in self._storage:
            del self._storage[key]
            return True
        return False


class MockSparkInterface:
    """Mock Spark interface simulating distributed data processing."""

    _storage: Dict[str, Any] = {}

    def __init__(self, bucket: str, spark_config: Optional[Dict] = None):
        self.bucket = bucket
        self.spark_config = spark_config or {}

    def read_dataframe(self, data_object, format: str = "parquet", **kwargs) -> Dict:
        """Read data as a DataFrame (simulated)."""
        key = f"{self.bucket}/{getattr(data_object, 'path', data_object.name)}"
        return self._storage.get(key, {"columns": [], "rows": []})

    def write_dataframe(self, data, data_object, format: str = "parquet", mode: str = "overwrite", **kwargs) -> bool:
        """Write data as a DataFrame (simulated)."""
        key = f"{self.bucket}/{getattr(data_object, 'path', data_object.name)}"
        self._storage[key] = data
        return True


class MockDeltaInterface:
    """Mock Delta Lake interface simulating ACID table operations."""

    _tables: Dict[str, Dict] = {}

    def __init__(self, bucket: str, catalog: str = "default"):
        self.bucket = bucket
        self.catalog = catalog

    def read_table(self, data_object, version: Optional[int] = None, **kwargs) -> Dict:
        """Read a Delta table."""
        table_name = getattr(data_object, "table", data_object.name)
        return self._tables.get(table_name, {"data": [], "version": 0})

    def write_table(self, data, data_object, mode: str = "overwrite", **kwargs) -> bool:
        """Write to a Delta table."""
        table_name = getattr(data_object, "table", data_object.name)
        current = self._tables.get(table_name, {"data": [], "version": 0})
        self._tables[table_name] = {"data": data, "version": current["version"] + 1}
        return True

    def upsert_table(self, data, data_object, merge_key: str, **kwargs) -> bool:
        """Upsert (merge) data into a Delta table."""
        table_name = getattr(data_object, "table", data_object.name)
        current = self._tables.get(table_name, {"data": [], "version": 0})
        current["data"] = data
        current["version"] += 1
        self._tables[table_name] = current
        return True
