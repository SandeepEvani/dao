# Shared utilities for examples
from .mock_interfaces import (
    MockDeltaInterface,
    MockSparkInterface,
    MockStorageInterface,
)

__all__ = ["MockStorageInterface", "MockSparkInterface", "MockDeltaInterface"]
