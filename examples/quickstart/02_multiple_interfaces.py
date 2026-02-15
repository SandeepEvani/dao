#!/usr/bin/env python3
"""
Example: Multiple Interfaces
============================

Demonstrates attaching multiple interface classes to a single DataStore
and selecting which interface to use for specific operations.

Use Case:
    - Use boto3 interface for small files (fast, simple)
    - Use Spark interface for large datasets (distributed processing)
    - Use Delta interface for ACID transactions

Prerequisites:
    - DAO library installed (`uv sync`)

Usage:
    python examples/quickstart/02_multiple_interfaces.py
"""

from dao import DataAccessObject
from dao.data_object import DataObject
from dao.data_store import DataStore

# For examples, we use mock interfaces (no AWS/Spark needed)
# In production, use real interfaces from examples/interfaces/
from examples.shared.mock_interfaces import MockSparkInterface, MockStorageInterface


def main():
    # Create a DataStore with multiple interfaces
    bronze_store = DataStore(name="bronze_layer")

    # Primary interface: Simple storage (like boto3 for S3)
    bronze_store.set_interface_class(
        class_=MockStorageInterface,
        args={"bucket": "ecommerce-bronze"},
        primary=True,
    )

    # Secondary interface: Spark for large-scale processing
    bronze_store.set_interface_class(
        class_=MockSparkInterface,
        args={"bucket": "ecommerce-bronze"},
        primary=False,
    )

    # Create DataObjects
    small_file = DataObject(name="config_file", data_store=bronze_store, key="config/settings.json")
    large_dataset = DataObject(name="events_raw", data_store=bronze_store, key="events/2025-01/")

    dao = DataAccessObject()

    # Use PRIMARY interface (default) - good for small files
    dao.write(data={"setting": "value"}, data_object=small_file)

    # Use SECONDARY interface (Spark) - specify with dao_interface_class
    dao.write(
        data={"events": [1, 2, 3]},
        data_object=large_dataset,
        dao_interface_class="MockSparkInterface",
    )


if __name__ == "__main__":
    main()
