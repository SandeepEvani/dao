#!/usr/bin/env python3
"""
Example: Basic DAO Usage
========================

Demonstrates the fundamental building blocks of the DAO library:
- Creating a DataStore (represents a storage layer)
- Attaching an Interface class (handles actual I/O)
- Creating a DataObject (represents a dataset within the store)
- Using the DataAccessObject singleton for operations

Prerequisites:
    - DAO library installed (`uv sync`)

Usage:
    python examples/quickstart/01_basic_usage.py
"""

from dao import DataAccessObject
from dao.data_object import DataObject
from dao.data_store import DataStore

# For examples, we use mock interfaces (no AWS/Spark needed)
# In production, use real interfaces from examples/interfaces/
from examples.shared.mock_interfaces import MockStorageInterface


def main():
    # Step 1: Create a DataStore
    # Represents a storage layer (S3 bucket, database schema, etc.)
    bronze_store = DataStore(name="bronze_layer")

    # Step 2: Attach an Interface Class
    # The interface knows HOW to read/write to the storage
    bronze_store.set_interface_class(
        class_=MockStorageInterface,
        args={"bucket": "ecommerce-bronze"},
        primary=True,
    )

    # Step 3: Create a DataObject
    # Represents a specific dataset within the DataStore
    customers_raw = DataObject(
        name="customers_raw",
        data_store=bronze_store,
        key="customers/2025-01-15.csv",
    )

    # Step 4: Get the DAO singleton and perform operations
    dao = DataAccessObject()

    # Write data
    sample_data = {"customer_id": "C001", "name": "Alice", "email": "alice@example.com"}
    dao.write(data=sample_data, data_object=customers_raw)

    # Read data back
    result = dao.read(data_object=customers_raw)
    print(f"Read result: {result}")


if __name__ == "__main__":
    main()
