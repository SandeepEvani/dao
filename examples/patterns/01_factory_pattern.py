#!/usr/bin/env python3
"""
Example: Factory Pattern
========================

Demonstrates using DataStoreFactory for configuration-driven DataStore creation.
Instead of manually creating DataStores and attaching interfaces, you define
everything in a configuration dictionary or JSON file.

Benefits over Basic Pattern:
    - Configuration separated from code
    - Easy to add new stores without code changes
    - Supports multiple interfaces per store

When to Use:
    - You have 3-10 DataStores
    - You want externalized configuration
    - You need different interfaces for different environments

Prerequisites:
    - DAO library installed (`uv sync`)

Usage:
    python examples/patterns/01_factory_pattern.py
"""

from dao import DataAccessObject
from dao.data_object import DataObject
from dao.data_store import DataStoreFactory


def main():
    # Define DataStore configuration (in production, load from JSON file)
    data_store_config = {
        "bronze": {
            "class": "MockStorageInterface",
            "module": "examples.shared.mock_interfaces",
            "args": {"bucket": "ecommerce-bronze", "region": "us-east-1"},
            "properties": {"layer": "raw"},
            "additional_interfaces": [
                {
                    "class": "MockSparkInterface",
                    "module": "examples.shared.mock_interfaces",
                    "args": {"bucket": "ecommerce-bronze"},
                },
            ],
        },
        "silver": {
            "class": "MockSparkInterface",
            "module": "examples.shared.mock_interfaces",
            "args": {"bucket": "ecommerce-silver"},
            "properties": {"layer": "cleansed"},
        },
        "gold": {
            "class": "MockDeltaInterface",
            "module": "examples.shared.mock_interfaces",
            "args": {"bucket": "ecommerce-gold", "catalog": "analytics"},
            "properties": {"layer": "curated"},
        },
    }

    # Load configuration into factory
    factory = DataStoreFactory()
    factory.load_configs(data_store_config)

    # Get DataStores from factory (lazy initialization)
    bronze_store = factory.get("bronze")
    silver_store = factory.get("silver")
    gold_store = factory.get("gold")

    # Create DataObjects
    customers_raw = DataObject(name="customers_raw", data_store=bronze_store, key="customers/2025-01-15.csv")
    customers_clean = DataObject(name="customers_clean", data_store=silver_store, path="customers_clean")
    customer_360 = DataObject(name="customer_360", data_store=gold_store, table="customer_360")

    # Use DAO for operations
    dao = DataAccessObject()

    # Write to bronze (uses primary interface)
    dao.write(data={"customer_id": "C001", "name": "Alice"}, data_object=customers_raw)

    # Write to bronze using Spark (specify interface)
    dao.write(
        data={"large_batch": True},
        data_object=customers_raw,
        dao_interface_class="MockSparkInterface",
    )

    # Write to silver and gold
    dao.write(data={"customer_id": "C001", "valid": True}, data_object=customers_clean)
    dao.write(data={"customer_id": "C001", "lifetime_value": 1500.00}, data_object=customer_360)


if __name__ == "__main__":
    main()
