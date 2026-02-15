#!/usr/bin/env python3
"""
Example: Catalog Pattern
========================

Demonstrates using FileCatalog for complete data discovery and governance.
Both DataStores AND DataObjects are defined in configuration files.

Benefits over Factory Pattern:
    - Complete metadata catalog for all datasets
    - Self-service data discovery
    - Centralized governance (owners, descriptions, schemas)
    - DataObjects retrieved by fully-qualified name

When to Use:
    - You have 10+ DataStores and DataObjects
    - Multiple teams need to discover and access data
    - You need data governance and documentation

Prerequisites:
    - DAO library installed (`uv sync`)

Usage:
    python examples/patterns/02_catalog_pattern.py
"""

from dao import DataAccessObject
from dao.catalog.file import FileCatalog


def main():
    # Initialize the Catalog from JSON configuration files
    catalog = FileCatalog(
        data_store_config_location="examples/patterns/config/data_stores.json",
        data_object_config_location="examples/patterns/config/data_objects.json",
    )

    # Discover available data (useful for data governance)
    stores = catalog.get_data_store_configs()  # noqa
    objects = catalog.get_data_object_configs()  # noqa

    # Get DataObjects by fully-qualified name ("store.object")
    # No need to manually create DataStore or DataObject instances!
    customers_raw = catalog.get_data_object("bronze.customers_raw")
    orders_raw = catalog.get_data_object("bronze.orders_raw")  # noqa
    customers_clean = catalog.get_data_object("silver.customers_clean")
    customer_360 = catalog.get_data_object("gold.customer_360")

    # Use DAO for operations
    dao = DataAccessObject()

    # Write to bronze
    dao.write(
        data={"customer_id": "C001", "name": "Alice", "email": "alice@example.com"},
        data_object=customers_raw,
    )

    # Read from bronze
    result = dao.read(data_object=customers_raw)  # noqa

    # Write to silver (cleansed data)
    dao.write(data={"customer_id": "C001", "name": "Alice", "valid": True}, data_object=customers_clean)

    # Write to gold (analytics-ready)
    dao.write(
        data={"customer_id": "C001", "lifetime_value": 1500.00, "segment": "premium"},
        data_object=customer_360,
    )

    # Check if data exists
    exists = catalog.check_data_object_exists("bronze.customers_raw")  # True # noqa
    not_exists = catalog.check_data_object_exists("bronze.nonexistent")  # False # noqa


if __name__ == "__main__":
    main()
