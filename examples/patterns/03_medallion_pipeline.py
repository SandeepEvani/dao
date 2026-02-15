#!/usr/bin/env python3
"""
Example: Medallion Architecture Pipeline
========================================

Demonstrates implementing Bronze → Silver → Gold data flow using DAO.

The Medallion Architecture:
    - Bronze: Raw data landing zone (as-is from source)
    - Silver: Cleansed, validated, deduplicated data
    - Gold: Business-ready aggregated data

Prerequisites:
    - DAO library installed (`uv sync`)

Usage:
    python examples/patterns/03_medallion_pipeline.py
"""

from dao import DataAccessObject
from dao.catalog.file import FileCatalog


def main():
    # Initialize catalog and DAO
    catalog = FileCatalog(
        data_store_config_location="examples/patterns/config/data_stores.json",
        data_object_config_location="examples/patterns/config/data_objects.json",
    )
    dao = DataAccessObject()

    # Get DataObjects for each layer
    customers_raw = catalog.get_data_object("bronze.customers_raw")
    customers_clean = catalog.get_data_object("silver.customers_clean")
    customer_360 = catalog.get_data_object("gold.customer_360")

    # BRONZE: Ingest raw data (as-is from source)
    raw_data = {"customer_id": "C001", "name": "Alice", "email": "alice@example.com"}
    dao.write(data=raw_data, data_object=customers_raw)

    # SILVER: Cleanse and validate data
    bronze_data = dao.read(data_object=customers_raw)
    clean_data = {**bronze_data, "is_valid": True, "processed_at": "2025-01-15"}
    dao.write(data=clean_data, data_object=customers_clean)

    # GOLD: Aggregate for analytics
    silver_data = dao.read(data_object=customers_clean)
    gold_data = {
        "customer_id": silver_data["customer_id"],
        "lifetime_value": 1500.00,
        "segment": "premium",
    }
    dao.write(data=gold_data, data_object=customer_360)

    # Verify final result
    result = dao.read(data_object=customer_360)  # noqa


if __name__ == "__main__":
    main()
