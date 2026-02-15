#!/usr/bin/env python3
"""
Example: Delta Lake Tables with S3DeltaObject
==============================================

Demonstrates using S3DeltaObject with S3DeltaInterface for ACID-compliant
table operations on S3.

Key Concepts:
    - S3DeltaObject: Delta table with partition_keys and cluster_keys
    - S3DeltaInterface: Delta Lake operations (create, read, upsert, time travel)
    - Table properties are used by the interface for optimized writes

Prerequisites:
    - uv sync --group example
    - AWS credentials configured
    - PySpark with Delta Lake

Usage:
    python examples/advanced/02_delta_lake_tables.py
"""

from pyspark.sql import SparkSession

from dao import DataAccessObject
from dao.data_store import DataStore
from examples.advanced.data_objects import S3DeltaObject

# Production interface and data objects
from examples.advanced.s3.deltalake import S3DeltaInterface


def main():
    # Initialize Spark with Delta Lake
    spark = (
        SparkSession.builder.appName("DAO Delta Example")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

    # Create DataStore with Delta interface
    silver_store = DataStore(name="silver_zone")
    silver_store.set_interface_class(
        class_=S3DeltaInterface,
        args={"bucket": "my-data-lake-silver", "prefix": "tables"},
        primary=True,
    )

    # S3DeltaObject: Partitioned Delta table
    # Interface uses partition_keys for optimized writes
    events_table = S3DeltaObject(
        name="user_events",
        data_store=silver_store,
        identifier="user_events",
        partition_keys="event_date,region",  # Used by interface for partitioning
        cluster_keys=None,
    )

    # S3DeltaObject: Clustered (Z-ordered) Delta table
    # Interface uses cluster_keys for data clustering
    customers_table = S3DeltaObject(  # noqa
        name="customers",
        data_store=silver_store,
        identifier="customers",
        partition_keys=None,
        cluster_keys="customer_id",  # Used by interface for Z-ordering
    )

    dao = DataAccessObject()

    # Create sample DataFrame
    events_df = spark.createDataFrame(
        [
            {"event_id": "E001", "customer_id": "C001", "event_date": "2025-01-15", "region": "US", "amount": 100.0},
            {"event_id": "E002", "customer_id": "C002", "event_date": "2025-01-15", "region": "EU", "amount": 200.0},
        ]
    )

    # Write DataFrame to Delta table (uses partition_keys from S3DeltaObject)
    dao.write(data=events_df, data_object=events_table, mode="overwrite")

    # Read Delta table
    result_df = dao.read(data_object=events_table)  # noqa

    # Read specific columns
    result_df = dao.read(data_object=events_table, columns=["event_id", "amount"])  # noqa

    # Upsert (merge) new data
    updates_df = spark.createDataFrame(
        [
            {"event_id": "E001", "customer_id": "C001", "event_date": "2025-01-15", "region": "US", "amount": 150.0},
            {"event_id": "E003", "customer_id": "C003", "event_date": "2025-01-16", "region": "US", "amount": 300.0},
        ]
    )
    dao.upsert(data=updates_df, data_object=events_table, merge_key="event_id")

    spark.stop()


if __name__ == "__main__":
    main()
