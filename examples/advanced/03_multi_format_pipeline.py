#!/usr/bin/env python3
"""
Example: Multi-Format Data Pipeline
====================================

Demonstrates a production pipeline using different DataObject types and
interfaces for each stage of processing.

Pipeline Flow:
    1. S3FileObject + S3Boto3Interface → Ingest raw JSON files
    2. S3DirectoryObject + S3SparkInterface → Batch process to Parquet
    3. S3DeltaObject + S3DeltaInterface → ACID-compliant analytics tables

Key Concepts:
    - Different DataObject types for different storage patterns
    - Interface selection based on processing needs
    - Medallion architecture with appropriate tools per layer

Prerequisites:
    - uv sync --group example
    - AWS credentials configured
    - PySpark with Delta Lake

Usage:
    python examples/advanced/03_multi_format_pipeline.py
"""

from pyspark.sql import SparkSession

from dao import DataAccessObject
from dao.data_store import DataStore

# Specialized data objects
from examples.advanced.data_objects import (
    S3DeltaObject,
    S3DirectoryObject,
    S3FileObject,
)
from examples.advanced.s3.deltalake import S3DeltaInterface

# Production interfaces
from examples.advanced.s3.s3_boto3 import S3Boto3Interface
from examples.advanced.s3.spark import S3SparkInterface


def main():
    # Initialize Spark
    spark = (
        SparkSession.builder.appName("DAO Multi-Format Pipeline")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0")
        .getOrCreate()
    )

    # --- BRONZE LAYER: Raw file ingestion ---
    # Use S3Boto3Interface for simple file operations
    bronze_store = DataStore(name="bronze")
    bronze_store.set_interface_class(
        class_=S3Boto3Interface,
        args={"bucket": "my-lake-bronze"},
        primary=True,
    )

    # Raw JSON files land here
    raw_events = S3FileObject(  # noqa
        name="raw_events",
        data_store=bronze_store,
        prefix="events/2025-01-15",
        identifier="batch_001",
        suffix="json",
    )

    # --- SILVER LAYER: Batch processing with Spark ---
    # Use S3SparkInterface for distributed processing
    silver_store = DataStore(name="silver")
    silver_store.set_interface_class(
        class_=S3SparkInterface,
        args={"bucket": "my-lake-silver", "prefix": "processed"},
        primary=True,
    )

    # Process multiple files into Parquet
    processed_events = S3DirectoryObject(
        name="processed_events",
        data_store=silver_store,
        prefix="events",
        identifier="events_clean",
    )

    # --- GOLD LAYER: Analytics with Delta Lake ---
    # Use S3DeltaInterface for ACID transactions
    gold_store = DataStore(name="gold")
    gold_store.set_interface_class(
        class_=S3DeltaInterface,
        args={"bucket": "my-lake-gold", "prefix": "analytics"},
        primary=True,
    )

    # Delta table with partitioning
    events_analytics = S3DeltaObject(
        name="events_analytics",
        data_store=gold_store,
        identifier="events_daily",
        partition_keys="event_date",
    )

    dao = DataAccessObject()

    # BRONZE → SILVER: Read raw, transform, write as Parquet
    raw_df = spark.read.json("s3a://my-lake-bronze/events/2025-01-15/")
    clean_df = raw_df.dropDuplicates().filter("event_id IS NOT NULL")
    dao.write(data=clean_df, data_object=processed_events, format="parquet")

    # SILVER → GOLD: Read Parquet, aggregate, write as Delta
    silver_df = dao.read(data_object=processed_events, format="parquet")
    gold_df = silver_df.groupBy("event_date", "region").agg({"amount": "sum"})
    dao.write(data=gold_df, data_object=events_analytics, mode="overwrite")

    # Query Gold layer with Delta features
    analytics_df = dao.read(data_object=events_analytics)  # noqa

    spark.stop()


if __name__ == "__main__":
    main()
