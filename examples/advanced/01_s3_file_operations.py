#!/usr/bin/env python3
"""
Example: S3 File Operations with Specialized Data Objects
=========================================================

Demonstrates using S3FileObject and S3DirectoryObject with S3Boto3Interface
for production S3 operations.

Key Concepts:
    - S3FileObject: Single file with computed key from prefix/identifier/suffix
    - S3DirectoryObject: Collection of files under a prefix
    - S3Boto3Interface: Direct S3 operations via boto3

Prerequisites:
    - uv sync --group example
    - AWS credentials configured

Usage:
    python examples/advanced/01_s3_file_operations.py
"""

from io import BytesIO

from dao import DataAccessObject
from dao.data_store import DataStore
from examples.advanced.data_objects import S3DirectoryObject, S3FileObject

# Production interface and data objects
from examples.advanced.s3.s3_boto3 import S3Boto3Interface


def main():
    # Create DataStore with production S3 interface
    raw_store = DataStore(name="raw_zone")
    raw_store.set_interface_class(
        class_=S3Boto3Interface,
        args={"bucket": "my-data-lake-raw"},  # Replace with your bucket
        primary=True,
    )

    # S3FileObject: Single file with computed key
    # key = prefix/identifier.suffix → "config/app_settings.json"
    config_file = S3FileObject(
        name="app_config",
        data_store=raw_store,
        prefix="config",
        identifier="app_settings",
        suffix="json",
    )

    # S3FileObject: CSV file in nested path
    # key = prefix/identifier.suffix → "data/customers/2025-01-15.csv"
    daily_export = S3FileObject(
        name="daily_customers",
        data_store=raw_store,
        prefix="data/customers",
        identifier="2025-01-15",
        suffix="csv",
    )

    # S3DirectoryObject: List/process multiple files under prefix
    # key = prefix/identifier → "data/customers"
    customers_dir = S3DirectoryObject(
        name="all_customers",
        data_store=raw_store,
        prefix="data",
        identifier="customers",
    )

    dao = DataAccessObject()

    # Write JSON config file
    config_data = BytesIO(b'{"app": "dao-example", "version": "1.0"}')
    dao.write(data=config_data, data_object=config_file)

    # Read config file back
    response = dao.read(data_object=config_file)
    content = response.read()  # StreamingBody → bytes # noqa

    # List all files in customers directory
    files = dao.list(data_object=customers_dir)  # noqa

    # Delete a file
    dao.delete(data_object=daily_export)


if __name__ == "__main__":
    main()
