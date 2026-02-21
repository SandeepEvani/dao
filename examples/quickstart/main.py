# Quickstart Example
# Demonstrates basic DAO usage with a single Redshift data store.
#
# This example shows how to:
#   1. Initialize the DAO with a data stores config
#   2. Create data objects (tables) from a catalog
#   3. Use dao.write(), dao.read(), and dao.run()

import os
import sys

# Add the quickstart directory to the path so data_classes can be imported
sys.path.insert(0, os.path.dirname(__file__))

from catalog import Catalog
from dao import dao


def main():
    # Resolve paths relative to this file
    base_dir = os.path.dirname(__file__)
    data_stores_path = os.path.join(base_dir, "data_stores.json")
    catalog_path = os.path.join(base_dir, "catalog.json")

    # Step 1: Initialize the DAO with the data stores config
    dao.init(data_stores_path)

    # Step 2: Create a catalog and get a table object
    catalog = Catalog(catalog_path)
    customers = catalog.get("warehouse.customers")
    orders = catalog.get("warehouse.orders")

    print("=== Write Example ===")
    # Step 3a: Write data — DAO routes to Redshift.write_dataframe
    result = dao.write(data={"customer_id": 1, "name": "Alice"}, data_object=customers)
    print(f"Write result: {result}\n")

    print("=== Read Example ===")
    # Step 3b: Read data — DAO routes to Redshift.read_table
    result = dao.read(data_object=customers)
    print(f"Read result: {result}\n")

    print("=== Run Example ===")
    # Step 3c: Run a query — DAO routes to Redshift.run_query
    result = dao.run(data_object=orders, query="SELECT * FROM orders LIMIT 10")
    print(f"Run result: {result}\n")


if __name__ == "__main__":
    main()

