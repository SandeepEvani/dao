# Advanced Example — Multiple Interfaces & Secondary Interface Switching
#
# This example demonstrates:
#   1. A data store with primary + secondary interfaces (S3, Delta, Hudi)
#   2. Switching between interfaces at runtime using dao_interface_class
#   3. Mixing multiple data stores (raw + warehouse) in a single DAO
#   4. Using lazy initialization

import os
import sys

# Add the advanced directory to the path so data_classes can be imported
sys.path.insert(0, os.path.dirname(__file__))

from catalog import Catalog
from dao import dao


def main():
    # Resolve paths relative to this file
    base_dir = os.path.dirname(__file__)
    data_stores_path = os.path.join(base_dir, "data_stores.json")
    catalog_path = os.path.join(base_dir, "catalog.json")

    # Initialize the DAO (try lazy=True to defer class initialization)
    dao.init(data_stores_path, lazy=True)

    catalog = Catalog(catalog_path)

    # -------------------------------------------------------------------------
    # Example 1: Writing to the 'raw' data store using the primary interface (S3)
    # -------------------------------------------------------------------------
    print("=" * 60)
    print("Example 1: Write via primary interface (S3)")
    print("=" * 60)

    customer = catalog.get("raw.customer")
    result = dao.write(data="customer_payload", data_object=customer)
    print(f"Result: {result}\n")

    # -------------------------------------------------------------------------
    # Example 2: Writing via secondary interface — Hudi
    # Pass dao_interface_class to target the Hudi interface
    # -------------------------------------------------------------------------
    print("=" * 60)
    print("Example 2: Write via secondary interface (Hudi)")
    print("=" * 60)

    result = dao.write(data="customer_payload", data_object=customer, dao_interface_class="Hudi")
    print(f"Result: {result}\n")

    # -------------------------------------------------------------------------
    # Example 3: Writing via secondary interface — Delta
    # -------------------------------------------------------------------------
    print("=" * 60)
    print("Example 3: Write via secondary interface (Delta)")
    print("=" * 60)

    result = dao.write(data="customer_payload", data_object=customer, dao_interface_class="Delta")
    print(f"Result: {result}\n")

    # -------------------------------------------------------------------------
    # Example 4: Reading from different interfaces
    # -------------------------------------------------------------------------
    print("=" * 60)
    print("Example 4: Read via primary interface (S3)")
    print("=" * 60)

    result = dao.read(data_object=customer)
    print(f"Result: {result}\n")

    print("=" * 60)
    print("Example 5: Read via secondary interface (Delta)")
    print("=" * 60)

    result = dao.read(data_object=customer, dao_interface_class="Delta")
    print(f"Result: {result}\n")

    # -------------------------------------------------------------------------
    # Example 6: Using a completely different data store (warehouse / Redshift)
    # -------------------------------------------------------------------------
    print("=" * 60)
    print("Example 6: Write to Redshift (warehouse data store)")
    print("=" * 60)

    dim_customer = catalog.get("warehouse.dim_customer")
    result = dao.write(data="dim_customer_data", data_object=dim_customer)
    print(f"Result: {result}\n")

    print("=" * 60)
    print("Example 7: Run query on Redshift")
    print("=" * 60)

    result = dao.run(data_object=dim_customer, query="SELECT * FROM dim_customer LIMIT 5")
    print(f"Result: {result}\n")


if __name__ == "__main__":
    main()

