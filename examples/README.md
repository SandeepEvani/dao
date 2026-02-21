# DAO — Examples

This directory contains runnable examples demonstrating how to use the DAO module.

## Structure

```
examples/
├── quickstart/          # Basic single-datastore example (Redshift)
│   ├── data_classes/
│   │   └── redshift.py  # Redshift interface class
│   ├── catalog.json     # Table catalog config
│   ├── catalog.py       # Catalog helper class
│   ├── data_stores.json # Data stores config (single Redshift store)
│   └── main.py          # Entry point
│
├── advanced/            # Multi-interface example (S3 + Delta + Hudi + Redshift)
│   ├── data_classes/
│   │   ├── s3.py        # S3 interface (primary for 'raw')
│   │   ├── delta.py     # Delta interface (secondary for 'raw')
│   │   ├── hudi.py      # Hudi interface (secondary for 'raw')
│   │   └── redshift.py  # Redshift interface (primary for 'warehouse')
│   ├── catalog.json     # Table catalog config
│   ├── catalog.py       # Catalog helper class
│   ├── data_stores.json # Data stores config (raw + warehouse)
│   └── main.py          # Entry point
│
└── README.md            # This file
```

---

## Quickstart Example

A minimal example with a **single data store** (`warehouse`) backed by a single **Redshift** interface class.

### What it demonstrates

- Initializing the DAO with a data stores config
- Creating `TableObject` instances via a `Catalog`
- Using `dao.write()`, `dao.read()`, and `dao.run()` with automatic routing

### How to run

```bash
cd examples/quickstart
python main.py
```

### Files

| File                        | Purpose                                                  |
|-----------------------------|----------------------------------------------------------|
| `data_classes/redshift.py`  | Interface class with `write_dataframe`, `read_table`, and `run_query` methods |
| `data_stores.json`          | Registers a single `warehouse` data store using the `Redshift` class |
| `catalog.json`              | Defines `customers` and `orders` tables in `warehouse`   |
| `catalog.py`                | Helper that builds `TableObject` instances from the JSON catalog |
| `main.py`                   | Initializes the DAO and demonstrates read/write/run operations |

### Key code from `main.py`

```python
from dao import dao
from catalog import Catalog

dao.init("data_stores.json")

catalog = Catalog("catalog.json")
customers = catalog.get("warehouse.customers")

# Write — routed to Redshift.write_dataframe
dao.write(data={"customer_id": 1, "name": "Alice"}, data_object=customers)

# Read — routed to Redshift.read_table
dao.read(data_object=customers)

# Run — routed to Redshift.run_query
dao.run(data_object=customers, query="SELECT * FROM customers LIMIT 10")
```

---

## Advanced Example

A more complex example with **multiple data stores** and **secondary interfaces**.

### What it demonstrates

- A data store (`raw`) with a primary interface (S3) and two secondary interfaces (Delta, Hudi)
- Switching between interfaces at runtime via `dao_interface_class`
- Mixing multiple data stores (`raw` + `warehouse`) in a single DAO
- Lazy initialization (`lazy=True`)

### How to run

```bash
cd examples/advanced
python main.py
```

### Files

| File                        | Purpose                                                  |
|-----------------------------|----------------------------------------------------------|
| `data_classes/s3.py`        | Primary interface for `raw` — S3 read/write              |
| `data_classes/delta.py`     | Secondary interface for `raw` — Delta Lake read/write    |
| `data_classes/hudi.py`      | Secondary interface for `raw` — Hudi read/write          |
| `data_classes/redshift.py`  | Primary interface for `warehouse` — Redshift operations  |
| `data_stores.json`          | Registers `raw` (with secondary interfaces) and `warehouse` |
| `catalog.json`              | Defines tables across both data stores                   |
| `catalog.py`                | Builds `TableObject` instances from the catalog config   |
| `main.py`                   | Demonstrates interface switching and multi-store usage    |

### Key code from `main.py`

```python
from dao import dao
from catalog import Catalog

dao.init("data_stores.json", lazy=True)
catalog = Catalog("catalog.json")

customer = catalog.get("raw.customer")

# Uses primary interface (S3) by default
dao.write(data="payload", data_object=customer)

# Switch to Hudi interface at runtime
dao.write(data="payload", data_object=customer, dao_interface_class="Hudi")

# Switch to Delta interface at runtime
dao.write(data="payload", data_object=customer, dao_interface_class="Delta")

# Use a different data store entirely (warehouse / Redshift)
dim_customer = catalog.get("warehouse.dim_customer")
dao.run(data_object=dim_customer, query="SELECT * FROM dim_customer LIMIT 5")
```

> **Note:** Any keyword argument prefixed with `dao_` (e.g., `dao_interface_class`) is treated as a
> DAO configuration option and is **not** forwarded to the interface method.
