# 📚  Understanding and Building Data Pipelines with DAO

## The Problem

Imagine you're a data engineer. Your job is simple in theory: move data from sources to storage to a warehouse. In practice, it's messy.

You write code that directly calls S3, specifying bucket names and credentials. You manage Spark sessions and worry about file formats. You craft JDBC URLs for Redshift connections. Every pipeline step has its own way of doing things.

Then one day, your company says: "We're switching from S3 to Delta Lake." Or: "We need to load this data to Snowflake instead." Your heart sinks. You know what's coming: rewrite half your pipelines. The business logic is buried under storage boilerplate.

**The root problem**: Your code is tightly coupled to storage.

## The Solution: DAO (Data Access Object)

DAO flips the script. Instead of your code knowing *how* to talk to storage, you tell DAO *what* data you want. DAO figures out *how*.

### The Three Building Blocks

**1. DataStore** — A ***Data Store*** refers to a complete or a segment of a storage layer which usually are Block storages,
File systems, Object storages or Databases — A logical home for your data

- An S3 bucket, Database schema, or Delta location
- A Filesystem Directory or a Prefix in Object Storage

**2. DataObject** — Unit of data within a DataStore
- A table, file or a object
- Files in a Directory, Tables in a Schema

**3. Interface** — the handler that does the I/O
- Backend-specific implementations
- Each interface knows how to access and perform various operations on all DataObjects in its DataStore
- You don't call interfaces directly; DAO routes to them

### Visual: How They Connect


![data_store.png](examples/docs/img.png)

In this diagram:
- The **large rectangles** = DataStore (logical storage layers)
- The **small rectangles inside** = DataObjects (datasets within each store)
- Each DataObject belongs to exactly one DataStore

![py_objects](examples/docs/data_store_py_relations.png)

In the above diagram, we can see how python objects(Virtual) relate to the Data Store and Data Objects (Real):
- A data store is represented by a DataStore class object
- A data object is represented by a DataObject class object which is associated with a DataStore class object
- Different data stores and data objects have different configurations and properties
- An interface class object is associated with a DataStore class object to perform the data access operations on the DataObjects present in the DataStore
- We can add multiple Interface class objects to a DataStore class object to have multiple ways to access the DataObjects present in the DataStore

- The Data Access Object(DAO) class object is the main entry point to perform data access operations on the DataObjects present in various DataStores
 
### One API, Many Backends

```python
# Same code works for S3, Redshift, Delta, or whatever you add next
dao.read(data_object=my_data)
dao.write(data=df, data_object=my_data, format='parquet')
```

Why this matters
- ✅ **Storage-agnostic code**: Change backends without rewriting pipelines
- ✅ **Clean separation**: I/O logic in interfaces, business logic in your code
- ✅ **Easy configuration**: Register stores once, use them everywhere

---

## 🏛️ The Medallion Architecture: A Real-World Example

Now let's apply DAO to a common data engineering pattern: the **medallion architecture**. Think of it as three zones in a data lake, each serving a different purpose and using different storage.

**The Journey of Data**

Raw data lands → gets cleaned → becomes analytics-ready. Each zone handles one job:

```
┌─────────────────────────────────────────────────────────────────┐
│                    Data's Journey in a Lake                     │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Raw Sources    Bronze Layer    Silver Layer    Gold Layer      │
│  (CSV, JSON)       (S3)           (S3)          (Redshift)      │
│      │               │              │              │             │
│  users.csv ──>  users_raw  ──>  users_clean  ──>  users_agg    │
│  events.json ─> events_raw ──>  events_enr  ──>  sales_daily   │
│                                                                   │
│  🔴 Raw         🟠 Cleaned      🟡 Optimized    🟢 Analytics    │
│  No schema      De-duped        Partitioned    Ready for BI    │
│  As-is upload   Type-coerced    Indexed         Aggregated      │
│                                                                   │
└─────────────────────────────────────────────────────────────────┘
```

### Layer 1: Bronze — Raw Ingestion

**What lives here**: Source files exactly as received (CSV, JSON, Parquet dumps)

**Your job**: Upload, store, minimal processing

**Storage**: S3 bucket (`bronze_store`)

**Interface**: `S3Boto3Interface` (simple file operations)

**Example DataObjects**:
- `users_raw` → CSV files from user export
- `events_raw` → JSON logs from API

```
S3: bronze_bucket/
├── users_raw/
│   ├── 2025-01-07/users.csv
│   └── 2025-01-08/users.csv
└── events_raw/
    ├── 2025-01-07/events.json
    └── 2025-01-08/events.json
```

### Layer 2: Silver — Cleaned & Structured

**What lives here**: Deduplicated, typed, partitioned Parquet/Delta tables

**Your job**: Transform, validate schema, partition for performance

**Storage**: S3 with Spark (`silver_store`)

**Interface**: `S3SparkInterface` (Spark read/write)

**Example DataObjects**:
- `users_clean` → deduplicated, typed user records
- `events_enriched` → events with added metadata

```
S3: silver_bucket/datasets/
├── users_clean/
│   ├── year=2025/month=01/day=07/part-*.parquet
│   └── year=2025/month=01/day=08/part-*.parquet
└── events_enriched/
    ├── year=2025/month=01/day=07/part-*.parquet
    └── year=2025/month=01/day=08/part-*.parquet
```

### Layer 3: Gold — Analytics Ready

**What lives here**: Aggregated, denormalized, joined tables ready for dashboards and reports

**Your job**: Aggregate, join, create facts/dimensions, optimize for BI queries

**Storage**: Data warehouse (Redshift in this example)

**Interface**: `RedshiftSparkInterface` (bulk load from Spark)

**Example DataObjects**:
- `users_agg` → user metrics (total purchases, account age, etc.)
- `sales_daily_agg` → daily sales rollup by region

```
Redshift: analytics_schema.
├── users_agg
│   Columns: user_id, total_purchases, avg_order_value, ...
├── sales_daily_agg
│   Columns: date, region, revenue, orders, ...
└── (other analytics tables)
```

---

## 🔧 How This Maps to Configuration

Each layer is declared once in a config file. DAO reads this and wires up the right interface for each store.

### The Config File (JSON)

```json
{
  "bronze": {
    "interface_class": "S3Boto3Interface",
    "interface_class_location": "interface_classes.s3.s3_boto3",
    "default_configs": { 
      "bucket": "my-company-bronze" 
    }
  },
  "silver": {
    "interface_class": "S3SparkInterface",
    "interface_class_location": "interface_classes.s3.spark",
    "default_configs": { 
      "bucket": "my-company-silver", 
      "prefix": "datasets" 
    }
  },
  "gold": {
    "interface_class": "RedshiftSparkInterface",
    "interface_class_location": "interface_classes.redshift.spark",
    "default_configs": { 
      "host": "redshift.company.redshift.amazonaws.com",
      "user": "pipeline_user",
      "password": "***",
      "database": "analytics",
      "s3_temp_dir": "s3://my-company-temp/redshift/",
      "iam_role_arn": "arn:aws:iam::123456789:role/redshift-role"
    }
  }
}
```

### How Config Becomes DAO

```
Config (JSON)
    │
    ├─ "bronze" ──> DataStore
    │              ├─ Name: "bronze"
    │              ├─ Interface: S3Boto3Interface
    │              └─ Defaults: { bucket: "my-company-bronze" }
    │
    ├─ "silver" ──> DataStore
    │              ├─ Name: "silver"
    │              ├─ Interface: S3SparkInterface
    │              └─ Defaults: { bucket: "my-company-silver", prefix: "datasets" }
    │
    └─ "gold" ───> DataStore
                   ├─ Name: "gold"
                   ├─ Interface: RedshiftSparkInterface
                   └─ Defaults: { host: "...", user: "...", ... }

Later, when you create DataObjects:

DataObject("users_raw", bronze_store)
    ├─ Name: "users_raw"
    ├─ DataStore: bronze (knows S3Boto3Interface)
    └─ Ready to use: dao.read(users_raw, ...)

DataObject("users_clean", silver_store)
    ├─ Name: "users_clean"
    ├─ DataStore: silver (knows S3SparkInterface)
    └─ Ready to use: dao.read(users_clean, format='parquet')

DataObject("users_agg", gold_store)
    ├─ Name: "users_agg"
    ├─ DataStore: gold (knows RedshiftSparkInterface)
    └─ Ready to use: dao.write(df, users_agg)
```

### What Each Config Field Means

| Field | Purpose | Example |
|-------|---------|---------|
| `interface_class` | The class name that will handle I/O | `S3Boto3Interface` |
| `interface_class_location` | Module path to find the class | `interface_classes.s3.s3_boto3` |
| `default_configs` | Arguments passed to the interface constructor | `{ "bucket": "..." }` |

---

## 💻 Writing the Pipeline Code

Once config is in place, your code becomes simple:

### Step 1: Initialize DAO with Your Config

```python
from dao.core.dao import DataAccessObject
from dao.data_object import DataObject
from dao.data_store import DataStoreRegistry

# Load config (JSON file or dict)
config = {
    "bronze": { ... },  # from above
    "silver": { ... },
    "gold": { ... }
}

# One line: DAO is ready
dao = DataAccessObject(config)
```

### Step 2: Ingest Raw Data to Bronze

```python
# Get the bronze store and create a data object
bronze_store = DataStoreRegistry.get('bronze')
users_raw = DataObject('users_raw', bronze_store)

# Upload a CSV file to S3 (uses S3Boto3Interface under the hood)
with open('sample/users.csv', 'rb') as f:
    dao.write(
        data_object=users_raw, 
        data=f, 
        path='s3://my-company-bronze/users_raw/2025-01-07/'
    )
# ✅ File is now in S3 bronze layer
```

### Step 3: Transform to Silver (With Spark)

```python
# Read from bronze using Spark (S3SparkInterface)
users_df = dao.read(
    data_object=users_raw,
    format='csv'
    # S3SparkInterface knows the bucket/path from defaults
)

# Clean it up
users_clean = (users_df
    .dropDuplicates(['id'])  # remove duplicates
    .select('id', 'name', 'email', 'created_at')  # trim columns
    .filter("email IS NOT NULL")  # remove nulls
)

# Write to silver (parquet, partitioned by date)
silver_store = DataStoreRegistry.get('silver')
users_clean_obj = DataObject('users_clean', silver_store)

dao.write(
    data=users_clean,
    data_object=users_clean_obj,
    format='parquet',
    path='s3://my-company-silver/datasets/users_clean/',
    spark_options={'mode': 'overwrite'}
)
# ✅ Cleaned data is now in S3 silver layer, ready for analytics
```

### Step 4: Load to Gold (Analytics-Ready in Redshift)

```python
# Read from silver
users_clean_data = dao.read(
    data_object=users_clean_obj,
    format='parquet'
)

# Aggregate: compute user metrics
from pyspark.sql.functions import col, count, avg, max

users_agg = (users_clean_data
    .groupBy('id')
    .agg(
        count('*').alias('total_purchases'),
        avg('order_value').alias('avg_order_value'),
        max('created_at').alias('last_purchase_date')
    )
)

# Write to Redshift (gold layer)
gold_store = DataStoreRegistry.get('gold')
users_agg_obj = DataObject('users_agg', gold_store)

dao.write(
    data=users_agg,
    data_object=users_agg_obj,
    format='parquet'  # or spark's native write
)
# ✅ Aggregated metrics now live in Redshift for BI teams
```

### The Full Picture (How Routes Work)

```
Your Code          DAO Router              Interface           Storage
──────────────────────────────────────────────────────────────────────
dao.read(users_raw,
  format='csv')  ──┐
                   ├──> knows users_raw
                   │    is in bronze_store ──> S3Boto3Interface ──> S3
                   │
dao.write(users_clean,
  format='parquet') ─┐
                     ├──> knows users_clean
                     │    is in silver_store ──> S3SparkInterface ──> S3
                     │
dao.write(users_agg) ──┐
                        ├──> knows users_agg
                        │    is in gold_store ──> RedshiftSparkInterface ──> Redshift
```

---

## 🎯 Key Design Patterns

### 1. Object Naming Across Layers
Keep the core entity name consistent, add suffixes for clarity:
- Bronze: `users_raw` (raw CSV/JSON)
- Silver: `users_clean` (cleaned parquet)
- Gold: `users_agg` (aggregated)

### 2. Default Configs Keep Code Clean
Define bucket names, prefixes, and connection strings once in config, not scattered in code:

```python
# Instead of repeating bucket names everywhere:
# ❌ Bad: dao.write(..., path='s3://my-company-silver/datasets/users_clean/')
# ✅ Good: DAO uses config defaults, you just specify data_object and format
```

### 3. Let DAO Route
Pass `format` and `spark_options` only when you need format-specific control. Otherwise, let DAO pick the right interface:

```python
# ✅ Simple: DAO auto-picks interface based on data_object's store
dao.read(data_object=my_data)

# ✅ Specific: You tell DAO the format when needed
dao.read(data_object=my_data, format='parquet')
```

---

## 📖 Next Steps

Ready to see this in action? Check out the example scripts in `examples/medallion/`:
1. `01_ingest_bronze.py` — upload raw data to bronze layer
2. `02_transform_silver.py` — clean and partition to silver layer
3. `03_load_gold.py` — aggregate and load to gold layer (Redshift)

See `catalog.json` for store and object configuration, and `sample_data/` for tiny test datasets.

To run locally:
- **Option A**: Use real AWS S3 (set `AWS_*` env vars)
- **Option B**: Use MinIO + docker-compose (portable, no AWS account needed)

Start with any example script and follow the inline comments.
