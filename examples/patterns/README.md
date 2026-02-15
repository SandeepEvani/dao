# Usage Patterns

These examples demonstrate common architectural patterns for using DAO in real-world scenarios.

## Examples

| File | Description | Complexity |
|------|-------------|------------|
| [01_factory_pattern.py](01_factory_pattern.py) | Config-driven DataStore creation | ⭐⭐ |
| [02_catalog_pattern.py](02_catalog_pattern.py) | Complete data catalog with discovery | ⭐⭐ |
| [03_medallion_pipeline.py](03_medallion_pipeline.py) | Bronze → Silver → Gold data flow | ⭐⭐⭐ |

## When to Use Each Pattern

### Factory Pattern
- You have 3-10 DataStores
- You want to externalize configuration
- You need multiple interfaces per store

### Catalog Pattern
- You have 10+ DataStores and DataObjects
- Multiple teams need data discovery
- You want centralized metadata management

### Medallion Architecture
- Building a data lakehouse
- Need structured data quality layers
- Following Databricks best practices

## Configuration Files

The `config/` directory contains JSON configurations:
- `data_stores.json` - DataStore definitions
- `data_objects.json` - DataObject catalog

## Running Examples

```bash
python examples/patterns/01_factory_pattern.py
python examples/patterns/02_catalog_pattern.py
python examples/patterns/03_medallion_pipeline.py
```
