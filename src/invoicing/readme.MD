
# Asset Contracts Ingestion

This script ingests asset contract data from MongoDB, applies transformations and normalization, and writes the cleaned dataset to a PostgreSQL table.

---

##  Script: `assets_contract.py`

### Purpose

- Extract asset contract data from the `asset_contracts` MongoDB collection.
- Normalize and clean key fields (e.g., `asset_id`, `price`, `fee`, `installed_capacity_kw`).
- Cast fields to appropriate SQL types.
- Load the transformed dataset into a PostgreSQL table.

---

##  How It Works

1. **Connect to MongoDB** and read the `asset_contracts` collection.
2. **Clean and transform**:
   - Ensure numerical values are correctly typed and non-null.
   - Normalize `asset_id` by removing prefixes like `MP-` and replacing `-DE-`.
3. **Define SQL schema types** using `sqlalchemy.types`.
4. **Write to PostgreSQL** using the `to_sql()` method.

---

##  Assumptions

- MongoDB contains a collection named `asset_contracts` with consistent schema.
- Fields `price`, `fee`, and `installed_capacity_kw` are numeric or convertible to numeric.
- `asset_id` follows a standardized format with possible `MP-` or `-DE-` prefixes.
- PostgreSQL schema and connection details are correctly defined in `config.py`.
- More details about the data sources can be found [here](../../../data/invoicing/README.md)


---

## **Run Example:**

```bash
PYTHONPATH=. python src/invoicing/assets_contract.py \
  --target_table asset_contracts \
  --db_schema landing
````


