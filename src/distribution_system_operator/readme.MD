
# DSO Asset Production & Redispatch Data Ingestion

This PACKAGE contains two ingestion scripts used to load energy asset-related data from various sources (CSV and MongoDB) into a PostgreSQL database for further processing and analysis.

---

##  Scripts Overview

### 1. `ingest_dso_asset_production_data.py`

**Purpose:**  
Ingests final production data for a set of distributed system operator (DSO) assets from CSV files into a PostgreSQL `landing` schema.

**Key Steps:**
- Reads asset-level CSVs using configured paths.
- Normalizes asset names and renames columns.
- Consolidates all data into a single DataFrame.
- Loads the merged data into PostgreSQL (`landing.dso_final_production` by default).

**Config Required:**
- `assets_to_process` list from `config.py` (tuples of file path and asset name).
- `POSTGRESQL_CONNECTION_STRING`.

**Run Example:**
```bash

PYTHONPATH=. python src/distribution_system_operator/redispatch_landing.py --final_production_table dso_final_production

````

---

### 2. `redispatch_landing.py`

**Purpose:**
Ingests redispatch compensation and redispatch flag data from MongoDB into PostgreSQL.

**Key Steps:**

* Reads `redispatch_compensation` and `redispatch_flag` collections from MongoDB.
* Normalizes timestamps, price fields, and asset identifiers.
* Adds missing metadata (e.g., hardcoded `asset_id` for redispatch flag).
* Loads both datasets into PostgreSQL under a specified schema.

**Config Required:**

* `MONGO_CONNECTION_STRING`, `MONGO_DB_NAME`.
* `POSTGRESQL_CONNECTION_STRING`.

**Run Example:**

```bash

PYTHONPATH=. python src/distribution_system_operator/redispatch_landing.py
  --target_table redispatch_compensation \
  --target_redispatch_flag_table redispatch_flag \
  --db_schema landing
```



---

##  Assumptions

* CSV files are well-formatted and accessible via paths defined in `assets_to_process`.
* All ingested data is written to a PostgreSQL schema named `landing`.
* More details about the data sources can be found [here](../../../data/distribution_system_operator/README.md)


---

##  Notes

* The redispatch flag data is currently hardcoded for `WND-003` — update logic if additional assets are introduced.
* Any schema or field changes in MongoDB must be reflected in the scripts for compatibility.
* Ensure that both database connectors (`mongo_db_connector.py` and `postgress_connection.py`) handle credentials securely.
