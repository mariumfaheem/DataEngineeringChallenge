# Exchange Trade Data Ingestion

This script ingests and merges trade data from two MongoDB collections—`private_exchange` and `public_exchange`—and loads the combined result into a PostgreSQL database for further use.

---

##  Script: `ingest_exchange_trade_data.py`

### Purpose

- Merge private and public exchange trade data from MongoDB.
- Tag each record with its source (`private` or `public`).
- Normalize the combined data.
- Load the result into the specified PostgreSQL table and schema.

---

##  How It Works

1. Connects to MongoDB using credentials in `config.py`.
2. Fetches documents from `private_exchange` and `public_exchange`.
3. Merges both datasets and tags them with a `source` column.
4. Normalizes column names and converts `_id` to string.
5. Writes the final dataset to a PostgreSQL database (`landing.trade` by default).

---

##  Assumptions

- Both MongoDB collections (`private_exchange` and `public_exchange`) exist and follow a similar schema.
- Required connection strings are configured in `config.py`:
  - `MONGO_CONNECTION_STRING`
  - `MONGO_DB_NAME`
  - `POSTGRESQL_CONNECTION_STRING`
- Target PostgreSQL schema and table exist or will be created.
- More details about the data sources can be found [here](../../../data/exchange/README.md)


---

## How to run it

```bash
PYTHONPATH=. python src/exchange/ingest_exchange_trade_data.py \
  --target_table trade \
  --db_schema landing
