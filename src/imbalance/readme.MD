# Imbalance Price Data Ingestion

This script ingests both **estimated** and **final** imbalance price data from CSV files and loads them into a PostgreSQL database.

---

## Script: `ingest_imbalance_price.py`

### Purpose

- Parse imbalance price data from semicolon-delimited CSV files.
- Normalize date and number formats (e.g. German decimal notation).
- Rename columns to standardized English names.
- Store results into dedicated PostgreSQL tables.

---

##  How It Works

1. Reads the paths for two CSV files from `config.py`:
   - `PRICE_ESTIMATION_FILE_PATH`
   - `PRICE_FINAL_FILE_PATH`
2. Parses and formats both CSVs using provided column mappings.
3. Converts `Datum` column to ISO date.
4. Writes each dataset into the `landing` schema in PostgreSQL.

---

##  Assumptions

- CSV files use `;` as delimiter and `,` as decimal separator (German-style).
- Column names in CSV files match expected German headers like `Datum`, `Einheit`, `AEP-Schätzer`, etc.
- File paths are configured in `config.py`.
- Target PostgreSQL schema (`landing` by default) exists or can be created.
- More details about the data sources can be found [here](../../../data/imbalance/README.md)

---

## **Run Example:**

```bash
PYTHONPATH=. python src/imbalance/ingest_imbalance_price.py \
  --price_estimation_target_table imbalance_price_estimation \
  --price_final_target_table imbalance_price_final \
  --db_schema landing
