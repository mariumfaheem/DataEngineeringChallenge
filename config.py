from dotenv import load_dotenv
from urllib.parse import quote_plus
import pandas as pd
from pathlib import Path
import os
load_dotenv()

DB_USER = quote_plus(os.getenv("DB_USER"))
DB_PASS = quote_plus(os.getenv("DB_PASSWORD"))
DB_NAME = os.getenv("DB_NAME")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"

POSTGRESQL_CONNECTION_STRING = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

MONGO_DB_USER = os.getenv("MONGO_DB_USER")
MONGO_DB_PASSWORD= os.getenv("MONGO_DB_PASSWORD")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME")
MONGO_DB_HOST = os.getenv("MONGO_DB_HOST")
MONGO_DB_PORT = os.getenv("MONGO_DB_PORT")
MONGO_CONNECTION_STRING = f"mongodb://{MONGO_DB_USER}:{MONGO_DB_PASSWORD}@{MONGO_DB_HOST}:{MONGO_DB_PORT}/"

ASSET_IDS = ["SOL-DE-001","SOL-DE-002","WND-DE-001", "WND-DE-002","WND-DE-003"]

if Path("/opt/airflow/data").exists():
    BASE_DIR = Path("/opt/airflow/data")
else:
    BASE_DIR = Path(__file__).resolve().parent.parent / "data"

assets_to_process = [
    (BASE_DIR / "/distribution_system_operator/final_production_MP-SOL-001_20250609.csv", "SOL-001"),
    (BASE_DIR / "/distribution_system_operator/final_production_MP-SOL-002_20250609.csv", "SOL-002"),
    (BASE_DIR / "/distribution_system_operator/final_production_MP-WND-002_20250609.csv", "WND-002"),
    (BASE_DIR / "/distribution_system_operator/final_production_MP-WND-003_20250609.csv", "WND-003"),
]

PRICE_ESTIMATION_FILE_PATH = BASE_DIR / "imbalance" / "imbalance_price_estimation_2025-06-07T041800Z.csv"
PRICE_FINAL_FILE_PATH = BASE_DIR / "imbalance" / "imbalance_price_final_20250608_170800.csv"

