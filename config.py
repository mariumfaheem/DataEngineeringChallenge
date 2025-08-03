
import os
from dotenv import load_dotenv
import os
from urllib.parse import quote_plus
import pandas as pd
load_dotenv()


from pathlib import Path

# # Automatically find the base path relative to the config file
# BASE_DIR = Path(__file__).resolve().parent.parent / "database" / "distribution_system_operator"
#
# assets_to_process = [
#     (BASE_DIR / "final_production_MP-SOL-001_20250609.csv", "SOL-001"),
#     (BASE_DIR / "final_production_MP-SOL-002_20250609.csv", "SOL-002"),
#     (BASE_DIR / "final_production_MP-WND-002_20250609.csv", "WND-002"),
#     (BASE_DIR / "final_production_MP-WND-003_20250609.csv", "WND-003"),
# ]


# from pathlib import Path
#
# BASE_DIR = Path(__file__).resolve().parent.parent / "database" / "distribution_system_operator"
# print("BASE_DIR",BASE_DIR)# Robust
#
#
# from pathlib import Path
#
# print("Current working directory:", Path.cwd())
# print("Path using relative string:", Path("database/distribution_system_operator").resolve())
# print("Path using __file__:", Path(__file__).resolve().parent.parent / "database" / "distribution_system_operator")
#
#
# print("assets_to_process",assets_to_process)

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
from pathlib import Path


from pathlib import Path
import os

from pathlib import Path
import os

if Path("/opt/airflow/database/distribution_system_operator").exists():
    BASE_DIR = Path("/opt/airflow/database/distribution_system_operator")
else:
    BASE_DIR = Path(__file__).resolve().parent.parent / "database" / "distribution_system_operator"

assets_to_process = [
    (BASE_DIR / "final_production_MP-SOL-001_20250609.csv", "SOL-001"),
    (BASE_DIR / "final_production_MP-SOL-002_20250609.csv", "SOL-002"),
    (BASE_DIR / "final_production_MP-WND-002_20250609.csv", "WND-002"),
    (BASE_DIR / "final_production_MP-WND-003_20250609.csv", "WND-003"),
]


#
# assets_to_process = [
#     (Path("database/distribution_system_operator/final_production_MP-SOL-001_20250609.csv"), "SOL-001"),
#     (Path("database/distribution_system_operator/final_production_MP-SOL-002_20250609.csv"), "SOL-002"),
#     (Path("database/distribution_system_operator/final_production_MP-WND-002_20250609.csv"), "WND-002"),
#     (Path("database/distribution_system_operator/final_production_MP-WND-003_20250609.csv"), "WND-003"),
# ]


PRICE_ESTIMATION_FILE_PATH = '/Users/Marium_Faheem/Library/CloudStorage/OneDrive-McKinsey&Company/Desktop/Mckinsey/DE-interviews/DataEngineeringChallenge-main/database/imbalance/imbalance_price_estimation_2025-06-07T041800Z.csv'
PRICE_FINAL_FILE_PATH="/Users/Marium_Faheem/Library/CloudStorage/OneDrive-McKinsey&Company/Desktop/Mckinsey/DE-interviews/DataEngineeringChallenge-main/database/imbalance/imbalance_price_final_20250608_170800.csv"

