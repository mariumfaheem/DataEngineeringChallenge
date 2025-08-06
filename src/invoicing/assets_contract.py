import argparse
import pandas as pd
from sqlalchemy.types import String, Numeric, Integer
from config import MONGO_CONNECTION_STRING, MONGO_DB_NAME, POSTGRESQL_CONNECTION_STRING
from common.mongo_db_connector import get_mongo_collection
from common.postgress_connection import get_postgres_engine


def normalize_asset_name(asset_name: str) -> str:
    if pd.isna(asset_name):
        return None
    if asset_name.startswith("MP-"):
        asset_name = asset_name[3:]
    return asset_name.replace("-DE-", "-")


def return_assets_data(mongo_conn_str, db_name, collection_name) -> pd.DataFrame:
    assets_data = get_mongo_collection(mongo_conn_str, db_name, collection_name)
    assets_data_df = pd.DataFrame(list(assets_data.find({})))

    if '_id' in assets_data_df.columns:
        assets_data_df['_id'] = assets_data_df['_id'].astype(str)

    assets_data_df.columns = assets_data_df.columns.astype(str).str.lower()
    return assets_data_df


def clean_and_transform_data(df: pd.DataFrame) -> pd.DataFrame:
    print("Cleaning and transforming data...")
    numeric_cols = {
        'price': float,
        'fee': float,
        'installed_capacity_kw': float
    }

    for col, _ in numeric_cols.items():
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    if 'asset_id' in df.columns:
        df['asset_id'] = df['asset_id'].apply(normalize_asset_name)

    return df


def write_dataframe_to_postgres(engine, table_name: str, schema_name: str, dataframe: pd.DataFrame):
    sql_type_mapping = {
        'contract_id': String(255),
        'asset_id': String(255),
        'pricing_model': String(50),
        'price': Numeric(10, 2),
        'fee_model': String(50),
        'fee': Numeric(10, 4),
        'installed_capacity_kw': Integer
    }

    dataframe.to_sql(
        name=table_name,
        con=engine,
        schema=schema_name,
        if_exists='replace',
        index=False,
        dtype=sql_type_mapping
    )
    print(f"Data written to {schema_name}.{table_name}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Ingest asset contract data from MongoDB to PostgreSQL.")
    parser.add_argument("--target_table", type=str, default="asset_contracts", help="Target PostgreSQL table name.")
    parser.add_argument("--db_schema", type=str, default="landing", help="Target PostgreSQL schema name.")
    args = parser.parse_args()

    print("Starting asset contract ingestion...")

    raw_df = return_assets_data(
        MONGO_CONNECTION_STRING,
        MONGO_DB_NAME,
        collection_name="asset_contracts",
    )

    transformed_df = clean_and_transform_data(raw_df)

    postgres_engine = get_postgres_engine(POSTGRESQL_CONNECTION_STRING)
    write_dataframe_to_postgres(postgres_engine, args.target_table, args.db_schema, transformed_df)