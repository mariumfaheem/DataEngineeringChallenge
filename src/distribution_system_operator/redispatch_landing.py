import argparse
import pandas as pd
import numpy as np
from sqlalchemy.types import String, Numeric, Integer
from config import MONGO_CONNECTION_STRING, MONGO_DB_NAME, POSTGRESQL_CONNECTION_STRING
from common.mongo_db_connector import get_mongo_collection
from common.postgress_connection import get_postgres_engine


def normalize_asset_name(asset_name: str) -> str:
    """
    Clean and normalize asset names by removing or replacing certain prefixes and substrings.
    """
    if pd.isna(asset_name):
        return None
    if asset_name.startswith("MP-"):
        asset_name = asset_name[3:]
    return asset_name.replace("-DE-", "-")


def return_redispatch_compensation(mongo_conn_str, db_name, collection_name) -> pd.DataFrame:
    """
    Fetches data from MongoDB and converts it to a pandas DataFrame with corrected datatypes.
    """
    collection = get_mongo_collection(mongo_conn_str, db_name, collection_name)
    df = pd.DataFrame(list(collection.find({})))

    if '_id' in df.columns:
        df['_id'] = df['_id'].astype(str)

    df.columns = df.columns.str.lower()

    if 'delivery_start__utc_' in df.columns:
        df['delivery_start_utc'] = pd.to_datetime(df['delivery_start__utc_'], errors='coerce')

    if 'delivery_end__utc_' in df.columns:
        df['delivery_end_utc'] = pd.to_datetime(df['delivery_end__utc_'], errors='coerce')

    if 'price__euro_per_mwh_' in df.columns:
        df['price_euro_per_mwh'] = pd.to_numeric(df['price__euro_per_mwh_'], errors='coerce')

    df.drop(['delivery_start__utc_', 'delivery_end__utc_', 'price__euro_per_mwh_'], axis=1, inplace=True)

    return df

#redispatch_flag

def return_redispatch_flag(mongo_conn_str, db_name, collection_name) -> pd.DataFrame:
    """
    Fetches data from MongoDB and converts it to a pandas DataFrame with corrected datatypes.
    """
    collection = get_mongo_collection(mongo_conn_str, db_name, collection_name)
    df = pd.DataFrame(list(collection.find({})))

    # Add the new asset_id column
    df['asset_id'] = 'WND-003'

    # Convert _id to string
    if '_id' in df.columns:
        df['_id'] = df['_id'].astype(str)

    # Normalize column names
    df.columns = df.columns.str.lower()

    # Convert date strings to datetime
    if 'delivery_start' in df.columns:
        df['delivery_start'] = pd.to_datetime(df['delivery_start'], errors='coerce')

    if 'delivery_end' in df.columns:
        df['delivery_end'] = pd.to_datetime(df['delivery_end'], errors='coerce')

    # Ensure is_redispatched is integer
    if 'is_redispatched' in df.columns:
        df['is_redispatched'] = pd.to_numeric(df['is_redispatched'], downcast='integer', errors='coerce')

    return df




def write_dataframe_to_postgres(engine, table_name: str, schema_name: str, dataframe: pd.DataFrame):
    """
    Writes a DataFrame to a specified PostgreSQL table, defining column types.
    """

    dataframe.to_sql(
        name=table_name,
        con=engine,
        schema=schema_name,
        if_exists='replace',
        index=False
    )
    print(f"Data written to {schema_name}.{table_name}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Ingest asset contract data from MongoDB to PostgreSQL.")
    parser.add_argument("--target_table", type=str, default="redispatch_compensation", help="Target PostgreSQL table name.")
    parser.add_argument("--target_redispatch_flag_table", type=str, default="redispatch_flag", help="Target PostgreSQL table name.")

    parser.add_argument("--db_schema", type=str, default="landing", help="Target PostgreSQL schema name.")
    args = parser.parse_args()

    print("Starting asset contract ingestion...")

    raw_df = return_redispatch_compensation(
        MONGO_CONNECTION_STRING,
        MONGO_DB_NAME,
        collection_name="redispatch_compensation",
    )
    redispatch_flag_df = return_redispatch_flag(
        MONGO_CONNECTION_STRING,
        MONGO_DB_NAME,
        collection_name="redispatch_flag",
    )


    postgres_engine = get_postgres_engine(POSTGRESQL_CONNECTION_STRING)
    write_dataframe_to_postgres(postgres_engine, args.target_table, args.db_schema, raw_df)
    write_dataframe_to_postgres(postgres_engine, args.target_redispatch_flag_table, args.db_schema, redispatch_flag_df)

