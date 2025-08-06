import argparse
import pandas as pd
from config import MONGO_CONNECTION_STRING, MONGO_DB_NAME, POSTGRESQL_CONNECTION_STRING
from common.mongo_db_connector import get_mongo_collection
from common.postgress_connection import get_postgres_engine


def merge_trade_collections(mongo_conn_str, db_name, private_collection, public_collection) -> pd.DataFrame:
    private_cursor = get_mongo_collection(mongo_conn_str, db_name, private_collection)
    public_cursor = get_mongo_collection(mongo_conn_str, db_name, public_collection)

    private_df = pd.DataFrame(list(private_cursor.find({})))
    public_df = pd.DataFrame(list(public_cursor.find({})))

    private_df['source'] = 'private'
    public_df['source'] = 'public'

    combined_df = pd.concat([private_df, public_df], ignore_index=True)

    if '_id' in combined_df.columns:
        combined_df['_id'] = combined_df['_id'].astype(str)

    combined_df.columns = combined_df.columns.str.lower()

    return combined_df


def write_dataframe_to_postgres(engine, table_name: str, schema_name: str, dataframe: pd.DataFrame):
    dataframe.to_sql(
        name=table_name,
        con=engine,
        schema=schema_name,
        if_exists='replace',
        index=False
    )
    print(f"Data written to {schema_name}.{table_name}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Merge trade data from MongoDB and store in PostgreSQL.")
    parser.add_argument("--target_table", type=str, default="trade", help="Target PostgreSQL table name.")
    parser.add_argument("--db_schema", type=str, default="landing", help="Target PostgreSQL schema name.")
    args = parser.parse_args()

    print("Starting trade data ingestion...")

    postgres_engine = get_postgres_engine(POSTGRESQL_CONNECTION_STRING)

    combined_trades_df = merge_trade_collections(
        MONGO_CONNECTION_STRING,
        MONGO_DB_NAME,
        private_collection="private_exchange",
        public_collection="public_exchange"
    )

    write_dataframe_to_postgres(postgres_engine, args.target_table, args.db_schema, combined_trades_df)
