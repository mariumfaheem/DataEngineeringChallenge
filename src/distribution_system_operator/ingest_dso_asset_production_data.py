import pandas as pd
import argparse
from config import assets_to_process, POSTGRESQL_CONNECTION_STRING
from common.postgress_connection import get_postgres_engine


def normalize_asset_name(asset_name: str) -> str:
    """
    Clean and normalize asset names by removing or replacing certain prefixes and substrings.
    """
    if asset_name.startswith("MP-"):
        asset_name = asset_name[3:]
    return asset_name.replace("-DE-", "-")


def load_and_format_asset_csv(asset_url: str, asset_id: str) -> pd.DataFrame:
    """
    Load CSV from a URL and format it with standard column names and an added asset_id.
    """
    df = pd.read_csv(asset_url)
    rename_mapping = {
        df.columns[0]: 'delivery_start_utc',
        df.columns[1]: 'actual_power_kw'
    }
    df.rename(columns=rename_mapping, inplace=True)
    df['asset_id'] = asset_id
    return df[['delivery_start_utc', 'actual_power_kw', 'asset_id']]


def process_asset_batch(asset_list) -> list:
    """
    Process a list of (url, asset_name) pairs into a list of formatted DataFrames.
    """
    processed_dfs = []
    for url, custom_asset_name in asset_list:
        try:
            print(f"Processing asset: {custom_asset_name}...")
            df = load_and_format_asset_csv(url, custom_asset_name)
            processed_dfs.append(df)
            print(f"  - {custom_asset_name} processed successfully.")
        except Exception as e:
            print(f"  - Failed to process {custom_asset_name} ({url}). Error: {e}")
    return processed_dfs


def write_dataframes_to_db(engine, table_name: str, dataframes: list):
    """
    Write merged DataFrame to the PostgreSQL database.
    """
    if dataframes:
        merged_df = pd.concat(dataframes, ignore_index=True)
        merged_df.to_sql(
            table_name,
            con=engine,
            schema="landing",
            if_exists='replace',
            index=False
        )
        print(f"\nSuccessfully written to DB table: landing.{table_name}")
    else:
        print("\nNo data was processed. Please check your asset configuration.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Ingest final production data into PostgreSQL.")
    parser.add_argument("--final_production_table", type=str, default="dso_final_production",
                        help="Name of the destination table.")
    args = parser.parse_args()

    print("Starting the ingestion process...")

    engine = get_postgres_engine(POSTGRESQL_CONNECTION_STRING)
    processed_dataframes = process_asset_batch(assets_to_process)
    write_dataframes_to_db(engine, args.final_production_table, processed_dataframes)
