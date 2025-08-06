import json
import argparse
import pandas as pd
import pendulum
from config import ASSET_IDS, POSTGRESQL_CONNECTION_STRING
from common.postgress_connection import get_postgres_engine
from src.vpp.client import get_forecast


def normalize_asset_id(asset_id: str) -> str:
    if asset_id.startswith("MP-"):
        asset_id = asset_id[3:]
    return asset_id.replace("-DE-", "-")


def fetch_asset_forecasts(asset_ids, version_timestamp) -> pd.DataFrame:
    all_forecasts = []

    for asset_id in asset_ids:
        print(f"Fetching forecast for asset: {asset_id}")
        forecast_json_str = get_forecast(asset_id=asset_id, version=version_timestamp)
        forecast_data = json.loads(forecast_json_str)

        df = pd.DataFrame(
            zip(*forecast_data['values']),
            columns=forecast_data['column_ids']
        )
        df['asset_id'] = normalize_asset_id(asset_id)
        all_forecasts.append(df)

    combined_df = pd.concat(all_forecasts, ignore_index=True)
    combined_df['power'] = pd.to_numeric(combined_df['power'], errors='coerce').fillna(0)
    combined_df['start_time_utc'] = pd.to_datetime(combined_df['start'], unit='s', utc=True)
    combined_df['version_time_utc'] = pd.to_datetime(combined_df['version'], unit='s', utc=True)

    return combined_df


def aggregate_portfolio_forecast(asset_forecasts: pd.DataFrame) -> pd.DataFrame:
    return asset_forecasts.groupby('start_time_utc')[['power']].sum().reset_index()


def write_to_postgres(df: pd.DataFrame, engine, table_name: str, schema: str = "data_product"):
    df.to_sql(
        name=table_name,
        con=engine,
        schema=schema,
        if_exists='replace',
        index=False
    )
    print(f"Written to {schema}.{table_name}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Load asset and portfolio forecasts into PostgreSQL.")
    parser.add_argument("--asset_forecasts_table", type=str, default="asset_forecasts",
                        help="PostgreSQL table for individual asset forecasts.")
    parser.add_argument("--portfolio_forecast_table", type=str, default="portfolio_forecast",
                        help="PostgreSQL table for aggregated portfolio forecasts.")
    args = parser.parse_args()

    delivery_start = pendulum.datetime(2025, 6, 8, 0, 0, tz="Europe/Berlin")
    latest_version_timestamp = delivery_start.subtract(minutes=15)
    print(f"Using forecast version: {latest_version_timestamp.to_iso8601_string()}")

    forecasts_df = fetch_asset_forecasts(ASSET_IDS, latest_version_timestamp)
    portfolio_df = aggregate_portfolio_forecast(forecasts_df)

    engine = get_postgres_engine(POSTGRESQL_CONNECTION_STRING)

    print("Writing asset forecasts...")
    write_to_postgres(forecasts_df, engine, args.asset_forecasts_table)

    print("Writing portfolio forecasts...")
    write_to_postgres(portfolio_df, engine, args.portfolio_forecast_table)
