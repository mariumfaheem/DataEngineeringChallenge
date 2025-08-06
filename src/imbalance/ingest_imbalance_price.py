import pandas as pd
import argparse
from config import (
    POSTGRESQL_CONNECTION_STRING,
    PRICE_ESTIMATION_FILE_PATH,
    PRICE_FINAL_FILE_PATH
)
from common.postgress_connection import get_postgres_engine


def load_and_format_csv(file_path: str, column_mapping: dict) -> pd.DataFrame:
    df = pd.read_csv(file_path, sep=';', decimal=',')
    df['Datum'] = pd.to_datetime(df['Datum'], format='%d.%m.%Y')
    return df.rename(columns=column_mapping)


def write_dataframe_to_postgres(engine, table_name: str, schema_name: str, dataframe: pd.DataFrame):
    dataframe.to_sql(
        name=table_name,
        schema=schema_name,
        con=engine,
        if_exists='replace',
        index=False
    )
    print(f"Data written to {schema_name}.{table_name}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Ingest imbalance price data into PostgreSQL.")
    parser.add_argument("--price_estimation_target_table", type=str, default="imbalance_price_estimation",
                        help="PostgreSQL table name for estimated imbalance prices.")
    parser.add_argument("--price_final_target_table", type=str, default="imbalance_price_final",
                        help="PostgreSQL table name for final imbalance prices.")
    parser.add_argument("--db_schema", type=str, default="landing",
                        help="Target schema name in PostgreSQL.")

    args = parser.parse_args()

    engine = get_postgres_engine(POSTGRESQL_CONNECTION_STRING)


    column_mapping_estimation = {
        'Datum': 'date',
        'Zeitzone': 'timezone',
        'von': 'from',
        'bis': 'to',
        'Datenkategorie': 'datacategory',
        'Datentyp': 'datatype',
        'Einheit': 'unit',
        'AEP-Schätzer': 'aep_estimator',
        'Status': 'status'
    }

    column_mapping_final = {
        'Datum': 'date',
        'Zeitzone': 'timezone',
        'von': 'from',
        'bis': 'to',
        'Datenkategorie': 'datacategory',
        'Datentyp': 'datatype',
        'Einheit': 'unit',
        'reBAP unterdeckt': 'rebap_shortfall',
        'reBAP ueberdeckt': 'rebap_surplus'
    }


    estimation_df = load_and_format_csv(PRICE_ESTIMATION_FILE_PATH, column_mapping_estimation)
    write_dataframe_to_postgres(engine, args.price_estimation_target_table, args.db_schema, estimation_df)

    final_df = load_and_format_csv(PRICE_FINAL_FILE_PATH, column_mapping_final)
    write_dataframe_to_postgres(engine, args.price_final_target_table, args.db_schema, final_df)
