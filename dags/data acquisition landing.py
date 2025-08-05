
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
import pendulum
from airflow.models.dag import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator


with DAG(
    dag_id='data_acquisition_landing',
    start_date=datetime(2024, 7, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['assets_forecast', 'dso_production_data','master_trade_data','imbalance_price','exchange_trade_data'],
) as dag:

    # Task 1
    assets_forecast_ = BashOperator(
        task_id='assets_forecast',
        bash_command="""
        export PYTHONPATH=/opt/airflow
        python /opt/airflow/src/vpp/vpp_asset_portfolio_forecasts.py 
        """,
        do_xcom_push=True,
    )

    #task  2 - cleaning of final production data
    dso_production_data_etl = BashOperator(
        task_id='dso_production_data_etl',
        bash_command="""
        # Set python path to ensure modules are found correctly
        export PYTHONPATH=/opt/airflow
        python /opt/airflow/src/distribution_system_operator/ingest_dso_asset_production_data.py
        """,
        do_xcom_push=True,
    )

    #task 3 -> master trade data
    ingest_exchange_trade_data = BashOperator(
        task_id='master_trade_data',
        bash_command="""
        # Set python path to ensure modules are found correctly
        export PYTHONPATH=/opt/airflow
        python /opt/airflow/src/exchange/ingest_exchange_trade_data.py 
        """,
        do_xcom_push=True,
    )

    # task 4 - Imbalance Cost Analysis
    ingest_imbalance_price  = BashOperator(
        task_id='Imbalance_Cost_Analysis',
        bash_command="""
        # Set python path to ensure modules are found correctly
        export PYTHONPATH=/opt/airflow
        python /opt/airflow/src/imbalance/ingest_imbalance_price.py 
        """,
        do_xcom_push=True,
    )
    #Task 5: Invoice Generation
    assets_contract_invoices  = BashOperator(
        task_id='assets_contract_invoices',
        bash_command="""
        # Set python path to ensure modules are found correctly
        export PYTHONPATH=/opt/airflow
        python /opt/airflow/src/invoicing/assets_contract.py 
        """,
        do_xcom_push=True,
    )

    assets_forecast_ >> dso_production_data_etl >> ingest_exchange_trade_data >> ingest_imbalance_price >> assets_contract_invoices



