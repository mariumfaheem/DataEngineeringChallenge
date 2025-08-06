from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
import pendulum
from airflow.models.dag import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator


portfolio_performance_t2_query = """
    DROP TABLE IF EXISTS data_product.portfolio_performance;

    CREATE TABLE data_product.portfolio_performance AS

    WITH AssetLevelAnalysis AS (
    SELECT
        f.start_time_utc,
        f.asset_id,
        f.power AS forecasted_power_kw,
        a.actual_power_kw,
        (a.actual_power_kw - f.power) AS deviation_kw
    FROM
        data_product.asset_forecasts AS f
    inner JOIN
        landing.dso_final_production AS a
        ON f.asset_id = a.asset_id AND f.start_time_utc = a.delivery_start_utc::timestamp with time zone
    ORDER BY
        f.asset_id,
        f.start_time_utc
    )

    SELECT
        start_time_utc,
        SUM(forecasted_power_kw) AS portfolio_forecast_kw,
        SUM(actual_power_kw) AS portfolio_actual_kw,
        SUM(deviation_kw) AS portfolio_deviation_kw
    FROM
        AssetLevelAnalysis
    GROUP BY
        start_time_utc
    ORDER BY
        start_time_utc;
"""

trading_performance_metrics_t3_query = """
    DROP TABLE IF EXISTS data_product.trading_performance_metrics;

    CREATE TABLE data_product.trading_performance_metrics AS
      SELECT
    
        SUM(CASE
            WHEN Side = 'SELL' THEN Price * Volume
            WHEN Side = 'BUY'  THEN -(Price * Volume)
            ELSE 0
        END) AS trading_revenue,
    
        COUNT(*) AS number_of_trades,
    
        SUM(CASE
            WHEN Side = 'SELL' THEN Volume
            WHEN Side = 'BUY'  THEN -Volume
            ELSE 0
        END) AS net_traded_volume_mwh,
        SUM(CASE WHEN Side = 'SELL' THEN Price * Volume ELSE -(Price * Volume) END) / SUM(Volume) AS vwap_as_per_text,
    
        SUM(Price * Volume) / SUM(Volume) AS vwap_standard_definition,
        source
    FROM
        landing.trade
    group by source

"""

imbalance_cost_analysis_t4_query="""
DROP TABLE IF EXISTS data_product.imbalance_cost_analysis;

CREATE TABLE data_product.imbalance_cost_analysis AS
        
WITH deviations AS (
SELECT
    f.start_time_utc,
    f.asset_id,
    f.power AS forecasted_power_kw,
    a.actual_power_kw,
    (a.actual_power_kw - f.power) AS deviation_kw
FROM
    data_product.asset_forecasts AS f
inner JOIN
    landing.dso_final_production AS a
    ON f.asset_id = a.asset_id AND f.start_time_utc = a.delivery_start_utc::timestamp with time zone
ORDER BY
    f.asset_id,
    f.start_time_utc
)

SELECT
    d.start_time_utc,
    d.asset_id,

    -- imbalance volume in Megawatts (MW) by converting from Kilowatts (kW)
    (d.actual_power_kw - d.forecasted_power_kw) / 1000.0 AS imbalance_volume_mw,


    -- logic for calculating the penalty: It uses the imbalance volume and applies the correct price based on the rules
    CASE
        -- Rule 1: Use the final price if it is available (fp.start_time_utc is not null)
        WHEN fp.date IS NOT NULL THEN
            CASE
                -- Rule 1a: If volume is negative (a shortfall), use the shortfall price
                WHEN (d.actual_power_kw - d.forecasted_power_kw) < 0
                THEN ((d.actual_power_kw - d.forecasted_power_kw) / 1000.0) * fp.rebap_shortfall

                -- Rule 1b: If volume is positive (a surplus), use the surplus price
                ELSE ((d.actual_power_kw - d.forecasted_power_kw) / 1000.0) * fp.rebap_surplus
            END

        -- Rule 2: If the final price is NOT available, use the estimated price as a fallback
        ELSE ((d.actual_power_kw - d.forecasted_power_kw) / 1000.0) * ep.aep_estimator
    END AS penalty_cost_eur

FROM
    deviations AS d
LEFT JOIN
    landing.imbalance_price_final AS fp ON d.start_time_utc = fp.date
LEFT JOIN
    landing.imbalance_price_estimation AS ep ON d.start_time_utc = ep.date
ORDER BY
    d.start_time_utc;

"""


with DAG(
    dag_id='asset_portfolio_kpis_dag',
    start_date=datetime(2024, 7, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['Asset and Portfolio Forecasting', 'Trading Performance Metrics','Imbalance Cost Analysis'],
) as dag:


    #Task 1: Asset and Portfolio Forecasting
    run_postgres_etl = PostgresOperator(
        task_id="portfolio_performance",
        postgres_conn_id="postgres_default",
        sql=portfolio_performance_t2_query,
    )

    #task 3 ->Trading Performance Metrics
    master_trade_data_query = PostgresOperator(
        task_id="trading_performance_metrics",
        postgres_conn_id="postgres_default",
        sql=trading_performance_metrics_t3_query,
    )

    # Task 4: Imbalance Cost Analysis
    imbalance_cost_analysis_query = PostgresOperator(
        task_id="imbalance_cost_analysis_query",
        postgres_conn_id="postgres_default",
        sql=imbalance_cost_analysis_t4_query,
    )

    run_postgres_etl >> master_trade_data_query >> imbalance_cost_analysis_query






