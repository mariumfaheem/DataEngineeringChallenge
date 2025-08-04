from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
import pendulum
from airflow.models.dag import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

infeed_payout_t5_query = """
    DROP TABLE IF EXISTS data_product.infeed_payout;

    CREATE TABLE data_product.infeed_payout AS

 WITH
-- CTE 1: Calculate produced energy (MWh) per 15-min interval for each asset.
-- This is used as a base for both market and fixed price calculations.
production_by_interval AS (
    SELECT
        delivery_start_utc::timestamp,
        asset_id,
        (actual_power_kw * 0.25 / 1000) AS produced_mwh
    FROM
        landing.dso_final_production
    WHERE
        actual_power_kw > 0
),

-- CTE 2: Calculate the volume-weighted average price (VWAP) for each 15-min interval.
-- This is needed for 'market' price model contracts.
market_prices AS (
    SELECT
        deliverystart::timestamp,
        SUM(price * volume) / SUM(volume) AS market_vwap
    FROM
        landing.trade
    WHERE
        source = 'public'
        AND volume > 0
    GROUP BY
        deliverystart
),

-- CTE 3: Aggregate interval production into a total monthly production for each asset.
-- This is needed for 'fixed' price model contracts.
monthly_production AS (
    SELECT
        asset_id,
        DATE_TRUNC('month', delivery_start_utc)::DATE AS invoicing_month,
        SUM(produced_mwh) AS total_monthly_production_mwh
    FROM
        production_by_interval
    GROUP BY
        asset_id, invoicing_month
)

-- Part 1: Calculate payouts for 'market' price models
SELECT
    c.asset_id,
    c.contract_id,
    DATE_TRUNC('month', pi.delivery_start_utc)::DATE AS invoicing_month,
    'market' AS price_model, -- Added for clarity in the final output
    COALESCE(SUM(pi.produced_mwh), 0) AS total_monthly_production_mwh,
    COALESCE(SUM(pi.produced_mwh * mp.market_vwap), 0) AS infeed_payout
FROM
    landing.asset_contracts c
LEFT JOIN
    production_by_interval pi ON pi.asset_id = c.asset_id
LEFT JOIN
    market_prices mp ON pi.delivery_start_utc = mp.deliverystart
WHERE
    c.price_model = 'market'
GROUP BY
    c.asset_id, c.contract_id, invoicing_month

UNION ALL

-- Part 2: Calculate payouts for 'fixed' price models
SELECT
    c.asset_id,
    c.contract_id,
    mp.invoicing_month,
    'fixed' AS price_model, -- Added for clarity in the final output
    COALESCE(mp.total_monthly_production_mwh, 0) AS total_monthly_production_mwh,
    COALESCE((mp.total_monthly_production_mwh * c.price), 0) AS infeed_payout
FROM
    landing.asset_contracts c
LEFT JOIN
    monthly_production mp ON mp.asset_id = c.asset_id
WHERE
    c.price_model = 'fixed'

-- Final sorting for the combined result set
ORDER BY
    asset_id, invoicing_month;
"""

fees_t5_query = """
    DROP TABLE IF EXISTS data_product.invoice_fees;

    CREATE TABLE data_product.invoice_fees AS
 WITH monthly_production AS (
    SELECT
        asset_id,
        DATE_TRUNC('month', delivery_start_utc::timestamp)::DATE AS invoicing_month,
        SUM(actual_power_kw::numeric * 0.25 / 1000) AS total_produced_mwh
    FROM
        landing.dso_final_production
    GROUP BY
        asset_id, invoicing_month
),

-- CTE 2: Calculate the single, volume-weighted average price (VWAP) for each month
monthly_market_vwap AS (
    SELECT
        DATE_TRUNC('month', deliverystart::timestamp)::DATE AS invoicing_month,
        SUM(price::numeric * volume::numeric) / SUM(volume::numeric) AS monthly_vwap
    FROM
        landing.trade
    WHERE
        source = 'public'
        AND volume::numeric > 0
    GROUP BY
        invoicing_month
),

-- CTE 3: Calculate the net fee amount using the CASE statement
fees_calculated AS (
    SELECT
        c.asset_id,
        c.contract_id,
        c.fee_model,
        p.invoicing_month,
        -- The CASE statement applies the correct formula based on the fee model
        CASE
            WHEN c.fee_model = 'fixed_as_produced'
            THEN p.total_produced_mwh * c.fee::numeric

            WHEN c.fee_model = 'fixed_for_capacity'
            THEN c.capacity::numeric * c.fee::numeric

            WHEN c.fee_model = 'percent_of_market'
            THEN p.total_produced_mwh * m.monthly_vwap * c.fee::numeric

            ELSE 0
        END AS fee_net_amount
    FROM
        landing.asset_contracts c
    LEFT JOIN
        monthly_production p ON c.asset_id = p.asset_id
    LEFT JOIN
        monthly_market_vwap m ON p.invoicing_month = m.invoicing_month
)

SELECT
    asset_id,
    contract_id,
    fee_model,
    invoicing_month,
    fee_net_amount,
    fee_net_amount * 0.19 AS vat_amount,
    fee_net_amount * 1.19 AS fee_gross_amount
FROM
    fees_calculated
ORDER BY
    asset_id, invoicing_month;

"""


with DAG(
        dag_id='invoicing_pipeline.py',
        start_date=datetime(2024, 7, 1),
        schedule_interval='@daily',
        catchup=False,
        tags=['Infeed payput', 'fees'],
) as dag:

    infeed_payout_t5_query = PostgresOperator(
        task_id="portfolio_performance",
        postgres_conn_id="postgres_default",
        sql=infeed_payout_t5_query,
    )


    fees_t5_query = PostgresOperator(
        task_id="trading_performance_metrics",
        postgres_conn_id="postgres_default",
        sql=fees_t5_query,
    )
    infeed_payout_t5_query >> fees_t5_query