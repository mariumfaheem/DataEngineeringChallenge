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