-- For the 'Market' Pricing Model: ❌
-- You are missing a critical piece of data: the quarter-hourly market VWAP (Volume-Weighted Average Price).
--
-- The asset_contracts file would tell you that an asset uses the "market" model.
--
-- The dso_final_production table gives you the production for each 15-minute interval.
--
-- However, to complete the calculation, you need to multiply the production of each interval by the corresponding market VWAP for that same interval. This market price data is not in either of the datasets you provided.
--
-- You would need a third dataset, let's call it market_prices, that contains timestamps and the VWAP for each 15-minute slot.


-- CTE 1: Calculate the quarter-hourly market VWAP from public trades
WITH market_prices AS (
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

-- CTE 2: Calculate the produced energy (MWh) per interval for each asset
production_energy AS (
    SELECT

        delivery_start_utc::timestamp,
        asset_id,
        (actual_power_kw * 0.25 / 1000) AS produced_mwh
    FROM
        landing.dso_final_production
    WHERE
        actual_power_kw > 0
)

-- Final Step: Join production, prices, and contracts to get the final payout
SELECT
    c.asset_id,
    c.contract_id,
    DATE_TRUNC('month', pe.delivery_start_utc)::DATE AS invoicing_month,
    SUM(pe.produced_mwh) AS total_monthly_production_mwh,
    SUM(pe.produced_mwh * mp.market_vwap) AS market_infeed_payout
FROM
landing.asset_contracts C
LEFT JOIN  production_energy pe ON pe.asset_id = c.asset_id
left JOIN market_prices mp ON pe.delivery_start_utc = mp.deliverystart
WHERE
    c.price_model = 'market'
GROUP BY
    c.asset_id,
    c.contract_id,
    invoicing_month
ORDER BY
    c.asset_id,
    invoicing_month;