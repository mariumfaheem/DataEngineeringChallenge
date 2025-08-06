-- You have enough data. The calculation would be: Total Produced Volume (MWh) * Fixed Price
--
-- Contract Data: The asset_contracts file would provide the asset_id, the pricing_model ("fixed"), and the agreed-upon price.
--
-- Production Data: The dso_final_production table provides the actual_power_kw for each asset_id in 15-minute intervals. You can calculate the total produced volume for the month from this.
--
-- For each 15-minute row, the energy in MWh is: (actual_power_kw * 0.25 hours) / 1000.
--
-- You would sum this value for the entire month to get the total volume.

WITH monthly_production AS (
    -- Step 1: Calculate the total produced energy (MWh) for each asset per month.
    SELECT
        asset_id,
        -- Cast text to timestamp for the date function.
        DATE_TRUNC('month', delivery_start_utc::timestamp)::DATE AS invoicing_month,
        -- The interval is 15 minutes (0.25 hours)
        SUM(actual_power_kw * 0.25 / 1000) AS total_produced_mwh
    FROM
        landing.dso_final_production
    GROUP BY
        asset_id,
        invoicing_month
)
-- Step 2: Join monthly production with contracts and calculate the payout.
SELECT
    c.asset_id,
    c.contract_id,
    p.invoicing_month,
    COALESCE(p.total_produced_mwh,0),
    c.price,
    COALESCE((p.total_produced_mwh * c.price),0) AS infeed_payout
FROM
    landing.asset_contracts c
LEFT JOIN
    monthly_production p ON p.asset_id = c.asset_id
WHERE
    c.price_model = 'fixed';

