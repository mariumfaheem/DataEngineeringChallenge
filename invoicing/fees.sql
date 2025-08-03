--fees: multiplying the production of the asset by the fee we agreed upon with the asset owner, depending on the fee model. For the fixed_as_produced fee model, we multiply the total produced volume with the fee from the base data. For the fixed_for_capacity fee model, we multiply the installed capacity of the asset with the fee from the base data. For the percent_of_market fee model, we multiply the total produced volume of the asset with the market VWAP and the fee percent from the base data. For each of these entries we also compute the unitary net amount, the VAT (at a rate of 19%) and the total gross amount.

-- CTE 1: Calculate total MWh produced per asset for each month
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

-- Final Step: Select from the calculated fees and compute VAT and Gross amounts
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