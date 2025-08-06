---t4 imbalance_cost_analysis

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

    -- Calculate the imbalance volume in Megawatts (MW) by converting from Kilowatts (kW)
    (d.actual_power_kw - d.forecasted_power_kw) / 1000.0 AS imbalance_volume_mw,

    -- This is the main logic for calculating the penalty
    -- It uses the imbalance volume and applies the correct price based on the rules
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