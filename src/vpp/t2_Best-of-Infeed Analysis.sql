---Best-of-Infeed Analysis
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