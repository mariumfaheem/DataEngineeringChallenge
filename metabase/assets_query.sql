SELECT
	asset_id,
    SUM(power) AS total_power_forecast
FROM
    data_product.asset_forecasts
GROUP BY
	asset_id
