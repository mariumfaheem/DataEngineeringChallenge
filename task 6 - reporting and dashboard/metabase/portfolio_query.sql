SELECT
    DATE_TRUNC('hour', start_time_utc) AS forecast_hour,
	    SUM(portfolio_actual_kw) AS portfolio_actual_kw,
    SUM(portfolio_forecast_kw) AS portfolio_forecast_kw,
    SUM(portfolio_deviation_kw) AS portfolio_deviation_kw
FROM
    data_product.portfolio_performance
GROUP BY
    forecast_hour
ORDER BY
    forecast_hour;