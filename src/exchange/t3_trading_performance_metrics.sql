--Task 3 trading_performance_metrics Trading Performance Metrics
--private is order for flexpower only
--public orders of all companies

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





