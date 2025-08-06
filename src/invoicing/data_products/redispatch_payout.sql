
WITH ranked_forecasts AS (
    SELECT
        start_time_utc,
        version_time_utc,
        asset_id,
        power,
        -- This is the window function that does the magic
        ROW_NUMBER() OVER(
            PARTITION BY asset_id
            ORDER BY start_time_utc DESC
        ) as rn
    FROM
        data_product.asset_forecasts
)


SELECT
    start_time_utc,
    version_time_utc,
    asset_id,
    power*price_euro_per_mwh as redispatch_payout
FROM
    ranked_forecasts r
	inner join landing.redispatch_compensation re on re.delivery_start_utc=r.start_time_utc
WHERE
    rn = 1;