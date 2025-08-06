# VPP Asset & Portfolio Forecast Ingestion

This script fetches **virtual power plant (VPP)** forecast data for individual assets, aggregates the forecasts at the portfolio level, and stores the results in PostgreSQL.

---

##  Script: `vpp_asset_portfolio_forecasts.py`

###  Purpose

- Retrieve time series forecasts for each asset from a remote VPP API.
- Normalize and format forecast data.
- Aggregate forecasts to generate a total portfolio-level forecast.
- Store both asset-level and portfolio-level forecasts in PostgreSQL.

---

##  How It Works

1. **Fetch forecast data** via the `get_forecast()` function for a predefined set of assets (`ASSET_IDS` from `config.py`).
2. Normalize and convert:
   - `asset_id` formatting
   - UNIX timestamps to UTC datetime
   - Forecast values to numeric
3. **Aggregate forecasts** by summing asset power per timestamp for the portfolio.
4. **Write to PostgreSQL**:
   - Asset-level forecasts: `data_product.asset_forecasts`
   - Portfolio-level forecasts: `data_product.portfolio_forecast`

---

##  Assumptions

- Forecasts are fetched using a client function `get_forecast()` located in `src/vpp/client.py`.
- Forecast values are provided in JSON format with fields `values`, `column_ids`, and timestamps (`start`, `version`).
- Forecast version timestamp is set to 15 minutes before midnight of the target delivery day (e.g., `2025-06-08`).
- PostgreSQL connection and asset list are configured in `config.py`.
- More details about the data sources can be found [here](../../../data/vpp/README.md)

---

## **Run Example:**

```bash
PYTHONPATH=. python src/vpp/vpp_asset_portfolio_forecasts.py \
  --asset_forecasts_table asset_forecasts \
  --portfolio_forecast_table portfolio_forecast
````

