#  Task 2:  Implementation - Orchestrate the pipeline

I’ve created a total of three DAGs:

1. data_acquisition_landing.py – Assumes data is coming from CSV, MongoDB, and API sources, and stores it in the landing layer.

2. asset_portfolio_kpis – Calculates various KPIs and stores them in the data products layer for business use.

3. invoicing – Includes KPIs required to calculate monthly invoicing.
