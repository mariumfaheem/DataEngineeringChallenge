#  Task 2:  Implementation - Orchestrate the pipeline
I've created a total of 3 DAGs:

1. data acquisition landing.py– here assumption is data comingm from csv, mangoDB and API and i'm storing data into landing layer
2. asset_portfolio_kpis: These are all the kpis i'm calcualting and storing it into data products layer for business to use it
3. invoicing: it includes kpis which are required to calculate monhtly invocinf
