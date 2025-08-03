#  Task 2:  Implementation - Orchestrate the pipeline
I've created a total of 4 DAGs:

1. End_to_End_Pipeline – This DAG covers the full flow from source all the way to the data warehouse (DWH). 
2. The second approach breaks things down into more modular DAGs, where each one triggers the next. For example, data_acquisition reads data from the sources and writes it into the raw layer. Then it triggers another DAG called process_staging_data_to_dwh, which continues the pipeline – and so on.
3. Dag schedular is @daily
