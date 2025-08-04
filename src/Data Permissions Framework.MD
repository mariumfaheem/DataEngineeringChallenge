

### Task 7: Data Permissions Framework
This framework is built on the principle of **Role-Based Access Control (RBAC)**, which is a standard and highly effective security model. The core idea is to assign permissions to *roles* rather than individual users, which makes the system much easier to manage.

The framework can be broken down into three steps:
1. Who
2. What
3. How

#### Step 1: Define the Roles (The "Who")

Based on the challenge description, I identify several key user roles. Some are internal to FlexPower, and one is external (the customer).

| Role | Description |
| :--- | :--- |
| **Trader** | An internal user who actively buys and sells energy. Needs real-time data to make market decisions. |
| **Sales Manager** | An internal user managing relationships with asset owners. Needs to see performance and financial data for the specific assets they manage. |
| **Operations Analyst** | An internal user responsible for monitoring the overall health and performance of the entire portfolio. |
| **Asset Owner** | An **external user** (the customer). They should only ever see data related to their own specific assets. |
| **Administrator** | A technical user (like a Data Engineer) who needs full access to maintain and debug the system. |

#### Step 2: Identify the Resources (The "What")

These are the data entities and reports that need to be secured. We can group them logically.

  * **Raw Data / Tables (`landing` schema):**

      * `asset_contracts`
      * `dso_final_production`.
      * `imbalance_price_estimate`
      * `imbalance_price_final`
      * `redispatch_compensation`
      * `redispatch_flag`
      * `trade`

  * **Processed Data / Reports (`data_product` schema):**

      * `asset_forecasts`
      * `imbalance_cost_analysis`
      * `portfolio_forecast`
      * `portfolio_performance`
      * `trading_performance_model`


#### Step 3: Create an Access Control Matrix (The "How")

This matrix connects the **Roles** to the **Resources** with specific **Permissions** (standard SQL privileges). This is the heart of your security concept. It enforces a clear separation of duties.

| Resource | `db_admin` | `data_engineer` | `data_analyst` | `etl_service` |
| :--- | :--- | :--- | :--- | :--- |
| **-- Database Schemas --** | | | | |
| **`landing` Schema** | `ALL PRIVILEGES` | `SELECT` | `NONE` | `SELECT`, `INSERT` |
| **`data_product` Schema** | `ALL PRIVILEGES` | `ALL PRIVILEGES` | `SELECT`, `EXECUTE` | `SELECT`, `INSERT`, `UPDATE` |
| **-- Reporting Layer --** | | | | |
| **BI Dashboards** | View (All) | View (All) | View (Relevant) | `NONE` |




  * **Strict Segregation:** The `data_analyst` is completely blocked from the raw `landing` schema. This is critical for ensuring they use the official, vetted "source of truth" in the `data_product` schema.
  * **Least Privilege:** The `etl_service` account can only do what it absolutely needs to: write to `landing` and then manage data in `data_product`. It cannot, for instance, create new tables. The `data_analyst` only gets `SELECT` access, preventing accidental data modification.
  * **Row-Level Security (RLS):** While not shown in this high-level matrix, you would implement RLS in your **BI Tool** or as a database policy. For instance, different analysts could view the same `portfolio_performance` report but be filtered to see only the data relevant to their business unit.


#### Query to Create Role and Grant Least Previleges 


      * **Create the roles:**
        ```sql
        CREATE ROLE data_engineer;
        CREATE ROLE data_analyst;
        CREATE ROLE etl_service;
        ```
      * **Grant permissions:**
        ```sql

        GRANT USAGE ON SCHEMA landing TO data_engineer, etl_service;
        GRANT USAGE ON SCHEMA data_product TO data_engineer, data_analyst, etl_service;


        GRANT SELECT ON ALL TABLES IN SCHEMA data_product TO data_analyst;
        ALTER DEFAULT PRIVILEGES IN SCHEMA data_product GRANT SELECT ON TABLES TO data_analyst;


        GRANT SELECT ON ALL TABLES IN SCHEMA landing TO data_engineer;
        GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA data_product TO data_engineer;

        ```
