# Data Engineer Assessment - Subscription Revenue Modeling

## Overview
Solution for the Data Engineer technical assessment focused on modeling Subscription Revenue (ARR) using **dbt** and **Snowflake**.

## Project Structure
- **Staging:** `stg_subscriptions.sql` cleans raw data and fixes inconsistent date records (End Date < Start Date).
- **Marts:**
  - `dim_dates.sql`: Generates a date spine to handle monthly reporting.
  - `fct_monthly_revenue.sql`: Expands subscriptions into monthly rows.
  - `fct_mrr_movements.sql`: Calculates monthly ARR changes and categorizes them (New, Churn, Upgrade, etc.).

## Technologies Used
- dbt Cloud
- Snowflake
- Python (for visualization)

## Author
**Nicolás Ezequiel Fani**
