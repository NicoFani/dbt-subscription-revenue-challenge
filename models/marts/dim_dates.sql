{{
    config(
        materialized = "table"
    )
}}

with date_spine as (

{{ dbt_utils.date_spine(
    datepart="month",
    start_date="cast('2020-01-01' as date)",
    end_date="cast('2027-01-01' as date)"
   )
}}

)

select
    date_month,
    -- "active on the final day of that month" 
    last_day(date_month) as last_day_of_month
from date_spine