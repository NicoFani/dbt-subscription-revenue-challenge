-- This test validates that the cleanup logic in stg_subscriptions works correctly.
-- It fails if any record has an end_date earlier than the start_date.
-- Since the goal of a dbt test is to return failing rows, this query should return 0 rows.

select
    subscription_id,
    start_date,
    end_date
from {{ ref('stg_subscriptions') }}
where end_date < start_date