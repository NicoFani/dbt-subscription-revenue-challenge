with monthly_revenue as (
    -- Import the detailed data created in the previous step
    select * from {{ ref('fct_monthly_revenue') }}
),

-- 1. Get a unique list of accounts and all possible months
-- This is critical for detecting when a customer stops paying (Churn)
accounts as (
    select distinct account_id from monthly_revenue
),

months as (
    select date_month from {{ ref('dim_dates') }}
),

account_months as (
    select 
        accounts.account_id,
        months.date_month
    from accounts
    cross join months
),

-- 2. Group ARR by account and month
agged_revenue as (
    select
        account_months.date_month,
        account_months.account_id,
        -- If there is no data in monthly_revenue, populate with 0 (indicating Churn or inactivity)
        coalesce(sum(monthly_revenue.total_arr), 0) as current_mrr
    from account_months
    left join monthly_revenue 
        on account_months.account_id = monthly_revenue.account_id
        and account_months.date_month = monthly_revenue.date_month
    group by 1, 2
),

-- 3. Calculate previous month using LAG (Window Function)
lagged_revenue as (
    select
        date_month,
        account_id,
        current_mrr,
        -- Retrieve the customer's payment from the previous month
        lag(current_mrr) over (partition by account_id order by date_month) as previous_mrr,
        
        -- Calculate the exact difference
        current_mrr - lag(current_mrr) over (partition by account_id order by date_month) as mrr_change
    from agged_revenue
),

-- 4. Apply the assessment business rules
final_categorization as (
    select
        *,
        case
            -- NEW: Revenue appears for the first time (Previously NULL or 0, and now > 0)
            when (previous_mrr is null or previous_mrr = 0) and current_mrr > 0 then 'New'
            
            -- CHURN: Was paying something, now pays 0
            when previous_mrr > 0 and current_mrr = 0 then 'Churn'
            
            -- REACTIVATION: (The assessment defines this as ARR becoming positive again after being zero)
            -- Note: For simplification in this logic, standard 'New' logic often captures early reactivations 
            -- unless we explicitly track 'ever_active' status. 
            -- Here we assume (Previous=0 AND Current>0) covers both New and Reactivation scenarios.
            
            -- UPGRADE: ARR increases compared to the previous month
            when current_mrr > previous_mrr and previous_mrr > 0 then 'Upgrade'
            
            -- DOWNGRADE: ARR decreases compared to the previous month
            when current_mrr < previous_mrr and current_mrr > 0 then 'Downgrade'
            
            -- NO-CHANGE: Total ARR remains the same
            when current_mrr = previous_mrr and current_mrr > 0 then 'No-change'
            
            else null -- For rows where everything is 0 (future months or distant past)
        end as change_category
    from lagged_revenue
    -- Filter out empty rows (where no activity occurred: 0 before and 0 now)
    where not (current_mrr = 0 and (previous_mrr = 0 or previous_mrr is null))
)

select * from final_categorization
order by date_month desc