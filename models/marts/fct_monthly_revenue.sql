with dates as (
    select * from {{ ref('dim_dates') }}
),

subscriptions as (
    select * from {{ ref('stg_subscriptions') }}
),

monthly_revenue as (
        dates.date_month,
        dates.last_day_of_month,
        subscriptions.account_id,
        subscriptions.subscription_id,
        subscriptions.product_line,
        
        subscriptions.quantity,
        subscriptions.arr as total_arr,
        
        subscriptions.start_date,
        subscriptions.end_date

    from dates
    inner join subscriptions
        on subscriptions.start_date <= dates.last_day_of_month
        and subscriptions.end_date > dates.last_day_of_month
)

select * from monthly_revenue