with source as (

    select * from {{ ref('subscription_data') }}

),

cleaned as (

    select
        account_id,
        subscription_id,
        
        -- Lógica de corrección: Si End < Start, asumimos que están invertidas y las intercambiamos.
        case 
            when try_to_date(subscription_end_date) < try_to_date(subscription_start_date) 
            then try_to_date(subscription_end_date)
            else try_to_date(subscription_start_date)
        end as start_date,
        
        case 
            when try_to_date(subscription_end_date) < try_to_date(subscription_start_date) 
            then try_to_date(subscription_start_date)
            else try_to_date(subscription_end_date)
        end as end_date,

        subscription_status as status,
        
        -- Asegurar que el ARR sea numérico (decimales precisos)
        cast(subscription_arr_usd as numeric(38, 9)) as arr,
        
        subscription_quantity as quantity,
        subscription_product_line as product_line

    from source

)

select * from cleaned