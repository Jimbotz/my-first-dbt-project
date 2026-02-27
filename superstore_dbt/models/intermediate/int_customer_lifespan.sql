with staging as (
    select * from {{ ref('stg_superstore') }}
),

customer_dates as (
    select
        customer_id,
        min(order_date) as first_order_date,
        max(order_date) as last_order_date,
        -- Días transcurridos entre su primera y última compra
        max(order_date) - min(order_date) as customer_tenure_days
    from staging
    group by 1
)

select * from customer_dates