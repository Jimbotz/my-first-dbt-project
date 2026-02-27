with orders as (
    select * from {{ ref('int_orders_aggregated') }}
),

customers as (
    select * from {{ ref('dim_customers') }}
),

regional_metrics as (
    select
        o.region,
        c.customer_id,
        c.customer_name,
        c.segment,
        count(o.order_id) as regional_orders,
        sum(o.order_total_sales) as regional_spent,
        sum(o.order_total_profit) as regional_profit
    from orders o
    left join customers c on o.customer_id = c.customer_id
    group by 1, 2, 3, 4
)

select * from regional_metrics
order by region asc, regional_spent desc