with stg_customers as (
    -- Extraemos los clientes únicos para evitar duplicados si un cliente compró en varias ciudades
    select
        customer_id,
        max(customer_name) as customer_name,
        max(segment) as segment
    from {{ ref('stg_superstore') }}
    group by 1
),

customer_lifespan as (
    -- Traemos las fechas de primera/última compra calculadas en el intermedio
    select * from {{ ref('int_customer_lifespan') }}
),

customer_orders as (
    -- Traemos las métricas ya sumadas a nivel pedido
    select
        customer_id,
        count(order_id) as total_orders,
        sum(order_total_sales) as total_spent,
        sum(order_total_profit) as total_profit
    from {{ ref('int_orders_aggregated') }}
    group by 1
)

select
    c.customer_id,
    c.customer_name,
    c.segment,
    l.first_order_date,
    l.last_order_date,
    l.customer_tenure_days,
    o.total_orders,
    coalesce(o.total_spent, 0) as total_spent,
    coalesce(o.total_profit, 0) as total_profit
from stg_customers c
left join customer_lifespan l on c.customer_id = l.customer_id
left join customer_orders o on c.customer_id = o.customer_id