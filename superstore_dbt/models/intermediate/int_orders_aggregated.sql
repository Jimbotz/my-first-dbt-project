with staging as (
    select * from {{ ref('stg_superstore') }}
),

orders_aggregated as (
    select
        order_id,
        customer_id,
        order_date,
        ship_date,
        ship_mode,
        market,
        region,
        -- Agregamos las métricas a nivel de pedido total
        count(product_id) as total_distinct_products,
        sum(quantity) as total_items_in_order,
        sum(sales) as order_total_sales,
        sum(profit) as order_total_profit,
        sum(shipping_cost) as order_total_shipping_cost,
        -- Identificar si el pedido tuvo algún descuento en alguno de sus items
        max(discount) as max_discount_applied
    from staging
    group by 1, 2, 3, 4, 5, 6, 7
)

select * from orders_aggregated