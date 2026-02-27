with orders as (
    select * from {{ ref('int_orders_aggregated') }}
),

market_metrics as (
    select
        market,
        count(order_id) as total_orders,
        sum(total_items_in_order) as total_items_sold,
        sum(order_total_sales) as total_sales,
        sum(order_total_profit) as total_profit,
        sum(order_total_shipping_cost) as total_shipping_cost
    from orders
    group by 1
),

final as (
    select
        *,
        case 
            when total_sales > 0 then (total_profit / total_sales) * 100 
            else 0 
        end as profit_margin_pct,
        case 
            when total_sales > 0 then (total_shipping_cost / total_sales) * 100 
            else 0 
        end as shipping_cost_pct
    from market_metrics
)

select * from final
order by total_profit desc