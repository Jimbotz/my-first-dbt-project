with staging as (
    select * from {{ ref('stg_superstore') }}
),

product_priority_metrics as (
    select
        product_id,
        product_name,
        category,
        count(distinct order_id) as total_orders,
        -- Contamos cuántas órdenes fueron críticas o altas
        sum(case when order_priority in ('Critical', 'High') then 1 else 0 end) as high_priority_orders
    from staging
    group by 1, 2, 3
),

final as (
    select
        product_id,
        product_name,
        category,
        total_orders,
        high_priority_orders,
        -- Calculamos el porcentaje de urgencia del producto
        case 
            when total_orders > 0 then (high_priority_orders * 100.0 / total_orders) 
            else 0 
        end as high_priority_percentage
    from product_priority_metrics
)

select * from final
order by high_priority_orders desc