with orders as (
    select * from {{ ref('stg_superstore') }} -- Ahora se hace al staging en lugar de la semilla .csv
),

final as (
    select
        order_id,
        order_date,
        customer_id,
        category,
        sub_category,
        sales,
        profit,
        quantity,
        -- Cálculo de margen de beneficio
        case 
            when sales > 0 then (profit / sales) * 100 
            else 0 
        end as profit_margin
    from orders
)

select * from final