with source as (
    select * from {{ ref('superstore') }} 
),

renamed as (
    select
        -- Identificadores (Primary & Foreign Keys)
        "Row.ID" as row_id,
        "Order.ID" as order_id,
        "Customer.ID" as customer_id,
        "Product.ID" as product_id,

        -- Fechas y extracción de componentes de tiempo
        cast("Order.Date" as date) as order_date,
        cast("Ship.Date" as date) as ship_date,
        extract(year from cast("Order.Date" as date)) as order_year_extracted,
        extract(month from cast("Order.Date" as date)) as order_month,
        extract(dow from cast("Order.Date" as date)) as order_day_of_week,
        
        -- Columnas de tiempo nativas del dataset
        cast("Year" as integer) as order_year,
        cast("weeknum" as integer) as order_weeknum,

        -- Información del Cliente y Geografía
        "Customer.Name" as customer_name,
        "Segment" as segment,
        "City" as city,
        "State" as state,
        "Country" as country,
        "Region" as region,
        "Market" as market,
        "Market2" as market2, -- Columna adicional de mercado

        -- Información del Producto
        "Category" as category,
        "Sub.Category" as sub_category,
        "Product.Name" as product_name,

        -- Detalles del Envío / Pedido
        "Ship.Mode" as ship_mode,
        "Order.Priority" as order_priority,

        -- Métricas Financieras y Cuantitativas
        cast("Sales" as numeric) as sales,
        cast("Quantity" as integer) as quantity,
        cast("Discount" as numeric) as discount,
        cast("Profit" as numeric) as profit,
        cast("Shipping.Cost" as numeric) as shipping_cost

        -- Nota: La columna "记录数" (ji_lu_shu / record_count) fue omitida intencionalmente 
        -- porque su valor es una constante (1) y no aporta valor analítico real.

    from source
)

select * from renamed