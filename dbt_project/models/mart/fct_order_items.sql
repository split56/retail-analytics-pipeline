/*
    FACT TABLE: One row per product within each order.
    Used for product-level and seller-level analysis.
*/

with items as (
    select * from {{ ref('stg_order_items') }}
),

orders as (
    select
        order_id,
        purchased_at,
        cast(purchased_at as date)          as purchase_date,
        strftime(purchased_at, '%Y-%m')     as year_month,
        order_status
    from {{ ref('stg_orders') }}
),

products as (
    select * from {{ ref('stg_products') }}
),

sellers as (
    select * from {{ ref('stg_sellers') }}
),

final as (
    select
        i.order_id,
        i.order_item_id,
        i.product_id,
        i.seller_id,

        o.purchased_at,
        o.purchase_date,
        o.year_month,
        o.order_status,

        p.category              as product_category,
        p.weight_g,

        s.city                  as seller_city,
        s.state                 as seller_state,

        i.price,
        i.freight_value,
        i.price + i.freight_value as total_item_value

    from items i
    left join orders   o on i.order_id   = o.order_id
    left join products p on i.product_id = p.product_id
    left join sellers  s on i.seller_id  = s.seller_id
)

select * from final
