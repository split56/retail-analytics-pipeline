with source as (
    select * from {{ source('raw','order_items') }}
),

cleaned as (
    select
        order_id,
        order_item_id,
        product_id,
        seller_id,
        try_cast(shipping_limit_date as timestamp) as shipping_limit_at,
        price,
        freight_value
    from source
)

select * from cleaned