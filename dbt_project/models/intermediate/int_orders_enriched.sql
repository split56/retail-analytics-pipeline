/*
    Combines orders with payments, reviews, and item counts.
    One row per order with all key metrics.
*/

with orders as (
    select * from {{ ref('stg_orders') }}
),

payments_agg as (
    select
        order_id,
        sum(payment_value)              as total_revenue,
        max(payment_installments)       as max_installments,
        string_agg(distinct payment_type, ', ' order by payment_type)
                                        as payment_types
    from {{ ref('stg_order_payments') }}
    group by order_id
),

reviews_deduped as (
    select
        order_id,
        review_score,
        comment_message,
        review_created_at,
        row_number() over (
            partition by order_id
            order by review_created_at desc
        ) as rn
    from {{ ref('stg_order_reviews') }}
),

reviews as (
    select * from reviews_deduped where rn = 1
),

items_agg as (
    select
        order_id,
        count(*)            as item_count,
        sum(price)          as items_subtotal,
        sum(freight_value)  as total_freight
    from {{ ref('stg_order_items') }}
    group by order_id
),

final as (
    select
        o.order_id,
        o.customer_id,
        o.order_status,
        o.purchased_at,
        o.approved_at,
        o.shipped_at,
        o.delivered_at,
        o.estimated_delivery_at,

        datediff('day', o.purchased_at, o.delivered_at)          as actual_delivery_days,
        datediff('day', o.delivered_at, o.estimated_delivery_at) as days_vs_estimate,

        case
            when o.delivered_at is null                          then 'Not Delivered'
            when o.delivered_at <= o.estimated_delivery_at       then 'On Time'
            else                                                      'Late'
        end as delivery_status,

        coalesce(p.total_revenue, 0)    as total_revenue,
        p.payment_types,
        coalesce(p.max_installments, 0) as max_installments,
        coalesce(i.item_count, 0)       as item_count,
        coalesce(i.items_subtotal, 0)   as items_subtotal,
        coalesce(i.total_freight, 0)    as total_freight,
        r.review_score,
        r.comment_message               as review_comment

    from orders o
    left join payments_agg  p using (order_id)
    left join reviews       r using (order_id)
    left join items_agg     i using (order_id)
)

select * from final
