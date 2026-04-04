/*
    FACT TABLE: One row per order.
    Primary table for the dashboard.
    Materialized as TABLE for fast queries.
*/

with orders as (
    select * from {{ ref('int_orders_enriched') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

final as (
    select
        o.order_id,
        o.customer_id,
        c.customer_unique_id,

        o.purchased_at,
        cast(o.purchased_at as date)                    as purchase_date,
        extract('year'  from o.purchased_at)::int       as purchase_year,
        extract('month' from o.purchased_at)::int       as purchase_month,
        strftime(o.purchased_at, '%Y-%m')               as year_month,

        o.order_status,
        o.delivery_status,

        c.city      as customer_city,
        c.state     as customer_state,

        o.total_revenue,
        o.items_subtotal,
        o.total_freight,
        o.item_count,
        o.payment_types,
        o.max_installments,

        o.actual_delivery_days,
        o.days_vs_estimate,

        o.review_score,
        o.review_comment

    from orders o
    left join customers c on o.customer_id = c.customer_id
)

select * from final
