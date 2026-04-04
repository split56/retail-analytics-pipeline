/*
    Aggregate order history per unique customer.
    Use the MOST RECENT city/state (customers can move).
*/

with customers as (
    select * from {{ ref('stg_customers') }}
),

orders as (
    select * from {{ ref('int_orders_enriched') }}
),

-- Get the most recent address per customer
latest_address as (
    select
        c.customer_unique_id,
        c.city,
        c.state,
        row_number() over (
            partition by c.customer_unique_id
            order by o.purchased_at desc
        ) as rn
    from customers c
    left join orders o on c.customer_id = o.customer_id
),

addresses as (
    select customer_unique_id, city, state
    from latest_address
    where rn = 1
),

-- Aggregate order metrics per unique customer
customer_metrics as (
    select
        c.customer_unique_id,

        count(distinct o.order_id)          as total_orders,
        sum(o.total_revenue)                as lifetime_value,
        avg(o.total_revenue)                as avg_order_value,
        min(o.purchased_at)                 as first_order_at,
        max(o.purchased_at)                 as last_order_at,
        avg(o.review_score)                 as avg_review_score,

        count(case when o.delivery_status = 'Late' then 1 end)
                                            as late_delivery_count

    from customers c
    left join orders o on c.customer_id = o.customer_id
    group by c.customer_unique_id
),

final as (
    select
        m.*,
        a.city,
        a.state
    from customer_metrics m
    left join addresses a using (customer_unique_id)
)

select * from final
