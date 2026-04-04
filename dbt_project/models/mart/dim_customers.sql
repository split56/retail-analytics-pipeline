/*
    DIMENSION TABLE: One row per unique customer with full history.
*/

select
    customer_unique_id,
    city,
    state,
    total_orders,
    round(lifetime_value, 2)    as lifetime_value,
    round(avg_order_value, 2)   as avg_order_value,
    first_order_at,
    last_order_at,
    round(avg_review_score, 2)  as avg_review_score,
    late_delivery_count,

    case
        when lifetime_value >= 500  then 'High Value'
        when lifetime_value >= 150  then 'Mid Value'
        else                             'Low Value'
    end as customer_segment,

    case
        when total_orders > 1 then true
        else false
    end as is_repeat_customer

from {{ ref('int_customer_summary') }}
