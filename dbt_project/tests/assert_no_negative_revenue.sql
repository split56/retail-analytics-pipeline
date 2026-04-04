-- Fails if any order has negative revenue
select order_id, total_revenue
from {{ ref('fct_orders') }}
where total_revenue < 0
