-- Fails if any order has 0 or fewer items
select order_id, item_count
from {{ ref('fct_orders') }}
where order_status = 'delivered'
  and item_count <= 0
