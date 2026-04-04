-- Fails if order was delivered before it was purchased
select order_id, actual_delivery_days
from {{ ref('fct_orders') }}
where actual_delivery_days is not null
  and actual_delivery_days < 0


