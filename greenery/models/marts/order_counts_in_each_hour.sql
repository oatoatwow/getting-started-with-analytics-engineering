with 
order_counts_in_each_hour as (
  select 
  date_trunc('hour', created_at) as hours
  ,count(distinct order_id) as order_count
  from {{ref('stg_greenery_order')}}
  group by 1
)

select avg(order_count)
from order_counts_in_each_hour