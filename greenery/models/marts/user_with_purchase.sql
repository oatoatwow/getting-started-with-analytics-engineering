with user_with_purchase as (
select 
  user_id
  ,case
  when count(order_id) = 1 then 'one_purchase'
  when count(order_id) = 2 then 'tow_purchase'
  when count(order_id) >= 3 then 'three_plus_purchase'
  end as puchase
from {{ref('stg_greenery_order')}}
group by 1
)

select puchase
,count(user_id) as user_count
from user_with_purchase
group by 1