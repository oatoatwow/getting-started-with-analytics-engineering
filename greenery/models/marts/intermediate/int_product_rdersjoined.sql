{{
    config(
    materialized = 'view'
    )
}}

with
orders as (
select
    order_id
    ,promo_id
    ,user_id
    ,address_id
    ,order_cost
    ,shipping_cost
    ,order_total
    ,shipping_service
    ,created_at as order_created_at_utc
    ,estimated_delivery_at as estimated_delivery_at_utc
    ,delivered_at as delivered_at_utc
    ,status as order_status
from {{ref('stg_greenery_order')}}
)
,order_items as (
select
    order_id
    , product_id
    , quantity
from {{ ref('stg_greenery_order_items')}}
)

,products as (
select
    product_id
    , name as product_name
    , inventory
from {{ref('stg_greenery_products')}}
)
,final as (
select
o.order_id
, user_id
, p.product_id
, product_name
,promo_id
,order_cost
,shipping_cost
,order_total
,shipping_service
,order_created_at_utc
,estimated_delivery_at_utc
,delivered_at_utc
,order_status
from orders as o
left join order_items as oi
on  o.order_id=oi.order_id
left join products as p
on  oi.product_id=p.product_id
)

select * from final