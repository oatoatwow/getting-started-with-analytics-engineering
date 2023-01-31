with 
orders as (
  select 
  *
  from {{ref('stg_greenery_order')}}
)

,order_cohort as (

    select 
    user_id
    ,count(distinct order_id) as user_orders
    from orders
    group by 1
)

,users_bucket as (

    select 
    user_id
    ,(user_orders>=2)::int as has_two_purchases
    from order_cohort
)

, final as (
    select 
    sum(has_two_purchases)::float /count(distinct user_id)::float as repeat_rate
    from users_bucket
)

select * from final