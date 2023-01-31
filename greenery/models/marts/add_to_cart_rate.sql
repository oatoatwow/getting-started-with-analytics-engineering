with
events as (
    select * from {{ref('stg_greenery_events')}}
)
, unique_sessions as (
select
    count(distinct session_id) as number_of_unique_sessions
from events
)
, unique_add_to_cart_sessions as (
select
count(distinct session_id) as number_of_unique_add_to_cart_sessions
from events
where event_type = 'add_to_cart'
)
, final as (
select
    number_of_unique_add_to_cart_sessions
    , number_of_unique_sessions
    , number_of_unique_add_to_cart_sessions::float / number_of_unique_sessions::float as add_to_car
from unique_add_to_cart_sessions, unique_sessions
)

select * from final