with user_orders as (
    select 
        user_id,
        count(distinct case when order_status = 'Complete' then order_id end) as completed_orders,
        max(case when order_status = 'Complete' then order_created_at end) as last_order_completed_at,
        count(distinct case when order_status = 'Returned' then order_id end) as returned_orders,
        max(case when order_status = 'Returned' then order_created_at end) as last_order_returned_at,
        count(distinct case when order_status = 'Cancelled' then order_id end) as cancelled_orders,
        max(case when order_status = 'Cancelled' then order_created_at end) as last_order_cancelled_at,
        count(distinct case when order_status in ('Complete', 'Returned', 'Cancelled') then order_id end) as total_orders, -- completed lifecycle
        min(order_created_at) as first_order_at,
        max(order_created_at) as last_order_at
    from {{ ref("stg_the_look__orders") }}
    group by 
        user_id
),
user_total_orders as (
    select 
        user_id,
        sum(order_item_sale_price) as user_ltv,
        count(order_item_id) as user_items_purchased
    from {{ ref("stg_the_look__order_items") }}
    where order_item_status = 'Complete'
    and order_item_created_at >= '2026-01-01'
    group by
        user_id
)
select 
    uo.user_id,
    uo.completed_orders,
    uo.last_order_completed_at,
    uo.returned_orders,
    uo.last_order_returned_at,
    uo.cancelled_orders,
    uo.last_order_cancelled_at,
    uo.total_orders,
    uo.first_order_at,
    uo.last_order_at,
    uot.user_ltv,
    uot.user_items_purchased,
    case when uo.last_order_at >= dateadd(day, -90, current_date) then true else false end as is_active_customer
from user_orders uo 
left join user_total_orders uot
    on uo.user_id = uot.user_id