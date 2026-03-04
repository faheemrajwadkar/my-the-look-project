with user_orders as (
    select 
        u.user_version_sk,
        count(distinct case when o.order_status = 'Complete' then o.order_id end) as completed_orders,
        max(case when o.order_status = 'Complete' then o.order_created_at end) as last_order_completed_at,
        count(distinct case when o.order_status = 'Returned' then o.order_id end) as returned_orders,
        max(case when o.order_status = 'Returned' then o.order_created_at end) as last_order_returned_at,
        count(distinct case when o.order_status = 'Cancelled' then o.order_id end) as cancelled_orders,
        max(case when o.order_status = 'Cancelled' then o.order_created_at end) as last_order_cancelled_at,
        count(distinct case when o.order_status in ('Complete', 'Returned', 'Cancelled') then o.order_id end) as total_orders, -- completed lifecycle
        min(o.order_created_at) as first_order_at,
        max(o.order_created_at) as last_order_at
    from {{ ref("stg_the_look__orders") }} o 
    left join {{ ref("stg_the_look__users") }} u 
        on o.user_id = u.user_id 
        and o.order_created_at between u.valid_from and u.valid_to
    group by 
        u.user_version_sk
),
user_total_orders as (
    select 
        u.user_version_sk,
        sum(oi.order_item_sale_price) as user_ltv,
        count(oi.order_item_id) as user_items_purchased
    from {{ ref("stg_the_look__order_items") }} oi
    left join {{ ref("stg_the_look__users") }} u 
        on oi.user_id = u.user_id 
        and oi.order_item_created_at between u.valid_from and u.valid_to
    where oi.order_item_status = 'Complete'
    group by
        u.user_version_sk
)
select 
    uo.user_version_sk,
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
    on uo.user_version_sk = uot.user_version_sk