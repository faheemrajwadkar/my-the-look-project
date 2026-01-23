with order_details_pre as (
    select 
        order_id,
        user_id,
        row_number() over (partition by user_id order by order_created_at, order_id) as user_order_number_asc,
        row_number() over (partition by user_id order by order_created_at desc, order_id desc) as user_order_number_desc,
        lag(order_created_at) over (partition by user_id order by order_created_at, order_id) as user_previous_order_created_at,
        lead(order_created_at) over (partition by user_id order by order_created_at, order_id) as user_next_order_created_at,
        order_status,
        user_gender,
        order_created_at,
        order_shipped_at,
        datediff('minute', order_created_at, order_shipped_at) as time_to_ship_mins,
        order_delivered_at,
        datediff('minute', order_shipped_at, order_delivered_at) as time_to_deliver_mins,
        datediff('minute', order_created_at, order_delivered_at) as time_to_complete_order_mins,
        order_returned_at,
        datediff('minute', order_delivered_at, order_returned_at) as time_to_return_mins,
        order_num_of_item as items_ordered
    from {{ ref("stg_the_look__orders") }}
),

order_details as (
    select 
        order_id,
        user_id,
        user_order_number_asc,
        case when user_order_number_asc = 1 then 1 else 0 end as is_first_order,
        user_order_number_desc,
        case when user_order_number_desc = 1 then 1 else 0 end as is_most_recent_order,
        user_previous_order_created_at,
        datediff('day', user_previous_order_created_at, order_created_at) as days_since_prior_order,
        user_next_order_created_at,
        datediff('day', order_created_at, user_next_order_created_at) as days_to_next_order,
        order_status,
        user_gender,
        order_created_at,
        order_shipped_at,
        time_to_ship_mins,
        order_delivered_at,
        time_to_deliver_mins,
        time_to_complete_order_mins,
        order_returned_at,
        time_to_return_mins,
        items_ordered
    from order_details_pre
),

order_metrics as (
    select 
        oi.order_id,
        count(case when oi.order_item_status = 'Complete' then oi.order_item_id end) as items_sold,
        count(case when oi.order_item_status = 'Returned' then oi.order_item_id end) as items_returned,
        sum(ii.inventory_item_cost) as items_cost,
        sum(oi.order_item_sale_price) as gross_revenue,
        sum(case when oi.order_item_status = 'Complete' then oi.order_item_sale_price else 0 end) as net_revenue    
    from {{ ref("stg_the_look__order_items") }} oi
    left join {{ ref("stg_the_look__inventory_items") }} ii 
        on oi.inventory_item_id = ii.inventory_item_id
    group by 
        oi.order_id
)

select 
    od.order_id,
    od.user_id,
    od.user_order_number_asc,
    od.is_first_order,
    od.user_order_number_desc,
    od.is_most_recent_order,
    od.user_previous_order_created_at,
    od.days_since_prior_order,
    od.user_next_order_created_at,
    od.days_to_next_order,
    od.order_status,
    od.user_gender,
    od.order_created_at,
    od.order_shipped_at,
    od.time_to_ship_mins,
    od.order_delivered_at,
    od.time_to_deliver_mins,
    od.time_to_complete_order_mins,
    od.order_returned_at,
    od.time_to_return_mins,
    od.items_ordered,
    om.items_sold,
    om.items_returned,
    om.items_cost,
    om.gross_revenue,
    om.net_revenue,
    case when om.net_revenue > 1.2*om.items_cost then 'Successful' else 'Failed' end as order_derived_status
from order_details od 
left join order_metrics om 
    on od.order_id = om.order_id 