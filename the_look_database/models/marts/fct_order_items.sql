select 
    oi.order_item_id,
    oi.order_id,
    oi.user_id,
    oi.product_id,
    oi.inventory_item_id,
    ii.product_distribution_center_id,
    oi.order_item_status,
    oi.order_item_created_at,
    oi.order_item_shipped_at,
    datediff('minute', oi.order_item_created_at, oi.order_item_shipped_at) as time_to_ship_item_mins,
    order_item_delivered_at,
    datediff('minute', oi.order_item_shipped_at, oi.order_item_delivered_at) as time_to_deliver_item_mins,
    datediff('minute', oi.order_item_created_at, oi.order_item_delivered_at) as time_to_complete_order_item_mins,
    oi.order_item_returned_at,
    datediff('minute', oi.order_item_delivered_at, oi.order_item_returned_at) as time_to_return_item_mins,
    oi.order_item_sale_price,
    ii.inventory_item_cost as order_item_cost_price
from {{ ref("stg_the_look__order_items") }} oi
left join {{ ref("stg_the_look__inventory_items") }} ii 
    on oi.inventory_item_id = ii.inventory_item_id