select  
    oi.product_id,
    p.distribution_center_id,
    count(case when oi.order_item_status = 'Complete' then oi.order_item_id end) as products_sold_successfully,
    max(case when oi.order_item_status = 'Complete' then oi.order_item_created_at end) as product_last_sold_successfully_at,
    count(case when oi.order_item_status = 'Returned' then oi.order_item_id end) as products_returned,
    max(case when oi.order_item_status = 'Returned' then oi.order_item_created_at end) as product_last_returned_at,
    count(case when oi.order_item_status = 'Cancelled' then oi.order_item_id end) as products_cancelled,
    max(case when oi.order_item_status = 'Cancelled' then oi.order_item_created_at end) as product_last_cancelled_at,
    sum(case when oi.order_item_status = 'Complete' then oi.order_item_sale_price end) as total_product_revenue
from {{ ref("stg_the_look__order_items") }} oi
left join {{ ref("stg_the_look__products") }} p 
    on oi.product_id = p.product_id
group by
    oi.product_id,
    p.distribution_center_id