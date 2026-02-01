select 
    p.product_id,
    p.distribution_center_id,
    least(min(ii.inventory_item_created_at), min(oi.order_item_created_at)) as product_first_added_at
from {{ ref("stg_the_look__products") }} p 
left join {{ ref("stg_the_look__inventory_items") }} ii 
    on p.product_id = ii.product_id
left join {{ ref("stg_the_look__order_items") }} oi 
    on p.product_id = oi.product_id
group by 
    p.product_id,
    p.distribution_center_id