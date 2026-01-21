select 
    ii.product_id,
    p.distribution_center_id,
    count(ii.inventory_item_id) as products_procured,
    count(case when ii.inventory_item_sold_at is not null then 1 end) as products_ordered,
    min(ii.inventory_item_created_at) as product_first_added_at,
    max(ii.inventory_item_created_at) as product_last_added_at,
    min(ii.inventory_item_sold_at) as product_first_ordered_at,
    max(ii.inventory_item_sold_at) as product_last_ordered_at,
    sum(ii.inventory_item_cost) as total_product_procurement_cost
from {{ ref("stg_the_look__inventory_items") }} ii
left join {{ ref("stg_the_look__products") }} p 
    on p.product_id = ii.product_id
group by 
    ii.product_id,
    p.distribution_center_id