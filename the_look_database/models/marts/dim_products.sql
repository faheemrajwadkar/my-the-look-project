with products as (
    select 
        p.product_id,
        p.product_cost,
        p.product_category,
        p.product_name,
        p.product_brand,
        p.product_retail_price,
        p.product_department,
        p.product_sku,
        dc.distribution_center_name
    from {{ ref("stg_the_look__products") }} p 
    left join {{ ref("stg_the_look__distribution_centers") }} dc 
        on p.distribution_center_id = dc.distribution_center_id
),

inventory_metrics as (
    select 
        product_id,
        sum(products_procured) as products_procured,
        sum(products_ordered) as products_ordered,
        min(product_first_added_at) as product_first_added_at,
        max(product_last_added_at) as product_last_added_at,
        min(product_first_ordered_at) as product_first_ordered_at,
        max(product_last_ordered_at) as product_last_ordered_at,
        sum(total_product_procurement_cost) as total_product_procurement_cost
    from {{ ref("inter_inventory_metrics") }}
    group by
        product_id
),

sales_metrics as (
    select  
        product_id,
        sum(products_sold_successfully) as products_sold_successfully,
        max(product_last_sold_successfully_at) as product_last_sold_successfully_at,
        sum(products_returned) as products_returned,
        max(product_last_returned_at) as product_last_returned_at,
        sum(products_cancelled) as products_cancelled,
        max(product_last_cancelled_at) as product_last_cancelled_at,
        sum(total_product_revenue) as total_product_revenue
    from {{ ref("inter_sales_metrics") }}
    group by
        product_id
)

select 
    p.product_id,
    p.product_cost,
    p.product_category,
    p.product_name,
    p.product_brand,
    p.product_retail_price,
    p.product_department,
    p.product_sku,
    p.distribution_center_name,
    im.products_procured, 
    im.products_ordered, 
    im.product_first_added_at, 
    im.product_last_added_at, 
    im.product_first_ordered_at, 
    im.product_last_ordered_at, 
    im.total_product_procurement_cost, 
    sm.products_sold_successfully,
    sm.product_last_sold_successfully_at,
    sm.products_returned,
    sm.product_last_returned_at,
    sm.products_cancelled,
    sm.product_last_cancelled_at,
    sm.total_product_revenue
from products p 
left join inventory_metrics im 
    on p.product_id = im.product_id
left join sales_metrics sm 
    on p.product_id = sm.product_id
