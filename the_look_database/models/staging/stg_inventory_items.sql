with source as (
    select * from {{ source('the_look_ecommerce', 'inventory_items') }}
),

renamed as (
    select 
        id as inventory_item_id,
        product_id,
        created_at as inventory_item_created_at,
        sold_at as inventory_item_sold_at,
        cost as inventory_item_cost,
        product_category,
        product_name,
        product_brand,
        product_retail_price,
        product_department,
        product_sku,
        product_distribution_center_id,
        _batched_at,
        _file_source
    from source
)

select * from renamed