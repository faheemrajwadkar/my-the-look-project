with source as (
    select * from {{ source('the_look_ecommerce', 'order_items') }}
),

renamed as (
    select 
        product_id,
        inventory_item_id,
        status as order_item_status,
        created_at as order_item_created_at,
        shipped_at as order_item_shipped_at,
        delivered_at as order_item_delivered_at,
        returned_at as order_item_returned_at,
        sale_price as order_item_sale_price,
        _batched_at,
        _file_source
    from source 
)

select * from renamed
