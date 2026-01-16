with source as (
    select * from {{ source('the_look_ecommerce', 'order_items') }}
),

renamed as (
    select 
        id as order_item_id,
        order_id,
        user_id,
        product_id,
        inventory_item_id,
        status as order_item_status,
        COALESCE(
            TRY_TO_TIMESTAMP_NTZ(created_at, 'YYYY-MM-DD HH:MI:SS UTC'),
            TRY_TO_TIMESTAMP_NTZ(created_at, 'YYYY-MM-DD HH:MI:SS.FF UTC')
        ) as order_item_created_at,
        COALESCE(
            TRY_TO_TIMESTAMP_NTZ(shipped_at, 'YYYY-MM-DD HH:MI:SS UTC'),
            TRY_TO_TIMESTAMP_NTZ(shipped_at, 'YYYY-MM-DD HH:MI:SS.FF UTC')
        ) as order_item_shipped_at,
        COALESCE(
            TRY_TO_TIMESTAMP_NTZ(delivered_at, 'YYYY-MM-DD HH:MI:SS UTC'),
            TRY_TO_TIMESTAMP_NTZ(delivered_at, 'YYYY-MM-DD HH:MI:SS.FF UTC')
        ) as order_item_delivered_at,
        COALESCE(
            TRY_TO_TIMESTAMP_NTZ(returned_at, 'YYYY-MM-DD HH:MI:SS UTC'),
            TRY_TO_TIMESTAMP_NTZ(returned_at, 'YYYY-MM-DD HH:MI:SS.FF UTC')
        ) as order_item_returned_at,
        sale_price as order_item_sale_price,
        _batched_at,
        _file_source
    from source 
)

select * from renamed
