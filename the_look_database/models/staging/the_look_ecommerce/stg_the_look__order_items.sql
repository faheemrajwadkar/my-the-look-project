with source as (
    select * from {{ ref("snp_the_look__order_items") }}
),

renamed as (
    select 
        id as order_item_id,
        order_id,
        user_id,
        product_id,
        inventory_item_id,
        status as order_item_status,
        {{ cast_as_timestamp("created_at") }} as order_item_created_at,
        {{ cast_as_timestamp("shipped_at") }} as order_item_shipped_at,
        {{ cast_as_timestamp("delivered_at") }} as order_item_delivered_at,
        {{ cast_as_timestamp("returned_at") }} as order_item_returned_at,
        cast(sale_price as number(38,2)) as order_item_sale_price,
        _batched_at,
        _file_source
    from source 
    where dbt_valid_to = '9999-12-31'
)

select * from renamed
