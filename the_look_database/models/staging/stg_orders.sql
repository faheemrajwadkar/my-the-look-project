with source as (
    select * from {{ source('the_look_ecommerce', 'orders') }}
),

renamed as (
    select
        order_id,
        user_id,
        status as order_status,
        gender as user_gender,
        created_at as order_created_at,
        shipped_at as order_shipped_at,
        delivered_at as order_delivered_at,
        returned_at as order_returned_at,
        num_of_item as order_num_of_item,
        _batched_at,
        _file_source
    from source
)

select * from renamed