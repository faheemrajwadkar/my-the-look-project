with source as (
    select * from {{ source('the_look_ecommerce', 'orders') }}
),

renamed as (
    select
        order_id,
        user_id,
        status as order_status,
        gender as user_gender,
        COALESCE(
            TRY_TO_TIMESTAMP_NTZ(created_at, 'YYYY-MM-DD HH:MI:SS UTC'),
            TRY_TO_TIMESTAMP_NTZ(created_at, 'YYYY-MM-DD HH:MI:SS.FF UTC')
        ) as order_created_at,
        COALESCE(
            TRY_TO_TIMESTAMP_NTZ(shipped_at, 'YYYY-MM-DD HH:MI:SS UTC'),
            TRY_TO_TIMESTAMP_NTZ(shipped_at, 'YYYY-MM-DD HH:MI:SS.FF UTC')
        ) as order_shipped_at,
        COALESCE(
            TRY_TO_TIMESTAMP_NTZ(delivered_at, 'YYYY-MM-DD HH:MI:SS UTC'),
            TRY_TO_TIMESTAMP_NTZ(delivered_at, 'YYYY-MM-DD HH:MI:SS.FF UTC')
        ) as order_delivered_at,
        COALESCE(
            TRY_TO_TIMESTAMP_NTZ(returned_at, 'YYYY-MM-DD HH:MI:SS UTC'),
            TRY_TO_TIMESTAMP_NTZ(returned_at, 'YYYY-MM-DD HH:MI:SS.FF UTC')
        ) as order_returned_at,
        num_of_item as order_num_of_item,
        _batched_at,
        _file_source
    from source
)

select * from renamed