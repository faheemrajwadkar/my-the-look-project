with source as (
    select * from {{ ref("snp_the_look__orders") }}
),

renamed as (
    select
        order_id,
        user_id,
        status as order_status,
        gender as user_gender,
        {{ cast_as_timestamp("created_at") }} as order_created_at,
        {{ cast_as_timestamp("shipped_at") }} as order_shipped_at,
        {{ cast_as_timestamp("delivered_at") }} as order_delivered_at,
        {{ cast_as_timestamp("returned_at") }} as order_returned_at,
        num_of_item as order_num_of_item,
        {{ dbt_utils.generate_surrogate_key(["order_id", "dbt_valid_from"]) }} as user_version_sk,
        case 
            when dbt_valid_from = (select min(dbt_valid_from) from source where order_id = s.order_id) 
            then {{ cast_as_timestamp("created_at") }} 
            else dbt_valid_from 
        end as valid_from,
        dbt_valid_to as valid_to,
        _batched_at,
        _file_source
    from source s
    where dbt_valid_to = '9999-12-31'
)

select * from renamed