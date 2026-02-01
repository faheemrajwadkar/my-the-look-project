with source as (
    select * from {{ source('the_look_ecommerce', 'events') }}
),

renamed as (
    select 
        id as event_id,
        user_id,
        sequence_number,
        session_id,
        {{ cast_as_timestamp("created_at") }} as event_created_at,
        ip_address as event_ip_address,
        city,
        state,
        postal_code,
        browser,
        traffic_source,
        uri as event_uri,
        event_type,
        _batched_at,
        _file_source
    from source 
)

select * from renamed
