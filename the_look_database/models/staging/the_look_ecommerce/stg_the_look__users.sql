with source as (
    select * from {{ ref('snp_the_look__users') }}
),

renamed as (
    select 
        id as user_id,
        first_name as user_first_name,
        last_name as user_last_name,
        email as user_email,
        age as user_age,
        gender as user_gender,
        state as user_state,
        street_address as user_street_address,
        postal_code as user_postal_code,
        city as user_city,
        country as user_country,
        latitude as user_latitude,
        longitude as user_longitude,
        traffic_source as user_traffic_source,
        {{ cast_as_timestamp("created_at") }} as user_created_at,
        TO_GEOGRAPHY(user_geom_string) user_geom,
        _batched_at,
        _file_source
    from source
    where dbt_valid_to = '9999-12-31'
)

select * from renamed