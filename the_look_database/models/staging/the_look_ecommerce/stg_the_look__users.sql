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
        {{ dbt_utils.generate_surrogate_key(["id", "dbt_valid_from"]) }} as user_version_sk,
        case 
            when dbt_valid_from = (select min(dbt_valid_from) from source where id = s.id) 
            then {{ cast_as_timestamp("created_at") }} 
            else dbt_valid_from 
        end as valid_from,
        dbt_valid_to as valid_to,
        _batched_at,
        _file_source
    from source s
)

select * from renamed