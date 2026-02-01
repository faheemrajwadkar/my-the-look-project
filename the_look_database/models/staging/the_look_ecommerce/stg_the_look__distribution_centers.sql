
with source as (
    select * from {{ ref("snp_the_look__distribution_centers") }}
),

renamed as (
    select
        id as distribution_center_id,
        name as distribution_center_name,
        latitude as distribution_center_location_latitude,
        longitude as distribution_center_location_longitude,
        TO_GEOGRAPHY(distribution_center_geom_string) as distribution_center_geom,
        _batched_at,
        _file_source
    from source
    where dbt_valid_to = '9999-12-31'
)

select * from renamed