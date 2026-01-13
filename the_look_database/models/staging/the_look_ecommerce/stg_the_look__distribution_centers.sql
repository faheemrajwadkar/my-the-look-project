
with source as (
    select * from {{ source('the_look_ecommerce', 'distribution_centers') }}
),

renamed as (
    select
        id as distribution_center_id,
        name as distribution_center_name,
        latitude as distribution_center_location_latitude,
        longitude as distribution_center_location_longitude,
        distribution_center_geom as distribution_center_geom,
        _batched_at,
        _file_source
    from source
)

select * from renamed