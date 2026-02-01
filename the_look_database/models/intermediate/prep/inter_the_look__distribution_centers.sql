select
    * exclude (distribution_center_geom), -- Replace 'location_col' with your actual geography column name
    ST_ASWKT(distribution_center_geom) as distribution_center_geom_string
from {{ source('the_look_ecommerce', 'distribution_centers') }}