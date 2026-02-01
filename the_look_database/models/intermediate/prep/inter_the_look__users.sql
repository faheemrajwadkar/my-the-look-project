select
    * exclude (user_geom), -- Replace 'location_col' with your actual geography column name
    ST_ASWKT(user_geom) as user_geom_string
from {{ source('the_look_ecommerce', 'users') }}