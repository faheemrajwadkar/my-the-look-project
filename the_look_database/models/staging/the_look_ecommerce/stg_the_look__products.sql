with source as (
    select * from {{ ref('snp_the_look__products') }}
),

renamed as (
    select 
        id as product_id,
        cost as product_cost,
        category as product_category,
        name as product_name,
        brand as product_brand,
        cast(retail_price as number(38,2)) as product_retail_price,
        department as product_department,
        sku as product_sku,
        distribution_center_id,
        _batched_at,
        _file_source
    from source
    where dbt_valid_to = '9999-12-31'
)

select * from renamed