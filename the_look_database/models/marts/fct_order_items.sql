{{
    config(
        materialized='incremental',
        unique_key='order_item_sk',
        incremental_strategy='merge'
    )
}}

{% if is_incremental() %}

with updated_order_items as (
    select distinct 
        order_item_id
    from {{ ref("stg_the_look__order_items") }}
    where GREATEST_IGNORE_NULLS(order_item_created_at, order_item_delivered_at, order_item_shipped_at, order_item_returned_at) 
            >= (select max(GREATEST_IGNORE_NULLS(order_item_created_at, order_item_delivered_at, order_item_shipped_at, order_item_returned_at)) from {{this}})
)    

{% endif %}

select 
    oi.order_item_id,
    {{ dbt_utils.generate_surrogate_key(["oi.order_item_id"]) }} as order_item_sk,
    oi.order_id,
    {{ dbt_utils.generate_surrogate_key(["oi.order_id"]) }} as order_sk,
    oi.user_id,
    {{ dbt_utils.generate_surrogate_key(["oi.user_id"]) }} as user_sk,
    oi.product_id,
    {{ dbt_utils.generate_surrogate_key(["oi.product_id"]) }} as product_sk,
    oi.inventory_item_id,
    {{ dbt_utils.generate_surrogate_key(["oi.inventory_item_id"]) }} as inventory_item_sk,
    ii.distribution_center_id,
    {{ dbt_utils.generate_surrogate_key(["ii.distribution_center_id"]) }} as distribution_center_sk,
    oi.order_item_status,
    oi.order_item_created_at,
    dt.date as order_item_created_date,
    oi.order_item_shipped_at,
    datediff('minute', oi.order_item_created_at, oi.order_item_shipped_at) as time_to_ship_item_mins,
    order_item_delivered_at,
    datediff('minute', oi.order_item_shipped_at, oi.order_item_delivered_at) as time_to_deliver_item_mins,
    datediff('minute', oi.order_item_created_at, oi.order_item_delivered_at) as time_to_complete_order_item_mins,
    oi.order_item_returned_at,
    datediff('minute', oi.order_item_delivered_at, oi.order_item_returned_at) as time_to_return_item_mins,
    oi.order_item_sale_price,
    ii.inventory_item_cost as order_item_cost_price
from {{ ref("stg_the_look__order_items") }} oi
left join {{ ref("stg_the_look__inventory_items") }} ii 
    on oi.inventory_item_id = ii.inventory_item_id
left join {{ ref("dim_dates") }} dt 
    on date(oi.order_item_created_at) = dt.date