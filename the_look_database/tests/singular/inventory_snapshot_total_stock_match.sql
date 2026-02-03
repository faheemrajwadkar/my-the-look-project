    with raw_value as (
        select
            (select count(*) from {{ ref("stg_the_look__inventory_items") }} ) -- as inventory_items_count
            -
            (select count(*) from {{ ref("stg_the_look__order_items") }} where order_item_status <> 'Cancelled') -- as order_items
            +
            (select count(*) from {{ ref("stg_the_look__order_items") }} where order_item_status = 'Returned') -- as order_items_returned
            as raw_val
    ),

    model_value as (
        select sum(total_inventory_in_stock_at_close) as model_val
        from {{ ref("fct_inventory_snapshots") }}
        where dt = (select max(dt) from {{ ref("fct_inventory_snapshots") }})
    )

    select 
        r.raw_val,
        m.model_val
    from raw_value r
    cross join model_value m
    where r.raw_val != m.model_val