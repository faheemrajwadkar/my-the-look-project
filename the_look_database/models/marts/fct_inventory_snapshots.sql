{{
    config(
        materialized='incremental',
        unique_key='inventory_snapshot_sk',
        incremental_strategy='merge'
    )
}}

with inventory_added as (
    select 
        date(inventory_item_created_at) as dt,
        product_id,
        distribution_center_id,
        count(ii.inventory_item_id) as counts_added,
        sum(ii.inventory_item_cost) as cost_added,
        sum(ii.product_retail_price) as value_added
    from {{ ref("stg_the_look__inventory_items") }} ii
    
    {% if is_incremental() %}

        where ii.inventory_item_created_at >= (select max(dt) - interval '3 day' from {{ this }})
    
    {% endif %}

    group by
        date(inventory_item_created_at),
        product_id,
        distribution_center_id
),
inventory_sold as (
    select 
        date(oi.order_item_created_at) as dt,
        oi.product_id,
        p.distribution_center_id,
        count(oi.order_item_id) as counts_removed_via_orders,
        sum(ii.inventory_item_cost) as cost_removed_via_orders,
        sum(ii.product_retail_price) as value_removed_via_orders
    from {{ ref("stg_the_look__order_items") }} oi 
    left join {{ ref("stg_the_look__products") }} p
        on oi.product_id = p.product_id
    left join {{ ref("stg_the_look__inventory_items") }} ii
        on oi.inventory_item_id = ii.inventory_item_id
    where oi.order_item_status <> 'Cancelled'
    
    {% if is_incremental() %}
    
        and oi.order_item_created_at >= (select max(dt) - interval '3 day' from {{ this }})

    {% endif %}
    
    group by 
        date(oi.order_item_created_at),
        oi.product_id,
        p.distribution_center_id
),
returned_items as (
    select 
        date(oi.order_item_returned_at) as dt,
        oi.product_id,
        p.distribution_center_id,
        count(oi.order_item_id) as counts_returned,
        sum(ii.inventory_item_cost) as cost_returned,
        sum(ii.product_retail_price) as value_returned
    from {{ ref("stg_the_look__order_items") }} oi 
    left join {{ ref("stg_the_look__products") }} p
        on oi.product_id = p.product_id
    left join {{ ref("stg_the_look__inventory_items") }} ii
        on oi.inventory_item_id = ii.inventory_item_id
    where oi.order_item_status = 'Returned'
    
    {% if is_incremental() %}

        and oi.order_item_returned_at >= (select max(dt) - interval '3 day' from {{ this }})
    
    {% endif %}

    group by
        date(oi.order_item_returned_at),
        oi.product_id,
        p.distribution_center_id
),
unionized as (
    select 
        ad.dt,
        ad.product_id,
        ad.distribution_center_id,
        ad.counts_added,
        0 as counts_removed_via_orders,
        0 as counts_returned,
        ad.cost_added,
        0 as cost_removed_via_orders,
        0 as cost_returned,
        ad.value_added,
        0 as value_removed_via_orders,
        0 as value_returned
    from inventory_added ad 

    union all 

    select 
        sl.dt,
        sl.product_id,
        sl.distribution_center_id,
        0 as counts_added,
        sl.counts_removed_via_orders,
        0 as counts_returned,
        0 as cost_added,
        sl.cost_removed_via_orders,
        0 as cost_returned,
        0 as value_added,
        sl.value_removed_via_orders,
        0 as value_returned
    from inventory_sold sl

    union all 
        
    select 
        rt.dt,
        rt.product_id,
        rt.distribution_center_id,
        0 as counts_added,
        0 as counts_removed_via_orders,
        rt.counts_returned,
        0 as cost_added,
        0 as cost_removed_via_orders,
        rt.cost_returned,
        0 as value_added,
        0 as value_removed_via_orders,
        rt.value_returned
    from returned_items rt
),
grouped as (
    select 
        dt,
        product_id,
        distribution_center_id,
        sum(counts_added) as counts_added_on_dt,
        sum(counts_removed_via_orders) as counts_removed_via_orders_on_dt,
        sum(counts_returned) as counts_returned_on_dt,
        sum(cost_added) as cost_added_on_dt,
        sum(cost_removed_via_orders) as cost_removed_via_orders_on_dt,
        sum(cost_returned) as cost_returned_on_dt,
        sum(value_added) as value_added_on_dt,
        sum(value_removed_via_orders) as value_removed_via_orders_on_dt,
        sum(value_returned) as value_returned_on_dt
    from unionized 
    group by 
        dt,
        product_id,
        distribution_center_id
),
cumulative as (
    select 
        dt,
        product_id,
        distribution_center_id,
        counts_added_on_dt,
        counts_removed_via_orders_on_dt,
        counts_returned_on_dt,
        cost_added_on_dt,
        cost_removed_via_orders_on_dt,
        cost_returned_on_dt,
        value_added_on_dt,
        value_removed_via_orders_on_dt,
        value_returned_on_dt,
        sum(counts_added_on_dt) over (partition by product_id, distribution_center_id order by dt rows between unbounded preceding and current row) as counts_added_till_dt,
        sum(counts_removed_via_orders_on_dt) over (partition by product_id, distribution_center_id order by dt rows between unbounded preceding and current row) as counts_removed_via_orders_till_dt,
        sum(counts_returned_on_dt) over (partition by product_id, distribution_center_id order by dt rows between unbounded preceding and current row) as counts_returned_till_dt,
        sum(cost_added_on_dt) over (partition by product_id, distribution_center_id order by dt rows between unbounded preceding and current row) as cost_added_till_dt,
        sum(cost_removed_via_orders_on_dt) over (partition by product_id, distribution_center_id order by dt rows between unbounded preceding and current row) as cost_removed_via_orders_till_dt,
        sum(cost_returned_on_dt) over (partition by product_id, distribution_center_id order by dt rows between unbounded preceding and current row) as cost_returned_till_dt,
        sum(value_added_on_dt) over (partition by product_id, distribution_center_id order by dt rows between unbounded preceding and current row) as value_added_till_dt,
        sum(value_removed_via_orders_on_dt) over (partition by product_id, distribution_center_id order by dt rows between unbounded preceding and current row) as value_removed_via_orders_till_dt,
        sum(value_returned_on_dt) over (partition by product_id, distribution_center_id order by dt rows between unbounded preceding and current row) as value_returned_till_dt,
        max(case when counts_added_on_dt > 0 then dt end) over (partition by product_id, distribution_center_id order by dt rows between unbounded preceding and 1 preceding) as last_restocked_at
    from grouped
),
inventory_metrics_dt as (
    select 
        dt,
        product_id,
        distribution_center_id,
        
        (
            (counts_added_till_dt - counts_added_on_dt)
            - (counts_removed_via_orders_till_dt - counts_removed_via_orders_on_dt) 
            + (counts_returned_till_dt - counts_returned_on_dt)
        ) as total_inventory_in_stock_at_open,
        (
            counts_added_till_dt 
            - counts_removed_via_orders_till_dt 
            + counts_returned_till_dt
        ) as total_inventory_in_stock_at_close,
        
        last_restocked_at,
        
        counts_added_on_dt as units_received_today,
        counts_removed_via_orders_on_dt as units_sold_today,
        counts_returned_on_dt as units_returned_today,
        
        (
            (cost_added_till_dt - cost_added_on_dt)
            - (cost_removed_via_orders_till_dt - cost_removed_via_orders_on_dt) 
            + (cost_returned_till_dt - cost_returned_on_dt)
        ) as total_inventory_cost_at_open,
        (
            cost_added_till_dt 
            - cost_removed_via_orders_till_dt
            + cost_returned_till_dt
        ) as total_inventory_cost_at_close,

        
        (
            (value_added_till_dt - value_added_on_dt)
            - (value_removed_via_orders_till_dt - value_removed_via_orders_on_dt) 
            + (value_returned_till_dt - value_returned_on_dt)
        ) as total_inventory_value_at_open,
        (
            value_added_till_dt
            - value_removed_via_orders_till_dt
            + value_returned_till_dt
        )total_inventory_value_at_close,

        lead(dt) over (partition by product_id, distribution_center_id order by dt) as next_dt
    
    from cumulative 
),
products as (
    select distinct 
        p.product_id,
        p.distribution_center_id,
        date(pad.product_first_added_at) as product_first_added_at
    from {{ ref("stg_the_look__products") }} p 
    left join {{ ref("inter_product_added_dates") }} pad 
        on p.product_id = pad.product_id
    where pad.product_first_added_at is not null
),
dates as (
    select 
        date as dt
    from {{ ref("dim_dates") }}
    where date <= current_date
),
dates_and_products as (
    select 
        d.dt,
        p.product_id,
        p.distribution_center_id
    from dates as d
    cross join products as p 
    where d.dt >= p.product_first_added_at

    {% if is_incremental() %}

        and d.dt >= (select max(dt) - interval '3 day' from {{ this }})

    {% endif %}
),
daily_inventory as (
    select 
        dp.dt,
        {{ generate_date_key("dp.dt") }} as dt_sk,
        dp.product_id,
        {{ dbt_utils.generate_surrogate_key(["dp.product_id"]) }} as product_sk,
        dp.distribution_center_id,
        {{ dbt_utils.generate_surrogate_key(["dp.distribution_center_id"]) }} as distribution_center_sk,
        
        {{ dbt_utils.generate_surrogate_key([
            "dp.dt", 
            "dp.product_id",
            "dp.distribution_center_id"
        ])}} as inventory_snapshot_sk,
    
        case when dp.dt = id.dt then id.total_inventory_in_stock_at_open else id.total_inventory_in_stock_at_close end as total_inventory_in_stock_at_open,
        id.total_inventory_in_stock_at_close,
        
        case when dp.dt <> id.dt and id.units_received_today >= 1 then id.dt else id.last_restocked_at end as last_restocked_at,
    
        case when dp.dt = id.dt then units_received_today else 0 end as units_received_today,
        case when dp.dt = id.dt then units_sold_today else 0 end as units_sold_today,
        case when dp.dt = id.dt then units_returned_today else 0 end as units_returned_today,
    
        case when dp.dt = id.dt then id.total_inventory_cost_at_open else id.total_inventory_cost_at_close end as total_inventory_cost_at_open,
        id.total_inventory_cost_at_close,
    
        case when dp.dt = id.dt then id.total_inventory_value_at_open else id.total_inventory_value_at_close end as total_inventory_value_at_open,
        id.total_inventory_value_at_close
        
    from dates_and_products dp
    left join inventory_metrics_dt id
        on dp.dt between id.dt and coalesce(id.next_dt - interval '1 day', current_date())
        and dp.product_id = id.product_id
        and dp.distribution_center_id = id.distribution_center_id
)

{% if is_incremental() %}

    ,incremental_combined as (
    select 
        *
    from daily_inventory 
    union all 
    select 
        * exclude(is_out_of_stock, is_low_stock)
    from {{ this }}
    where dt = (select max(dt) - interval '4 day' from {{ this }})
    ),
    incremental_final as (
        select 
            dt,
            dt_sk,
            product_id,
            product_sk,
            distribution_center_id,
            distribution_center_sk,
            inventory_snapshot_sk,
            sum(total_inventory_in_stock_at_open) over (partition by product_id, distribution_center_id order by dt rows between unbounded preceding and current row) as total_inventory_in_stock_at_open,
            sum(total_inventory_in_stock_at_close) over (partition by product_id, distribution_center_id order by dt rows between unbounded preceding and current row) as total_inventory_in_stock_at_close,
            max(last_restocked_at) over (partition by product_id, distribution_center_id order by dt rows between unbounded preceding and current row) as last_restocked_at,
            units_received_today,
            units_sold_today,
            units_returned_today,
            sum(total_inventory_cost_at_open) over (partition by product_id, distribution_center_id order by dt rows between unbounded preceding and current row) as total_inventory_cost_at_open,
            sum(total_inventory_cost_at_close) over (partition by product_id, distribution_center_id order by dt rows between unbounded preceding and current row) as total_inventory_cost_at_close,
            sum(total_inventory_value_at_open) over (partition by product_id, distribution_center_id order by dt rows between unbounded preceding and current row) as total_inventory_value_at_open,
            sum(total_inventory_value_at_close) over (partition by product_id, distribution_center_id order by dt rows between unbounded preceding and current row) as total_inventory_value_at_close
        from incremental_combined
    )
    select 
        *,
        case when total_inventory_in_stock_at_open = 0 then 1 else 0 end as is_out_of_stock,
        case when total_inventory_in_stock_at_open < 5 then 1 else 0 end as is_low_stock
    from incremental_final

{% else %}

    select 
        *,
        case when total_inventory_in_stock_at_open = 0 then 1 else 0 end as is_out_of_stock,
        case when total_inventory_in_stock_at_open < 5 then 1 else 0 end as is_low_stock
    from daily_inventory 

{% endif %}


