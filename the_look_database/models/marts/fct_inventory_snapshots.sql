with products as (
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
    from {{ ref("inter_dates") }}
    where date <= current_date
),

date_products as (
    select 
        d.dt,
        p.product_id,
        p.distribution_center_id
    from dates as d
    cross join products as p 
    where d.dt >= p.product_first_added_at
),

inventory_added as (
    select 
        date(inventory_item_created_at) as dt,
        product_id,
        distribution_center_id,
        count(ii.inventory_item_id) as counts_added,
        sum(ii.inventory_item_cost) as cost_added,
        sum(ii.product_retail_price) as value_added
    from {{ ref("stg_the_look__inventory_items") }} ii
    -- where ii.product_id = 22457
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
        count(*) as counts_removed_via_orders
    from {{ ref("stg_the_look__order_items") }} oi 
    left join {{ ref("stg_the_look__products") }} p
        on oi.product_id = p.product_id
    where oi.order_item_status <> 'Cancelled'
    -- and oi.product_id = 22457
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
        count(*) as counts_returned
    from {{ ref("stg_the_look__order_items") }} oi 
    left join {{ ref("stg_the_look__products") }} p
        on oi.product_id = p.product_id
    where oi.order_item_status = 'Returned'
    -- and oi.product_id = 22457
    group by
        date(oi.order_item_returned_at),
        oi.product_id,
        p.distribution_center_id
),

day_level_counts as (
    select 
        dp.dt,
        dp.product_id,
        dp.distribution_center_id,
        coalesce(ad.counts_added, 0) as counts_added,
        coalesce(sl.counts_removed_via_orders, 0) as counts_removed_via_orders,
        coalesce(rt.counts_returned, 0) as counts_returned,
        coalesce(ad.cost_added, 0) as cost_added,
        coalesce(ad.value_added, 0) as value_added
    from date_products dp 
    left join inventory_added ad 
        on dp.dt = ad.dt
        and dp.product_id = ad.product_id
        and dp.distribution_center_id = ad.distribution_center_id
    left join inventory_sold sl
        on dp.dt = sl.dt
        and dp.product_id = sl.product_id
        and dp.distribution_center_id = sl.distribution_center_id
    left join returned_items rt
        on dp.dt = rt.dt
        and dp.product_id = rt.product_id
        and dp.distribution_center_id = rt.distribution_center_id
),

cumulative as (
    select 
        dt,
        product_id,
        distribution_center_id,
        counts_added as counts_added_on_dt,
        sum(counts_added) over (partition by product_id, distribution_center_id order by dt rows between unbounded preceding and current row) as counts_added_till_date,
        counts_removed_via_orders as counts_removed_via_orders_on_dt,
        sum(counts_removed_via_orders) over (partition by product_id, distribution_center_id order by dt rows between unbounded preceding and current row) as counts_removed_via_orders_till_date,
        counts_returned as counts_returned_on_dt,
        sum(counts_returned) over (partition by product_id, distribution_center_id order by dt rows between unbounded preceding and current row) as counts_returned_till_date,
        max(case when counts_added > 0 then dt end) over (partition by product_id, distribution_center_id order by dt rows between unbounded preceding and current row) as last_restocked_at,
        cost_added as cost_added_on_dt,
        sum(cost_added) over (partition by product_id, distribution_center_id order by dt rows between unbounded preceding and current row) as cost_added_till_date,
        value_added as value_added_on_dt,
        sum(value_added) over (partition by product_id, distribution_center_id order by dt rows between unbounded preceding and current row) as value_added_till_date
    from day_level_counts
),

main as (
    select 
        dt,
        product_id,
        distribution_center_id,
        
        lag(counts_added_till_date - counts_removed_via_orders_till_date + counts_returned_till_date) over (partition by product_id, distribution_center_id order by dt) as total_inventory_in_stock_at_open,
        (counts_added_till_date - counts_removed_via_orders_till_date + counts_returned_till_date) as total_inventory_in_stock_at_close,
        
        last_restocked_at,
        counts_added_on_dt as units_received_today,
        counts_removed_via_orders_on_dt as units_sold_today,
        counts_returned_on_dt as units_returned_today,
        
        lag(cost_added_till_date 
            - coalesce((cost_added_till_date/nullif(counts_added_till_date, 0)), 0) * counts_removed_via_orders_till_date 
            + coalesce((cost_added_till_date/nullif(counts_added_till_date, 0)), 0) * counts_returned_till_date
        ) over (partition by product_id, distribution_center_id order by dt) as total_inventory_cost_at_open,
        (cost_added_till_date 
            - coalesce((cost_added_till_date/nullif(counts_added_till_date, 0)), 0) * counts_removed_via_orders_till_date 
            + coalesce((cost_added_till_date/nullif(counts_added_till_date, 0)), 0) * counts_returned_till_date
        ) as total_inventory_cost_at_close,
        
        lag(value_added_till_date 
            - coalesce((value_added_till_date/nullif(counts_added_till_date, 0)), 0) * counts_removed_via_orders_till_date 
            + coalesce((value_added_till_date/nullif(counts_added_till_date, 0)), 0) * counts_returned_till_date
        ) over (partition by product_id, distribution_center_id order by dt) as total_inventory_value_at_open,
        (value_added_till_date 
            - coalesce((value_added_till_date/nullif(counts_added_till_date, 0)), 0) * counts_removed_via_orders_till_date 
            + coalesce((value_added_till_date/nullif(counts_added_till_date, 0)), 0) * counts_returned_till_date
        ) as total_inventory_value_at_close
    from cumulative
),

final as (
    select 
        m.dt,
        dt.date_day_sk as dt_sk,
        m.product_id,
        {{ dbt_utils.generate_surrogate_key(["m.product_id"]) }} as product_sk,
        m.distribution_center_id,
        {{ dbt_utils.generate_surrogate_key(["m.distribution_center_id"]) }} as distribution_center_sk,

        {{ dbt_utils.generate_surrogate_key([
            "m.dt", 
            "m.product_id",
            "m.distribution_center_id"
        ])}} as inventory_snapshot_sk,
        
        coalesce(m.total_inventory_in_stock_at_open, 0) as total_inventory_in_stock_at_open,
        coalesce(m.total_inventory_in_stock_at_close, 0) as total_inventory_in_stock_at_close,
        
        m.last_restocked_at,
        m.units_received_today,
        m.units_sold_today,
        m.units_returned_today,
        
        coalesce(m.total_inventory_cost_at_open, 0) as total_inventory_cost_at_open,
        coalesce(m.total_inventory_cost_at_close, 0) as total_inventory_cost_at_close,
        
        coalesce(m.total_inventory_value_at_open, 0) as total_inventory_value_at_open,
        coalesce(m.total_inventory_value_at_close, 0) as total_inventory_value_at_close,
    
        case when coalesce(m.total_inventory_in_stock_at_open, 0) = 0 then 1 else 0 end as is_out_of_stock,
        case when coalesce(m.total_inventory_in_stock_at_open, 0) < 5 then 1 else 0 end as is_low_stock
    from main m 
    left join {{ ref("dim_dates") }} dt 
        on date(m.dt) = dt.date
)

select * from final