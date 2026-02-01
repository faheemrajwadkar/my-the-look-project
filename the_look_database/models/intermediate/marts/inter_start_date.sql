with all_dates as (
    select 
        min(event_created_at) as min_dt
    from {{ ref("stg_the_look__events") }}
    
    union all 
    
    select 
        min(inventory_item_created_at) as min_dt
    from {{ ref("stg_the_look__inventory_items") }}
    
    union all 
    
    select 
        min(order_item_created_at) as min_dt
    from {{ ref("stg_the_look__order_items") }}
    
    union all 
    
    select 
        min(order_created_at) as min_dt
    from {{ ref("stg_the_look__orders") }}
    
    union all
    
    select 
        min(user_created_at) as min_dt
    from {{ ref("stg_the_look__users") }}
)
select date_trunc(month, min(min_dt))::date as start_date from all_dates