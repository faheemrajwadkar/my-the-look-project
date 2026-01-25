with events_table as (
    select 
        *,
        row_number() over (partition by session_id order by sequence_number desc) as sequence_number_desc
    from {{ ref("stg_the_look__events") }}
),
session_start_details as (
    select 
        session_id,
        user_id,
        event_ip_address as session_ip_address,
        city as session_city,
        state as session_state,
        postal_code as session_postal_code,
        browser as session_browser,
        traffic_source as session_source,
        event_uri as session_event_first_uri,
        case when user_id is not null then 1 else 0 end as session_by_logging_in
    from events_table
    where sequence_number = 1
),
session_end_details as (
    select 
        session_id,
        event_uri as session_event_last_uri,
        event_type as session_last_event,
        case when sequence_number = 1 and event_type not in ('cart', 'purchase') then 1 else 0 end as is_bounced_session
    from events_table
    where sequence_number_desc = 1
),
session_summary as (
    select 
        session_id,
        min(event_created_at) as session_created_at,
        max(event_created_at) as session_ended_at,
        count(event_id) as session_clicks,
        count(case when event_type = 'product' then event_id end) as session_product_page_visits,
        count(case when event_type = 'cart' then event_id end) as session_cart_visits,
        count(case when event_type = 'purchase' then event_id end) as session_purchases,
        count(case when event_type = 'cancel' then event_id end) as session_canceled_counts
    from events_table
    group by 
        session_id
),
final as(
    select 
        ss.session_id,
        ss.user_id,
        ss.session_ip_address,
        ss.session_city,
        ss.session_state,
        ss.session_postal_code,
        ss.session_browser,
        ss.session_source,
        sm.session_created_at,
        sm.session_ended_at,
        datediff('minute', sm.session_created_at, sm.session_ended_at) as session_duration_mins,
        ss.session_event_first_uri,
        se.session_event_last_uri,
        sm.session_clicks,
        sm.session_product_page_visits,
        sm.session_cart_visits,
        sm.session_purchases,
        sm.session_canceled_counts,
        case 
            when session_purchases > 0 then 'purchase_completed'
            when session_purchases = 0 and session_cart_visits > 0 then 'cart_abandoned'
            when session_last_event = 'cancel' then 'cancelled'
            else 'abandoned'
        end as session_result,
        ss.session_by_logging_in,
        se.is_bounced_session
    from session_start_details ss 
    left join session_end_details se 
        on ss.session_id = se.session_id
    left join session_summary sm
        on sm.session_id = ss.session_id
)
select * from final