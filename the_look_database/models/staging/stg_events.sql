select 
    id as event_id,
    user_id,
    sequence_number,
    session_id,
    created_at as event_created_at,
    ip_address as event_ip_address,
    city,
    state,
    postal_code,
    browser,
    traffic_source,
    uri as event_uri,
    event_type
from events 
limit 10;