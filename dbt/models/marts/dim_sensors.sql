-- Gold layer: one row per sensor.
select
    sensor_id,
    arg_max(location, event_ts) as location,
    min(event_ts) as first_seen_at,
    max(event_ts) as last_seen_at,
    arg_max(battery_pct, event_ts) as latest_battery_pct,
    count(*) as total_readings
from {{ ref('stg_iot_readings') }}
group by sensor_id
