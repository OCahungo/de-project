-- Gold layer: hourly aggregates per sensor (out-of-range readings are counted, not averaged).
select
    sensor_id,
    date_trunc('hour', event_ts) as reading_hour,
    count(*) filter (where not is_out_of_range) as valid_readings,
    count(*) filter (where is_out_of_range) as anomaly_readings,
    round(avg(temperature_c) filter (where not is_out_of_range), 2) as avg_temperature_c,
    round(min(temperature_c) filter (where not is_out_of_range), 2) as min_temperature_c,
    round(max(temperature_c) filter (where not is_out_of_range), 2) as max_temperature_c,
    round(avg(humidity_pct) filter (where not is_out_of_range), 2) as avg_humidity_pct
from {{ ref('stg_iot_readings') }}
group by 1, 2
