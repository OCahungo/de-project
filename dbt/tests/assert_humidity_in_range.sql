-- Fails if an hourly average humidity is outside 0-100 %.
select *
from {{ ref('fct_sensor_readings_hourly') }}
where avg_humidity_pct < 0 or avg_humidity_pct > 100
