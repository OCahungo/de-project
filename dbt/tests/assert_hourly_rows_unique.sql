-- Fails if a (sensor, hour) pair appears more than once.
select sensor_id, reading_hour, count(*) as n
from {{ ref('fct_sensor_readings_hourly') }}
group by 1, 2
having count(*) > 1
