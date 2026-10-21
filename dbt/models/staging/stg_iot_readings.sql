-- Silver layer: reads the bronze Parquet files written by Spark,
-- drops rows without a key, de-duplicates and flags impossible values.
with source as (

    select *
    from read_parquet(
        '{{ env_var("BRONZE_PATH", "data/bronze/iot_readings") }}/**/*.parquet',
        hive_partitioning = true
    )

),

cleaned as (

    select
        cast(sensor_id as varchar) as sensor_id,
        cast(location as varchar) as location,
        cast(event_ts as timestamp) as event_ts,
        cast(temperature_c as double) as temperature_c,
        cast(humidity_pct as double) as humidity_pct,
        cast(battery_pct as double) as battery_pct,
        kafka_ts,
        ingested_at
    from source
    where sensor_id is not null
      and event_ts is not null

),

deduped as (

    select
        *,
        row_number() over (
            partition by sensor_id, event_ts
            order by ingested_at desc
        ) as row_num
    from cleaned

)

select
    * exclude (row_num),
    coalesce(
        temperature_c < -40 or temperature_c > 60
        or humidity_pct < 0 or humidity_pct > 100,
        false
    ) as is_out_of_range
from deduped
where row_num = 1
