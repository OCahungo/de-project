"""Write a small fake bronze dataset (same layout Spark produces) using DuckDB only.

Used by CI and by `make dbt-ci`, so dbt can be tested without Kafka or Spark.
Usage: python scripts/make_sample_bronze.py [output_dir]
"""

import sys
from pathlib import Path

import duckdb


def make_sample(out_dir: Path, rows: int = 3000) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("select setseed(0.42)")
    con.execute(
        f"""
        copy (
            with base as (
                select
                    i,
                    'sensor-' || lpad(((i % 10) + 1)::varchar, 3, '0') as sensor_id,
                    (['warehouse-a','warehouse-b','office','server-room','cold-storage'])[(i % 5) + 1]
                        as location,
                    timestamp '2026-01-01 00:00:00' + i * interval 30 second as event_ts
                from range({rows}) t(i)
            ),
            readings as (
                select
                    sensor_id,
                    location,
                    event_ts,
                    case
                        when i % 500 = 0 then 95.0
                        when i % 333 = 0 then null
                        else 20 + 5 * sin(i / 200.0) + (random() - 0.5) * 2
                    end as temperature_c,
                    case
                        when i % 411 = 0 then 120.0
                        else 55 + 10 * cos(i / 300.0) + (random() - 0.5) * 4
                    end as humidity_pct,
                    20 + random() * 80 as battery_pct,
                    event_ts as kafka_ts,
                    event_ts + interval 2 second as ingested_at
                from base
            )
            select *, cast(event_ts as date) as event_date from readings
            union all  -- duplicates, to exercise deduplication
            select *, cast(event_ts as date) as event_date from readings where hash(sensor_id, event_ts) % 97 = 0
        ) to '{out_dir.as_posix()}'
        (format parquet, partition_by (event_date), overwrite_or_ignore)
        """
    )


if __name__ == "__main__":
    target = Path(sys.argv[1] if len(sys.argv) > 1 else "data/bronze/iot_readings")
    make_sample(target)
    print(f"sample bronze data written to {target}")
