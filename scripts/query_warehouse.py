"""Run a SQL query against the DuckDB warehouse. Usage: query_warehouse.py "select ..." """

import os
import sys

import duckdb

DB = os.getenv("DUCKDB_PATH", "/opt/airflow/data/warehouse/iot.duckdb")
sql = (
    sys.argv[1]
    if len(sys.argv) > 1 and sys.argv[1]
    else ("select * from fct_sensor_readings_hourly order by reading_hour desc, sensor_id limit 15")
)
con = duckdb.connect(DB, read_only=True)
print(con.sql(sql))
