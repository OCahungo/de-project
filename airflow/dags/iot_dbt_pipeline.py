"""Hourly batch: check the bronze data is fresh, then run dbt (silver -> gold) into DuckDB."""

from __future__ import annotations

import glob
import os
import time
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task

BRONZE = os.environ.get("BRONZE_PATH", "/opt/airflow/data/bronze/iot_readings")
DBT = "/opt/airflow/dbt-venv/bin/dbt"
DBT_ARGS = "--project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt --target dev"
MAX_AGE_MINUTES = 120


@dag(
    dag_id="iot_dbt_pipeline",
    schedule="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["iot", "dbt"],
)
def iot_dbt_pipeline():
    @task
    def check_bronze_freshness() -> int:
        files = glob.glob(f"{BRONZE}/**/*.parquet", recursive=True)
        if not files:
            raise FileNotFoundError(f"No Parquet files in {BRONZE}. Is the streaming job running?")
        age_min = (time.time() - max(os.path.getmtime(f) for f in files)) / 60
        if age_min > MAX_AGE_MINUTES:
            raise ValueError(
                f"Newest bronze file is {age_min:.0f} min old (limit {MAX_AGE_MINUTES})"
            )
        return len(files)

    dbt_build = BashOperator(task_id="dbt_build", bash_command=f"{DBT} build {DBT_ARGS}")

    check_bronze_freshness() >> dbt_build


iot_dbt_pipeline()
