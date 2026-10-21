FROM docker.io/apache/airflow:3.3.1

USER airflow

# dbt + DuckDB in an isolated venv (avoids dependency conflicts with Airflow)
RUN python -m venv /opt/airflow/dbt-venv \
 && /opt/airflow/dbt-venv/bin/pip install --no-cache-dir "dbt-core==1.12.6" "dbt-duckdb==1.11.0"
