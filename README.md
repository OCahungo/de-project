# IoT data engineering stack (Kafka · Spark · dbt · DuckDB · Airflow)

```
producer (fake sensors) → Kafka → Spark Structured Streaming → Parquet (bronze)
                                                                   ↓
                         Airflow DAG (hourly) → dbt-duckdb → DuckDB (silver → gold)
```

| Part | Image / package | Folder |
|---|---|---|
| Kafka (KRaft) | `apache/kafka:4.3.1` | `compose.yaml` |
| Spark | `spark:4.1.3-java21-python3` | `spark/` |
| Airflow + dbt + DuckDB | `localhost/de-airflow:dev` (built from `airflow/Dockerfile`) | `airflow/`, `dbt/` |
| Postgres (Airflow metadata) | `postgres:16-alpine` | `compose.yaml` |
| Fake sensors | `producer/Dockerfile` (python + confluent-kafka) | `producer/` |

Start only what you need (RAM): **profiles** `streaming` and `batch`.

## Run it

```bash
make init              # creates data/ folders (world-writable for rootless Podman)

make up-streaming      # Kafka + topic + producer + Spark job
make logs S=spark-stream     # wait for "Batch: 0" ... then Parquet appears in data/bronze/
ls data/bronze/iot_readings  # event_date=YYYY-MM-DD folders after ~1 min

make up-batch          # Postgres + Airflow
# open http://127.0.0.1:8080 (use 127.0.0.1, not localhost), trigger DAG "iot_dbt_pipeline"

make query SQL="select * from dim_sensors order by sensor_id"
make query             # latest hourly aggregates
make down              # stop everything (data/ is kept)
```

Spark UI: http://127.0.0.1:4040 (while the streaming job runs).
`make` uses `podman` by default (shell aliases are invisible to make). Use `make DOCKER=docker ...` to change it.

## Develop and test without containers (this is what CI runs)

```bash
make venv
make ci                # ruff + pytest + dbt build on generated sample data
```

CI/CD: `.github/workflows/ci.yml` runs lint, unit tests, `dbt build` on sample data, builds the images,
checks the DAG imports inside the Airflow image and, on `main`, pushes the images to GHCR.

## Troubleshooting

- **Login page / password in Airflow:** this setup disables login (`SIMPLE_AUTH_MANAGER_ALL_ADMINS`). If it still asks for one:
  `podman compose exec airflow cat /opt/airflow/simple_auth_manager_passwords.json.generated`
- **Permission denied on `data/`:** run `make init` again (`chmod -R a+rwX data`).
- **Spark can't download the Kafka connector:** the first start needs internet (Ivy cache is kept in `data/ivy`).
- **Spark error about offsets after recreating Kafka:** run `make down`, then `make clean`, then start again.
- **Out of memory:** run streaming and batch separately; check `podman stats --no-stream`.
- **Free disk:** `make down`, then `make clean` (deletes generated data), then `podman image prune -a` if you also want the images gone.
