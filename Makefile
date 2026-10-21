.RECIPEPREFIX = >
# Run with Podman by default (a shell alias is NOT visible to make).
# Override with: make DOCKER=docker up-batch
DOCKER  ?= podman
COMPOSE ?= $(DOCKER) compose
BIN     := .venv/bin/

.PHONY: help init build up-streaming up-batch down ps logs dbt-docker query venv lint fmt test dbt-ci ci sample clean

help:
> @grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "%-14s %s\n", $$1, $$2}'

init: ## create data folders (world-writable so rootless containers can write)
> mkdir -p data/bronze data/checkpoints data/warehouse data/ivy
> chmod -R a+rwX data

build: ## build the custom images (airflow+dbt, producer)
> $(COMPOSE) --profile batch --profile streaming build

up-streaming: init ## start Kafka + producer + Spark streaming job
> $(COMPOSE) --profile streaming up -d --build

up-batch: init ## start Postgres + Airflow (with dbt + DuckDB)
> $(COMPOSE) --profile batch up -d --build

down: ## stop and remove containers (data/ is kept)
> $(COMPOSE) --profile batch --profile streaming down

ps: ## list containers
> $(DOCKER) ps

logs: ## follow logs: make logs S=spark-stream
> $(COMPOSE) --profile batch --profile streaming logs -f $(S)

dbt-docker: ## run dbt build inside the Airflow container
> $(COMPOSE) --profile batch exec airflow /opt/airflow/dbt-venv/bin/dbt build --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt --target dev

query: ## query the warehouse: make query SQL="select * from dim_sensors"
> $(COMPOSE) --profile batch exec airflow /opt/airflow/dbt-venv/bin/python /opt/airflow/scripts/query_warehouse.py "$(SQL)"

venv: ## local virtualenv for lint/tests/dbt
> python3 -m venv .venv
> $(BIN)pip install -q -r requirements-dev.txt

lint: ## ruff checks
> $(BIN)ruff check .
> $(BIN)ruff format --check .

fmt: ## auto-format Python
> $(BIN)ruff format .

test: ## unit tests
> $(BIN)pytest -q

sample: ## generate a small fake bronze dataset (no Kafka/Spark needed)
> $(BIN)python scripts/make_sample_bronze.py data/bronze/iot_readings

dbt-ci: sample ## dbt build against the sample data (same as CI)
> mkdir -p data/warehouse
> BRONZE_PATH=$(CURDIR)/data/bronze/iot_readings DUCKDB_PATH=$(CURDIR)/data/warehouse/ci.duckdb $(BIN)dbt build --project-dir dbt --profiles-dir dbt --target ci

ci: lint test dbt-ci ## everything CI runs

clean: ## DELETE all generated data (bronze, checkpoints, warehouse)
> $(DOCKER) unshare rm -rf data/bronze data/checkpoints data/warehouse
