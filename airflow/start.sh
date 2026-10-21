#!/usr/bin/env bash
# Wait for Postgres, then start Airflow (all components in one container).
set -euo pipefail

echo "Waiting for Postgres..."
for _ in {1..60}; do
  if python -c "import socket; socket.create_connection(('postgres', 5432), 2).close()" 2>/dev/null; then
    echo "Postgres is up."
    break
  fi
  sleep 2
done

exec airflow standalone
