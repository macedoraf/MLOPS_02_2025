#!/bin/bash
set -e

if ! airflow db check > /dev/null 2>&1; then
  echo "⚙️ Database not initialized yet, running first setup"
  docker compose up airflow-init
else
  echo "⚙️ Database already initialized"
fi

echo "🚀 Initialize docker compose: $@"
docker compose up -d
exec "$@"
