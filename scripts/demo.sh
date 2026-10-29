#!/usr/bin/env bash
set -euo pipefail
mkdir -p data && chmod 0777 data
bash scripts/init_kafka.sh
# Build the container once. Java/Python/Delta dependency downloads require internet.
docker compose build spark
run_spark() { docker compose run --rm spark "$@"; }
run_spark python -m factory_pipeline.producer --ticks 60 --inject duplicate late schema invalid
run_spark bash scripts/submit.sh src/factory_pipeline/bronze.py --available-now
run_spark bash scripts/submit.sh src/factory_pipeline/silver.py --available-now
run_spark bash scripts/submit.sh src/factory_pipeline/daily_jobs.py gold --date auto
run_spark bash scripts/submit.sh src/factory_pipeline/daily_jobs.py quality --date auto
run_spark bash scripts/submit.sh src/factory_pipeline/daily_jobs.py reconcile --date auto
run_spark bash scripts/submit.sh src/factory_pipeline/daily_jobs.py report --date auto
printf '\nDemo complete. See data/reports/YYYY-MM-DD/summary.md and CSV/JSON files.\n'
