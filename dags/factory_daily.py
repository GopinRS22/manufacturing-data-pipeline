"""Airflow 3 daily bounded processing. Do not run with live stream writers.

The data interval start is the UTC business date being finalized.
For ad-hoc demo datasets, use scripts/demo.sh instead.
"""
from datetime import datetime, timedelta, timezone
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

COMMAND_PREFIX = "cd /app && bash scripts/submit.sh"
DAY = "{{ data_interval_start | ds }}"

with DAG(
    dag_id="factory_daily_reconciliation",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["capstone", "manufacturing", "spark", "delta"],
    description="Bounded Kafka -> Delta catch-up, curated Gold, quality and reporting",
) as dag:
    bronze = BashOperator(
        task_id="bronze_catchup",
        bash_command=f"{COMMAND_PREFIX} src/factory_pipeline/bronze.py --available-now",
        execution_timeout=timedelta(hours=1),
    )
    silver = BashOperator(
        task_id="silver_catchup",
        bash_command=f"{COMMAND_PREFIX} src/factory_pipeline/silver.py --available-now",
        execution_timeout=timedelta(hours=1),
    )
    gold = BashOperator(
        task_id="build_gold",
        bash_command=f"{COMMAND_PREFIX} src/factory_pipeline/daily_jobs.py gold --date {DAY}",
    )
    quality = BashOperator(
        task_id="data_quality_and_quarantine",
        bash_command=f"{COMMAND_PREFIX} src/factory_pipeline/daily_jobs.py quality --date {DAY}",
    )
    reconcile = BashOperator(
        task_id="reconcile",
        bash_command=f"{COMMAND_PREFIX} src/factory_pipeline/daily_jobs.py reconcile --date {DAY}",
    )
    report = BashOperator(
        task_id="publish_report",
        bash_command=f"{COMMAND_PREFIX} src/factory_pipeline/daily_jobs.py report --date {DAY}",
    )
    bronze >> silver >> [gold, quality]
    [gold, quality] >> reconcile >> report
