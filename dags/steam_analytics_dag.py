"""
DAG: steam_analytics_pipeline

Daily pipeline:
  1. ingest_player_counts   - pull raw data from Steam API, land as JSON
  2. validate_raw_data      - fail fast if the raw file is empty/malformed
  3. load_to_warehouse      - load raw JSON into Snowflake staging table
  4. dbt_run                - build staging -> intermediate -> marts models
  5. dbt_test               - run dbt data quality tests

The validation step matters: it's the difference between "a script that
runs" and a pipeline that catches bad data before it poisons the warehouse.
"""

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

import sys
sys.path.append(str(Path(__file__).parents[1] / "src" / "ingestion"))
from steam_api import run_ingestion  # noqa: E402
from load_to_snowflake import run_load  # noqa: E402

DBT_PROJECT_DIR = str(Path(__file__).parents[1] / "dbt" / "gaming_analytics")

default_args = {
    "owner": "marwa",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


def _ingest(**context):
    output_path = run_ingestion()
    # Pass the file path downstream via XCom so validation checks the exact file produced
    context["ti"].xcom_push(key="raw_file_path", value=str(output_path))


def _validate_raw_data(**context):
    import json

    raw_file_path = context["ti"].xcom_pull(key="raw_file_path", task_ids="ingest_player_counts")
    with open(raw_file_path) as f:
        records = json.load(f)

    if not records:
        raise ValueError(f"Validation failed: {raw_file_path} contains zero records")

    valid_records = [r for r in records if r.get("player_count") is not None]
    if len(valid_records) == 0:
        raise ValueError(f"Validation failed: all records in {raw_file_path} have null player_count")

    if len(valid_records) < len(records):
        # Don't fail the run for partial failures, but make it visible in logs
        print(f"WARNING: {len(records) - len(valid_records)} of {len(records)} records failed to fetch")

    print(f"Validation passed: {len(valid_records)}/{len(records)} valid records")


with DAG(
    dag_id="steam_analytics_pipeline",
    description="Daily ingestion and modeling of Steam player count trends",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["gaming", "portfolio-project"],
) as dag:

    ingest_player_counts = PythonOperator(
        task_id="ingest_player_counts",
        python_callable=_ingest,
    )

    validate_raw_data = PythonOperator(
        task_id="validate_raw_data",
        python_callable=_validate_raw_data,
    )

    load_to_warehouse = PythonOperator(
        task_id="load_to_warehouse",
        python_callable=run_load,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test",
    )

    ingest_player_counts >> validate_raw_data >> load_to_warehouse >> dbt_run >> dbt_test
