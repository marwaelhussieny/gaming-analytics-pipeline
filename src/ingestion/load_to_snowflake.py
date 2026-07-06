"""
Loads the latest raw player-count JSON file into Snowflake as VARIANT rows.

This replaces the placeholder bash step in the DAG. Each raw record is
inserted as a single VARIANT column plus a load timestamp — dbt's staging
layer is responsible for flattening it into typed columns, not this script.
Keeping the loader "dumb" (just land it) is intentional: it means schema
changes in the source API don't break ingestion, only the dbt models that
read from it.

Requires: snowflake-connector-python (add to requirements.txt if not present)
Env vars: SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD
"""

import json
import logging
import os
from pathlib import Path

import snowflake.connector

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("snowflake_loader")

RAW_DATA_DIR = Path(os.environ.get("RAW_DATA_DIR", Path(__file__).parents[2] / "data" / "raw"))
DATABASE = os.environ.get("SNOWFLAKE_DATABASE", "GAMING_ANALYTICS")
SCHEMA = "RAW"
TABLE = "PLAYER_COUNTS"


def get_connection():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    )


def ensure_schema(cur):
    cur.execute(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {DATABASE}.{SCHEMA}")
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {DATABASE}.{SCHEMA}.{TABLE} (
            raw_payload VARIANT,
            _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
        """
    )


def latest_raw_file() -> Path:
    files = sorted(RAW_DATA_DIR.glob("player_counts_*.json"))
    if not files:
        raise FileNotFoundError(f"No raw files found in {RAW_DATA_DIR}. Run steam_api.py first.")
    return files[-1]


def load_file(cur, file_path: Path):
    with open(file_path) as f:
        records = json.load(f)

    valid_records = [r for r in records if r.get("player_count") is not None]
    if not valid_records:
        raise ValueError(f"No valid records to load in {file_path}")

    for record in valid_records:
        cur.execute(
            f"INSERT INTO {DATABASE}.{SCHEMA}.{TABLE} (raw_payload) SELECT PARSE_JSON(%s)",
            (json.dumps(record),),
        )

    logger.info("Loaded %d records from %s into %s.%s.%s", len(valid_records), file_path, DATABASE, SCHEMA, TABLE)


def run_load():
    file_path = latest_raw_file()
    conn = get_connection()
    try:
        cur = conn.cursor()
        ensure_schema(cur)
        load_file(cur, file_path)
        conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    run_load()
