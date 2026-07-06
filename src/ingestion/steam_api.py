"""
Steam API ingestion module.

Pulls current player counts for a tracked list of games and lands the raw
response as timestamped JSON. This is the immutable "landing zone" layer —
nothing here is transformed, only captured exactly as the API returned it.

Why land raw JSON instead of writing straight to a table?
  - Replayable: if a downstream transform has a bug, you can re-run against
    the exact historical raw payload instead of losing the data.
  - Auditable: you can always prove what the source actually said at time T.
  - Decouples ingestion failures from transform failures.

Usage:
    python steam_api.py

Environment variables (see .env.example):
    STEAM_API_KEY      - your Steam Web API key
    RAW_DATA_DIR        - where to land the JSON (defaults to ../../data/raw)
"""

import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("steam_ingestion")

STEAM_API_KEY = os.environ.get("STEAM_API_KEY", "")
RAW_DATA_DIR = Path(os.environ.get("RAW_DATA_DIR", Path(__file__).parents[2] / "data" / "raw"))

# Steam AppIDs for the games we're tracking. Expand this list as needed.
# (These are just well-known, high-traffic titles so the pipeline always
# has data to show — swap in whatever games make sense for your story.)
TRACKED_APPS = {
    570: "Dota 2",
    730: "Counter-Strike 2",
    578080: "PUBG: Battlegrounds",
    1172470: "Apex Legends",
    252490: "Rust",
}

STEAM_PLAYER_COUNT_URL = "https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/"

MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 5


def fetch_player_count(app_id: int) -> dict:
    """Fetch current player count for a single AppID, with retry on failure."""
    params = {"appid": app_id}
    last_error = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(STEAM_PLAYER_COUNT_URL, params=params, timeout=10)
            response.raise_for_status()
            payload = response.json()
            result = payload.get("response", {})

            if result.get("result") != 1:
                raise ValueError(f"Steam API returned non-success result for app {app_id}: {result}")

            return {
                "app_id": app_id,
                "game_name": TRACKED_APPS.get(app_id, "unknown"),
                "player_count": result.get("player_count"),
                "fetched_at": datetime.now(timezone.utc).isoformat(),
            }

        except (requests.RequestException, ValueError) as exc:
            last_error = exc
            logger.warning(
                "Attempt %d/%d failed for app %d: %s", attempt, MAX_RETRIES, app_id, exc
            )
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF_SECONDS * attempt)

    logger.error("All retries exhausted for app %d: %s", app_id, last_error)
    return {
        "app_id": app_id,
        "game_name": TRACKED_APPS.get(app_id, "unknown"),
        "player_count": None,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "error": str(last_error),
    }


def run_ingestion() -> Path:
    """Pull player counts for all tracked apps and write one raw JSON file."""
    if not STEAM_API_KEY:
        logger.warning(
            "STEAM_API_KEY not set. GetNumberOfCurrentPlayers does not require a key, "
            "but other endpoints (e.g. GetPlayerSummaries) will need one."
        )

    logger.info("Starting ingestion for %d tracked apps", len(TRACKED_APPS))
    records = [fetch_player_count(app_id) for app_id in TRACKED_APPS]

    failed = [r for r in records if r.get("player_count") is None]
    if failed:
        logger.warning("%d/%d apps failed to fetch", len(failed), len(records))

    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
    run_timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_path = RAW_DATA_DIR / f"player_counts_{run_timestamp}.json"

    with open(output_path, "w") as f:
        json.dump(records, f, indent=2)

    logger.info("Wrote %d records to %s", len(records), output_path)

    if not records or all(r.get("player_count") is None for r in records):
        raise RuntimeError("Ingestion produced no valid records — failing loudly so Airflow marks this run failed.")

    return output_path


if __name__ == "__main__":
    run_ingestion()
