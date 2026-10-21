import json
import logging
import os
import sys

import requests
from psycopg2.extras import execute_values
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from database.db import get_connection

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("movebank_sync")

URL = "https://www.movebank.org/movebank/service/public/json"
SOURCE = "movebank"

# Config: override with environment variables instead of editing the code.
STUDY_ID = int(os.getenv("MOVEBANK_STUDY_ID", "2911040"))
ANIMAL_IDS = [
    a.strip()
    for a in os.getenv("MOVEBANK_ANIMAL_IDS", "4262-84830876").split(",")
    if a.strip()
]
MAX_EVENTS = int(os.getenv("MOVEBANK_MAX_EVENTS", "2000"))

INSERT_QUERY = """
    INSERT INTO raw_events (source, source_record_id, payload)
    VALUES %s
    ON CONFLICT (source, source_record_id) DO NOTHING
    RETURNING 1
"""


def make_session():
    """HTTP session that retries on temporary server errors with backoff."""
    retry = Retry(
        total=3,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def fetch_individuals():
    params = {
        "study_id": STUDY_ID,
        "individual_local_identifiers": ANIMAL_IDS,  # a list is sent as repeated params
        "sensor_type": "gps",
        "max_events_per_individual": MAX_EVENTS,
    }
    response = make_session().get(URL, params=params, timeout=120)
    response.raise_for_status()
    log.info("Movebank responded with status %s", response.status_code)

    individuals = response.json().get("individuals", [])
    if not individuals:
        raise RuntimeError(f"No individuals returned for study {STUDY_ID} / {ANIMAL_IDS}")
    return individuals


def build_rows(individuals):
    rows, skipped = [], 0
    for individual in individuals:
        animal_id = individual["individual_local_identifier"]
        for location in individual.get("locations", []):
            timestamp = location.get("timestamp")
            if timestamp is None:
                skipped += 1
                continue
            rows.append((SOURCE, f"{animal_id}_{timestamp}", json.dumps(location)))
    if skipped:
        log.warning("Skipped %d locations without a timestamp", skipped)
    return rows


def main():
    conn = get_connection()
    cur = conn.cursor()
    try:
        individuals = fetch_individuals()
        rows = build_rows(individuals)
        log.info("Fetched %d locations for %d animal(s)", len(rows), len(individuals))

        inserted = execute_values(cur, INSERT_QUERY, rows, fetch=True) if rows else []
        conn.commit()
        log.info("Inserted %d new rows, %d already existed", len(inserted), len(rows) - len(inserted))
    except Exception:
        conn.rollback()
        log.exception("Movebank sync failed")
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    try:
        main()
    except Exception:
        sys.exit(1)  # non-zero exit so Airflow marks the task as failed