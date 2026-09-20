import requests
import json
from datetime import datetime, UTC
from database.db import get_connection

conn = get_connection()
cur = conn.cursor()

url = 'https://www.movebank.org/movebank/service/public/json'

params = {
"study_id": 2911040,
"individual_local_identifiers": "4262-84830876",
"sensor_type": "gps",
"max_events_per_individual": 10
}

response = requests.get(url, params=params,timeout=30)

print(f"Status code: {response.status_code}")

data = response.json()

individual = data["individuals"][0]

animal_id = individual["individual_local_identifier"]

for location in individual["locations"]:
    source_record_id = (f"{animal_id}_{location["timestamp"]}")

    cur.execute(
    """
    INSERT INTO raw_events (
        source,
        source_record_id,
        payload
    )
    VALUES (%s, %s, %s)
    ON CONFLICT (source, source_record_id)
    DO NOTHING
    """,
    (
        "movebank",
        source_record_id,
        json.dumps(location)
    )
    )

conn.commit()

cur.close()
conn.close()

print("Loaded records into raw_events")