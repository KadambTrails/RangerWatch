import requests
import time
import json 
from datetime import datetime, UTC
from database.db import get_connection

url = "https://archive-api.open-meteo.com/v1/archive"

positions_query = """
select 
animal_id , event_time::date as day, avg(latitude) latitude, avg(longitude) longitude
from  public.normalized_positions
    GROUP BY animal_id, event_time::date
    ORDER BY animal_id, day;
"""

insert_query = insert_query = """
INSERT INTO public.weather_observations
    (observation_time, latitude, longitude, temperature_c, wind_speed_kmh, precipitation_mm)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (observation_time, latitude, longitude) DO NOTHING
"""


def fetch_weather(latitude, longitude, day):
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": day.isoformat(),
        "end_date": day.isoformat(),
        "hourly": "temperature_2m,precipitation,windspeed_10m"
    }

    response = requests.get(url,params=params,timeout=30)
    response.raise_for_status()
    return response.json()

def build_rows(data, latitude, longitude):
    hourly = data["hourly"]
    rows = []
    for t, temp, precip, wind in zip(
        hourly["time"],
        hourly["temperature_2m"],
        hourly["precipitation"],
        hourly["windspeed_10m"],
    ):
        rows.append((
            t, 
            latitude, 
            longitude, 
            temp, 
            wind, 
            precip
        ))
    return rows

def main():
    conn = get_connection()
    cur = conn.cursor()

    try: 
        cur.execute(positions_query)
        positions= cur.fetchall()
        print(f"Found {len(positions)} animal/day combinations")

        for animal_id, day, lat, lon in positions:
            lat, lon = round(float(lat),4), round(float(lon),4)

            try:
                data=fetch_weather(lat, lon, day)
                rows=build_rows(data, lat, lon)

                cur.executemany(insert_query,rows)
                conn.commit()
                print(f"[{animal_id} {day}] processed {len(rows)} hourly rows")

            except requests.RequestException as e:
                print(f"[{animal_id} {day}] request failed: {e}")

            except Exception as e:
                conn.rollback()
                print(f"[{animal_id} {day}] insert failed: {e}")

            time.sleep(0.5)

    finally:
        cur.close()
        conn.close()

if  __name__ == "__main__":
    main()


