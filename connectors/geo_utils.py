import requests

OVERPASS_URL = "https://overpass-api.de/api/interpreter"

def get_bbox(cur, buffer_deg=0.5):
    """Bounding box of all positions plus a buffer (0.5 deg is roughly 55 km).
    Returns (south, west, north, east), the order Overpass expects."""
    cur.execute("""
        SELECT MIN(latitude), MIN(longitude), MAX(latitude), MAX(longitude)
        FROM public.normalized_positions
    """)

    south, west, north, east = cur.fetchone()
    if south is None:
        raise RuntimeError("normalized_positions is empty - load positions first")
    return (south - buffer_deg, west - buffer_deg,
            north + buffer_deg, east + buffer_deg)

def run_overpass(query):
    response = requests.post(
        OVERPASS_URL,
        data={"data": query},
        headers={"User-Agent": "rangerwatch/1.0"},
        timeout=180,
    )
    response.raise_for_status()
    return response.json()