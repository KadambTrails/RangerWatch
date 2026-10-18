"""Load protected areas (OpenStreetMap boundaries) into public.protected_areas.

Needs one extra package:  pip install osm2geojson
Run: python -m connectors.protected_areas.load
"""
import json
import osm2geojson
from database.db import get_connection
from connectors.geo_utils import get_bbox, run_overpass

INSERT_QUERY = """
INSERT INTO public.protected_areas (osm_id, name, designation, geom)
VALUES (
    %s, %s, %s,
    ST_Multi(ST_CollectionExtract(
        ST_MakeValid(ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326)), 3))
)
ON CONFLICT (osm_id) DO UPDATE SET
    name        = EXCLUDED.name,
    designation = EXCLUDED.designation,
    geom        = EXCLUDED.geom
"""


def build_rows(data):
    geojson = osm2geojson.json2geojson(data)
    rows = []
    for feature in geojson.get("features", []):
        geometry = feature.get("geometry") or {}
        if geometry.get("type") not in ("Polygon", "MultiPolygon"):
            continue  # skip lines/points - we only want areas

        props = feature.get("properties", {})
        tags = props.get("tags", {})
        designation = (tags.get("protect_class") and f"protect_class {tags['protect_class']}") \
            or tags.get("designation") or tags.get("boundary") or tags.get("leisure")

        rows.append((
            f"{props.get('type')}/{props.get('id')}",
            tags.get("name"),
            designation,
            json.dumps(geometry),
        ))
    return rows


def main():
    conn = get_connection()
    cur = conn.cursor()
    try:
        s, w, n, e = get_bbox(cur)
        print(f"Bounding box: S={s:.3f} W={w:.3f} N={n:.3f} E={e:.3f}")

        bbox = f"({s},{w},{n},{e})"
        query = bbox = f"({s},{w},{n},{e})"
        query = f"""
            [out:json][timeout:180];
            (
            way["boundary"~"^(protected_area|national_park)$"]{bbox};
            relation["boundary"~"^(protected_area|national_park)$"]{bbox};
            way["leisure"="nature_reserve"]{bbox};
            relation["leisure"="nature_reserve"]{bbox};
            way["protect_class"]{bbox};
            relation["protect_class"]{bbox};
            );
            out geom;
        """
        data = run_overpass(query)
        print(f"Overpass returned {len(data.get('elements', []))} raw elements")
        rows = build_rows(data)
        print(f"Fetched {len(rows)} protected area polygons")

        if rows:
            cur.executemany(INSERT_QUERY, rows)
            conn.commit()
            print("Done")
    except Exception as ex:
        conn.rollback()
        print(f"Protected area load failed: {ex!r}")
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()