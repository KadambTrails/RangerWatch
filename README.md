## Architecture

```mermaid
flowchart LR
    subgraph SRC["External sources"]
        MB["Movebank API"]
        OSM["OpenStreetMap Overpass API"]
        OM["Open-Meteo Archive API"]
    end

    subgraph AF["Airflow DAG: rangerwatch_pipeline (daily)"]
        J1["Movebank ingest"]
        J2["Spark: normalize positions"]
        J3["Protected areas load"]
        J4["Weather sync"]
        J5["Spark: movement metrics"]
        J6["Spark: anomaly detection"]
    end

    subgraph DB["PostgreSQL + PostGIS"]
        RAW[("raw_events")]
        NP[("normalized_positions")]
        PA[("protected_areas")]
        WO[("weather_observations")]
        MM[("movement_metrics")]
        AL[("alerts")]
    end

    GF["Grafana dashboard"]

    MB --> J1 --> RAW
    RAW --> J2 --> NP
    OSM --> J3 --> PA
    OM --> J4
    NP --> J4 --> WO
    NP --> J5 --> MM
    MM --> J6
    NP --> J6
    PA --> J6
    J6 --> AL

    NP & MM & AL & WO & PA --> GF
```

Spark jobs run on a standalone Spark cluster (master + worker) and are submitted from Airflow.
The protected-area check runs in PostGIS (`ST_Intersects`); Spark detects the inside-to-outside transition.
