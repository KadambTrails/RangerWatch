# RangerWatch

Wildlife Telemetry & Conservation Intelligence Platform

RangerWatch is a data engineering project inspired by real-world conservation monitoring systems. The project ingests telemetry, environmental, and geospatial data from multiple sources, processes it through Airflow and PySpark, and generates actionable alerts and analytics.

## Project Goals

This project is designed to provide hands-on experience with:

- Python development
- Data ingestion and integration
- Apache Airflow orchestration
- Apache Spark (PySpark)
- Geospatial analytics
- PostGIS
- API integrations
- Retry and backoff strategies
- Incremental synchronization
- Historical backfills
- Data quality validation
- Alert generation

---

## Data Sources

### Movebank

Primary source of wildlife telemetry data.

### Open-Meteo

Weather data used for enrichment and environmental context.

### Protected Planet

Protected area boundaries used for geofence analysis.

### OpenStreetMap

Villages, settlements, and road network data used for proximity analysis.

### Vendor A / Vendor B / Vendor C

Simulated external systems used to test:

- Schema drift
- Duplicate data
- Rate limiting
- Authentication changes
- Out-of-order delivery

---

## Technology Stack

### Infrastructure

- Docker Compose
- PostgreSQL
- PostGIS

### Data Engineering

- Python
- Apache Spark (PySpark)
- Pandas

### Orchestration

- Apache Airflow

### Geospatial

- PostGIS
- GeoPandas

### Visualization

- Streamlit

---

## Repository Structure

text
rangerwatch/

├── airflow/
│   ├── dags/
│   ├── logs/
│   └── plugins/
│
├── connectors/
│   ├── movebank/
│   ├── open_meteo/
│   ├── protected_planet/
│   ├── osm/
│   ├── vendor_a/
│   ├── vendor_b/
│   └── vendor_c/
│
├── database/
│   └── init.sql
│
├── spark/
│   └── jobs/
│
├── dashboard/
│
├── tests/
│
├── docker-compose.yml
├── requirements.txt
└── README.md


---

## Current Scope

### Ingestion

- Retrieve telemetry from Movebank
- Retrieve weather data from Open-Meteo
- Retrieve geospatial reference data
- Maintain connector state and synchronization metadata

### Processing

- Schema normalization
- Deduplication
- Entity resolution
- Weather enrichment
- Geofence evaluation
- Anomaly detection

### Storage

- Raw ingestion layer
- Curated telemetry layer
- Alert generation layer

### Analytics

- Animal movement tracking
- Geofence breach detection
- Settlement proximity alerts
- Speed and distance calculations
- Historical replay and backfills

---

## Getting Started

Start the platform:

```bash
docker compose up -d
```

Verify running services:

```bash
docker ps
```

Airflow UI:

```text
http://localhost:8080
```

Spark UI:

```text
http://localhost:8081
```

---

## Roadmap

### Phase 1

- Infrastructure setup
- Postgres/PostGIS
- Airflow
- Spark

### Phase 2

- Movebank connector
- Raw data ingestion

### Phase 3

- Airflow orchestration
- Scheduled ingestion

### Phase 4

- Spark transformations
- Data quality checks
- Deduplication

### Phase 5

- Weather enrichment
- Geofence analysis

### Phase 6

- Alerting
- Dashboard
- Historical backfills

---

## License

Educational and portfolio project.