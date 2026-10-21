FROM apache/airflow:2.10.0

# Java is required by spark-submit
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

USER airflow
# pyspark provides spark-submit; its version must match the cluster (apache/spark:4.2.0).
# osm2geojson is needed by connectors/protected_areas/sync.py
RUN pip install --no-cache-dir pyspark==4.2.0 osm2geojson