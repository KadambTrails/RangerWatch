CREATE EXTENSION IF NOT EXISTS postgis;

-- =====================================================
-- RAW INGESTION LAYER
-- =====================================================

CREATE TABLE IF NOT EXISTS raw_events (
    id BIGSERIAL PRIMARY KEY,
    source VARCHAR(100) NOT NULL,
    source_record_id TEXT,
    payload JSONB NOT NULL,
    received_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_raw_events_source
ON raw_events(source);

CREATE INDEX idx_raw_events_received_at
ON raw_events(received_at);

-- =====================================================
-- CONNECTOR STATE
-- =====================================================

CREATE TABLE IF NOT EXISTS connector_state (
    connector_name VARCHAR(100) PRIMARY KEY,
    cursor_value TEXT,
    last_successful_sync TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- =====================================================
-- NORMALIZED TELEMETRY
-- =====================================================

CREATE TABLE IF NOT EXISTS normalized_positions (
    id BIGSERIAL PRIMARY KEY,

    animal_id VARCHAR(255) NOT NULL,

    event_time TIMESTAMP NOT NULL,

    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,

    source VARCHAR(100) NOT NULL,

    geom GEOGRAPHY(POINT, 4326),

    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_positions_animal
ON normalized_positions(animal_id);

CREATE INDEX idx_positions_event_time
ON normalized_positions(event_time);

CREATE INDEX idx_positions_geom
ON normalized_positions
USING GIST(geom);

-- =====================================================
-- WEATHER ENRICHMENT
-- =====================================================

CREATE TABLE IF NOT EXISTS weather_observations (
    id BIGSERIAL PRIMARY KEY,

    observation_time TIMESTAMP NOT NULL,

    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,

    temperature_c DOUBLE PRECISION,
    wind_speed_kmh DOUBLE PRECISION,
    precipitation_mm DOUBLE PRECISION,

    created_at TIMESTAMP DEFAULT NOW()
);

-- =====================================================
-- PROTECTED AREAS
-- =====================================================

CREATE TABLE IF NOT EXISTS protected_areas (
    id BIGSERIAL PRIMARY KEY,

    area_name VARCHAR(255) NOT NULL,
    source VARCHAR(100),

    geom GEOMETRY(MULTIPOLYGON, 4326)
);

CREATE INDEX idx_protected_areas_geom
ON protected_areas
USING GIST (geom);

-- =====================================================
-- Unique Constraints
-- =====================================================

ALTER TABLE raw_events
ADD CONSTRAINT uq_raw_events
UNIQUE (source, source_record_id);