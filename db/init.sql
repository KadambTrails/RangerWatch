CREATE EXTENSION IF NOT EXISTS postgis;

-- Tracks per-connector sync progress so restarts resume instead of reprocessing.
CREATE TABLE IF NOT EXISTS connector_sync_state (
    vendor_name       TEXT PRIMARY KEY,
    cursor_value      TEXT,               -- opaque cursor/offset, vendor-specific format
    last_synced_at    TIMESTAMPTZ,
    last_success_at   TIMESTAMPTZ,
    last_error        TEXT,
    consecutive_errors INT NOT NULL DEFAULT 0
);

-- Raw, minimally-touched events as landed from each vendor. Kept for
-- reprocessing/backfill — the Spark job reads FROM here, never from the vendor
-- API directly, so backfill = replaying this table, not re-hitting vendors.
CREATE TABLE IF NOT EXISTS raw_events (
    id              BIGSERIAL PRIMARY KEY,
    vendor_name     TEXT NOT NULL,
    source_event_id TEXT,                 -- vendor's own id, when it has one
    raw_payload     JSONB NOT NULL,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- natural key used for idempotent dedupe: vendor + device + event timestamp.
    -- NOT source_event_id alone, since not every vendor guarantees one.
    dedupe_key      TEXT NOT NULL,
    UNIQUE (vendor_name, dedupe_key)
);

CREATE INDEX IF NOT EXISTS idx_raw_events_vendor_ingested
    ON raw_events (vendor_name, ingested_at);

-- Normalized, cross-vendor position records — this is what the Spark job
-- writes to after schema mapping, dedup, and entity resolution.
CREATE TABLE IF NOT EXISTS normalized_positions (
    id              BIGSERIAL PRIMARY KEY,
    animal_id       TEXT NOT NULL,
    species         TEXT,
    source_vendor   TEXT NOT NULL,
    recorded_at     TIMESTAMPTZ NOT NULL,
    geom            GEOGRAPHY(POINT, 4326) NOT NULL,
    speed_kmh       DOUBLE PRECISION,      -- computed vs. previous fix
    is_anomalous    BOOLEAN NOT NULL DEFAULT FALSE,
    anomaly_reason  TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (animal_id, recorded_at, source_vendor)
);

CREATE INDEX IF NOT EXISTS idx_normalized_positions_animal_time
    ON normalized_positions (animal_id, recorded_at);
CREATE INDEX IF NOT EXISTS idx_normalized_positions_geom
    ON normalized_positions USING GIST (geom);

-- Protected area boundaries for geofence checks.
CREATE TABLE IF NOT EXISTS protected_areas (
    id      SERIAL PRIMARY KEY,
    name    TEXT NOT NULL,
    boundary GEOGRAPHY(POLYGON, 4326) NOT NULL
);

-- Alerts raised by the Spark job (offline tracker, geofence exit, anomalous jump).
CREATE TABLE IF NOT EXISTS alerts (
    id              BIGSERIAL PRIMARY KEY,
    animal_id       TEXT NOT NULL,
    alert_type      TEXT NOT NULL,    -- 'geofence_exit' | 'tracker_offline' | 'anomalous_speed'
    details         JSONB,
    raised_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at     TIMESTAMPTZ
);
