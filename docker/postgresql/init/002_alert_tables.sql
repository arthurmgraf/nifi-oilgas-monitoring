-- Alert History and Escalation Tables

CREATE TABLE IF NOT EXISTS alert_history (
    alert_id        TEXT PRIMARY KEY,
    sensor_id       TEXT NOT NULL,
    platform_id     TEXT NOT NULL,
    anomaly_type    TEXT NOT NULL,
    severity        TEXT NOT NULL,
    actual_value    DOUBLE PRECISION NOT NULL,
    threshold_value DOUBLE PRECISION NOT NULL,
    sensor_type     TEXT NOT NULL,
    unit            TEXT NOT NULL,
    description     TEXT,
    reading_id      TEXT,
    acknowledged    BOOLEAN DEFAULT FALSE,
    acknowledged_by TEXT,
    acknowledged_at TIMESTAMPTZ,
    resolved        BOOLEAN DEFAULT FALSE,
    resolved_at     TIMESTAMPTZ,
    detected_at     TIMESTAMPTZ NOT NULL,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS alert_escalation_rules (
    rule_id         SERIAL PRIMARY KEY,
    severity        TEXT NOT NULL,
    action_type     TEXT NOT NULL,
    target_endpoint TEXT,
    delay_seconds   INTEGER DEFAULT 0,
    max_retries     INTEGER DEFAULT 3,
    enabled         BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_alerts_platform ON alert_history(platform_id, detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_sensor ON alert_history(sensor_id, detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_severity ON alert_history(severity, detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_unresolved ON alert_history(resolved, detected_at DESC)
    WHERE resolved = FALSE;

-- Read-only user for Grafana
-- Password set here is the init default; override at runtime with:
--   ALTER ROLE grafana_reader WITH PASSWORD '<value from GRAFANA_READER_PASSWORD env>';
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'grafana_reader') THEN
        CREATE ROLE grafana_reader WITH LOGIN PASSWORD 'grafana_readonly_pass';
    END IF;
END $$;

GRANT CONNECT ON DATABASE refdata TO grafana_reader;
GRANT USAGE ON SCHEMA public TO grafana_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO grafana_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO grafana_reader;

-- NiFi service user
-- Password set here is the init default; override at runtime with:
--   ALTER ROLE nifi WITH PASSWORD '<value from PG_REF_PASSWORD env>';
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'nifi') THEN
        CREATE ROLE nifi WITH LOGIN PASSWORD 'nifi_pg_pass';
    END IF;
END $$;

GRANT CONNECT ON DATABASE refdata TO nifi;
GRANT USAGE ON SCHEMA public TO nifi;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO nifi;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO nifi;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE ON TABLES TO nifi;
