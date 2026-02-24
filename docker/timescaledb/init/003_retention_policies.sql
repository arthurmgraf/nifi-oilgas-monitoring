-- Retention Policies: Automatic data lifecycle management

-- Raw data: keep 90 days
SELECT add_retention_policy('sensor_readings',
    drop_after => INTERVAL '90 days',
    if_not_exists => TRUE
);

-- 1-minute aggregate: keep 180 days
SELECT add_retention_policy('sensor_1min_agg',
    drop_after => INTERVAL '180 days',
    if_not_exists => TRUE
);

-- 5-minute aggregate: keep 365 days
SELECT add_retention_policy('sensor_5min_agg',
    drop_after => INTERVAL '365 days',
    if_not_exists => TRUE
);

-- 15-minute aggregate: keep forever (no retention policy)

-- Read-only user for Grafana
-- Password set here is the init default; override at runtime with:
--   ALTER ROLE grafana_reader WITH PASSWORD '<value from GRAFANA_READER_PASSWORD env>';
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'grafana_reader') THEN
        CREATE ROLE grafana_reader WITH LOGIN PASSWORD 'grafana_readonly_pass';
    END IF;
END $$;

GRANT CONNECT ON DATABASE sensordb TO grafana_reader;
GRANT USAGE ON SCHEMA public TO grafana_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO grafana_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO grafana_reader;
