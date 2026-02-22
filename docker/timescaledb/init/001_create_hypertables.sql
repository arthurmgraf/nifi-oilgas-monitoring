-- TimescaleDB: Sensor Readings Hypertable
-- Auto-partitioned by time for optimal time-series performance

CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS sensor_readings (
    time            TIMESTAMPTZ         NOT NULL,
    reading_id      TEXT                NOT NULL,
    platform_id     TEXT                NOT NULL,
    sensor_id       TEXT                NOT NULL,
    sensor_type     TEXT                NOT NULL,
    value           DOUBLE PRECISION    NOT NULL,
    unit            TEXT                NOT NULL,
    quality_flag    TEXT                DEFAULT 'GOOD'
);

SELECT create_hypertable('sensor_readings', by_range('time'),
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS idx_readings_platform
    ON sensor_readings (platform_id, time DESC);

CREATE INDEX IF NOT EXISTS idx_readings_sensor
    ON sensor_readings (sensor_id, time DESC);

CREATE INDEX IF NOT EXISTS idx_readings_type
    ON sensor_readings (sensor_type, time DESC);

CREATE INDEX IF NOT EXISTS idx_readings_id
    ON sensor_readings (reading_id);
