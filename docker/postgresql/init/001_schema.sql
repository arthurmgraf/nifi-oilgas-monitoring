-- PostgreSQL: Reference Data Schema for Oil & Gas Monitoring Platform

CREATE TABLE IF NOT EXISTS platforms (
    platform_id     TEXT PRIMARY KEY,
    platform_name   TEXT NOT NULL,
    platform_type   TEXT NOT NULL,
    latitude        DOUBLE PRECISION NOT NULL,
    longitude       DOUBLE PRECISION NOT NULL,
    region          TEXT NOT NULL,
    timezone        TEXT NOT NULL DEFAULT 'UTC',
    status          TEXT NOT NULL DEFAULT 'ACTIVE',
    commissioned_at TIMESTAMPTZ,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS equipment (
    equipment_id    TEXT PRIMARY KEY,
    platform_id     TEXT NOT NULL REFERENCES platforms(platform_id),
    equipment_name  TEXT NOT NULL,
    equipment_type  TEXT NOT NULL,
    manufacturer    TEXT,
    model           TEXT,
    install_date    DATE,
    last_maintenance TIMESTAMPTZ,
    maintenance_interval_hours INTEGER DEFAULT 8760,
    status          TEXT NOT NULL DEFAULT 'OPERATIONAL',
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS sensors (
    sensor_id       TEXT PRIMARY KEY,
    equipment_id    TEXT NOT NULL REFERENCES equipment(equipment_id),
    platform_id     TEXT NOT NULL REFERENCES platforms(platform_id),
    sensor_type     TEXT NOT NULL,
    unit            TEXT NOT NULL,
    min_range       DOUBLE PRECISION,
    max_range       DOUBLE PRECISION,
    accuracy        DOUBLE PRECISION,
    install_date    DATE,
    status          TEXT NOT NULL DEFAULT 'ACTIVE',
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS sensor_thresholds (
    threshold_id    SERIAL PRIMARY KEY,
    sensor_type     TEXT NOT NULL,
    subtype         TEXT,
    min_normal      DOUBLE PRECISION NOT NULL,
    max_normal      DOUBLE PRECISION NOT NULL,
    critical_low    DOUBLE PRECISION,
    critical_high   DOUBLE PRECISION NOT NULL,
    warning_low     DOUBLE PRECISION,
    warning_high    DOUBLE PRECISION NOT NULL,
    unit            TEXT NOT NULL,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(sensor_type, subtype)
);

CREATE INDEX IF NOT EXISTS idx_equipment_platform ON equipment(platform_id);
CREATE INDEX IF NOT EXISTS idx_sensors_equipment ON sensors(equipment_id);
CREATE INDEX IF NOT EXISTS idx_sensors_platform ON sensors(platform_id);
CREATE INDEX IF NOT EXISTS idx_sensors_type ON sensors(sensor_type);
CREATE INDEX IF NOT EXISTS idx_thresholds_type ON sensor_thresholds(sensor_type);
