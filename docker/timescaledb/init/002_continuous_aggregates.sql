-- Continuous Aggregates: Automatic multi-resolution rollups

-- 1-minute aggregate
CREATE MATERIALIZED VIEW IF NOT EXISTS sensor_1min_agg
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 minute', time)   AS bucket,
    platform_id,
    sensor_id,
    sensor_type,
    AVG(value)                      AS avg_value,
    MIN(value)                      AS min_value,
    MAX(value)                      AS max_value,
    COUNT(*)                        AS reading_count,
    STDDEV(value)                   AS stddev_value
FROM sensor_readings
GROUP BY bucket, platform_id, sensor_id, sensor_type
WITH NO DATA;

SELECT add_continuous_aggregate_policy('sensor_1min_agg',
    start_offset    => INTERVAL '2 hours',
    end_offset      => INTERVAL '1 minute',
    schedule_interval => INTERVAL '1 minute',
    if_not_exists   => TRUE
);

-- 5-minute aggregate
CREATE MATERIALIZED VIEW IF NOT EXISTS sensor_5min_agg
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('5 minutes', time)  AS bucket,
    platform_id,
    sensor_id,
    sensor_type,
    AVG(value)                      AS avg_value,
    MIN(value)                      AS min_value,
    MAX(value)                      AS max_value,
    COUNT(*)                        AS reading_count,
    STDDEV(value)                   AS stddev_value
FROM sensor_readings
GROUP BY bucket, platform_id, sensor_id, sensor_type
WITH NO DATA;

SELECT add_continuous_aggregate_policy('sensor_5min_agg',
    start_offset    => INTERVAL '6 hours',
    end_offset      => INTERVAL '5 minutes',
    schedule_interval => INTERVAL '5 minutes',
    if_not_exists   => TRUE
);

-- 15-minute aggregate
CREATE MATERIALIZED VIEW IF NOT EXISTS sensor_15min_agg
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('15 minutes', time) AS bucket,
    platform_id,
    sensor_id,
    sensor_type,
    AVG(value)                      AS avg_value,
    MIN(value)                      AS min_value,
    MAX(value)                      AS max_value,
    COUNT(*)                        AS reading_count,
    STDDEV(value)                   AS stddev_value
FROM sensor_readings
GROUP BY bucket, platform_id, sensor_id, sensor_type
WITH NO DATA;

SELECT add_continuous_aggregate_policy('sensor_15min_agg',
    start_offset    => INTERVAL '1 day',
    end_offset      => INTERVAL '15 minutes',
    schedule_interval => INTERVAL '15 minutes',
    if_not_exists   => TRUE
);
