#!/usr/bin/env bash
# =============================================================================
# NiFi Oil & Gas Monitoring Platform - Kafka Topic Initialization
# =============================================================================
# Creates all required Kafka topics with appropriate partitioning and retention.
# Executed as a one-shot init container after Kafka is healthy.
#
# Topics:
#   sensor.raw            - Raw ingest from MQTT/NiFi (high throughput)
#   sensor.validated      - After schema validation + quality checks
#   sensor.enriched       - After reference data join
#   sensor.aggregated     - 1-min/5-min aggregations
#   alerts.critical       - Anomaly alerts (single partition for ordering)
#   equipment.status      - Equipment health updates
#   compliance.emissions  - Regulatory emission records (1-year retention)
# =============================================================================

set -euo pipefail

BOOTSTRAP_SERVER="${KAFKA_BOOTSTRAP_SERVER:-kafka:29092}"
MAX_RETRIES=30
RETRY_INTERVAL=5

# ---------------------------------------------------------------------------
# Wait for Kafka broker to be ready
# ---------------------------------------------------------------------------
echo "[kafka-init] Waiting for Kafka broker at ${BOOTSTRAP_SERVER} ..."

retries=0
until /opt/kafka/bin/kafka-broker-api-versions.sh \
        --bootstrap-server "${BOOTSTRAP_SERVER}" > /dev/null 2>&1; do
    retries=$((retries + 1))
    if [ "${retries}" -ge "${MAX_RETRIES}" ]; then
        echo "[kafka-init] ERROR: Kafka not ready after $((MAX_RETRIES * RETRY_INTERVAL))s. Exiting."
        exit 1
    fi
    echo "[kafka-init] Kafka not ready (attempt ${retries}/${MAX_RETRIES}). Retrying in ${RETRY_INTERVAL}s ..."
    sleep "${RETRY_INTERVAL}"
done

echo "[kafka-init] Kafka broker is ready."

# ---------------------------------------------------------------------------
# Helper: create topic idempotently
# ---------------------------------------------------------------------------
create_topic() {
    local topic_name="$1"
    local partitions="$2"
    local retention_ms="$3"
    local extra_configs="${4:-}"

    echo "[kafka-init] Creating topic: ${topic_name} (partitions=${partitions}, retention=${retention_ms}ms)"

    /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server "${BOOTSTRAP_SERVER}" \
        --create \
        --if-not-exists \
        --topic "${topic_name}" \
        --partitions "${partitions}" \
        --replication-factor 1 \
        --config "retention.ms=${retention_ms}" \
        --config "cleanup.policy=delete" \
        ${extra_configs:+--config "${extra_configs}"}

    echo "[kafka-init] Topic '${topic_name}' ready."
}

# ---------------------------------------------------------------------------
# Create all topics
# ---------------------------------------------------------------------------

# Retention constants (in milliseconds)
RETENTION_7D=604800000       #   7 days
RETENTION_3D=259200000       #   3 days
RETENTION_30D=2592000000     #  30 days
RETENTION_90D=7776000000     #  90 days
RETENTION_365D=31536000000   # 365 days

# sensor.raw -- high-throughput ingest, 7-day retention
create_topic "sensor.raw"             5  "${RETENTION_7D}"

# sensor.validated -- after schema + quality gate, 3-day retention
create_topic "sensor.validated"       5  "${RETENTION_3D}"

# sensor.enriched -- after reference data join, 3-day retention
create_topic "sensor.enriched"        5  "${RETENTION_3D}"

# sensor.aggregated -- windowed aggregations, 30-day retention
create_topic "sensor.aggregated"      3  "${RETENTION_30D}"

# alerts.critical -- single partition preserves global ordering, 90-day retention
create_topic "alerts.critical"        1  "${RETENTION_90D}"

# equipment.status -- equipment health events, 30-day retention
create_topic "equipment.status"       5  "${RETENTION_30D}"

# compliance.emissions -- regulatory records, 365-day retention (compacted)
create_topic "compliance.emissions"   3  "${RETENTION_365D}" "cleanup.policy=compact,delete"

# ---------------------------------------------------------------------------
# Verify
# ---------------------------------------------------------------------------
echo ""
echo "[kafka-init] ===== Topic summary ====="
/opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "${BOOTSTRAP_SERVER}" \
    --list

echo ""
echo "[kafka-init] All topics created successfully."
