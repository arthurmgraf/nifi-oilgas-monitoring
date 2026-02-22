#!/usr/bin/env bash
# =============================================================================
# NiFi Oil & Gas Monitoring Platform - Schema Registry Initialization
# =============================================================================
# Registers all Avro schemas from /schemas/ with Confluent Schema Registry.
# Executed as a one-shot init container after Schema Registry is healthy.
#
# Schema-to-subject mapping:
#   sensor-reading.avsc       -> sensor.raw-value
#   sensor-validated.avsc     -> sensor.validated-value
#   sensor-enriched.avsc      -> sensor.enriched-value
#   equipment-status.avsc     -> equipment.status-value
#   anomaly-alert.avsc        -> alerts.critical-value
#   compliance-emission.avsc  -> compliance.emissions-value
# =============================================================================

set -euo pipefail

SCHEMA_REGISTRY_URL="${SCHEMA_REGISTRY_URL:-http://schema-registry:8081}"
SCHEMAS_DIR="${SCHEMAS_DIR:-/schemas}"
MAX_RETRIES=30
RETRY_INTERVAL=5

# ---------------------------------------------------------------------------
# Wait for Schema Registry to be ready
# ---------------------------------------------------------------------------
echo "[schema-init] Waiting for Schema Registry at ${SCHEMA_REGISTRY_URL} ..."

retries=0
until curl -sf "${SCHEMA_REGISTRY_URL}/subjects" > /dev/null 2>&1; do
    retries=$((retries + 1))
    if [ "${retries}" -ge "${MAX_RETRIES}" ]; then
        echo "[schema-init] ERROR: Schema Registry not ready after $((MAX_RETRIES * RETRY_INTERVAL))s. Exiting."
        exit 1
    fi
    echo "[schema-init] Schema Registry not ready (attempt ${retries}/${MAX_RETRIES}). Retrying in ${RETRY_INTERVAL}s ..."
    sleep "${RETRY_INTERVAL}"
done

echo "[schema-init] Schema Registry is ready."

# ---------------------------------------------------------------------------
# Schema file -> Kafka subject mapping
# ---------------------------------------------------------------------------
# Associative array: schema_filename -> subject_name
declare -A SCHEMA_SUBJECTS=(
    ["sensor-reading.avsc"]="sensor.raw-value"
    ["sensor-validated.avsc"]="sensor.validated-value"
    ["sensor-enriched.avsc"]="sensor.enriched-value"
    ["equipment-status.avsc"]="equipment.status-value"
    ["anomaly-alert.avsc"]="alerts.critical-value"
    ["compliance-emission.avsc"]="compliance.emissions-value"
)

# ---------------------------------------------------------------------------
# Helper: register a single schema
# ---------------------------------------------------------------------------
register_schema() {
    local schema_file="$1"
    local subject="$2"

    if [ ! -f "${schema_file}" ]; then
        echo "[schema-init] WARNING: Schema file not found: ${schema_file}. Skipping."
        return 1
    fi

    echo "[schema-init] Registering: ${schema_file} -> subject '${subject}'"

    # Read the schema JSON and escape it for embedding in the request payload.
    # The Schema Registry expects {"schema": "<escaped-json-string>"}.
    local schema_json
    schema_json=$(cat "${schema_file}")

    # Build the request payload using jq if available, otherwise use Python.
    local payload
    if command -v jq &> /dev/null; then
        payload=$(jq -n --arg schema "${schema_json}" '{"schema": $schema}')
    elif command -v python3 &> /dev/null; then
        payload=$(python3 -c "
import json, sys
schema = open('${schema_file}').read()
print(json.dumps({'schema': schema}))
")
    else
        # Fallback: manual JSON escaping (handles basic cases)
        local escaped
        escaped=$(echo "${schema_json}" | sed 's/\\/\\\\/g; s/"/\\"/g' | tr -d '\n')
        payload="{\"schema\": \"${escaped}\"}"
    fi

    local http_code
    http_code=$(curl -s -o /tmp/sr_response.json -w "%{http_code}" \
        -X POST \
        -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        -d "${payload}" \
        "${SCHEMA_REGISTRY_URL}/subjects/${subject}/versions")

    if [ "${http_code}" -eq 200 ] || [ "${http_code}" -eq 409 ]; then
        local schema_id
        schema_id=$(cat /tmp/sr_response.json)
        echo "[schema-init] OK: ${subject} registered (response: ${schema_id})"
        return 0
    else
        echo "[schema-init] ERROR: Failed to register ${subject} (HTTP ${http_code})"
        echo "[schema-init]   Response: $(cat /tmp/sr_response.json)"
        return 1
    fi
}

# ---------------------------------------------------------------------------
# Register all schemas
# ---------------------------------------------------------------------------
echo ""
echo "[schema-init] ===== Registering schemas from ${SCHEMAS_DIR} ====="
echo ""

errors=0

for schema_file in "${!SCHEMA_SUBJECTS[@]}"; do
    subject="${SCHEMA_SUBJECTS[${schema_file}]}"
    if ! register_schema "${SCHEMAS_DIR}/${schema_file}" "${subject}"; then
        errors=$((errors + 1))
    fi
done

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
echo "[schema-init] ===== Registered subjects ====="
curl -sf "${SCHEMA_REGISTRY_URL}/subjects" 2>/dev/null | \
    (command -v jq &>/dev/null && jq '.' || cat)

echo ""
if [ "${errors}" -gt 0 ]; then
    echo "[schema-init] WARNING: ${errors} schema(s) failed to register."
    exit 1
else
    echo "[schema-init] All schemas registered successfully."
fi
