#!/usr/bin/env bash
# =============================================================================
# NiFi Oil & Gas Monitoring Platform - Register Avro Schemas (Host-Side)
# =============================================================================
# Usage: ./scripts/kafka/register-schemas.sh [profile]
# Registers all .avsc schema files from ./schemas/ to the Schema Registry.
# =============================================================================
set -euo pipefail

# ---------------------------------------------------------------------------
# Colors and helpers
# ---------------------------------------------------------------------------
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

info()    { echo -e "${CYAN}[INFO]${NC}  $*"; }
success() { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error()   { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

# ---------------------------------------------------------------------------
# Load environment
# ---------------------------------------------------------------------------
PROFILE="${1:-dev}"
ENV_FILE="${PROJECT_ROOT}/.env.${PROFILE}"

if [[ -f "${ENV_FILE}" ]]; then
    set -a
    # shellcheck disable=SC1090
    source "${ENV_FILE}"
    set +a
fi

SR_PORT="${SCHEMA_REGISTRY_PORT:-8081}"
SR_URL="http://localhost:${SR_PORT}"
SCHEMAS_DIR="${PROJECT_ROOT}/schemas"

echo -e "${BOLD}========================================================${NC}"
echo -e "${BOLD}  Schema Registration [${PROFILE}]                      ${NC}"
echo -e "${BOLD}========================================================${NC}"
echo ""

# ---------------------------------------------------------------------------
# Check Schema Registry is reachable
# ---------------------------------------------------------------------------
info "Checking Schema Registry at ${SR_URL}..."

if ! curl -sf "${SR_URL}/subjects" &>/dev/null; then
    error "Schema Registry not reachable at ${SR_URL}. Is the stack running?"
fi

success "Schema Registry is reachable."

# ---------------------------------------------------------------------------
# Check schemas directory
# ---------------------------------------------------------------------------
if [[ ! -d "${SCHEMAS_DIR}" ]]; then
    error "Schemas directory not found: ${SCHEMAS_DIR}"
fi

SCHEMA_FILES=$(find "${SCHEMAS_DIR}" -name "*.avsc" -type f 2>/dev/null | sort)

if [[ -z "${SCHEMA_FILES}" ]]; then
    warn "No .avsc files found in ${SCHEMAS_DIR}"
    exit 0
fi

# ---------------------------------------------------------------------------
# Schema-to-subject mapping
# ---------------------------------------------------------------------------
# Map schema filenames to Kafka subject names (topic-value convention).
# The subject name follows: <topic>-value pattern.
declare -A SCHEMA_SUBJECT_MAP=(
    ["sensor-reading.avsc"]="sensor.raw.readings-value"
    ["sensor-validated.avsc"]="sensor.validated-value"
    ["sensor-enriched.avsc"]="sensor.enriched-value"
    ["anomaly-alert.avsc"]="sensor.anomalies-value"
    ["equipment-status.avsc"]="equipment.status-value"
    ["compliance-emission.avsc"]="compliance.emissions-value"
)

# ---------------------------------------------------------------------------
# Register schemas
# ---------------------------------------------------------------------------
REGISTERED=0
UPDATED=0
FAILED=0

for schema_file in ${SCHEMA_FILES}; do
    FILENAME=$(basename "${schema_file}")

    # Determine subject name
    SUBJECT="${SCHEMA_SUBJECT_MAP[${FILENAME}]:-}"
    if [[ -z "${SUBJECT}" ]]; then
        # Default: use filename without extension + "-value"
        SUBJECT="$(echo "${FILENAME}" | sed 's/\.avsc$//')-value"
    fi

    info "Registering: ${FILENAME} -> subject '${SUBJECT}'..."

    # Read and escape schema for JSON payload
    SCHEMA_JSON=$(python3 -c "
import json
with open('${schema_file}') as f:
    schema = json.load(f)
# Schema Registry expects the schema as a string within a JSON payload
payload = {'schema': json.dumps(schema), 'schemaType': 'AVRO'}
print(json.dumps(payload))
" 2>/dev/null || echo "")

    if [[ -z "${SCHEMA_JSON}" ]]; then
        warn "  Failed to parse schema: ${FILENAME}"
        FAILED=$((FAILED + 1))
        continue
    fi

    # Register with Schema Registry
    RESPONSE=$(curl -sf -X POST \
        "${SR_URL}/subjects/${SUBJECT}/versions" \
        -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        -d "${SCHEMA_JSON}" \
        2>/dev/null || echo "")

    if [[ -n "${RESPONSE}" ]]; then
        SCHEMA_ID=$(python3 -c "
import json
try:
    data = json.loads('${RESPONSE}')
    print(data.get('id', 'unknown'))
except Exception:
    print('unknown')
" 2>/dev/null || echo "unknown")

        success "  Registered: ${SUBJECT} (schema ID: ${SCHEMA_ID})"
        REGISTERED=$((REGISTERED + 1))
    else
        warn "  Failed to register: ${SUBJECT}"
        FAILED=$((FAILED + 1))
    fi
done

# ---------------------------------------------------------------------------
# List registered subjects
# ---------------------------------------------------------------------------
echo ""
info "Registered subjects in Schema Registry:"
echo ""

SUBJECTS=$(curl -sf "${SR_URL}/subjects" 2>/dev/null || echo "[]")

python3 -c "
import json
try:
    data = json.loads('${SUBJECTS}')
    for s in sorted(data):
        print(f'    {s}')
except Exception:
    print('    (none)')
" 2>/dev/null || echo "    (none)"

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
echo -e "${BOLD}========================================================${NC}"
echo -e "${BOLD}  Schema Registration Summary                           ${NC}"
echo -e "${BOLD}========================================================${NC}"
echo ""
echo -e "  ${GREEN}Registered:${NC}  ${REGISTERED}"
if [[ ${FAILED} -gt 0 ]]; then
    echo -e "  ${RED}Failed:${NC}      ${FAILED}"
fi
echo ""

if [[ ${FAILED} -gt 0 ]]; then
    exit 1
fi

success "Schema registration complete."
