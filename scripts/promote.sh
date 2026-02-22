#!/usr/bin/env bash
# =============================================================================
# NiFi Oil & Gas Monitoring Platform - Flow Promotion
# =============================================================================
# Usage: ./scripts/promote.sh <from_env> <to_env>
# Promotes NiFi flows between environments (dev -> staging -> prod).
# Uses NiFi Registry API and NiFi Toolkit CLI.
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

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# ---------------------------------------------------------------------------
# Validate arguments
# ---------------------------------------------------------------------------
if [[ $# -lt 2 ]]; then
    echo "Usage: $0 <from_env> <to_env>"
    echo ""
    echo "Arguments:"
    echo "  from_env    Source environment (dev, staging, prod)"
    echo "  to_env      Target environment (dev, staging, prod)"
    echo ""
    echo "Examples:"
    echo "  $0 dev staging       # Promote dev flows to staging"
    echo "  $0 staging prod      # Promote staging flows to production"
    exit 1
fi

FROM_ENV="$1"
TO_ENV="$2"

VALID_ENVS=("dev" "staging" "prod")
if [[ ! " ${VALID_ENVS[*]} " =~ ${FROM_ENV} ]]; then
    error "Invalid source environment: ${FROM_ENV}. Must be one of: ${VALID_ENVS[*]}"
fi
if [[ ! " ${VALID_ENVS[*]} " =~ ${TO_ENV} ]]; then
    error "Invalid target environment: ${TO_ENV}. Must be one of: ${VALID_ENVS[*]}"
fi
if [[ "${FROM_ENV}" == "${TO_ENV}" ]]; then
    error "Source and target environments must be different."
fi

# ---------------------------------------------------------------------------
# Load environment configurations
# ---------------------------------------------------------------------------
FROM_ENV_FILE="${PROJECT_ROOT}/.env.${FROM_ENV}"
TO_ENV_FILE="${PROJECT_ROOT}/.env.${TO_ENV}"

if [[ ! -f "${FROM_ENV_FILE}" ]]; then
    error "Source environment file not found: ${FROM_ENV_FILE}"
fi
if [[ ! -f "${TO_ENV_FILE}" ]]; then
    error "Target environment file not found: ${TO_ENV_FILE}"
fi

echo -e "${BOLD}========================================================${NC}"
echo -e "${BOLD}  Flow Promotion: ${FROM_ENV} -> ${TO_ENV}              ${NC}"
echo -e "${BOLD}========================================================${NC}"
echo ""

# ---------------------------------------------------------------------------
# NiFi Toolkit check
# ---------------------------------------------------------------------------
TOOLKIT_DIR="${PROJECT_ROOT}/tools/nifi-toolkit"
TOOLKIT_CLI="${TOOLKIT_DIR}/bin/cli.sh"

if [[ ! -f "${TOOLKIT_CLI}" ]]; then
    warn "NiFi Toolkit not found at ${TOOLKIT_DIR}"
    info "Install it with: ./scripts/nifi-cli/install-toolkit.sh"
    info "Falling back to REST API method."
    USE_TOOLKIT=false
else
    USE_TOOLKIT=true
fi

# ---------------------------------------------------------------------------
# Load source environment
# ---------------------------------------------------------------------------
info "Loading source environment (${FROM_ENV})..."
# shellcheck disable=SC1090
FROM_REGISTRY_PORT=$(grep -E '^NIFI_REGISTRY_PORT=' "${FROM_ENV_FILE}" | cut -d= -f2 || echo "18080")
FROM_NIFI_PORT=$(grep -E '^NIFI_WEB_PORT=' "${FROM_ENV_FILE}" | cut -d= -f2 || echo "8443")
FROM_NIFI_USER=$(grep -E '^NIFI_USERNAME=' "${FROM_ENV_FILE}" | cut -d= -f2 || echo "admin")
FROM_NIFI_PASS=$(grep -E '^NIFI_PASSWORD=' "${FROM_ENV_FILE}" | cut -d= -f2 || echo "")

# Load target environment
info "Loading target environment (${TO_ENV})..."
TO_REGISTRY_PORT=$(grep -E '^NIFI_REGISTRY_PORT=' "${TO_ENV_FILE}" | cut -d= -f2 || echo "18080")
TO_NIFI_PORT=$(grep -E '^NIFI_WEB_PORT=' "${TO_ENV_FILE}" | cut -d= -f2 || echo "8443")
TO_NIFI_USER=$(grep -E '^NIFI_USERNAME=' "${TO_ENV_FILE}" | cut -d= -f2 || echo "admin")
TO_NIFI_PASS=$(grep -E '^NIFI_PASSWORD=' "${TO_ENV_FILE}" | cut -d= -f2 || echo "")

FROM_REGISTRY_URL="http://localhost:${FROM_REGISTRY_PORT}"
TO_REGISTRY_URL="http://localhost:${TO_REGISTRY_PORT}"

# ---------------------------------------------------------------------------
# Step 1: Export flows from source Registry
# ---------------------------------------------------------------------------
info "Step 1: Exporting flows from ${FROM_ENV} Registry..."

EXPORT_DIR="${PROJECT_ROOT}/nifi-flows/promote_${FROM_ENV}_to_${TO_ENV}_$(date +%Y%m%d_%H%M%S)"
mkdir -p "${EXPORT_DIR}"

if ${USE_TOOLKIT}; then
    # Use NiFi Toolkit CLI
    info "Using NiFi Toolkit CLI..."

    # List buckets in source registry
    "${TOOLKIT_CLI}" registry list-buckets \
        --baseUrl "${FROM_REGISTRY_URL}" \
        --outputType json \
        > "${EXPORT_DIR}/buckets.json" 2>/dev/null || true

    # Export each flow from the source bucket
    BUCKET_IDS=$(python3 -c "
import json, sys
try:
    with open('${EXPORT_DIR}/buckets.json') as f:
        data = json.load(f)
    for bucket in data:
        print(bucket.get('identifier', ''))
except Exception:
    pass
" 2>/dev/null || echo "")

    for bucket_id in ${BUCKET_IDS}; do
        if [[ -n "${bucket_id}" ]]; then
            "${TOOLKIT_CLI}" registry list-flows \
                --baseUrl "${FROM_REGISTRY_URL}" \
                --bucketIdentifier "${bucket_id}" \
                --outputType json \
                > "${EXPORT_DIR}/flows_${bucket_id}.json" 2>/dev/null || true

            # Export each flow version
            FLOW_IDS=$(python3 -c "
import json
try:
    with open('${EXPORT_DIR}/flows_${bucket_id}.json') as f:
        data = json.load(f)
    for flow in data:
        print(flow.get('identifier', ''))
except Exception:
    pass
" 2>/dev/null || echo "")

            for flow_id in ${FLOW_IDS}; do
                if [[ -n "${flow_id}" ]]; then
                    "${TOOLKIT_CLI}" registry export-flow-version \
                        --baseUrl "${FROM_REGISTRY_URL}" \
                        --flowIdentifier "${flow_id}" \
                        --outputFile "${EXPORT_DIR}/flow_${flow_id}.json" \
                        2>/dev/null || true
                fi
            done
        fi
    done

    success "Flows exported via Toolkit CLI."
else
    # Fallback: REST API
    info "Using NiFi Registry REST API..."

    # List buckets
    curl -sf "${FROM_REGISTRY_URL}/nifi-registry-api/buckets" \
        -o "${EXPORT_DIR}/buckets.json" 2>/dev/null || warn "Could not list buckets from source Registry."

    if [[ -f "${EXPORT_DIR}/buckets.json" ]]; then
        BUCKET_IDS=$(python3 -c "
import json
try:
    with open('${EXPORT_DIR}/buckets.json') as f:
        data = json.load(f)
    for bucket in data:
        print(bucket.get('identifier', ''))
except Exception:
    pass
" 2>/dev/null || echo "")

        for bucket_id in ${BUCKET_IDS}; do
            if [[ -n "${bucket_id}" ]]; then
                # List flows in bucket
                curl -sf "${FROM_REGISTRY_URL}/nifi-registry-api/buckets/${bucket_id}/flows" \
                    -o "${EXPORT_DIR}/flows_${bucket_id}.json" 2>/dev/null || true

                FLOW_IDS=$(python3 -c "
import json
try:
    with open('${EXPORT_DIR}/flows_${bucket_id}.json') as f:
        data = json.load(f)
    for flow in data:
        print(flow.get('identifier', ''))
except Exception:
    pass
" 2>/dev/null || echo "")

                for flow_id in ${FLOW_IDS}; do
                    if [[ -n "${flow_id}" ]]; then
                        # Get latest version
                        curl -sf "${FROM_REGISTRY_URL}/nifi-registry-api/buckets/${bucket_id}/flows/${flow_id}/versions/latest" \
                            -o "${EXPORT_DIR}/flow_${flow_id}.json" 2>/dev/null || true
                    fi
                done
            fi
        done
    fi

    success "Flows exported via REST API."
fi

# ---------------------------------------------------------------------------
# Step 2: Import flows to target Registry
# ---------------------------------------------------------------------------
info "Step 2: Importing flows to ${TO_ENV} Registry..."

# Ensure target bucket exists
TARGET_BUCKET_NAME="oilgas-${TO_ENV}"
TARGET_BUCKET_ID=""

EXISTING_BUCKETS=$(curl -sf "${TO_REGISTRY_URL}/nifi-registry-api/buckets" 2>/dev/null || echo "[]")

TARGET_BUCKET_ID=$(python3 -c "
import json
try:
    data = json.loads('${EXISTING_BUCKETS}')
    for b in data:
        if b.get('name') == '${TARGET_BUCKET_NAME}':
            print(b.get('identifier', ''))
            break
except Exception:
    pass
" 2>/dev/null || echo "")

if [[ -z "${TARGET_BUCKET_ID}" ]]; then
    info "Creating target bucket '${TARGET_BUCKET_NAME}'..."
    TARGET_BUCKET_ID=$(curl -sf -X POST \
        "${TO_REGISTRY_URL}/nifi-registry-api/buckets" \
        -H "Content-Type: application/json" \
        -d "{\"name\": \"${TARGET_BUCKET_NAME}\", \"description\": \"Flows promoted from ${FROM_ENV}\"}" \
        2>/dev/null | python3 -c "import json,sys; print(json.load(sys.stdin).get('identifier',''))" 2>/dev/null || echo "")

    if [[ -n "${TARGET_BUCKET_ID}" ]]; then
        success "Created bucket '${TARGET_BUCKET_NAME}' (${TARGET_BUCKET_ID})"
    else
        warn "Could not create target bucket. Registry may not be accessible."
    fi
fi

# Import flow files
FLOW_FILES=$(find "${EXPORT_DIR}" -name "flow_*.json" -type f 2>/dev/null || echo "")
IMPORTED=0

for flow_file in ${FLOW_FILES}; do
    if [[ -f "${flow_file}" ]] && [[ -n "${TARGET_BUCKET_ID}" ]]; then
        FLOW_NAME=$(python3 -c "
import json
try:
    with open('${flow_file}') as f:
        data = json.load(f)
    name = data.get('flowContents', data).get('name', data.get('name', 'unknown'))
    print(name)
except Exception:
    print('unknown')
" 2>/dev/null || echo "unknown")

        info "  Importing flow: ${FLOW_NAME}..."

        if ${USE_TOOLKIT}; then
            "${TOOLKIT_CLI}" registry import-flow-version \
                --baseUrl "${TO_REGISTRY_URL}" \
                --bucketIdentifier "${TARGET_BUCKET_ID}" \
                --input "${flow_file}" \
                2>/dev/null || warn "  Failed to import ${FLOW_NAME} via Toolkit."
        else
            curl -sf -X POST \
                "${TO_REGISTRY_URL}/nifi-registry-api/buckets/${TARGET_BUCKET_ID}/flows" \
                -H "Content-Type: application/json" \
                -d @"${flow_file}" \
                -o /dev/null 2>/dev/null || warn "  Failed to import ${FLOW_NAME} via REST API."
        fi

        IMPORTED=$((IMPORTED + 1))
    fi
done

success "Imported ${IMPORTED} flow(s) to ${TO_ENV} Registry."

# ---------------------------------------------------------------------------
# Step 3: Update parameter context for target environment
# ---------------------------------------------------------------------------
info "Step 3: Updating parameter context for ${TO_ENV}..."

# Build parameter context updates from target env file
PARAM_UPDATES=$(python3 -c "
import sys

params = {}
try:
    with open('${TO_ENV_FILE}') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, val = line.split('=', 1)
                params[key.strip()] = val.strip()
except Exception:
    pass

# Print key env vars that NiFi parameter contexts typically use
important_keys = [
    'PG_REF_HOST', 'PG_REF_PORT', 'PG_REF_DATABASE', 'PG_REF_USERNAME',
    'TSDB_HOST', 'TSDB_PORT', 'TSDB_DATABASE', 'TSDB_USERNAME',
    'KAFKA_BROKER_PORT', 'KAFKA_SASL_USERNAME',
    'MINIO_PORT', 'MINIO_ACCESS_KEY',
    'MQTT_PORT', 'MQTT_USERNAME',
]

for k in important_keys:
    if k in params:
        print(f'{k}={params[k]}')
" 2>/dev/null || echo "")

if [[ -n "${PARAM_UPDATES}" ]]; then
    info "Parameter context values for ${TO_ENV}:"
    echo "${PARAM_UPDATES}" | while IFS= read -r line; do
        key=$(echo "${line}" | cut -d= -f1)
        val=$(echo "${line}" | cut -d= -f2)
        echo -e "    ${CYAN}${key}${NC} = ${val}"
    done
    echo ""
    info "Update these values in the NiFi UI parameter contexts for ${TO_ENV}."
    info "NiFi UI: https://localhost:${TO_NIFI_PORT}/nifi"
else
    warn "No parameter updates detected."
fi

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
echo -e "${BOLD}========================================================${NC}"
echo -e "${BOLD}  Promotion Summary                                     ${NC}"
echo -e "${BOLD}========================================================${NC}"
echo ""
echo -e "  ${CYAN}Direction:${NC}     ${FROM_ENV} -> ${TO_ENV}"
echo -e "  ${CYAN}Flows exported:${NC} $(find "${EXPORT_DIR}" -name "flow_*.json" -type f 2>/dev/null | wc -l | tr -d ' ')"
echo -e "  ${CYAN}Flows imported:${NC} ${IMPORTED}"
echo -e "  ${CYAN}Export dir:${NC}    ${EXPORT_DIR}"
echo ""
echo -e "${BOLD}Next steps:${NC}"
echo -e "  1. Verify flows in ${TO_ENV} NiFi Registry: ${TO_REGISTRY_URL}"
echo -e "  2. Update parameter contexts in ${TO_ENV} NiFi UI"
echo -e "  3. Start process groups in ${TO_ENV} NiFi"
echo -e "  4. Run integration tests: ./scripts/health-check.sh"
echo ""
success "Promotion complete."
