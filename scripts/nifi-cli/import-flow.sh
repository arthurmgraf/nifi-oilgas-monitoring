#!/usr/bin/env bash
# =============================================================================
# NiFi Oil & Gas Monitoring Platform - Import Flow to NiFi Registry
# =============================================================================
# Usage: ./scripts/nifi-cli/import-flow.sh <environment> <flow_file>
# Imports a flow JSON file into the given environment's Registry bucket.
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
# Validate arguments
# ---------------------------------------------------------------------------
if [[ $# -lt 2 ]]; then
    echo "Usage: $0 <environment> <flow_file>"
    echo ""
    echo "Arguments:"
    echo "  environment    Target environment (dev, staging, prod)"
    echo "  flow_file      Path to flow JSON file to import"
    echo ""
    echo "Examples:"
    echo "  $0 dev ./nifi-flows/sensor-ingestion.json"
    echo "  $0 staging ./nifi-flows/export_dev/flow_SensorIngestion.json"
    exit 1
fi

ENV="$1"
FLOW_FILE="$2"

if [[ ! -f "${FLOW_FILE}" ]]; then
    error "Flow file not found: ${FLOW_FILE}"
fi

ENV_FILE="${PROJECT_ROOT}/.env.${ENV}"

if [[ ! -f "${ENV_FILE}" ]]; then
    error "Environment file not found: ${ENV_FILE}"
fi

# ---------------------------------------------------------------------------
# Load environment
# ---------------------------------------------------------------------------
REGISTRY_PORT=$(grep -E '^NIFI_REGISTRY_PORT=' "${ENV_FILE}" | cut -d= -f2 || echo "18080")
REGISTRY_URL="http://localhost:${REGISTRY_PORT}"

# ---------------------------------------------------------------------------
# Check NiFi Toolkit
# ---------------------------------------------------------------------------
TOOLKIT_CLI="${PROJECT_ROOT}/tools/nifi-toolkit/bin/cli.sh"
USE_TOOLKIT=false

if [[ -f "${TOOLKIT_CLI}" ]]; then
    USE_TOOLKIT=true
fi

echo -e "${BOLD}========================================================${NC}"
echo -e "${BOLD}  Import Flow to NiFi Registry [${ENV}]                 ${NC}"
echo -e "${BOLD}========================================================${NC}"
echo ""

# ---------------------------------------------------------------------------
# Step 1: Ensure target bucket exists
# ---------------------------------------------------------------------------
BUCKET_NAME="oilgas-${ENV}"

info "Checking for bucket '${BUCKET_NAME}' in Registry..."

BUCKETS_JSON=$(curl -sf "${REGISTRY_URL}/nifi-registry-api/buckets" 2>/dev/null || echo "[]")

BUCKET_ID=$(python3 -c "
import json
try:
    data = json.loads('''${BUCKETS_JSON}''')
    for b in data:
        if b.get('name') == '${BUCKET_NAME}':
            print(b['identifier'])
            break
except Exception:
    pass
" 2>/dev/null || echo "")

if [[ -z "${BUCKET_ID}" ]]; then
    info "Bucket '${BUCKET_NAME}' not found. Creating..."

    BUCKET_RESPONSE=$(curl -sf -X POST \
        "${REGISTRY_URL}/nifi-registry-api/buckets" \
        -H "Content-Type: application/json" \
        -d "{\"name\": \"${BUCKET_NAME}\", \"description\": \"Oil & Gas flows for ${ENV} environment\"}" \
        2>/dev/null || echo "{}")

    BUCKET_ID=$(python3 -c "
import json
try:
    data = json.loads('''${BUCKET_RESPONSE}''')
    print(data.get('identifier', ''))
except Exception:
    pass
" 2>/dev/null || echo "")

    if [[ -n "${BUCKET_ID}" ]]; then
        success "Created bucket '${BUCKET_NAME}' (${BUCKET_ID})"
    else
        error "Failed to create bucket '${BUCKET_NAME}' in Registry."
    fi
else
    success "Found bucket '${BUCKET_NAME}' (${BUCKET_ID})"
fi

# ---------------------------------------------------------------------------
# Step 2: Extract flow name from file
# ---------------------------------------------------------------------------
FLOW_NAME=$(python3 -c "
import json
try:
    with open('${FLOW_FILE}') as f:
        data = json.load(f)
    # Try different possible structures
    name = data.get('flowContents', data).get('name', '')
    if not name:
        name = data.get('name', '')
    if not name:
        name = data.get('snapshotMetadata', {}).get('flowName', 'imported-flow')
    print(name)
except Exception:
    print('imported-flow')
" 2>/dev/null || echo "imported-flow")

info "Flow name: ${FLOW_NAME}"

# ---------------------------------------------------------------------------
# Step 3: Import flow
# ---------------------------------------------------------------------------
info "Importing flow to bucket '${BUCKET_NAME}'..."

if ${USE_TOOLKIT}; then
    # Use NiFi Toolkit CLI
    "${TOOLKIT_CLI}" registry import-flow-version \
        --baseUrl "${REGISTRY_URL}" \
        --bucketIdentifier "${BUCKET_ID}" \
        --flowName "${FLOW_NAME}" \
        --input "${FLOW_FILE}" \
        2>/dev/null

    if [[ $? -eq 0 ]]; then
        success "Flow '${FLOW_NAME}' imported via Toolkit CLI."
    else
        warn "Toolkit import failed. Trying REST API fallback..."
        USE_TOOLKIT=false
    fi
fi

if ! ${USE_TOOLKIT}; then
    # REST API fallback: create flow, then create first version
    # Step 3a: Create flow in bucket
    FLOW_RESPONSE=$(curl -sf -X POST \
        "${REGISTRY_URL}/nifi-registry-api/buckets/${BUCKET_ID}/flows" \
        -H "Content-Type: application/json" \
        -d "{\"name\": \"${FLOW_NAME}\", \"description\": \"Imported flow\", \"type\": \"Flow\"}" \
        2>/dev/null || echo "{}")

    FLOW_ID=$(python3 -c "
import json
try:
    data = json.loads('''${FLOW_RESPONSE}''')
    print(data.get('identifier', ''))
except Exception:
    pass
" 2>/dev/null || echo "")

    if [[ -z "${FLOW_ID}" ]]; then
        error "Failed to create flow '${FLOW_NAME}' in Registry."
    fi

    # Step 3b: Create first version with the flow content
    FLOW_CONTENT=$(python3 -c "
import json
with open('${FLOW_FILE}') as f:
    data = json.load(f)
# Extract just the flow contents if wrapped
content = data.get('flowContents', data)
# Wrap in version snapshot format
version = {
    'flowContents': content,
    'comments': 'Imported via import-flow.sh'
}
print(json.dumps(version))
" 2>/dev/null || echo "")

    if [[ -n "${FLOW_CONTENT}" ]]; then
        HTTP_CODE=$(curl -sf -X POST \
            "${REGISTRY_URL}/nifi-registry-api/buckets/${BUCKET_ID}/flows/${FLOW_ID}/versions" \
            -H "Content-Type: application/json" \
            -d "${FLOW_CONTENT}" \
            -w "%{http_code}" \
            -o /dev/null 2>/dev/null || echo "000")

        if [[ "${HTTP_CODE}" =~ ^2[0-9][0-9]$ ]]; then
            success "Flow '${FLOW_NAME}' imported via REST API."
        else
            warn "Import returned HTTP ${HTTP_CODE}. Check Registry logs."
        fi
    fi
fi

# ---------------------------------------------------------------------------
# Step 4: Verify
# ---------------------------------------------------------------------------
info "Verifying import..."

FLOWS_IN_BUCKET=$(curl -sf "${REGISTRY_URL}/nifi-registry-api/buckets/${BUCKET_ID}/flows" 2>/dev/null || echo "[]")

python3 -c "
import json
try:
    data = json.loads('''${FLOWS_IN_BUCKET}''')
    print(f'  Flows in bucket: {len(data)}')
    for f in data:
        name = f.get('name', 'unknown')
        versions = f.get('versionCount', 0)
        print(f'    - {name} (v{versions})')
except Exception:
    print('  Could not verify.')
" 2>/dev/null || echo "  Could not verify."

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
echo -e "${BOLD}========================================================${NC}"
echo -e "${BOLD}  Import Summary                                        ${NC}"
echo -e "${BOLD}========================================================${NC}"
echo ""
echo -e "  ${CYAN}Environment:${NC}  ${ENV}"
echo -e "  ${CYAN}Bucket:${NC}       ${BUCKET_NAME}"
echo -e "  ${CYAN}Flow:${NC}         ${FLOW_NAME}"
echo -e "  ${CYAN}Source file:${NC}  ${FLOW_FILE}"
echo ""
echo -e "${BOLD}Next steps:${NC}"
echo -e "  1. Deploy flow to NiFi: ${CYAN}./scripts/nifi-cli/deploy-flow.sh ${ENV}${NC}"
echo -e "  2. Verify in NiFi Registry UI: ${CYAN}http://localhost:${REGISTRY_PORT}/nifi-registry${NC}"
echo ""
success "Import complete."
