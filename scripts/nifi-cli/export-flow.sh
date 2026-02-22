#!/usr/bin/env bash
# =============================================================================
# NiFi Oil & Gas Monitoring Platform - Export Flows from NiFi Registry
# =============================================================================
# Usage: ./scripts/nifi-cli/export-flow.sh <environment> [output_dir]
# Exports all flows from the given environment's Registry bucket as JSON files.
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
if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <environment> [output_dir]"
    echo ""
    echo "Arguments:"
    echo "  environment    Environment to export from (dev, staging, prod)"
    echo "  output_dir     Output directory (default: ./nifi-flows/export_<env>_<timestamp>)"
    echo ""
    echo "Examples:"
    echo "  $0 dev"
    echo "  $0 staging ./exports/staging-flows"
    exit 1
fi

ENV="$1"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
OUTPUT_DIR="${2:-${PROJECT_ROOT}/nifi-flows/export_${ENV}_${TIMESTAMP}}"

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
echo -e "${BOLD}  Export Flows from NiFi Registry [${ENV}]              ${NC}"
echo -e "${BOLD}========================================================${NC}"
echo ""

mkdir -p "${OUTPUT_DIR}"

# ---------------------------------------------------------------------------
# List buckets
# ---------------------------------------------------------------------------
info "Listing buckets from Registry at ${REGISTRY_URL}..."

BUCKETS_JSON=$(curl -sf "${REGISTRY_URL}/nifi-registry-api/buckets" 2>/dev/null || echo "[]")

BUCKET_COUNT=$(python3 -c "
import json
try:
    data = json.loads('''${BUCKETS_JSON}''')
    print(len(data))
except Exception:
    print(0)
" 2>/dev/null || echo "0")

if [[ "${BUCKET_COUNT}" -eq 0 ]]; then
    warn "No buckets found in Registry. Is NiFi Registry running?"
    exit 1
fi

info "Found ${BUCKET_COUNT} bucket(s)."

# Save buckets metadata
echo "${BUCKETS_JSON}" | python3 -m json.tool > "${OUTPUT_DIR}/buckets.json" 2>/dev/null || true

# ---------------------------------------------------------------------------
# Export flows from each bucket
# ---------------------------------------------------------------------------
EXPORTED=0

python3 -c "
import json
try:
    data = json.loads('''${BUCKETS_JSON}''')
    for b in data:
        print(f\"{b.get('identifier')}|{b.get('name')}\")
except Exception:
    pass
" 2>/dev/null | while IFS='|' read -r bucket_id bucket_name; do
    if [[ -z "${bucket_id}" ]]; then
        continue
    fi

    info "Processing bucket: ${bucket_name} (${bucket_id})..."

    # List flows in bucket
    FLOWS_JSON=$(curl -sf "${REGISTRY_URL}/nifi-registry-api/buckets/${bucket_id}/flows" 2>/dev/null || echo "[]")

    echo "${FLOWS_JSON}" | python3 -m json.tool > "${OUTPUT_DIR}/flows_${bucket_id}.json" 2>/dev/null || true

    python3 -c "
import json
try:
    data = json.loads('''${FLOWS_JSON}''')
    for f in data:
        print(f\"{f.get('identifier')}|{f.get('name')}|{f.get('versionCount',1)}\")
except Exception:
    pass
" 2>/dev/null | while IFS='|' read -r flow_id flow_name version_count; do
        if [[ -z "${flow_id}" ]]; then
            continue
        fi

        # Sanitize flow name for filename
        SAFE_NAME=$(echo "${flow_name}" | tr ' /' '_' | tr -cd '[:alnum:]_-')
        FLOW_FILE="${OUTPUT_DIR}/flow_${SAFE_NAME}_${flow_id}.json"

        info "  Exporting: ${flow_name} (v${version_count})..."

        if ${USE_TOOLKIT}; then
            "${TOOLKIT_CLI}" registry export-flow-version \
                --baseUrl "${REGISTRY_URL}" \
                --flowIdentifier "${flow_id}" \
                --flowVersion "${version_count}" \
                --outputFile "${FLOW_FILE}" \
                2>/dev/null && success "  Exported: ${flow_name}" || warn "  Failed: ${flow_name}"
        else
            # REST API fallback
            if curl -sf \
                "${REGISTRY_URL}/nifi-registry-api/buckets/${bucket_id}/flows/${flow_id}/versions/${version_count}" \
                -o "${FLOW_FILE}" 2>/dev/null; then
                # Pretty-print the JSON
                python3 -c "
import json
with open('${FLOW_FILE}') as f:
    data = json.load(f)
with open('${FLOW_FILE}', 'w') as f:
    json.dump(data, f, indent=2)
" 2>/dev/null || true
                success "  Exported: ${flow_name}"
            else
                warn "  Failed to export: ${flow_name}"
            fi
        fi
    done
done

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
TOTAL_FILES=$(find "${OUTPUT_DIR}" -name "flow_*.json" -type f 2>/dev/null | wc -l | tr -d ' ')

echo ""
echo -e "${BOLD}========================================================${NC}"
echo -e "${BOLD}  Export Summary                                        ${NC}"
echo -e "${BOLD}========================================================${NC}"
echo ""
echo -e "  ${CYAN}Environment:${NC}  ${ENV}"
echo -e "  ${CYAN}Output dir:${NC}   ${OUTPUT_DIR}"
echo -e "  ${CYAN}Flows exported:${NC} ${TOTAL_FILES}"
echo ""

if [[ -d "${OUTPUT_DIR}" ]]; then
    info "Exported files:"
    ls -la "${OUTPUT_DIR}/"*.json 2>/dev/null | while read -r line; do
        echo "    ${line}"
    done
fi

echo ""
success "Export complete."
