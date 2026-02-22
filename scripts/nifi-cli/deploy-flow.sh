#!/usr/bin/env bash
# =============================================================================
# NiFi Oil & Gas Monitoring Platform - Deploy Flow
# =============================================================================
# Usage: ./scripts/nifi-cli/deploy-flow.sh <environment>
# Deploys flows from NiFi Registry to the NiFi instance for the given env.
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
    echo "Usage: $0 <environment>"
    echo ""
    echo "Arguments:"
    echo "  environment    Target environment (dev, staging, prod)"
    echo ""
    echo "Examples:"
    echo "  $0 dev          # Deploy flows to dev NiFi instance"
    echo "  $0 staging      # Deploy flows to staging NiFi instance"
    exit 1
fi

ENV="$1"
ENV_FILE="${PROJECT_ROOT}/.env.${ENV}"

if [[ ! -f "${ENV_FILE}" ]]; then
    error "Environment file not found: ${ENV_FILE}"
fi

# ---------------------------------------------------------------------------
# Load environment
# ---------------------------------------------------------------------------
set -a
# shellcheck disable=SC1090
source "${ENV_FILE}"
set +a

NIFI_PORT="${NIFI_WEB_PORT:-8443}"
NIFI_USER="${NIFI_USERNAME:-admin}"
NIFI_PASS="${NIFI_PASSWORD:-}"
REGISTRY_PORT="${NIFI_REGISTRY_PORT:-18080}"

NIFI_URL="https://localhost:${NIFI_PORT}"
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
echo -e "${BOLD}  Deploy Flow to NiFi [${ENV}]                          ${NC}"
echo -e "${BOLD}========================================================${NC}"
echo ""

# ---------------------------------------------------------------------------
# Step 1: Get NiFi access token
# ---------------------------------------------------------------------------
info "Authenticating with NiFi..."

NIFI_TOKEN=""
if [[ -n "${NIFI_PASS}" ]]; then
    NIFI_TOKEN=$(curl -ks -X POST \
        "${NIFI_URL}/nifi-api/access/token" \
        -H "Content-Type: application/x-www-form-urlencoded" \
        -d "username=${NIFI_USER}&password=${NIFI_PASS}" \
        2>/dev/null || echo "")
fi

if [[ -n "${NIFI_TOKEN}" ]]; then
    AUTH_HEADER="Authorization: Bearer ${NIFI_TOKEN}"
    success "Authenticated with NiFi."
else
    AUTH_HEADER=""
    warn "Could not authenticate. Proceeding without token."
fi

# ---------------------------------------------------------------------------
# Step 2: Get root process group ID
# ---------------------------------------------------------------------------
info "Getting root process group..."

ROOT_PG_ID=$(curl -ks ${AUTH_HEADER:+-H "$AUTH_HEADER"} \
    "${NIFI_URL}/nifi-api/flow/process-groups/root" \
    2>/dev/null | python3 -c "
import json, sys
try:
    data = json.load(sys.stdin)
    print(data['processGroupFlow']['id'])
except Exception:
    print('')
" 2>/dev/null || echo "")

if [[ -z "${ROOT_PG_ID}" ]]; then
    error "Could not get root process group ID. Is NiFi running?"
fi

success "Root process group: ${ROOT_PG_ID}"

# ---------------------------------------------------------------------------
# Step 3: List available flows from Registry
# ---------------------------------------------------------------------------
info "Listing flows from Registry..."

BUCKET_NAME="oilgas-${ENV}"

BUCKETS=$(curl -sf "${REGISTRY_URL}/nifi-registry-api/buckets" 2>/dev/null || echo "[]")
BUCKET_ID=$(python3 -c "
import json
try:
    data = json.loads('${BUCKETS}')
    for b in data:
        if b.get('name') == '${BUCKET_NAME}':
            print(b['identifier'])
            break
except Exception:
    pass
" 2>/dev/null || echo "")

if [[ -z "${BUCKET_ID}" ]]; then
    warn "No bucket '${BUCKET_NAME}' found in Registry."
    info "Available buckets:"
    echo "${BUCKETS}" | python3 -c "
import json, sys
try:
    data = json.load(sys.stdin)
    for b in data:
        print(f\"  - {b.get('name')} ({b.get('identifier')})\")
except Exception:
    print('  (none)')
" 2>/dev/null || echo "  (none)"
    error "Create a bucket named '${BUCKET_NAME}' in NiFi Registry first."
fi

success "Found bucket: ${BUCKET_NAME} (${BUCKET_ID})"

# List flows
FLOWS=$(curl -sf "${REGISTRY_URL}/nifi-registry-api/buckets/${BUCKET_ID}/flows" 2>/dev/null || echo "[]")

FLOW_COUNT=$(python3 -c "
import json
try:
    data = json.loads('${FLOWS}')
    print(len(data))
except Exception:
    print(0)
" 2>/dev/null || echo "0")

if [[ "${FLOW_COUNT}" -eq 0 ]]; then
    warn "No flows found in bucket '${BUCKET_NAME}'."
    info "Import flows first: ./scripts/nifi-cli/import-flow.sh ${ENV} <flow.json>"
    exit 0
fi

info "Found ${FLOW_COUNT} flow(s) in bucket."

# ---------------------------------------------------------------------------
# Step 4: Deploy each flow as a process group
# ---------------------------------------------------------------------------
info "Deploying flows to NiFi..."

DEPLOYED=0

python3 -c "
import json
try:
    data = json.loads('${FLOWS}')
    for f in data:
        print(f\"{f.get('identifier')}|{f.get('name')}|{f.get('versionCount',1)}\")
except Exception:
    pass
" 2>/dev/null | while IFS='|' read -r flow_id flow_name version_count; do
    if [[ -z "${flow_id}" ]]; then
        continue
    fi

    info "  Deploying: ${flow_name} (version ${version_count})..."

    if ${USE_TOOLKIT}; then
        "${TOOLKIT_CLI}" nifi pg-import \
            --baseUrl "${NIFI_URL}" \
            --truststoreType "" \
            --registryClientUrl "${REGISTRY_URL}" \
            --bucketIdentifier "${BUCKET_ID}" \
            --flowIdentifier "${flow_id}" \
            --flowVersion "${version_count}" \
            --processGroupId "${ROOT_PG_ID}" \
            2>/dev/null && success "  Deployed: ${flow_name}" || warn "  Failed: ${flow_name}"
    else
        # REST API fallback: create a process group from Registry flow
        DEPLOY_PAYLOAD=$(python3 -c "
import json
payload = {
    'revision': {'version': 0},
    'component': {
        'position': {'x': 0, 'y': 0},
        'versionControlInformation': {
            'registryId': '',
            'bucketId': '${BUCKET_ID}',
            'flowId': '${flow_id}',
            'version': int('${version_count}')
        }
    }
}
print(json.dumps(payload))
" 2>/dev/null || echo "")

        if [[ -n "${DEPLOY_PAYLOAD}" ]]; then
            curl -ks ${AUTH_HEADER:+-H "$AUTH_HEADER"} \
                -X POST "${NIFI_URL}/nifi-api/process-groups/${ROOT_PG_ID}/process-groups" \
                -H "Content-Type: application/json" \
                -d "${DEPLOY_PAYLOAD}" \
                -o /dev/null 2>/dev/null && success "  Deployed: ${flow_name}" || warn "  Failed: ${flow_name}"
        fi
    fi
done

# ---------------------------------------------------------------------------
# Step 5: Verify process groups are running
# ---------------------------------------------------------------------------
info "Verifying process groups..."

sleep 5  # Give NiFi a moment to instantiate

PG_STATUS=$(curl -ks ${AUTH_HEADER:+-H "$AUTH_HEADER"} \
    "${NIFI_URL}/nifi-api/flow/process-groups/${ROOT_PG_ID}" \
    2>/dev/null || echo "{}")

python3 -c "
import json
try:
    data = json.loads('''${PG_STATUS}''')
    pgs = data.get('processGroupFlow', {}).get('flow', {}).get('processGroups', [])
    for pg in pgs:
        name = pg.get('component', {}).get('name', 'unknown')
        state = pg.get('status', {}).get('aggregateSnapshot', {}).get('runStatus', 'unknown')
        print(f'  {name}: {state}')
except Exception:
    print('  Could not verify process groups.')
" 2>/dev/null || echo "  Could not verify process groups."

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
success "Flow deployment to ${ENV} complete."
echo ""
echo -e "${BOLD}Next steps:${NC}"
echo -e "  1. Open NiFi UI: ${CYAN}${NIFI_URL}/nifi${NC}"
echo -e "  2. Verify all process groups are configured correctly"
echo -e "  3. Start process groups if not auto-started"
echo -e "  4. Run health check: ${CYAN}./scripts/health-check.sh${NC}"
echo ""
