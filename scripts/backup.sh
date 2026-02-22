#!/usr/bin/env bash
# =============================================================================
# NiFi Oil & Gas Monitoring Platform - Backup Script
# =============================================================================
# Usage: ./scripts/backup.sh
# Creates a timestamped backup of NiFi flows, PostgreSQL, TimescaleDB, MinIO.
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
# Load environment
# ---------------------------------------------------------------------------
PROFILE="${1:-dev}"
ENV_FILE="${PROJECT_ROOT}/.env.${PROFILE}"

if [[ -f "${ENV_FILE}" ]]; then
    set -a
    # shellcheck disable=SC1090
    source "${ENV_FILE}"
    set +a
else
    warn "No .env.${PROFILE} found. Using environment variables."
fi

# ---------------------------------------------------------------------------
# Create timestamped backup directory
# ---------------------------------------------------------------------------
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
BACKUP_DIR="${PROJECT_ROOT}/backups/backup_${TIMESTAMP}"
mkdir -p "${BACKUP_DIR}"

echo -e "${BOLD}========================================================${NC}"
echo -e "${BOLD}  NiFi Oil & Gas Platform - Backup                      ${NC}"
echo -e "${BOLD}  Timestamp: ${TIMESTAMP}                               ${NC}"
echo -e "${BOLD}========================================================${NC}"
echo ""

ERRORS=0

# ---------------------------------------------------------------------------
# 1. Export NiFi flow definitions
# ---------------------------------------------------------------------------
info "Exporting NiFi flow definitions..."

NIFI_PORT="${NIFI_WEB_PORT:-8443}"
NIFI_USER="${NIFI_USERNAME:-admin}"
NIFI_PASS="${NIFI_PASSWORD:-}"

mkdir -p "${BACKUP_DIR}/nifi-flows"

# Get access token
NIFI_TOKEN=""
if [[ -n "${NIFI_PASS}" ]]; then
    NIFI_TOKEN=$(curl -ks -X POST \
        "https://localhost:${NIFI_PORT}/nifi-api/access/token" \
        -H "Content-Type: application/x-www-form-urlencoded" \
        -d "username=${NIFI_USER}&password=${NIFI_PASS}" \
        2>/dev/null || echo "")
fi

AUTH_HEADER=""
if [[ -n "${NIFI_TOKEN}" ]]; then
    AUTH_HEADER="Authorization: Bearer ${NIFI_TOKEN}"
fi

# Export root process group
if curl -ks ${AUTH_HEADER:+-H "$AUTH_HEADER"} \
    "https://localhost:${NIFI_PORT}/nifi-api/flow/process-groups/root" \
    -o "${BACKUP_DIR}/nifi-flows/root-process-group.json" 2>/dev/null; then
    success "NiFi root process group exported."
else
    warn "Failed to export NiFi flow. Is NiFi running?"
    ERRORS=$((ERRORS + 1))
fi

# Export all process groups (top-level children)
if [[ -f "${BACKUP_DIR}/nifi-flows/root-process-group.json" ]]; then
    # Extract child process group IDs and export each
    PG_IDS=$(python3 -c "
import json, sys
try:
    with open('${BACKUP_DIR}/nifi-flows/root-process-group.json') as f:
        data = json.load(f)
    for pg in data.get('processGroupFlow', {}).get('flow', {}).get('processGroups', []):
        print(pg.get('id', ''))
except Exception:
    pass
" 2>/dev/null || echo "")

    for pg_id in ${PG_IDS}; do
        if [[ -n "${pg_id}" ]]; then
            curl -ks ${AUTH_HEADER:+-H "$AUTH_HEADER"} \
                "https://localhost:${NIFI_PORT}/nifi-api/flow/process-groups/${pg_id}" \
                -o "${BACKUP_DIR}/nifi-flows/process-group-${pg_id}.json" 2>/dev/null || true
        fi
    done
    info "Exported child process groups."
fi

# ---------------------------------------------------------------------------
# 2. Backup PostgreSQL (refdata)
# ---------------------------------------------------------------------------
info "Backing up PostgreSQL (refdata)..."

PG_HOST_VAL="${PG_REF_HOST:-postgres}"
PG_PORT_VAL="${PG_REF_PORT:-5432}"
PG_DB_VAL="${PG_REF_DATABASE:-refdata}"
PG_USER_VAL="${PG_REF_USERNAME:-nifi}"
PG_PASS_VAL="${PG_REF_PASSWORD:-}"

mkdir -p "${BACKUP_DIR}/postgresql"

if docker exec -e PGPASSWORD="${PG_PASS_VAL}" \
    "$(docker ps -q -f name=postgres | head -1)" \
    pg_dump -U "${PG_USER_VAL}" -d "${PG_DB_VAL}" --no-owner --clean --if-exists \
    > "${BACKUP_DIR}/postgresql/refdata.sql" 2>/dev/null; then
    success "PostgreSQL refdata backup complete."
else
    warn "Failed to backup PostgreSQL. Is the container running?"
    ERRORS=$((ERRORS + 1))
fi

# ---------------------------------------------------------------------------
# 3. Backup TimescaleDB (sensordb)
# ---------------------------------------------------------------------------
info "Backing up TimescaleDB (sensordb)..."

TSDB_HOST_VAL="${TSDB_HOST:-timescaledb}"
TSDB_PORT_VAL="${TSDB_PORT:-5433}"
TSDB_DB_VAL="${TSDB_DATABASE:-sensordb}"
TSDB_USER_VAL="${TSDB_USERNAME:-nifi}"
TSDB_PASS_VAL="${TSDB_PASSWORD:-}"

mkdir -p "${BACKUP_DIR}/timescaledb"

if docker exec -e PGPASSWORD="${TSDB_PASS_VAL}" \
    "$(docker ps -q -f name=timescaledb | head -1)" \
    pg_dump -U "${TSDB_USER_VAL}" -d "${TSDB_DB_VAL}" --no-owner --clean --if-exists \
    > "${BACKUP_DIR}/timescaledb/sensordb.sql" 2>/dev/null; then
    success "TimescaleDB sensordb backup complete."
else
    warn "Failed to backup TimescaleDB. Is the container running?"
    ERRORS=$((ERRORS + 1))
fi

# ---------------------------------------------------------------------------
# 4. MinIO mirror
# ---------------------------------------------------------------------------
info "Backing up MinIO data lake..."

MINIO_USER_VAL="${MINIO_ROOT_USER:-minioadmin}"
MINIO_PASS_VAL="${MINIO_ROOT_PASSWORD:-}"
MINIO_PORT_VAL="${MINIO_PORT:-9000}"

mkdir -p "${BACKUP_DIR}/minio"

# Configure mc alias inside minio container
if docker exec "$(docker ps -q -f name=minio | head -1)" \
    mc alias set backup http://localhost:9000 "${MINIO_USER_VAL}" "${MINIO_PASS_VAL}" \
    &>/dev/null 2>&1; then

    # Mirror all buckets to a temp dir inside container, then copy out
    docker exec "$(docker ps -q -f name=minio | head -1)" \
        mc mirror --quiet backup/ /tmp/minio-backup/ 2>/dev/null || true

    docker cp "$(docker ps -q -f name=minio | head -1):/tmp/minio-backup/" \
        "${BACKUP_DIR}/minio/" 2>/dev/null || true

    # Cleanup temp dir in container
    docker exec "$(docker ps -q -f name=minio | head -1)" \
        rm -rf /tmp/minio-backup/ 2>/dev/null || true

    success "MinIO backup complete."
else
    # Fallback: use mc from host if available
    if command -v mc &>/dev/null; then
        mc alias set oilgas-backup "http://localhost:${MINIO_PORT_VAL}" \
            "${MINIO_USER_VAL}" "${MINIO_PASS_VAL}" --quiet 2>/dev/null || true
        mc mirror --quiet oilgas-backup/ "${BACKUP_DIR}/minio/" 2>/dev/null || true
        success "MinIO backup complete (host mc)."
    else
        warn "Failed to backup MinIO. mc client not available."
        ERRORS=$((ERRORS + 1))
    fi
fi

# ---------------------------------------------------------------------------
# 5. Compress backup
# ---------------------------------------------------------------------------
info "Compressing backup..."

ARCHIVE="${PROJECT_ROOT}/backups/backup_${TIMESTAMP}.tar.gz"
tar -czf "${ARCHIVE}" -C "${PROJECT_ROOT}/backups" "backup_${TIMESTAMP}"

# Remove uncompressed directory
rm -rf "${BACKUP_DIR}"

ARCHIVE_SIZE=$(du -h "${ARCHIVE}" | cut -f1)

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
echo -e "${BOLD}========================================================${NC}"
echo -e "${BOLD}  Backup Summary                                        ${NC}"
echo -e "${BOLD}========================================================${NC}"
echo ""
echo -e "  ${CYAN}Timestamp:${NC}  ${TIMESTAMP}"
echo -e "  ${CYAN}Location:${NC}   ${ARCHIVE}"
echo -e "  ${CYAN}Size:${NC}       ${ARCHIVE_SIZE}"
echo -e "  ${CYAN}Contents:${NC}   NiFi flows, PostgreSQL, TimescaleDB, MinIO"

if [[ ${ERRORS} -gt 0 ]]; then
    echo ""
    warn "${ERRORS} component(s) had backup issues. Review warnings above."
    echo ""
    exit 1
else
    echo ""
    success "Backup completed successfully."
    echo ""
fi
