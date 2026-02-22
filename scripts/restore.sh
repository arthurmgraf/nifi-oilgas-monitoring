#!/usr/bin/env bash
# =============================================================================
# NiFi Oil & Gas Monitoring Platform - Restore Script
# =============================================================================
# Usage: ./scripts/restore.sh <backup_archive>
# Restores NiFi flows, PostgreSQL, TimescaleDB, and MinIO from a backup archive.
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
if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <backup_archive.tar.gz> [profile]"
    echo ""
    echo "Arguments:"
    echo "  backup_archive    Path to backup tar.gz file (from ./scripts/backup.sh)"
    echo "  profile           Environment profile (default: dev)"
    echo ""
    echo "Example:"
    echo "  $0 ./backups/backup_20260221_143000.tar.gz dev"
    exit 1
fi

BACKUP_ARCHIVE="$1"
PROFILE="${2:-dev}"
ENV_FILE="${PROJECT_ROOT}/.env.${PROFILE}"

if [[ ! -f "${BACKUP_ARCHIVE}" ]]; then
    error "Backup archive not found: ${BACKUP_ARCHIVE}"
fi

# ---------------------------------------------------------------------------
# Load environment
# ---------------------------------------------------------------------------
if [[ -f "${ENV_FILE}" ]]; then
    set -a
    # shellcheck disable=SC1090
    source "${ENV_FILE}"
    set +a
else
    warn "No .env.${PROFILE} found. Using environment variables."
fi

echo -e "${BOLD}========================================================${NC}"
echo -e "${BOLD}  NiFi Oil & Gas Platform - Restore                     ${NC}"
echo -e "${BOLD}  Archive: $(basename "${BACKUP_ARCHIVE}")              ${NC}"
echo -e "${BOLD}========================================================${NC}"
echo ""

# ---------------------------------------------------------------------------
# Confirmation prompt
# ---------------------------------------------------------------------------
echo -e "${RED}WARNING: This will overwrite current data in PostgreSQL, TimescaleDB, and MinIO.${NC}"
echo ""
read -rp "Are you sure you want to continue? (yes/no): " CONFIRM

if [[ "${CONFIRM}" != "yes" ]]; then
    info "Restore cancelled."
    exit 0
fi

# ---------------------------------------------------------------------------
# Extract backup
# ---------------------------------------------------------------------------
info "Extracting backup archive..."

TEMP_DIR=$(mktemp -d)
tar -xzf "${BACKUP_ARCHIVE}" -C "${TEMP_DIR}"

# Find the backup directory inside (first subdirectory)
BACKUP_DIR=$(find "${TEMP_DIR}" -maxdepth 1 -type d -name "backup_*" | head -1)

if [[ -z "${BACKUP_DIR}" ]]; then
    # Maybe files are directly in temp_dir
    BACKUP_DIR="${TEMP_DIR}"
fi

success "Backup extracted to temporary directory."

ERRORS=0
RESTORED=0

# ---------------------------------------------------------------------------
# 1. Restore PostgreSQL (refdata)
# ---------------------------------------------------------------------------
if [[ -f "${BACKUP_DIR}/postgresql/refdata.sql" ]]; then
    info "Restoring PostgreSQL (refdata)..."

    PG_USER_VAL="${PG_REF_USERNAME:-nifi}"
    PG_PASS_VAL="${PG_REF_PASSWORD:-}"
    PG_DB_VAL="${PG_REF_DATABASE:-refdata}"

    PG_CONTAINER=$(docker ps -q -f name=postgres | head -1)

    if [[ -n "${PG_CONTAINER}" ]]; then
        docker cp "${BACKUP_DIR}/postgresql/refdata.sql" "${PG_CONTAINER}:/tmp/refdata_restore.sql"

        if docker exec -e PGPASSWORD="${PG_PASS_VAL}" "${PG_CONTAINER}" \
            psql -U "${PG_USER_VAL}" -d "${PG_DB_VAL}" -f /tmp/refdata_restore.sql \
            &>/dev/null; then
            success "PostgreSQL refdata restored."
            RESTORED=$((RESTORED + 1))
        else
            warn "PostgreSQL restore completed with warnings. Check data manually."
            RESTORED=$((RESTORED + 1))
        fi

        docker exec "${PG_CONTAINER}" rm -f /tmp/refdata_restore.sql 2>/dev/null || true
    else
        warn "PostgreSQL container not running. Skipping restore."
        ERRORS=$((ERRORS + 1))
    fi
else
    warn "No PostgreSQL backup found in archive. Skipping."
fi

# ---------------------------------------------------------------------------
# 2. Restore TimescaleDB (sensordb)
# ---------------------------------------------------------------------------
if [[ -f "${BACKUP_DIR}/timescaledb/sensordb.sql" ]]; then
    info "Restoring TimescaleDB (sensordb)..."

    TSDB_USER_VAL="${TSDB_USERNAME:-nifi}"
    TSDB_PASS_VAL="${TSDB_PASSWORD:-}"
    TSDB_DB_VAL="${TSDB_DATABASE:-sensordb}"

    TSDB_CONTAINER=$(docker ps -q -f name=timescaledb | head -1)

    if [[ -n "${TSDB_CONTAINER}" ]]; then
        docker cp "${BACKUP_DIR}/timescaledb/sensordb.sql" "${TSDB_CONTAINER}:/tmp/sensordb_restore.sql"

        if docker exec -e PGPASSWORD="${TSDB_PASS_VAL}" "${TSDB_CONTAINER}" \
            psql -U "${TSDB_USER_VAL}" -d "${TSDB_DB_VAL}" -f /tmp/sensordb_restore.sql \
            &>/dev/null; then
            success "TimescaleDB sensordb restored."
            RESTORED=$((RESTORED + 1))
        else
            warn "TimescaleDB restore completed with warnings. Check data manually."
            RESTORED=$((RESTORED + 1))
        fi

        docker exec "${TSDB_CONTAINER}" rm -f /tmp/sensordb_restore.sql 2>/dev/null || true
    else
        warn "TimescaleDB container not running. Skipping restore."
        ERRORS=$((ERRORS + 1))
    fi
else
    warn "No TimescaleDB backup found in archive. Skipping."
fi

# ---------------------------------------------------------------------------
# 3. Restore MinIO
# ---------------------------------------------------------------------------
if [[ -d "${BACKUP_DIR}/minio" ]] && [[ "$(ls -A "${BACKUP_DIR}/minio" 2>/dev/null)" ]]; then
    info "Restoring MinIO data lake..."

    MINIO_USER_VAL="${MINIO_ROOT_USER:-minioadmin}"
    MINIO_PASS_VAL="${MINIO_ROOT_PASSWORD:-}"
    MINIO_PORT_VAL="${MINIO_PORT:-9000}"

    MINIO_CONTAINER=$(docker ps -q -f name=minio | head -1)

    if [[ -n "${MINIO_CONTAINER}" ]]; then
        # Copy backup data into container
        docker cp "${BACKUP_DIR}/minio/." "${MINIO_CONTAINER}:/tmp/minio-restore/"

        # Configure mc and mirror back
        docker exec "${MINIO_CONTAINER}" \
            mc alias set restore http://localhost:9000 "${MINIO_USER_VAL}" "${MINIO_PASS_VAL}" \
            &>/dev/null 2>&1 || true

        docker exec "${MINIO_CONTAINER}" \
            mc mirror --overwrite --quiet /tmp/minio-restore/ restore/ \
            2>/dev/null || true

        docker exec "${MINIO_CONTAINER}" rm -rf /tmp/minio-restore/ 2>/dev/null || true

        success "MinIO data lake restored."
        RESTORED=$((RESTORED + 1))
    else
        # Fallback to host mc
        if command -v mc &>/dev/null; then
            mc alias set oilgas-restore "http://localhost:${MINIO_PORT_VAL}" \
                "${MINIO_USER_VAL}" "${MINIO_PASS_VAL}" --quiet 2>/dev/null || true
            mc mirror --overwrite --quiet "${BACKUP_DIR}/minio/" oilgas-restore/ 2>/dev/null || true
            success "MinIO data lake restored (host mc)."
            RESTORED=$((RESTORED + 1))
        else
            warn "Cannot restore MinIO. Neither container mc nor host mc available."
            ERRORS=$((ERRORS + 1))
        fi
    fi
else
    warn "No MinIO backup found in archive. Skipping."
fi

# ---------------------------------------------------------------------------
# 4. Import NiFi flow
# ---------------------------------------------------------------------------
if [[ -f "${BACKUP_DIR}/nifi-flows/root-process-group.json" ]]; then
    info "NiFi flow backup found."
    info "NiFi flows are exported as read-only snapshots."
    info "To import, use NiFi Registry or manually upload via NiFi UI."
    info "  Flow file: ${BACKUP_DIR}/nifi-flows/root-process-group.json"

    # Copy flow files to a permanent location
    FLOW_RESTORE_DIR="${PROJECT_ROOT}/nifi-flows/restored_${PROFILE}"
    mkdir -p "${FLOW_RESTORE_DIR}"
    cp "${BACKUP_DIR}/nifi-flows/"*.json "${FLOW_RESTORE_DIR}/" 2>/dev/null || true

    success "NiFi flow definitions saved to: ${FLOW_RESTORE_DIR}/"
    RESTORED=$((RESTORED + 1))
else
    warn "No NiFi flow backup found in archive. Skipping."
fi

# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------
rm -rf "${TEMP_DIR}"

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
echo -e "${BOLD}========================================================${NC}"
echo -e "${BOLD}  Restore Summary                                       ${NC}"
echo -e "${BOLD}========================================================${NC}"
echo ""
echo -e "  ${CYAN}Archive:${NC}     $(basename "${BACKUP_ARCHIVE}")"
echo -e "  ${CYAN}Profile:${NC}     ${PROFILE}"
echo -e "  ${CYAN}Restored:${NC}    ${RESTORED} component(s)"

if [[ ${ERRORS} -gt 0 ]]; then
    echo -e "  ${RED}Errors:${NC}      ${ERRORS} component(s) failed"
    echo ""
    warn "Review warnings above and retry failed components manually."
    exit 1
else
    echo ""
    success "Restore completed successfully."
fi
echo ""
