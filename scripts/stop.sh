#!/usr/bin/env bash
# =============================================================================
# NiFi Oil & Gas Monitoring Platform - Graceful Stop
# =============================================================================
# Usage: ./scripts/stop.sh [--profile dev|staging|prod]
# Gracefully stops all services.
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
# Parse arguments
# ---------------------------------------------------------------------------
PROFILE="dev"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --profile|-p)
            PROFILE="${2:-dev}"
            shift 2
            ;;
        *)
            warn "Unknown argument: $1"
            shift
            ;;
    esac
done

ENV_FILE="${PROJECT_ROOT}/.env.${PROFILE}"

echo -e "${BOLD}========================================================${NC}"
echo -e "${BOLD}  Stopping NiFi Oil & Gas Platform [${PROFILE}]         ${NC}"
echo -e "${BOLD}========================================================${NC}"
echo ""

# ---------------------------------------------------------------------------
# Determine compose command
# ---------------------------------------------------------------------------
if docker compose version &>/dev/null; then
    COMPOSE_CMD="docker compose"
else
    COMPOSE_CMD="docker-compose"
fi

COMPOSE_FILE="${PROJECT_ROOT}/docker-compose.yml"

if [[ ! -f "${COMPOSE_FILE}" ]]; then
    error "docker-compose.yml not found at ${COMPOSE_FILE}"
fi

# ---------------------------------------------------------------------------
# Stop services
# ---------------------------------------------------------------------------
info "Stopping services with profile '${PROFILE}'..."

COMPOSE_ARGS=(-f "${COMPOSE_FILE}" --profile "${PROFILE}" down)

if [[ -f "${ENV_FILE}" ]]; then
    COMPOSE_ARGS=(--env-file "${ENV_FILE}" "${COMPOSE_ARGS[@]}")
fi

${COMPOSE_CMD} "${COMPOSE_ARGS[@]}"

echo ""
success "All services stopped for profile '${PROFILE}'."
echo ""
echo -e "  ${CYAN}To restart:${NC}  ./scripts/start.sh --profile ${PROFILE}"
echo -e "  ${CYAN}To cleanup:${NC}  docker volume prune (removes unused volumes)"
echo ""
