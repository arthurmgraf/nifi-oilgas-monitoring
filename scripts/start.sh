#!/usr/bin/env bash
# =============================================================================
# NiFi Oil & Gas Monitoring Platform - Start Stack
# =============================================================================
# Usage: ./scripts/start.sh [--profile dev|staging|prod]
# Starts all services, waits for health checks, prints status table.
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
echo -e "${BOLD}  Starting NiFi Oil & Gas Platform [${PROFILE}]          ${NC}"
echo -e "${BOLD}========================================================${NC}"
echo ""

# ---------------------------------------------------------------------------
# Validate environment file
# ---------------------------------------------------------------------------
if [[ ! -f "${ENV_FILE}" ]]; then
    error "Environment file not found: ${ENV_FILE}\nRun ./scripts/setup.sh first, then edit .env.${PROFILE}"
fi

# Check for unchanged passwords
if grep -q "CHANGE_ME" "${ENV_FILE}"; then
    warn "Found CHANGE_ME values in ${ENV_FILE}. Update passwords before production use!"
fi

# ---------------------------------------------------------------------------
# Source environment variables
# ---------------------------------------------------------------------------
info "Loading environment from .env.${PROFILE}..."
set -a
# shellcheck disable=SC1090
source "${ENV_FILE}"
set +a
success "Environment loaded."

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
# Start services
# ---------------------------------------------------------------------------
info "Starting services with profile '${PROFILE}'..."

${COMPOSE_CMD} \
    --env-file "${ENV_FILE}" \
    -f "${COMPOSE_FILE}" \
    --profile "${PROFILE}" \
    up -d

success "Containers started."

# ---------------------------------------------------------------------------
# Wait for services to be healthy
# ---------------------------------------------------------------------------
info "Waiting for services to become healthy..."

SERVICES=(
    "nifi"
    "kafka"
    "schema-registry"
    "postgres"
    "timescaledb"
    "minio"
    "grafana"
    "prometheus"
    "mosquitto"
)

MAX_WAIT=300  # 5 minutes
INTERVAL=10
ELAPSED=0

wait_for_healthy() {
    local service_name="$1"
    local container_id

    container_id=$(docker ps -q -f "name=${service_name}" 2>/dev/null | head -1)
    if [[ -z "${container_id}" ]]; then
        return 2  # not found
    fi

    local health
    health=$(docker inspect --format='{{.State.Health.Status}}' "${container_id}" 2>/dev/null || echo "none")

    case "${health}" in
        healthy)   return 0 ;;
        none)      return 0 ;;  # no healthcheck defined = assume OK
        *)         return 1 ;;  # starting, unhealthy
    esac
}

all_healthy=false
while [[ ${ELAPSED} -lt ${MAX_WAIT} ]]; do
    all_ready=true
    for svc in "${SERVICES[@]}"; do
        if ! wait_for_healthy "${svc}"; then
            all_ready=false
            break
        fi
    done

    if ${all_ready}; then
        all_healthy=true
        break
    fi

    echo -ne "\r  Waiting... ${ELAPSED}s / ${MAX_WAIT}s"
    sleep "${INTERVAL}"
    ELAPSED=$((ELAPSED + INTERVAL))
done

echo ""

if ${all_healthy}; then
    success "All services are healthy."
else
    warn "Some services may not be fully healthy after ${MAX_WAIT}s. Check with: ./scripts/health-check.sh"
fi

# ---------------------------------------------------------------------------
# Print status table with URLs
# ---------------------------------------------------------------------------
echo ""
echo -e "${BOLD}========================================================${NC}"
echo -e "${BOLD}  Service Status & URLs                                 ${NC}"
echo -e "${BOLD}========================================================${NC}"
echo ""

NIFI_PORT="${NIFI_WEB_PORT:-8443}"
REGISTRY_PORT="${NIFI_REGISTRY_PORT:-18080}"
GRAFANA_PORT_VAL="${GRAFANA_PORT:-3000}"
MINIO_CONSOLE="${MINIO_CONSOLE_PORT:-9001}"
PROM_PORT="${PROMETHEUS_PORT:-9090}"
SR_PORT="${SCHEMA_REGISTRY_PORT:-8081}"

printf "  %-22s %-10s %s\n" "Service" "Status" "URL"
printf "  %-22s %-10s %s\n" "----------------------" "----------" "--------------------------------------------"

print_status() {
    local name="$1"
    local url="$2"
    local container_name="$3"

    local container_id
    container_id=$(docker ps -q -f "name=${container_name}" 2>/dev/null | head -1)

    if [[ -n "${container_id}" ]]; then
        local status
        status=$(docker inspect --format='{{.State.Status}}' "${container_id}" 2>/dev/null || echo "unknown")
        if [[ "${status}" == "running" ]]; then
            printf "  %-22s ${GREEN}%-10s${NC} %s\n" "${name}" "running" "${url}"
        else
            printf "  %-22s ${RED}%-10s${NC} %s\n" "${name}" "${status}" "${url}"
        fi
    else
        printf "  %-22s ${RED}%-10s${NC} %s\n" "${name}" "not found" "${url}"
    fi
}

print_status "NiFi UI"            "https://localhost:${NIFI_PORT}/nifi"          "nifi"
print_status "NiFi Registry"      "http://localhost:${REGISTRY_PORT}/nifi-registry" "nifi-registry"
print_status "Grafana"            "http://localhost:${GRAFANA_PORT_VAL}"         "grafana"
print_status "MinIO Console"      "http://localhost:${MINIO_CONSOLE}"            "minio"
print_status "Prometheus"         "http://localhost:${PROM_PORT}"                "prometheus"
print_status "Schema Registry"    "http://localhost:${SR_PORT}"                  "schema-registry"
print_status "Kafka"              "localhost:${KAFKA_BROKER_PORT:-9092}"         "kafka"
print_status "PostgreSQL"         "localhost:${PG_REF_PORT:-5432}"              "postgres"
print_status "TimescaleDB"        "localhost:${TSDB_PORT:-5433}"                "timescaledb"
print_status "Mosquitto (MQTT)"   "localhost:${MQTT_PORT:-1883}"                "mosquitto"
print_status "Data Generator"     "(internal)"                                   "data-generator"

echo ""
echo -e "${BOLD}Credentials:${NC}"
echo -e "  NiFi:    ${CYAN}${NIFI_USERNAME:-admin}${NC} / (see .env.${PROFILE})"
echo -e "  Grafana: ${CYAN}${GRAFANA_ADMIN_USER:-admin}${NC} / (see .env.${PROFILE})"
echo -e "  MinIO:   ${CYAN}${MINIO_ROOT_USER:-minioadmin}${NC} / (see .env.${PROFILE})"
echo ""
