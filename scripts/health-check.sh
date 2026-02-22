#!/usr/bin/env bash
# =============================================================================
# NiFi Oil & Gas Monitoring Platform - Health Check
# =============================================================================
# Usage: ./scripts/health-check.sh [profile]
# Checks the health of all platform services and prints a status table.
# Exit code: 0 = all healthy, 1 = one or more failed.
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

CHECK="${GREEN}[PASS]${NC}"
CROSS="${RED}[FAIL]${NC}"
SKIP="${YELLOW}[SKIP]${NC}"

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
fi

NIFI_PORT="${NIFI_WEB_PORT:-8443}"
REGISTRY_PORT="${NIFI_REGISTRY_PORT:-18080}"
KAFKA_PORT="${KAFKA_BROKER_PORT:-9092}"
SR_PORT="${SCHEMA_REGISTRY_PORT:-8081}"
PG_PORT="${PG_REF_PORT:-5432}"
TSDB_PORT_VAL="${TSDB_PORT:-5433}"
MINIO_PORT_VAL="${MINIO_PORT:-9000}"
GRAFANA_PORT_VAL="${GRAFANA_PORT:-3000}"
PROM_PORT="${PROMETHEUS_PORT:-9090}"
MQTT_PORT_VAL="${MQTT_PORT:-1883}"

echo -e "${BOLD}========================================================${NC}"
echo -e "${BOLD}  NiFi Oil & Gas Platform - Health Check [${PROFILE}]   ${NC}"
echo -e "${BOLD}========================================================${NC}"
echo ""

TOTAL=0
PASSED=0
FAILED=0

# ---------------------------------------------------------------------------
# Check function
# ---------------------------------------------------------------------------
check_service() {
    local name="$1"
    local check_cmd="$2"
    local timeout_sec="${3:-5}"

    TOTAL=$((TOTAL + 1))

    if timeout "${timeout_sec}" bash -c "${check_cmd}" &>/dev/null 2>&1; then
        printf "  %-25s %b\n" "${name}" "${CHECK}"
        PASSED=$((PASSED + 1))
    else
        printf "  %-25s %b\n" "${name}" "${CROSS}"
        FAILED=$((FAILED + 1))
    fi
}

printf "  %-25s %s\n" "Service" "Status"
printf "  %-25s %s\n" "-------------------------" "------"

# ---------------------------------------------------------------------------
# 1. NiFi (HTTPS)
# ---------------------------------------------------------------------------
check_service "NiFi" \
    "curl -kfs https://localhost:${NIFI_PORT}/nifi-api/system-diagnostics" \
    10

# ---------------------------------------------------------------------------
# 2. NiFi Registry
# ---------------------------------------------------------------------------
check_service "NiFi Registry" \
    "curl -fs http://localhost:${REGISTRY_PORT}/nifi-registry-api/buckets" \
    5

# ---------------------------------------------------------------------------
# 3. Kafka (broker API versions via docker exec)
# ---------------------------------------------------------------------------
KAFKA_CONTAINER=$(docker ps -q -f name=kafka 2>/dev/null | head -1 || echo "")
if [[ -n "${KAFKA_CONTAINER}" ]]; then
    check_service "Kafka" \
        "docker exec ${KAFKA_CONTAINER} kafka-broker-api-versions --bootstrap-server localhost:29092" \
        10
else
    TOTAL=$((TOTAL + 1))
    FAILED=$((FAILED + 1))
    printf "  %-25s %b\n" "Kafka" "${CROSS}"
fi

# ---------------------------------------------------------------------------
# 4. Schema Registry
# ---------------------------------------------------------------------------
check_service "Schema Registry" \
    "curl -fs http://localhost:${SR_PORT}/subjects" \
    5

# ---------------------------------------------------------------------------
# 5. PostgreSQL (pg_isready via docker exec)
# ---------------------------------------------------------------------------
PG_CONTAINER=$(docker ps -q -f name=postgres 2>/dev/null | head -1 || echo "")
if [[ -n "${PG_CONTAINER}" ]]; then
    check_service "PostgreSQL" \
        "docker exec ${PG_CONTAINER} pg_isready -U ${PG_REF_USERNAME:-nifi} -d ${PG_REF_DATABASE:-refdata}" \
        5
else
    TOTAL=$((TOTAL + 1))
    FAILED=$((FAILED + 1))
    printf "  %-25s %b\n" "PostgreSQL" "${CROSS}"
fi

# ---------------------------------------------------------------------------
# 6. TimescaleDB (pg_isready via docker exec)
# ---------------------------------------------------------------------------
TSDB_CONTAINER=$(docker ps -q -f name=timescaledb 2>/dev/null | head -1 || echo "")
if [[ -n "${TSDB_CONTAINER}" ]]; then
    check_service "TimescaleDB" \
        "docker exec ${TSDB_CONTAINER} pg_isready -U ${TSDB_USERNAME:-nifi} -d ${TSDB_DATABASE:-sensordb} -p 5432" \
        5
else
    TOTAL=$((TOTAL + 1))
    FAILED=$((FAILED + 1))
    printf "  %-25s %b\n" "TimescaleDB" "${CROSS}"
fi

# ---------------------------------------------------------------------------
# 7. MinIO
# ---------------------------------------------------------------------------
check_service "MinIO" \
    "curl -fs http://localhost:${MINIO_PORT_VAL}/minio/health/live" \
    5

# ---------------------------------------------------------------------------
# 8. Grafana
# ---------------------------------------------------------------------------
check_service "Grafana" \
    "curl -fs http://localhost:${GRAFANA_PORT_VAL}/api/health" \
    5

# ---------------------------------------------------------------------------
# 9. Prometheus
# ---------------------------------------------------------------------------
check_service "Prometheus" \
    "curl -fs http://localhost:${PROM_PORT}/-/healthy" \
    5

# ---------------------------------------------------------------------------
# 10. Mosquitto (MQTT) - try a quick sub with timeout
# ---------------------------------------------------------------------------
MQTT_CONTAINER=$(docker ps -q -f name=mosquitto 2>/dev/null | head -1 || echo "")
if [[ -n "${MQTT_CONTAINER}" ]]; then
    check_service "Mosquitto (MQTT)" \
        "docker exec ${MQTT_CONTAINER} mosquitto_sub -h localhost -p 1883 -t '\$SYS/broker/version' -C 1 -W 3" \
        8
else
    # Try from host if mosquitto_sub is available
    if command -v mosquitto_sub &>/dev/null; then
        check_service "Mosquitto (MQTT)" \
            "mosquitto_sub -h localhost -p ${MQTT_PORT_VAL} -t '\$SYS/broker/version' -C 1 -W 3" \
            8
    else
        TOTAL=$((TOTAL + 1))
        FAILED=$((FAILED + 1))
        printf "  %-25s %b\n" "Mosquitto (MQTT)" "${CROSS}"
    fi
fi

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
echo -e "${BOLD}========================================================${NC}"
printf "  ${BOLD}Results:${NC} %d/%d passed" "${PASSED}" "${TOTAL}"

if [[ ${FAILED} -gt 0 ]]; then
    printf ", ${RED}%d failed${NC}" "${FAILED}"
fi
echo ""
echo -e "${BOLD}========================================================${NC}"
echo ""

if [[ ${FAILED} -gt 0 ]]; then
    echo -e "${RED}Some services are unhealthy. Check Docker logs:${NC}"
    echo -e "  ${CYAN}docker compose logs <service-name>${NC}"
    echo ""
    exit 1
else
    echo -e "${GREEN}All services are healthy!${NC}"
    echo ""
    exit 0
fi
