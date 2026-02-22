#!/usr/bin/env bash
# =============================================================================
# NiFi Oil & Gas Monitoring Platform - Create Kafka Topics (Host-Side)
# =============================================================================
# Usage: ./scripts/kafka/create-topics.sh [profile]
# Creates all required Kafka topics by running commands inside the Kafka container.
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

KAFKA_INTERNAL="${KAFKA_INTERNAL_PORT:-29092}"

echo -e "${BOLD}========================================================${NC}"
echo -e "${BOLD}  Kafka Topic Creation (Host-Side) [${PROFILE}]         ${NC}"
echo -e "${BOLD}========================================================${NC}"
echo ""

# ---------------------------------------------------------------------------
# Find Kafka container
# ---------------------------------------------------------------------------
KAFKA_CONTAINER=$(docker ps -q -f name=kafka 2>/dev/null | head -1)

if [[ -z "${KAFKA_CONTAINER}" ]]; then
    error "Kafka container not found. Is the stack running? Start with: ./scripts/start.sh"
fi

success "Found Kafka container: ${KAFKA_CONTAINER}"

# ---------------------------------------------------------------------------
# Topic definitions
# ---------------------------------------------------------------------------
# Format: topic_name:partitions:replication_factor:retention_ms:cleanup_policy
TOPICS=(
    "sensor.raw.readings:6:1:604800000:delete"
    "sensor.validated:6:1:604800000:delete"
    "sensor.enriched:6:1:604800000:delete"
    "sensor.anomalies:3:1:2592000000:delete"
    "equipment.status:3:1:2592000000:compact"
    "compliance.emissions:3:1:2592000000:delete"
    "alerts.critical:3:1:2592000000:delete"
    "alerts.warning:3:1:604800000:delete"
    "dlq.sensor.readings:3:1:2592000000:delete"
    "dlq.processing.errors:3:1:2592000000:delete"
)

# ---------------------------------------------------------------------------
# Create topics
# ---------------------------------------------------------------------------
CREATED=0
EXISTING=0
FAILED=0

for topic_def in "${TOPICS[@]}"; do
    IFS=':' read -r TOPIC PARTITIONS REPLICATION RETENTION CLEANUP <<< "${topic_def}"

    info "Creating topic: ${TOPIC} (partitions=${PARTITIONS}, retention=${RETENTION}ms)..."

    # Check if topic already exists
    if docker exec "${KAFKA_CONTAINER}" \
        kafka-topics --bootstrap-server "localhost:${KAFKA_INTERNAL}" \
        --describe --topic "${TOPIC}" &>/dev/null 2>&1; then
        warn "  Topic '${TOPIC}' already exists. Skipping."
        EXISTING=$((EXISTING + 1))
        continue
    fi

    # Create topic
    if docker exec "${KAFKA_CONTAINER}" \
        kafka-topics --bootstrap-server "localhost:${KAFKA_INTERNAL}" \
        --create \
        --topic "${TOPIC}" \
        --partitions "${PARTITIONS}" \
        --replication-factor "${REPLICATION}" \
        --config "retention.ms=${RETENTION}" \
        --config "cleanup.policy=${CLEANUP}" \
        2>/dev/null; then
        success "  Created: ${TOPIC}"
        CREATED=$((CREATED + 1))
    else
        warn "  Failed to create: ${TOPIC}"
        FAILED=$((FAILED + 1))
    fi
done

# ---------------------------------------------------------------------------
# List all topics
# ---------------------------------------------------------------------------
echo ""
info "Current topics in Kafka cluster:"
echo ""

docker exec "${KAFKA_CONTAINER}" \
    kafka-topics --bootstrap-server "localhost:${KAFKA_INTERNAL}" \
    --list 2>/dev/null | while read -r topic; do
    echo "    ${topic}"
done

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
echo -e "${BOLD}========================================================${NC}"
echo -e "${BOLD}  Topic Creation Summary                                ${NC}"
echo -e "${BOLD}========================================================${NC}"
echo ""
echo -e "  ${GREEN}Created:${NC}   ${CREATED}"
echo -e "  ${YELLOW}Existing:${NC}  ${EXISTING}"
if [[ ${FAILED} -gt 0 ]]; then
    echo -e "  ${RED}Failed:${NC}    ${FAILED}"
fi
echo ""

if [[ ${FAILED} -gt 0 ]]; then
    exit 1
fi

success "Topic creation complete."
