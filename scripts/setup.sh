#!/usr/bin/env bash
# =============================================================================
# NiFi Oil & Gas Monitoring Platform - One-Command Setup
# =============================================================================
# Usage: ./scripts/setup.sh
# Checks prerequisites, creates env file, pulls/builds images, creates network.
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
NC='\033[0m' # No Color

info()    { echo -e "${CYAN}[INFO]${NC}  $*"; }
success() { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error()   { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo -e "${BOLD}========================================================${NC}"
echo -e "${BOLD}  NiFi Oil & Gas Monitoring Platform - Setup            ${NC}"
echo -e "${BOLD}========================================================${NC}"
echo ""

# ---------------------------------------------------------------------------
# 1. Check Docker
# ---------------------------------------------------------------------------
info "Checking prerequisites..."

if ! command -v docker &>/dev/null; then
    error "Docker is not installed. Please install Docker: https://docs.docker.com/get-docker/"
fi
success "Docker found: $(docker --version)"

# ---------------------------------------------------------------------------
# 2. Check Docker Compose (v2 plugin or standalone)
# ---------------------------------------------------------------------------
if docker compose version &>/dev/null; then
    COMPOSE_CMD="docker compose"
    success "Docker Compose (plugin) found: $(docker compose version --short)"
elif command -v docker-compose &>/dev/null; then
    COMPOSE_CMD="docker-compose"
    success "Docker Compose (standalone) found: $(docker-compose version --short)"
else
    error "Docker Compose is not installed. Please install Docker Compose v2: https://docs.docker.com/compose/install/"
fi

# ---------------------------------------------------------------------------
# 3. Copy .env.example to .env.dev if not exists
# ---------------------------------------------------------------------------
info "Checking environment configuration..."

ENV_EXAMPLE="${PROJECT_ROOT}/.env.example"
ENV_DEV="${PROJECT_ROOT}/.env.dev"

if [[ ! -f "${ENV_EXAMPLE}" ]]; then
    error ".env.example not found at ${ENV_EXAMPLE}. Repository may be incomplete."
fi

if [[ -f "${ENV_DEV}" ]]; then
    warn ".env.dev already exists. Skipping copy. Review and update if needed."
else
    cp "${ENV_EXAMPLE}" "${ENV_DEV}"
    success "Created .env.dev from .env.example"
    warn "IMPORTANT: Edit .env.dev and replace all CHANGE_ME values before starting!"
fi

# ---------------------------------------------------------------------------
# 4. Pull all Docker images
# ---------------------------------------------------------------------------
info "Pulling Docker images (this may take several minutes on first run)..."

IMAGES=(
    "apache/nifi:1.28.1"
    "apache/nifi-registry:2.0.0"
    "confluentinc/cp-kafka:7.7.1"
    "confluentinc/cp-schema-registry:7.7.1"
    "postgres:16-alpine"
    "timescale/timescaledb:latest-pg16"
    "minio/minio:latest"
    "minio/mc:latest"
    "grafana/grafana-oss:11.4.0"
    "prom/prometheus:v2.54.1"
    "eclipse-mosquitto:2.0"
    "python:3.11-slim"
)

for image in "${IMAGES[@]}"; do
    info "  Pulling ${image}..."
    docker pull "${image}" --quiet || warn "Failed to pull ${image}. Will attempt build anyway."
done
success "Docker images pulled."

# ---------------------------------------------------------------------------
# 5. Build custom images
# ---------------------------------------------------------------------------
info "Building custom images..."

info "  Building nifi-oilgas (custom NiFi 1.28.1)..."
docker build \
    -t nifi-oilgas:latest \
    -f "${PROJECT_ROOT}/docker/nifi/Dockerfile" \
    "${PROJECT_ROOT}/docker/nifi/" \
    --quiet \
    || error "Failed to build nifi-oilgas image."
success "Built nifi-oilgas:latest"

info "  Building data-generator..."
docker build \
    -t oilgas-data-generator:latest \
    -f "${PROJECT_ROOT}/data-generator/Dockerfile" \
    "${PROJECT_ROOT}/data-generator/" \
    --quiet \
    || error "Failed to build data-generator image."
success "Built oilgas-data-generator:latest"

# ---------------------------------------------------------------------------
# 6. Create Docker network if not exists
# ---------------------------------------------------------------------------
info "Ensuring Docker network exists..."

NETWORK_NAME="oilgas-monitoring"

if docker network inspect "${NETWORK_NAME}" &>/dev/null; then
    success "Network '${NETWORK_NAME}' already exists."
else
    docker network create "${NETWORK_NAME}" --driver bridge
    success "Created Docker network '${NETWORK_NAME}'."
fi

# ---------------------------------------------------------------------------
# 7. Create required directories
# ---------------------------------------------------------------------------
info "Creating required directories..."

mkdir -p "${PROJECT_ROOT}/data/nifi/state"
mkdir -p "${PROJECT_ROOT}/data/nifi/database"
mkdir -p "${PROJECT_ROOT}/data/nifi/flowfile"
mkdir -p "${PROJECT_ROOT}/data/nifi/content"
mkdir -p "${PROJECT_ROOT}/data/nifi/provenance"
mkdir -p "${PROJECT_ROOT}/data/kafka"
mkdir -p "${PROJECT_ROOT}/data/postgresql"
mkdir -p "${PROJECT_ROOT}/data/timescaledb"
mkdir -p "${PROJECT_ROOT}/data/minio"
mkdir -p "${PROJECT_ROOT}/data/grafana"
mkdir -p "${PROJECT_ROOT}/data/prometheus"
mkdir -p "${PROJECT_ROOT}/backups"
mkdir -p "${PROJECT_ROOT}/tools"

success "Directories created."

# ---------------------------------------------------------------------------
# 8. Print success message
# ---------------------------------------------------------------------------
echo ""
echo -e "${GREEN}========================================================${NC}"
echo -e "${GREEN}  Setup Complete!                                       ${NC}"
echo -e "${GREEN}========================================================${NC}"
echo ""
echo -e "${BOLD}Next steps:${NC}"
echo ""
echo -e "  1. ${YELLOW}Edit .env.dev${NC} and replace all ${RED}CHANGE_ME${NC} values"
echo -e "     ${CYAN}vi ${ENV_DEV}${NC}"
echo ""
echo -e "  2. ${YELLOW}Start the platform:${NC}"
echo -e "     ${CYAN}./scripts/start.sh${NC}"
echo ""
echo -e "  3. ${YELLOW}Check service health:${NC}"
echo -e "     ${CYAN}./scripts/health-check.sh${NC}"
echo ""
echo -e "  4. ${YELLOW}Access NiFi UI:${NC}"
echo -e "     ${CYAN}https://localhost:8443/nifi${NC}"
echo ""
