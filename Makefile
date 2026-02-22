.PHONY: setup start stop restart test lint health backup restore promote clean logs status

# Configuration
PROFILE ?= dev
ENV_FILE = .env.$(PROFILE)
COMPOSE = docker compose --profile $(PROFILE) --env-file $(ENV_FILE)

# Colors
GREEN  := \033[0;32m
YELLOW := \033[0;33m
RED    := \033[0;31m
NC     := \033[0m

# ============================================================================
# Setup & Lifecycle
# ============================================================================

setup: ## One-time setup: copy env, pull images, build custom images
	@echo "$(GREEN)Setting up Oil & Gas NiFi Monitoring Platform...$(NC)"
	@test -f .env.dev || cp .env.example .env.dev
	@echo "$(YELLOW)Pulling Docker images...$(NC)"
	@$(COMPOSE) pull --ignore-buildable
	@echo "$(YELLOW)Building custom images...$(NC)"
	@$(COMPOSE) build
	@echo "$(GREEN)Setup complete!$(NC)"
	@echo "Run 'make start' to launch the platform."

start: ## Start all services (PROFILE=dev|staging|prod)
	@echo "$(GREEN)Starting platform with profile: $(PROFILE)$(NC)"
	@$(COMPOSE) up -d
	@echo "$(YELLOW)Waiting for services to be healthy...$(NC)"
	@bash scripts/health-check.sh || true
	@echo ""
	@echo "$(GREEN)==========================================$(NC)"
	@echo "$(GREEN) Platform is ready!$(NC)"
	@echo "$(GREEN)==========================================$(NC)"
	@echo " NiFi UI:        https://localhost:8443/nifi/"
	@echo " NiFi Registry:  http://localhost:18080/nifi-registry/"
	@echo " Grafana:        http://localhost:3000"
	@echo " MinIO Console:  http://localhost:9001"
	@echo " Prometheus:     http://localhost:9090"
	@echo " Schema Registry: http://localhost:8081"
	@echo "$(GREEN)==========================================$(NC)"

stop: ## Stop all services
	@echo "$(YELLOW)Stopping platform...$(NC)"
	@$(COMPOSE) down
	@echo "$(GREEN)Platform stopped.$(NC)"

restart: stop start ## Restart all services

# ============================================================================
# Development
# ============================================================================

generate: ## Start data generator (PROFILE=dev)
	@echo "$(GREEN)Starting data generator...$(NC)"
	@$(COMPOSE) run --rm data-generator

test: ## Run unit tests
	@echo "$(GREEN)Running unit tests...$(NC)"
	@cd data-generator && pip install -r requirements.txt -q 2>/dev/null; cd ..
	@python -m pytest tests/unit/ -v --tb=short

test-integration: ## Run integration tests (requires running stack)
	@echo "$(GREEN)Running integration tests...$(NC)"
	@python -m pytest tests/integration/ -v --tb=short -m integration

test-e2e: ## Run end-to-end tests (requires running stack + data generator)
	@echo "$(GREEN)Running E2E tests...$(NC)"
	@python -m pytest tests/integration/ -v --tb=short -m e2e

test-all: test test-integration test-e2e ## Run all tests

lint: ## Run linters
	@echo "$(GREEN)Running linters...$(NC)"
	@ruff check data-generator/ tests/ nifi-flows/
	@ruff format --check data-generator/ tests/
	@echo "$(GREEN)Lint passed!$(NC)"

lint-fix: ## Fix lint issues automatically
	@ruff check --fix data-generator/ tests/ nifi-flows/
	@ruff format data-generator/ tests/

# ============================================================================
# Operations
# ============================================================================

health: ## Check health of all services
	@bash scripts/health-check.sh

status: ## Show status of all containers
	@$(COMPOSE) ps

logs: ## Tail logs from all services (ctrl+c to stop)
	@$(COMPOSE) logs -f --tail=50

logs-nifi: ## Tail NiFi logs only
	@$(COMPOSE) logs -f --tail=100 nifi

logs-kafka: ## Tail Kafka logs only
	@$(COMPOSE) logs -f --tail=100 kafka

backup: ## Create backup of flows, databases, and object storage
	@bash scripts/backup.sh $(PROFILE)

restore: ## Restore from backup (BACKUP_DIR required)
	@test -n "$(BACKUP_DIR)" || (echo "$(RED)Usage: make restore BACKUP_DIR=./backups/2026-02-21_120000$(NC)" && exit 1)
	@bash scripts/restore.sh $(PROFILE) $(BACKUP_DIR)

promote: ## Promote flows between environments (FROM, TO required)
	@test -n "$(FROM)" -a -n "$(TO)" || (echo "$(RED)Usage: make promote FROM=dev TO=staging$(NC)" && exit 1)
	@bash scripts/promote.sh $(FROM) $(TO)

# ============================================================================
# NiFi CLI
# ============================================================================

nifi-cli-install: ## Install NiFi Toolkit CLI
	@bash scripts/nifi-cli/install-toolkit.sh

nifi-deploy: ## Deploy flow to NiFi (ENV required)
	@test -n "$(ENV)" || (echo "$(RED)Usage: make nifi-deploy ENV=dev$(NC)" && exit 1)
	@bash scripts/nifi-cli/deploy-flow.sh $(ENV)

nifi-export: ## Export flow from NiFi Registry
	@test -n "$(ENV)" || (echo "$(RED)Usage: make nifi-export ENV=dev$(NC)" && exit 1)
	@bash scripts/nifi-cli/export-flow.sh $(ENV)

# ============================================================================
# Kafka & Schema Registry
# ============================================================================

kafka-topics: ## Create Kafka topics
	@bash scripts/kafka/create-topics.sh

kafka-schemas: ## Register Avro schemas with Schema Registry
	@bash scripts/kafka/register-schemas.sh

# ============================================================================
# Cleanup
# ============================================================================

clean: ## Remove all containers and volumes (DESTRUCTIVE)
	@echo "$(RED)WARNING: This will delete all data!$(NC)"
	@read -p "Are you sure? [y/N] " confirm && [ "$$confirm" = "y" ] || exit 1
	@$(COMPOSE) down -v --remove-orphans
	@docker network rm oilgas-net 2>/dev/null || true
	@echo "$(GREEN)All containers and volumes removed.$(NC)"

clean-data: ## Remove data volumes only (keeps images)
	@$(COMPOSE) down -v
	@echo "$(GREEN)Data volumes removed.$(NC)"

# ============================================================================
# Help
# ============================================================================

help: ## Show this help message
	@echo "$(GREEN)NiFi Oil & Gas Monitoring Platform$(NC)"
	@echo "===================================="
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  $(YELLOW)%-20s$(NC) %s\n", $$1, $$2}'

.DEFAULT_GOAL := help
