# FraudWatch local development — run from the repository root.
# Prerequisites: Docker + Compose v2, Go, Git, Python 3.12 with venv support, make.
.DEFAULT_GOAL := help
SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c

COMPOSE := docker compose -f infra/compose.yaml
PYTHON3 ?= /usr/bin/python3
PY := .venv/bin/python
MLFLOW := .venv/bin/mlflow
GO_DIR := services/ingestion
TEST_DATABASE_URL ?= postgres://fraudwatch:fraudwatch_dev@127.0.0.1:5434/fraudwatch_test?sslmode=disable
DATA_REPO := https://github.com/Fraud-Detection-Handbook/simulated-data-raw.git

.PHONY: help check venv install data kafka-permissions up down status \
        wait-db init-test-db migrate wait-kafka topics go-deps \
        model-bootstrap train mlflow-ui smoke run-api run-scoring run-labels \
        test test-python test-go setup

help: ## Show available commands
	@awk 'BEGIN {FS=":.*## "} /^[a-zA-Z0-9_-]+:.*## / {printf "  %-20s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

check: ## Check tools required for first-time bootstrap
	@command -v docker >/dev/null
	@docker compose version >/dev/null
	@command -v go >/dev/null
	@command -v git >/dev/null
	@command -v "$(PYTHON3)" >/dev/null
	@echo 'Prerequisites: OK'

venv: ## Create .venv if needed
	@if [ ! -x "$(PY)" ]; then "$(PYTHON3)" -m venv .venv; fi

install: venv ## Install Python package and dev/MLOps/streaming dependencies
	@$(PY) -m pip install -e '.[dev,mlops,streaming]'

data: ## Clone trusted simulated fraud data only if missing
	@if [ ! -d data/handbook-raw/.git ]; then \
		mkdir -p data; git clone --depth 1 "$(DATA_REPO)" data/handbook-raw; \
	fi
	@test -f data/handbook-raw/data/2018-04-01.pkl || { echo 'Dataset missing daily pickle files' >&2; exit 1; }

# Kafka's official image runs as UID 1000. Docker may initialize this named volume
# as root-owned, preventing KRaft metadata creation. This is safe to rerun locally.
kafka-permissions: ## Ensure Kafka named volume is writable by UID 1000
	@$(COMPOSE) run --rm --no-deps --user 0:0 --entrypoint /bin/sh kafka \
		-c 'mkdir -p /tmp/kraft-combined-logs && chown -R 1000:1000 /tmp/kraft-combined-logs && chmod u+rwx /tmp/kraft-combined-logs'

up: kafka-permissions ## Start local Kafka and PostgreSQL (preserve volumes)
	@$(COMPOSE) up -d kafka postgres

down: ## Stop containers (preserve all volumes and MLflow artifacts)
	@$(COMPOSE) down

status: ## Display local infrastructure state
	@$(COMPOSE) ps -a

wait-db: up ## Wait for PostgreSQL to accept connections
	@for i in $$(seq 1 60); do \
		if $(COMPOSE) exec -T postgres pg_isready -U fraudwatch -d fraudwatch >/dev/null 2>&1; then \
			echo 'PostgreSQL: ready'; exit 0; \
		fi; sleep 2; \
	done; echo 'PostgreSQL readiness timed out' >&2; exit 1

init-test-db: wait-db ## Create separate integration-test database if absent
	@if ! $(COMPOSE) exec -T postgres \
		psql -U fraudwatch -d postgres -Atqc "SELECT 1 FROM pg_database WHERE datname='fraudwatch_test'" | grep -qx 1; then \
		$(COMPOSE) exec -T postgres \
			psql -v ON_ERROR_STOP=1 -U fraudwatch -d postgres -c 'CREATE DATABASE fraudwatch_test'; \
	fi

migrate: init-test-db ## Apply numbered SQL migrations to app and test databases
	@for db in fraudwatch fraudwatch_test; do \
		for file in infra/migrations/*.sql; do \
			[ -f "$$file" ] || { echo 'No SQL migrations found' >&2; exit 1; }; \
			echo "Applying $$file to $$db"; \
			$(COMPOSE) exec -T postgres \
				psql -v ON_ERROR_STOP=1 -U fraudwatch -d "$$db" < "$$file"; \
		done; \
	done

wait-kafka: up ## Wait for Kafka to respond to a metadata request
	@for i in $$(seq 1 60); do \
		if $(COMPOSE) exec -T kafka /opt/kafka/bin/kafka-topics.sh \
			--bootstrap-server localhost:19092 --list >/dev/null 2>&1; then \
			echo 'Kafka: ready'; exit 0; \
		fi; sleep 2; \
	done; $(COMPOSE) logs --tail=100 kafka; echo 'Kafka readiness timed out' >&2; exit 1

topics: wait-kafka ## Ensure both versioned Kafka topics exist
	@for topic in transactions.v1 fraud-labels.v1; do \
		$(COMPOSE) exec -T kafka /opt/kafka/bin/kafka-topics.sh \
			--bootstrap-server localhost:19092 \
			--create --if-not-exists --topic "$$topic" \
			--partitions 3 --replication-factor 1; \
	done

go-deps: ## Download Go ingestion-service dependencies
	@cd "$(GO_DIR)" && go mod download

model-bootstrap: install data ## Reuse candidate model, or train/register a fresh one
	@$(PY) scripts/bootstrap_model.py

train: install data ## Log a new training run (does not change candidate alias)
	@$(PY) scripts/train_mlflow.py

mlflow-ui: venv ## Start MLflow UI on http://127.0.0.1:5001
	@$(MLFLOW) ui --backend-store-uri "sqlite:///$(CURDIR)/mlflow.db" \
		--host 127.0.0.1 --port 5001

smoke: venv ## Verify registered model inference
	@$(PY) scripts/smoke_scoring.py

run-api: ## Run Go ingestion API (requires make setup)
	@cd "$(GO_DIR)" && go run ./cmd/ingestion

run-scoring: venv ## Run Kafka transaction scoring consumer (foreground)
	@$(PY) services/scoring/worker.py

run-labels: venv ## Run Kafka delayed fraud-label consumer (foreground)
	@$(PY) services/scoring/label_worker.py

test-python: venv migrate ## Test Python features, labels, and PostgreSQL parity
	@TEST_DATABASE_URL="$(TEST_DATABASE_URL)" $(PY) -m pytest tests/ -v

test-go: migrate ## Run Go tests, including PostgreSQL/kfake integration and race detection
	@cd "$(GO_DIR)" && TEST_DATABASE_URL="$(TEST_DATABASE_URL)" go test ./... -race -v -count=1

test: test-python test-go ## Run complete Python + Go test suites

setup: check install data up migrate topics go-deps model-bootstrap ## Bootstrap complete local development environment
	@echo 'Setup complete. Run make test, then start API/scoring/labels in separate terminals.'

-include infra/observability/Makefile.inc

-include infra/observability/Makefile.alerts.inc

-include infra/Makefile.app.inc
