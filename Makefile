# FraudWatch local development — run from the repository root.
# Prerequisites: Docker + Compose v2, Go, Git, Python 3.12 with venv support, make.
.DEFAULT_GOAL := help
SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c

# Each target intentionally uses only part of the shared Compose project. Keep
# Compose from misclassifying services started by another overlay as orphans.
export COMPOSE_IGNORE_ORPHANS := true

COMPOSE := docker compose -f infra/compose.yaml
# Use every overlay for project-wide lifecycle operations. This prevents a
# service from being left running merely because it belongs to another overlay.
STACK_COMPOSE := docker compose -f infra/compose.yaml \
	-f infra/compose.app.yaml \
	-f infra/compose.observability.yaml
PYTHON3 ?= /usr/bin/python3
PY := .venv/bin/python
MLFLOW := .venv/bin/mlflow
GO_DIR := services/ingestion
TEST_DATABASE_URL ?= postgres://fraudwatch:fraudwatch_dev@127.0.0.1:5434/fraudwatch_test?sslmode=disable
DATA_REPO := https://github.com/Fraud-Detection-Handbook/simulated-data-raw.git

.PHONY: help check venv install data kafka-permissions up down stop-all down-all status \
        wait-db init-test-db migrate wait-kafka topics go-deps \
        model-bootstrap train mlflow-ui smoke run-api run-scoring run-labels \
        test test-python test-go setup clean clean-all

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

down: ## Remove Kafka/PostgreSQL containers (preserve named volumes)
	@$(COMPOSE) down

stop-all: ## Stop all Compose services (preserve containers and data)
	@$(STACK_COMPOSE) stop

down-all: ## Remove all project containers/network (preserve volumes and files)
	@$(STACK_COMPOSE) down --remove-orphans

status: ## Display local infrastructure state
	@$(COMPOSE) ps -a

clean: ## Remove generated caches and build/test output (preserve local runtime data)
	@rm -rf -- \
		"$(CURDIR)/.pytest_cache" \
		"$(CURDIR)/.mypy_cache" \
		"$(CURDIR)/.ruff_cache" \
		"$(CURDIR)/htmlcov" \
		"$(CURDIR)/build" \
		"$(CURDIR)/dist" \
		"$(CURDIR)/fraudwatch_ml.egg-info"
	@find "$(CURDIR)" -maxdepth 1 -type f \
		\( -name '.coverage' -o -name '.coverage.*' -o -name 'coverage.xml' \) -delete
	@for path in fraudwatch_ml scripts services tests; do \
		if [ -d "$(CURDIR)/$$path" ]; then \
			find "$(CURDIR)/$$path" -type f \
				\( -name '*.pyc' -o -name '*.pyo' \) -delete; \
			find "$(CURDIR)/$$path" -depth -type d -name __pycache__ \
				-exec rm -rf -- {} +; \
		fi; \
	done
	@echo 'Generated caches and build/test output removed.'

clean-all: ## DESTRUCTIVE: stop stack and delete all local state (CONFIRM=clean-all)
	@test "$(CONFIRM)" = 'clean-all' || { \
		echo 'Refusing full reset without CONFIRM=clean-all.' >&2; \
		echo 'This deletes Compose volumes, local data/models, MLflow state, .venv, and project images.' >&2; \
		exit 2; \
	}
	@echo 'Stopping the complete Compose stack and deleting its named volumes...'
	@$(STACK_COMPOSE) down --volumes --remove-orphans
	@$(MAKE) --no-print-directory clean
	@rm -rf -- \
		"$(CURDIR)/.venv" \
		"$(CURDIR)/artifacts" \
		"$(CURDIR)/data" \
		"$(CURDIR)/mlruns"
	@find "$(CURDIR)" -maxdepth 1 -type f -name 'mlflow.db*' -delete
	-@docker image rm fraudwatch/ingestion:local fraudwatch/python-workers:local >/dev/null 2>&1
	@echo 'Full local reset complete. Source files, Git metadata, .env files, and review/ were preserved.'

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
		if $(COMPOSE) exec -T kafka /opt/kafka/bin/kafka-topics.sh \
			--bootstrap-server localhost:19092 \
			--describe --topic "$$topic" >/dev/null 2>&1; then \
			echo "Kafka topic: $$topic (exists)"; \
		else \
			$(COMPOSE) exec -T kafka /opt/kafka/bin/kafka-topics.sh \
				--bootstrap-server localhost:19092 \
				--create --topic "$$topic" \
				--partitions 3 --replication-factor 1; \
		fi; \
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
