# Local application containers; intentionally separate from host-run development.
APP_COMPOSE := docker compose -f infra/compose.yaml -f infra/compose.app.yaml

.PHONY: app-export-model app-identity app-config app-build app-up app-stop app-status app-logs app-check

app-export-model: ## Export MLflow candidate model into a portable read-only bundle
	@$(PY) scripts/export_candidate_model.py

app-identity: ## Show source/model identities used by immutable application images
	@$(PYTHON3) scripts/image_identity.py --format json

app-config: ## Validate combined Compose configuration
	@eval "$$($(PYTHON3) scripts/image_identity.py)"; $(APP_COMPOSE) config --quiet
	@echo 'Application Compose configuration: OK'

app-build: ## Build immutable role-specific images using host build networking
	@test -f artifacts/candidate-model/model/MLmodel || { echo 'Run make app-export-model first' >&2; exit 1; }
	@eval "$$($(PYTHON3) scripts/image_identity.py)"; \
		$(STACK_COMPOSE) --profile tools build \
		ingestion scoring labels monitor model-monitor-evaluator operations

app-up: app-build ## Build then start the containerized API and workers
	@eval "$$($(PYTHON3) scripts/image_identity.py)"; \
		$(APP_COMPOSE) up -d --no-build \
		ingestion scoring labels model-monitor-evaluator

app-stop: ## Stop ONLY app containers; preserve Kafka/PostgreSQL and monitoring
	@$(APP_COMPOSE) stop ingestion scoring labels model-monitor-evaluator

app-status: ## Inspect API, worker, and model-evaluator containers
	@$(APP_COMPOSE) ps ingestion scoring labels model-monitor-evaluator

app-logs: ## Follow logs from API, workers, and model evaluator
	@$(APP_COMPOSE) logs -f --tail=80 ingestion scoring labels model-monitor-evaluator

app-check: ## Check application HTTP and metrics endpoints
	@curl -fsS http://127.0.0.1:8080/livez >/dev/null && echo 'Ingestion liveness: OK'
	@curl -fsS http://127.0.0.1:8080/readyz >/dev/null && echo 'Ingestion readiness: OK'
	@curl -fsS http://127.0.0.1:8080/metrics >/dev/null && echo 'Ingestion metrics: OK'
	@curl -fsS http://127.0.0.1:9101/livez >/dev/null && echo 'Scoring liveness: OK'
	@curl -fsS http://127.0.0.1:9101/readyz >/dev/null && echo 'Scoring readiness: OK'
	@curl -fsS http://127.0.0.1:9101/metrics >/dev/null && echo 'Scoring metrics: OK'
	@curl -fsS http://127.0.0.1:9102/livez >/dev/null && echo 'Label liveness: OK'
	@curl -fsS http://127.0.0.1:9102/readyz >/dev/null && echo 'Label readiness: OK'
	@curl -fsS http://127.0.0.1:9102/metrics >/dev/null && echo 'Label metrics: OK'
