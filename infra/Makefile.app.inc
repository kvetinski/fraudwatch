# Local application containers; intentionally separate from host-run development.
APP_COMPOSE := docker compose -f infra/compose.yaml -f infra/compose.app.yaml

.PHONY: app-export-model app-config app-build app-up app-stop app-status app-logs app-check

app-export-model: ## Export MLflow candidate model into a portable read-only bundle
	@$(PY) scripts/export_candidate_model.py

app-config: ## Validate combined Compose configuration
	@$(APP_COMPOSE) config --quiet
	@echo 'Application Compose configuration: OK'

app-build: ## Build Go API and shared Python worker images
	@$(APP_COMPOSE) build ingestion scoring

app-up: ## Start containerized API and workers (requires model export, migrations, topics)
	@test -f artifacts/candidate-model/model/MLmodel || { echo 'Run make app-export-model first' >&2; exit 1; }
	@$(APP_COMPOSE) up -d --build ingestion scoring labels

app-stop: ## Stop ONLY app containers; preserve Kafka/PostgreSQL and monitoring
	@$(APP_COMPOSE) stop ingestion scoring labels

app-status: ## Inspect the three app containers
	@$(APP_COMPOSE) ps ingestion scoring labels

app-logs: ## Follow logs from Go API and both Python workers
	@$(APP_COMPOSE) logs -f --tail=80 ingestion scoring labels

app-check: ## Check application HTTP and metrics endpoints
	@curl -fsS http://127.0.0.1:8080/healthz >/dev/null && echo 'Ingestion API: OK'
	@curl -fsS http://127.0.0.1:9101/metrics >/dev/null && echo 'Scoring metrics: OK'
	@curl -fsS http://127.0.0.1:9102/metrics >/dev/null && echo 'Label metrics: OK'
