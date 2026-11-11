# Included by root Makefile. Linux host-network local observability stack only.
OBS_COMPOSE := docker compose -f infra/compose.yaml -f infra/compose.observability.yaml

.PHONY: obs-install obs-up obs-stop obs-status obs-check
obs-install: venv ## Install Prometheus Python client for local metrics exporters
	@$(PY) -m pip install 'prometheus-client>=0.20,<1'

obs-up: ## Start Prometheus and Grafana (preserve volumes)
	@$(OBS_COMPOSE) up -d prometheus grafana

obs-stop: ## Stop monitoring without stopping Kafka or PostgreSQL
	@$(OBS_COMPOSE) stop grafana prometheus

obs-status: ## Show Prometheus/Grafana containers
	@$(OBS_COMPOSE) ps prometheus grafana

obs-check: ## Verify worker exporters and Prometheus target endpoints
	@curl --fail --silent --show-error http://127.0.0.1:9101/metrics >/dev/null && echo 'Scoring exporter: OK'
	@curl --fail --silent --show-error http://127.0.0.1:9102/metrics >/dev/null && echo 'Label exporter: OK'
	@curl --fail --silent --show-error http://127.0.0.1:9090/-/ready >/dev/null && echo 'Prometheus: OK'
	@curl --fail --silent --show-error http://127.0.0.1:3000/api/health >/dev/null && echo 'Grafana: OK'
