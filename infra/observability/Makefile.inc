# Included by root Makefile. Linux host-network local observability stack only.
OBS_COMPOSE := docker compose -f infra/compose.yaml -f infra/compose.observability.yaml
LOKI_IMAGE := grafana/loki:3.7.0
ALLOY_IMAGE := grafana/alloy:v1.11.3

.PHONY: obs-install obs-config obs-up obs-stop obs-status obs-check model-monitor-baseline model-monitor-evaluate
obs-install: venv ## Install Prometheus Python client for local metrics exporters
	@$(PY) -m pip install 'prometheus-client>=0.20,<1'

obs-config: ## Validate Compose, Loki, and Alloy configurations
	@$(OBS_COMPOSE) config --quiet
	@docker run --rm --network none \
		--mount "type=bind,source=$(CURDIR)/infra/observability/loki.yml,target=/etc/loki/local-config.yaml,readonly" \
		$(LOKI_IMAGE) -verify-config=true -config.file=/etc/loki/local-config.yaml
	@docker run --rm --network none \
		--mount "type=bind,source=$(CURDIR)/infra/observability/alloy/config.alloy,target=/etc/alloy/config.alloy,readonly" \
		$(ALLOY_IMAGE) validate /etc/alloy/config.alloy

obs-up: ## Start the complete local monitoring/logging stack (preserve volumes)
	@eval "$$($(PYTHON3) scripts/image_identity.py --source-only)"; \
		$(OBS_COMPOSE) up -d --build monitor prometheus alertmanager loki alloy grafana

obs-stop: ## Stop monitoring without stopping Kafka or PostgreSQL
	@$(OBS_COMPOSE) stop grafana alloy loki alertmanager prometheus monitor

obs-status: ## Show all local observability containers
	@$(OBS_COMPOSE) ps -a monitor prometheus alertmanager loki-init loki alloy grafana

obs-check: ## Verify worker exporters and Prometheus target endpoints
	@curl --fail --silent --show-error http://127.0.0.1:8080/metrics >/dev/null && echo 'Ingestion exporter: OK'
	@curl --fail --silent --show-error http://127.0.0.1:9101/metrics >/dev/null && echo 'Scoring exporter: OK'
	@curl --fail --silent --show-error http://127.0.0.1:9102/metrics >/dev/null && echo 'Label exporter: OK'
	@curl --fail --silent --show-error http://127.0.0.1:9103/metrics >/dev/null && echo 'Independent monitor: OK'
	@curl --fail --silent --show-error http://127.0.0.1:9090/-/ready >/dev/null && echo 'Prometheus: OK'
	@curl --fail --silent --show-error http://127.0.0.1:9093/-/ready >/dev/null && echo 'Alertmanager: OK'
	@curl --fail --silent --show-error http://127.0.0.1:3100/ready >/dev/null && echo 'Loki: OK'
	@curl --fail --silent --show-error http://127.0.0.1:12345/-/ready >/dev/null && echo 'Alloy: OK'
	@curl --fail --silent --show-error http://127.0.0.1:3000/api/health >/dev/null && echo 'Grafana: OK'

model-monitor-baseline: ## Build reference bins for a legacy exported candidate
	@$(PY) scripts/model_monitor.py baseline

model-monitor-evaluate: ## Persist a drift/quality window (pass MODEL_MONITOR_ARGS='...')
	@$(PY) scripts/model_monitor.py evaluate $(MODEL_MONITOR_ARGS)
