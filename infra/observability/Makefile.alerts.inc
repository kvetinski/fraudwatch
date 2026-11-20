# Included by the root Makefile for local Prometheus rule operations.
ALERTS_COMPOSE := docker compose -f infra/compose.yaml -f infra/compose.observability.yaml
PROMETHEUS_IMAGE := prom/prometheus:v3.5.0
ALERTMANAGER_IMAGE := prom/alertmanager:v0.28.1

.PHONY: alerts-check alerts-test alerts-up alerts-status
alerts-check: ## Validate Prometheus configuration and FraudWatch alert rules (without modifying the stack)
	@docker run --rm --network none --entrypoint /bin/promtool \
		--mount "type=bind,source=$(CURDIR)/infra/observability/prometheus.yml,target=/etc/prometheus/prometheus.yml,readonly" \
		--mount "type=bind,source=$(CURDIR)/infra/observability/rules,target=/etc/prometheus/rules,readonly" \
		$(PROMETHEUS_IMAGE) check config /etc/prometheus/prometheus.yml
	@docker run --rm --network none --entrypoint /bin/promtool \
		--mount "type=bind,source=$(CURDIR)/infra/observability/rules,target=/etc/prometheus/rules,readonly" \
		$(PROMETHEUS_IMAGE) check rules /etc/prometheus/rules/fraudwatch-alerts.yml
	@docker run --rm --network none --entrypoint /bin/amtool \
		--mount "type=bind,source=$(CURDIR)/infra/observability/alertmanager.yml,target=/etc/alertmanager/alertmanager.yml,readonly" \
		$(ALERTMANAGER_IMAGE) check-config /etc/alertmanager/alertmanager.yml

alerts-test: ## Run promtool unit tests of alert conditions using synthetic metric series
	@docker run --rm --network none --entrypoint /bin/promtool \
		--mount "type=bind,source=$(CURDIR)/infra/observability/rules,target=/etc/prometheus/rules,readonly" \
		$(PROMETHEUS_IMAGE) test rules /etc/prometheus/rules/tests/fraudwatch-alerts.test.yml

alerts-up: alerts-check alerts-test ## Recreate only Prometheus to load the new rule mount
	@$(ALERTS_COMPOSE) up -d --no-deps --force-recreate prometheus

alerts-status: ## Display Prometheus rule states and currently active alerts
	@$(PY) scripts/check_alert_rules.py
