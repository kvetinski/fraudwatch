# Included by the root Makefile for local Prometheus rule operations.
ALERTS_COMPOSE := docker compose -f infra/compose.yaml -f infra/compose.observability.yaml
PROMETHEUS_IMAGE := prom/prometheus:v3.5.0
ALERTMANAGER_IMAGE := prom/alertmanager:v0.28.1

.PHONY: alerts-check alerts-test observability-k8s-check alerts-up alerts-status
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

observability-k8s-check: alerts-check ## Validate Kubernetes Prometheus, Loki, and Alloy configs
	@docker run --rm --network none --entrypoint /bin/promtool \
		--mount "type=bind,source=$(CURDIR)/infra/observability/k8s/config/prometheus.yml,target=/etc/prometheus/prometheus.yml,readonly" \
		--mount "type=bind,source=$(CURDIR)/infra/observability/rules,target=/etc/prometheus/rules,readonly" \
		$(PROMETHEUS_IMAGE) check config /etc/prometheus/prometheus.yml
	@docker run --rm --network none --entrypoint /usr/bin/loki \
		--mount "type=bind,source=$(CURDIR)/infra/observability/k8s/config/loki.yml,target=/etc/loki/loki.yml,readonly" \
		grafana/loki:3.7.0 -config.file=/etc/loki/loki.yml -verify-config=true
	@docker run --rm --network none --entrypoint /bin/alloy \
		--mount "type=bind,source=$(CURDIR)/infra/observability/k8s/config/config.alloy,target=/etc/alloy/config.alloy,readonly" \
		grafana/alloy:v1.11.3 validate /etc/alloy/config.alloy

alerts-up: alerts-check alerts-test ## Recreate only Prometheus to load the new rule mount
	@$(ALERTS_COMPOSE) up -d --no-deps --force-recreate prometheus

alerts-status: ## Display Prometheus rule states and currently active alerts
	@$(PY) scripts/check_alert_rules.py
