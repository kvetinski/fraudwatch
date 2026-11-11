# Included by the root Makefile for local Prometheus rule operations.
ALERTS_COMPOSE := docker compose -f infra/compose.yaml -f infra/compose.observability.yaml

.PHONY: alerts-check alerts-test alerts-up alerts-status
alerts-check: ## Validate Prometheus configuration and FraudWatch alert rules (without modifying the stack)
	@$(ALERTS_COMPOSE) run --rm --no-deps --entrypoint /bin/promtool prometheus check config /etc/prometheus/prometheus.yml
	@$(ALERTS_COMPOSE) run --rm --no-deps --entrypoint /bin/promtool prometheus check rules /etc/prometheus/rules/fraudwatch-alerts.yml

alerts-test: ## Run promtool unit tests of alert conditions using synthetic metric series
	@$(ALERTS_COMPOSE) run --rm --no-deps --entrypoint /bin/promtool prometheus test rules /etc/prometheus/rules/tests/fraudwatch-alerts.test.yml

alerts-up: alerts-check alerts-test ## Recreate only Prometheus to load the new rule mount
	@$(ALERTS_COMPOSE) up -d --no-deps --force-recreate prometheus

alerts-status: ## Display Prometheus rule states and currently active alerts
	@$(PY) scripts/check_alert_rules.py
