# Context-safe local Kubernetes lifecycle. All mutating kubectl calls also pass
# the expected context explicitly through scripts/deploy_k8s.py.
MINIKUBE_PROFILE := fraudwatch
MINIKUBE_CONTEXT := fraudwatch
MINIKUBE_CPUS ?= 4
MINIKUBE_MEMORY ?= 7168
KUBERNETES_VERSION ?= v1.29.2
KIND_CLUSTER := fraudwatch-ci
KIND_CONTEXT := kind-fraudwatch-ci
KIND_NODE_IMAGE ?= kindest/node:v1.29.2@sha256:51a1434a5397193442f0be2a297b488b6c919ce8a3931be0ce822606ea5ca245
K8S_POSTGRES_IMAGE := postgres:17-alpine@sha256:3540c0362c809df084ffac0ae69fe794515f9de92692f02178bcdce0ef0302ac
K8S_KAFKA_IMAGE := apache/kafka:4.4.0@sha256:833b867a9585a141da455dc2b57431c65cbccb27f11c91f694444cca9f777e0f
K8S_PROMETHEUS_IMAGE := prom/prometheus:v3.5.0@sha256:63805ebb8d2b3920190daf1cb14a60871b16fd38bed42b857a3182bc621f4996
K8S_ALERTMANAGER_IMAGE := prom/alertmanager:v0.28.1@sha256:27c475db5fb156cab31d5c18a4251ac7ed567746a2483ff264516437a39b15ba
K8S_GRAFANA_IMAGE := grafana/grafana:12.1.0@sha256:6ac590e7cabc2fbe8d7b8fc1ce9c9f0582177b334e0df9c927ebd9670469440f
K8S_LOKI_IMAGE := grafana/loki:3.7.0@sha256:c316b7c7589a5eeca843b6926c7446149d18300b79ac8538dc4ae063bc478da2
K8S_ALLOY_IMAGE := grafana/alloy:v1.11.3@sha256:8c7256f412feb9f5f48f9f6f9394dc97ca887f63dea9304f347970ecc1787669
K8S_KUBE_STATE_METRICS_IMAGE := registry.k8s.io/kube-state-metrics/kube-state-metrics:v2.14.0@sha256:37d841299325c23b56e5951176ce8ef317d537447c0f1b2d2437dddbb1f51165
K8S_LOCAL_SECRET_ENV ?= infra/k8s/fraudwatch.local.env
K8S_CI_SECRET_ENV ?= infra/k8s/fraudwatch.local.env
K8S_WAIT_TIMEOUT ?= 300s
K8S_SECRET_CHANGE_ARGS ?=

.PHONY: k8s-check k8s-platform-images k8s-local-secrets k8s-local-start k8s-local-context \
	k8s-local-images k8s-local-deploy k8s-local-up k8s-local-status \
	k8s-local-smoke k8s-local-port-forward k8s-local-logs k8s-local-stop k8s-local-delete \
	k8s-local-grafana k8s-local-rollout-secrets \
	k8s-ci-start k8s-ci-context k8s-ci-images k8s-ci-deploy k8s-ci-up \
	k8s-ci-smoke k8s-ci-status k8s-ci-delete

define wait_for_kubernetes_api
	@for attempt in $$(seq 1 60); do \
		if kubectl --context "$(1)" --request-timeout=5s get --raw=/readyz >/dev/null 2>&1; then \
			echo 'Kubernetes API: ready'; exit 0; \
		fi; \
		sleep 2; \
	done; \
	echo "Kubernetes API did not recover for context $(1)" >&2; exit 1
endef

k8s-check: ## Check shared Kubernetes tooling
	@command -v kubectl >/dev/null
	@command -v docker >/dev/null
	@echo 'Shared Kubernetes tools: OK'

k8s-platform-images: ## Cache exact platform and observability images in host Docker
	@for image in \
		"$(K8S_POSTGRES_IMAGE)" \
		"$(K8S_KAFKA_IMAGE)" \
		"$(K8S_PROMETHEUS_IMAGE)" \
		"$(K8S_ALERTMANAGER_IMAGE)" \
		"$(K8S_GRAFANA_IMAGE)" \
		"$(K8S_LOKI_IMAGE)" \
		"$(K8S_ALLOY_IMAGE)" \
		"$(K8S_KUBE_STATE_METRICS_IMAGE)"; do \
		if ! docker image inspect "$$image" >/dev/null 2>&1; then docker pull "$$image"; fi; \
	done

k8s-local-secrets: ## Create/upgrade ignored mode-0600 Minikube credentials
	@$(PYTHON3) scripts/init_k8s_secrets.py "$(K8S_LOCAL_SECRET_ENV)"

k8s-local-start: k8s-check ## Start the persistent fraudwatch Minikube profile
	@command -v minikube >/dev/null
	@minikube start --profile "$(MINIKUBE_PROFILE)" --driver=docker \
		--container-runtime=containerd --cpus="$(MINIKUBE_CPUS)" \
		--memory="$(MINIKUBE_MEMORY)" \
		--kubernetes-version="$(KUBERNETES_VERSION)"
	@$(MAKE) --no-print-directory k8s-local-context

k8s-local-context: ## Refuse unless the active context is the FraudWatch Minikube profile
	@current="$$(kubectl config current-context 2>/dev/null || true)"; \
		test "$$current" = "$(MINIKUBE_CONTEXT)" || { \
			echo "Refusing: current Kubernetes context is '$$current', expected '$(MINIKUBE_CONTEXT)'" >&2; \
			exit 2; \
		}

# Docker archives preserve tags, while the digest is verified before import
# and retained in each manifest as tag@digest for the kubelet identity check.
k8s-local-images: app-build k8s-platform-images k8s-local-context ## Load exact local images into Minikube
	@eval "$$($(PYTHON3) scripts/image_identity.py)"; \
		images=( \
			"$(K8S_POSTGRES_IMAGE)" \
			"$(K8S_KAFKA_IMAGE)" \
			"$(K8S_PROMETHEUS_IMAGE)" \
			"$(K8S_ALERTMANAGER_IMAGE)" \
			"$(K8S_GRAFANA_IMAGE)" \
			"$(K8S_LOKI_IMAGE)" \
			"$(K8S_ALLOY_IMAGE)" \
			"$(K8S_KUBE_STATE_METRICS_IMAGE)" \
			"fraudwatch/ingestion:$$FRAUDWATCH_APP_TAG" \
			"fraudwatch/labels:$$FRAUDWATCH_APP_TAG" \
			"fraudwatch/monitor:$$FRAUDWATCH_APP_TAG" \
			"fraudwatch/operations:$$FRAUDWATCH_APP_TAG" \
			"fraudwatch/scoring:$$FRAUDWATCH_SCORING_TAG" \
			"fraudwatch/model-monitor:$$FRAUDWATCH_SCORING_TAG" \
		); \
		for image in "$${images[@]}"; do \
			minikube image load --profile "$(MINIKUBE_PROFILE)" "$${image%@*}"; \
		done

k8s-local-deploy: k8s-local-secrets k8s-local-context ## Deploy ordered local overlay, migrations, and topics
	$(call wait_for_kubernetes_api,$(MINIKUBE_CONTEXT))
	@$(PYTHON3) scripts/deploy_k8s.py \
		--context "$(MINIKUBE_CONTEXT)" \
		--overlay infra/k8s/overlays/local \
		--secret-env "$(K8S_LOCAL_SECRET_ENV)" \
		--timeout "$(K8S_WAIT_TIMEOUT)" $(K8S_SECRET_CHANGE_ARGS)

k8s-local-rollout-secrets: ## Roll out an already-coordinated credential change (CONFIRM=secret-rollout)
	@test "$(CONFIRM)" = 'secret-rollout' || { \
		echo 'Refusing Secret rollout without CONFIRM=secret-rollout.' >&2; \
		echo 'Rotate the persisted PostgreSQL and Grafana credentials first.' >&2; \
		exit 2; \
	}
	@$(MAKE) --no-print-directory k8s-local-deploy \
		K8S_SECRET_CHANGE_ARGS=--allow-secret-change

k8s-local-up: ## Start Minikube, build/load exact images, and deploy everything
	@$(MAKE) --no-print-directory k8s-local-start
	@$(MAKE) --no-print-directory k8s-local-images
	@$(MAKE) --no-print-directory k8s-local-deploy

k8s-local-status: ## Show FraudWatch resources in the persistent local cluster
	@kubectl --context "$(MINIKUBE_CONTEXT)" --namespace fraudwatch get \
		pods,services,statefulsets,deployments,jobs

k8s-local-smoke: k8s-local-context ## Test API, health, metrics, outbox, Kafka, and scoring
	@$(PYTHON3) scripts/smoke_k8s.py --context "$(MINIKUBE_CONTEXT)"

k8s-local-port-forward: k8s-local-context ## Forward ingestion to http://127.0.0.1:8080
	@kubectl --context "$(MINIKUBE_CONTEXT)" --namespace fraudwatch \
		port-forward service/ingestion 8080:8080

k8s-local-grafana: k8s-local-context ## Forward Grafana to http://127.0.0.1:3000
	@kubectl --context "$(MINIKUBE_CONTEXT)" --namespace fraudwatch \
		port-forward service/grafana 3000:3000

k8s-local-logs: k8s-local-context ## Follow logs from all application pods
	@kubectl --context "$(MINIKUBE_CONTEXT)" --namespace fraudwatch logs \
		--selector app.kubernetes.io/part-of=fraudwatch \
		--all-containers=true --prefix=true --tail=100 --follow

k8s-local-stop: ## Stop the persistent Minikube VM/container and preserve data
	@minikube stop --profile "$(MINIKUBE_PROFILE)"

k8s-local-delete: ## DESTRUCTIVE: delete local cluster (CONFIRM=fraudwatch)
	@test "$(CONFIRM)" = "$(MINIKUBE_PROFILE)" || { \
		echo 'Refusing cluster deletion without CONFIRM=$(MINIKUBE_PROFILE)' >&2; exit 2; \
	}
	@minikube delete --profile "$(MINIKUBE_PROFILE)"

k8s-ci-start: k8s-check ## Create the ephemeral Kind CI cluster
	@command -v kind >/dev/null
	@if kind get clusters | grep -qx "$(KIND_CLUSTER)"; then \
		echo 'Kind cluster already exists: $(KIND_CLUSTER)'; \
	else \
		kind create cluster --name "$(KIND_CLUSTER)" \
			--config infra/k8s/kind-ci.yaml --image "$(KIND_NODE_IMAGE)" --wait 120s; \
	fi
	@$(MAKE) --no-print-directory k8s-ci-context

k8s-ci-context: ## Refuse unless the active context is the ephemeral FraudWatch Kind cluster
	@current="$$(kubectl config current-context 2>/dev/null || true)"; \
		test "$$current" = "$(KIND_CONTEXT)" || { \
			echo "Refusing: current Kubernetes context is '$$current', expected '$(KIND_CONTEXT)'" >&2; \
			exit 2; \
		}

k8s-ci-images: app-build k8s-platform-images k8s-ci-context ## Load exact local images into Kind
	@eval "$$($(PYTHON3) scripts/image_identity.py)"; \
		images=( \
			"$(K8S_POSTGRES_IMAGE)" \
			"$(K8S_KAFKA_IMAGE)" \
			"$(K8S_PROMETHEUS_IMAGE)" \
			"$(K8S_ALERTMANAGER_IMAGE)" \
			"$(K8S_GRAFANA_IMAGE)" \
			"$(K8S_LOKI_IMAGE)" \
			"$(K8S_ALLOY_IMAGE)" \
			"$(K8S_KUBE_STATE_METRICS_IMAGE)" \
			"fraudwatch/ingestion:$$FRAUDWATCH_APP_TAG" \
			"fraudwatch/labels:$$FRAUDWATCH_APP_TAG" \
			"fraudwatch/monitor:$$FRAUDWATCH_APP_TAG" \
			"fraudwatch/operations:$$FRAUDWATCH_APP_TAG" \
			"fraudwatch/scoring:$$FRAUDWATCH_SCORING_TAG" \
			"fraudwatch/model-monitor:$$FRAUDWATCH_SCORING_TAG" \
		); \
		load_images=(); \
		for image in "$${images[@]}"; do load_images+=("$${image%@*}"); done; \
		kind load docker-image --name "$(KIND_CLUSTER)" "$${load_images[@]}"

k8s-ci-deploy: k8s-ci-context ## Deploy ordered CI overlay, migrations, and topics
	@test -f "$(K8S_CI_SECRET_ENV)" || { \
		echo 'Missing K8S_CI_SECRET_ENV=$(K8S_CI_SECRET_ENV)' >&2; exit 2; \
	}
	$(call wait_for_kubernetes_api,$(KIND_CONTEXT))
	@$(PYTHON3) scripts/deploy_k8s.py \
		--context "$(KIND_CONTEXT)" \
		--overlay infra/k8s/overlays/ci \
		--secret-env "$(K8S_CI_SECRET_ENV)" \
		--timeout "$(K8S_WAIT_TIMEOUT)"

k8s-ci-up: ## Create Kind, build/load exact images, and deploy everything
	@$(MAKE) --no-print-directory k8s-ci-start
	@$(MAKE) --no-print-directory k8s-ci-images
	@$(MAKE) --no-print-directory k8s-ci-deploy

k8s-ci-status: ## Show FraudWatch resources in the ephemeral CI cluster
	@kubectl --context "$(KIND_CONTEXT)" --namespace fraudwatch get \
		pods,services,statefulsets,deployments,jobs

k8s-ci-smoke: k8s-ci-context ## Run the end-to-end smoke test against Kind
	@$(PYTHON3) scripts/smoke_k8s.py --context "$(KIND_CONTEXT)"

k8s-ci-delete: ## Delete the ephemeral Kind CI cluster
	@kind delete cluster --name "$(KIND_CLUSTER)"
