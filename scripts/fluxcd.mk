# FluxCD Integration
# Manages FluxCD setup and configuration in the k3d cluster with an OCI registry.

# Define wait timeout for kubectl operations
WAIT_TIMEOUT ?= 5m

.PHONY: fluxcd-help fluxcd-pull-images fluxcd-push-images fluxcd-install fluxcd-push-artifacts fluxcd-oci-setup fluxcd-setup fluxcd-verify-artifacts

# FluxCD container images — MUST match the versions the installed flux CLI
# deploys (flux CLI 2.9.0 → controllers v1.9.1, helm-controller v1.6.1). If you
# upgrade the flux CLI, refresh these:
#   kubectl -n flux-system get deploy -o jsonpath='{range .items[*]}{.spec.template.spec.containers[0].image}{"\n"}{end}'
FLUXCD_SOURCE := ghcr.io/fluxcd/source-controller:v1.9.1
FLUXCD_NOTIFICATION := ghcr.io/fluxcd/notification-controller:v1.9.1
FLUXCD_KUSTOMIZE := ghcr.io/fluxcd/kustomize-controller:v1.9.1
FLUXCD_HELM := ghcr.io/fluxcd/helm-controller:v1.6.1
FLUXCD_IMAGES := $(FLUXCD_SOURCE) $(FLUXCD_NOTIFICATION) $(FLUXCD_KUSTOMIZE) $(FLUXCD_HELM)

# Artifact directories
CLUSTERS_MANIFESTS_DIR := $(shell pwd)/kubernetes/clusters
OPERATORS_MANIFESTS_DIR := $(shell pwd)/kubernetes/operators
INFRA_MANIFESTS_DIR := $(shell pwd)/kubernetes/infra
ANALYTICS_MANIFESTS_DIR := $(shell pwd)/kubernetes/analytics

# Artifact settings
CLUSTERS_ARTIFACT_NAME := cluster-sync
OPERATORS_ARTIFACT_NAME := operators-sync
INFRA_ARTIFACT_NAME := infra-sync
ANALYTICS_ARTIFACT_NAME := analytics-sync

# Artifacts to push - each pair is a "directory:name" format
ARTIFACTS_TO_PUSH := $(CLUSTERS_MANIFESTS_DIR):$(CLUSTERS_ARTIFACT_NAME) \
                     $(OPERATORS_MANIFESTS_DIR):$(OPERATORS_ARTIFACT_NAME) \
                     $(INFRA_MANIFESTS_DIR):$(INFRA_ARTIFACT_NAME) \
                     $(ANALYTICS_MANIFESTS_DIR):$(ANALYTICS_ARTIFACT_NAME)

# OCI Registry settings (shared with scripts/k8s.mk via `include scripts/*`)
REG_NAME ?= k3d-local-dev-registry
REG_HOST_PORT ?= 5050
REG_CLUSTER_PORT ?= 5000

# Registry URLs. Push from the host to localhost:5050; the in-cluster name is
# $(REG_NAME):5000 (used by the OCIRepository manifests).
OCI_LOCAL_URL := localhost:$(REG_HOST_PORT)
OCI_CLUSTER_URL := $(REG_NAME):$(REG_CLUSTER_PORT)

# Help for FluxCD commands
fluxcd-help:
	@echo "FluxCD Commands:"
	@echo "---------------"
	@echo "  fluxcd-pull-images    - Pull all FluxCD container images"
	@echo "  fluxcd-push-images    - Push FluxCD images to the local registry"
	@echo "  fluxcd-install        - Install FluxCD components in the cluster"
	@echo "  fluxcd-push-artifacts - Push manifests to OCI registry"
	@echo "  fluxcd-oci-setup      - Setup FluxCD with OCI repository"
	@echo "  fluxcd-setup          - Complete FluxCD setup (all steps)"
	@echo ""

# Pull all FluxCD container images
fluxcd-pull-images:
	@echo "==> Pulling FluxCD images..."
	@for img in $(FLUXCD_IMAGES); do \
		echo "  - Pulling $$img"; \
		docker pull $$img; \
	done
	@echo "==> All FluxCD images pulled"

# Push the FluxCD controller images to the local registry.
#
# The Flux images live on ghcr.io, which the cluster mirrors to the local
# registry (see k3d/cluster.yaml). We strip the `ghcr.io/` prefix on push so the
# repository path matches what the mirror requests (e.g. ghcr.io/fluxcd/foo ->
# localhost:5050/fluxcd/foo). Same clean single-platform push as the project
# images, so it works under Docker Desktop's containerd image store.
fluxcd-push-images:
	@echo "==> Pushing FluxCD images to local registry '$(REG_NAME)' (localhost:$(REG_HOST_PORT))..."
	@if ! docker ps | grep -q $(REG_NAME); then \
		echo "Error: registry '$(REG_NAME)' is not running - run 'make cluster-create' first"; \
		exit 1; \
	fi
	@for img in $(FLUXCD_IMAGES); do \
		LOCAL_REF="localhost:$(REG_HOST_PORT)/$${img#ghcr.io/}"; \
		echo "  - $$img -> $$LOCAL_REF"; \
		docker tag $$img $$LOCAL_REF; \
		docker push $$LOCAL_REF || { echo "Error pushing $$img"; exit 1; }; \
	done
	@echo "==> All FluxCD images pushed (served via the ghcr.io -> $(REG_NAME) mirror)"

# Install FluxCD components in the cluster
fluxcd-install:
	@echo "==> Installing FluxCD in the cluster..."
	@flux check --pre > /dev/null 2>&1 || { echo "Error: flux CLI not working properly"; exit 1; }
	@flux install --components=source-controller,kustomize-controller,helm-controller,notification-controller
	@echo "==> Waiting for FluxCD controllers to be ready"
	@kubectl -n flux-system wait --timeout=$(WAIT_TIMEOUT) --for=condition=Available deployments --all
	@echo "==> FluxCD installed successfully"

# Push artifacts to OCI registry
fluxcd-push-artifacts:
	@echo "==> Pushing artifacts to OCI registry..."

	@if ! docker ps | grep -q $(REG_NAME); then \
		echo "Error: Registry container '$(REG_NAME)' is not running"; \
		echo "Please run 'make cluster-create' first or start the registry manually"; \
		exit 1; \
	fi

	@for artifact in $(ARTIFACTS_TO_PUSH); do \
		ARTIFACT_DIR=$$(echo $$artifact | cut -d: -f1); \
		ARTIFACT_NAME=$$(echo $$artifact | cut -d: -f2); \
		\
		if [ ! -d $$ARTIFACT_DIR ]; then \
			echo "Error: Directory '$$ARTIFACT_DIR' not found, skipping..."; \
			continue; \
		fi; \
		\
		echo "  - Pushing $$ARTIFACT_DIR to OCI registry at $(OCI_LOCAL_URL)/$$ARTIFACT_NAME"; \
		REVISION=$$(date +%s); \
		echo "  - Using revision: $$REVISION and tag: local"; \
		FLUX_OUTPUT=$$(flux push artifact oci://$(OCI_LOCAL_URL)/$$ARTIFACT_NAME:local \
			--path="$$ARTIFACT_DIR" \
			--source="local-development" \
			--revision="$$REVISION" 2>&1) || EXIT_CODE=$$?; \
		\
		if [ -n "$$EXIT_CODE" ] && [ $$EXIT_CODE -ne 0 ]; then \
			echo "$$FLUX_OUTPUT"; \
			exit 1; \
		fi; \
		\
		OCI_URL=$$(echo "$$FLUX_OUTPUT" | grep -o 'oci://.*'); \
		echo "  ✅ Pushed to $$OCI_URL"; \
	done

	@echo "==> Artifact push complete"
	@echo "==> Verifying pushed artifacts..."
	@make fluxcd-verify-artifacts

# Verify pushed artifacts
fluxcd-verify-artifacts:
	@echo "==> Verifying artifacts in registry"
	@for artifact in $(ARTIFACTS_TO_PUSH); do \
		ARTIFACT_NAME=$$(echo $$artifact | cut -d: -f2); \
		echo "  - Checking $$ARTIFACT_NAME in registry at http://$(OCI_LOCAL_URL)/v2/$$ARTIFACT_NAME/tags/list"; \
		TAGS=$$(curl -s "http://$(OCI_LOCAL_URL)/v2/$$ARTIFACT_NAME/tags/list" 2>/dev/null || echo '{"tags":[]}'); \
		if echo "$$TAGS" | grep -q '"local"'; then \
			echo "    ✅ Found $$ARTIFACT_NAME with tag 'local'"; \
		else \
			echo "    ❌ Artifact $$ARTIFACT_NAME with tag 'local' not found in registry!"; \
			echo "       Registry response: $$TAGS"; \
			exit 1; \
		fi; \
	done
	@echo "==> All artifacts verified"

# Create FluxCD resources for OCI repository
fluxcd-oci-setup: fluxcd-verify-artifacts
	@echo "==> Creating FluxCD resources from existing manifests..."
	@kubectl apply -f $(CLUSTERS_MANIFESTS_DIR)/local/flux-system/cluster-source.yaml
	@kubectl apply -f $(CLUSTERS_MANIFESTS_DIR)/local/flux-system/cluster-sync.yaml
	@echo "==> FluxCD OCI resources created successfully"
	@echo "==> Verify status with: kubectl get ocirepositories -n flux-system"

# Complete FluxCD setup (all steps)
fluxcd-setup: fluxcd-pull-images fluxcd-push-images fluxcd-install fluxcd-push-artifacts fluxcd-oci-setup
	@echo "==> FluxCD setup complete"
