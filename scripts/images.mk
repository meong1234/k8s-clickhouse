# Docker Image Management for K8s ClickHouse Project
# Handles pulling and pushing project images into the local registry.

.PHONY: images-help images-pull-all images-push-all images-manage-all

# Local registry coordinates (defined/exported in scripts/k8s.mk, shared via
# `include scripts/*` in the Makefile). Push from the host to localhost:5050.
REG_NAME ?= k3d-local-dev-registry
REG_HOST_PORT ?= 5050

# Project images
#
# Operator + metrics exporter come from the Altinity clickhouse-operator chart
# (chart 0.27.1 => appVersion 0.27.1). ClickHouse server & keeper use the
# Altinity Stable (LTS) builds - 26.3 is the newest LTS line.
CLICKHOUSE_OPERATOR_IMAGE := altinity/clickhouse-operator:0.27.1
CLICKHOUSE_METRICS_IMAGE  := altinity/metrics-exporter:0.27.1
CLICKHOUSE_SERVER_IMAGE   := altinity/clickhouse-server:26.3.16.10001.altinitystable
CLICKHOUSE_KEEPER_IMAGE   := altinity/clickhouse-keeper:26.3.16.10001.altinitystable

# All project images (add new images here)
PROJECT_IMAGES := $(CLICKHOUSE_OPERATOR_IMAGE) \
                  $(CLICKHOUSE_METRICS_IMAGE) \
                  $(CLICKHOUSE_SERVER_IMAGE) \
                  $(CLICKHOUSE_KEEPER_IMAGE)

# Help for image management commands
images-help:
	@echo "Docker Image Management Commands:"
	@echo "--------------------------------"
	@echo "  images-pull-all     - Pull all required Docker images"
	@echo "  images-push-all     - Push all required images to the in-cluster registry"
	@echo "  images-manage-all   - Pull and push all required Docker images (complete process)"
	@echo ""

# Pull all project images
images-pull-all:
	@echo "==> Pulling project Docker images..."
	@for img in $(PROJECT_IMAGES); do \
		echo "  - Pulling $$img"; \
		docker pull $$img || { echo "Error pulling $$img"; exit 1; }; \
	done
	@echo "==> All project images pulled successfully"

# Preload all project images by PUSHING them to the local registry
# (k3d-local-dev-registry), rather than baking them into node images.
#
# Why a registry (not `k3d image import` / `kind load`)? Those run
# `ctr images import --all-platforms`, which fails with "content digest ... not
# found" under Docker Desktop's *containerd image store*: a tag is a multi-platform
# OCI index (amd64 + arm64 + SBOM/provenance manifests) but only the host
# platform's blobs are present, so importing "all platforms" hits missing content.
#
# `docker push` instead sends a clean single-platform image ("only the available
# single-platform image was pushed"), and the cluster is configured (k3d/cluster.yaml)
# to mirror docker.io to this registry - so nodes pull the pre-pushed images
# locally, falling back to real Docker Hub for anything not preloaded. Works
# regardless of the Docker Desktop image-store setting.
images-push-all:
	@echo "==> Pushing project images to local registry '$(REG_NAME)' (localhost:$(REG_HOST_PORT))..."
	@if ! docker ps | grep -q $(REG_NAME); then \
		echo "Error: registry '$(REG_NAME)' is not running"; \
		echo "Please run 'make registry-create' (or 'make cluster-create') first"; \
		exit 1; \
	fi
	@for img in $(PROJECT_IMAGES); do \
		LOCAL_REF="localhost:$(REG_HOST_PORT)/$$img"; \
		echo "  - $$img -> $$LOCAL_REF"; \
		docker tag $$img $$LOCAL_REF; \
		docker push $$LOCAL_REF || { echo "Error pushing $$img"; exit 1; }; \
	done
	@echo "==> All project images pushed to the local registry"
	@echo "==> Nodes will pull them via the docker.io -> $(REG_NAME) containerd mirror"

# Pull and push all project images (complete process)
images-manage-all: images-pull-all images-push-all
	@echo "==> Image management complete"
