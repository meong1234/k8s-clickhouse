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
# (chart 0.27.1 => appVersion 0.27.1). Keeper stays on the Altinity Stable (LTS)
# build; the SERVER runs Altinity Antalya 26.6 for the CAS experiment
# (docs/cas-local-s3-plan.md) - 26.6.4 is the minimum, the CAS on-disk format
# changed there. Experimental: do not roll this tag forward with data in the
# pool without reading the release notes.
CLICKHOUSE_OPERATOR_IMAGE := altinity/clickhouse-operator:0.27.1
CLICKHOUSE_METRICS_IMAGE  := altinity/metrics-exporter:0.27.1
CLICKHOUSE_SERVER_IMAGE   := altinity/clickhouse-server:26.6.4.20001.altinityantalya
CLICKHOUSE_KEEPER_IMAGE   := altinity/clickhouse-keeper:26.3.16.10001.altinitystable

# In-cluster S3 for the CAS disk. `minio/minio` no longer exists on Docker Hub
# (community images stopped Oct 2025, repos removed Sep 2026). The binary here is
# Silo, MinIO's community successor (same lineage as the pgsty/minio fork, renamed):
# chosen because it enforces conditional DELETE (412 on an `If-Match` mismatch),
# which pgsty/minio ignored - and CAS's boot probe refuses to open the pool without
# it. `pgsty/mc` still speaks to it. Everything else here still says "minio" (ns,
# service, dir) on purpose: the name is the layer, not the binary.
MINIO_IMAGE               := pgsty/silo:RELEASE.2026-09-16T00-00-00Z
MINIO_MC_IMAGE            := pgsty/mc:RELEASE.2026-09-16T00-00-00Z

# `mc` has no curl and no way to send conditional-write headers, so the Phase A
# probe (`make minio-probe`) drives raw S3 from an ephemeral curl pod instead:
# curl >= 7.75 signs requests itself with --aws-sigv4, which is the only in-cluster
# way to send If-None-Match / If-Match / Range against MinIO.
CURL_IMAGE                := curlimages/curl:8.18.0

# All project images (add new images here)
PROJECT_IMAGES := $(CLICKHOUSE_OPERATOR_IMAGE) \
                  $(CLICKHOUSE_METRICS_IMAGE) \
                  $(CLICKHOUSE_SERVER_IMAGE) \
                  $(CLICKHOUSE_KEEPER_IMAGE) \
                  $(MINIO_IMAGE) \
                  $(MINIO_MC_IMAGE) \
                  $(CURL_IMAGE)

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
