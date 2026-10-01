# Kubernetes local ClickHouse Deployment
# Main entry point for all operations

.PHONY: help up down

# Include all makefiles from scripts directory
include scripts/*

# Default target - orchestrates help from all script files
help: k8s-help setup-help fluxcd-help images-help clickhouse-help minio-help cas-help warehouse-help
	@echo "Kubernetes local ClickHouse Deployment"
	@echo "======================================"
	@echo ""
	@echo "High-Level Commands:"
	@echo "-----------------"
	@echo "  up               - Create cluster + registry, preload images, and setup FluxCD"
	@echo "  down             - Delete the k3d cluster (keeps the registry image cache)"
	@echo ""

# High-level command to set up the complete environment
up: cluster-create images-manage-all fluxcd-setup
	@echo "==> Environment is now ready!"
	@echo "==> Watch ClickHouse come up with: make ch-status"
	@echo "==> Once ready, run the replication demo with: make ch-demo"

# High-level command to tear down the environment
down: cluster-delete
	@echo "==> Environment has been destroyed"
