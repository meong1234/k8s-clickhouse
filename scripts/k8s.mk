# Cluster support targets (k3d / k3s)
#
# The cluster is defined declaratively in k3d/cluster.yaml. The local registry
# is managed as a SEPARATE, idempotent resource so that images pushed into it
# survive `make down` (image cache) and are simply re-attached on the next
# `make up`.

.PHONY: k8s-help registry-create registry-delete cluster-create cluster-delete cluster-status

# Configuration (keep in sync with k3d/cluster.yaml)
CLUSTER_NAME       := local-dev
K3D_CONFIG         := k3d/cluster.yaml
WAIT_TIMEOUT       := 120s

# Local registry. k3d names the container `k3d-<REG_SHORT>` and it ALWAYS
# listens on 5000 inside the cluster network; the host port is mapped separately.
# So: push from the host to localhost:$(REG_HOST_PORT); pull / OCIRepository URLs
# in-cluster use $(REG_NAME):$(REG_CLUSTER_PORT).
REG_SHORT          := local-dev-registry
export REG_NAME          := k3d-local-dev-registry
export REG_HOST_PORT     := 5050
export REG_CLUSTER_PORT  := 5000

# Help for cluster commands
k8s-help:
	@echo "Cluster (k3d/k3s) Commands:"
	@echo "--------------------------"
	@echo "  cluster-create   - Create the local registry (if needed) and the k3d cluster"
	@echo "  cluster-delete   - Delete the k3d cluster (keeps the registry / image cache)"
	@echo "  cluster-status   - Show cluster, nodes and registry status"
	@echo "  registry-create  - Create the local image registry (idempotent)"
	@echo "  registry-delete  - Delete the local image registry"
	@echo ""

# Create the local registry if it does not already exist. Host localhost:5050
# maps to the registry's in-cluster port 5000.
registry-create:
	@if docker ps -a --format '{{.Names}}' | grep -qx "$(REG_NAME)"; then \
		echo "==> Registry '$(REG_NAME)' already exists"; \
	else \
		echo "==> Creating local registry '$(REG_NAME)' on localhost:$(REG_HOST_PORT)"; \
		k3d registry create $(REG_SHORT) --port $(REG_HOST_PORT); \
	fi

# Create the cluster from the declarative config. The registry is attached via
# `registries.use` in k3d/cluster.yaml, so it must exist first.
cluster-create: registry-create
	@if k3d cluster list 2>/dev/null | awk 'NR>1{print $$1}' | grep -qx "$(CLUSTER_NAME)"; then \
		echo "==> Cluster '$(CLUSTER_NAME)' already exists"; \
	else \
		echo "==> Creating k3d cluster '$(CLUSTER_NAME)' from $(K3D_CONFIG)"; \
		k3d cluster create --config $(K3D_CONFIG) --wait; \
	fi
	@echo "==> Waiting for the default StorageClass provisioner to be ready"
	@kubectl -n kube-system rollout status deploy/local-path-provisioner --timeout=$(WAIT_TIMEOUT) 2>/dev/null || true
	@kubectl wait --for=condition=Ready nodes --all --timeout=$(WAIT_TIMEOUT)
	@echo "==> Cluster is ready!"
	@kubectl get nodes -o wide

# Delete the cluster but keep the registry (so the image cache persists).
cluster-delete:
	@echo "==> Deleting k3d cluster '$(CLUSTER_NAME)'"
	@k3d cluster delete $(CLUSTER_NAME) 2>/dev/null || echo "  (cluster not found)"
	@echo "==> Registry '$(REG_NAME)' left running (image cache). Remove with: make registry-delete"

# Delete the local registry.
registry-delete:
	@echo "==> Deleting local registry '$(REG_NAME)'"
	@k3d registry delete $(REG_NAME) 2>/dev/null || echo "  (registry not found)"

# Show the state of the cluster + registry.
cluster-status:
	@echo "==> k3d clusters:"
	@k3d cluster list 2>/dev/null || echo "  (k3d not available)"
	@echo "==> Nodes:"
	@kubectl get nodes -o wide 2>/dev/null || echo "  (cluster not running)"
	@echo "==> Registries:"
	@k3d registry list 2>/dev/null || echo "  (none)"
