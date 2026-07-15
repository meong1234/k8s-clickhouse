# Warehouse (dbt + medallion) helpers
# Dev-loop targets for authoring and running the Nimbus dbt project against the
# in-cluster ClickHouse. Namespace / pod vars (CH_NAMESPACE, CH_POD_0/1) are
# defined in scripts/clickhouse.mk and shared via `include scripts/*`.

.PHONY: warehouse-help wh-setup wh-portforward wh-debug wh-build-local wh-test-local

# Project layout
WH_DIR            ?= warehouse
DBT_DIR           ?= $(WH_DIR)/dbt
DBT_PROFILES_DIR  ?= $(DBT_DIR)/profiles
WH_VENV           ?= $(WH_DIR)/.venv

# Pinned so host dev-loop and the (P3) in-cluster image use the same adapter.
WH_DBT_PKG        ?= dbt-clickhouse

# The operator-created CHI service that exposes HTTP 8123 (verify: make ch-status).
WH_CH_SVC         ?= clickhouse-clickhouse

# Run dbt inside the venv, always pointed at this project + profiles dir.
DBT = . $(WH_VENV)/bin/activate && dbt

warehouse-help:
	@echo "Warehouse (dbt) Commands:"
	@echo "------------------------"
	@echo "  wh-setup         - Create warehouse/.venv and install $(WH_DBT_PKG)"
	@echo "  wh-portforward   - Port-forward the CHI HTTP port (svc/$(WH_CH_SVC) 8123) to localhost"
	@echo "  wh-debug         - dbt debug against the port-forwarded ClickHouse (spike helper)"
	@echo "  wh-build-local   - dbt build from the host (needs wh-portforward running)"
	@echo "  wh-test-local    - dbt test from the host (needs wh-portforward running)"
	@echo ""

# Host virtualenv + the dbt-clickhouse adapter (pulls dbt-core + clickhouse-connect).
wh-setup:
	@echo "==> Creating venv at $(WH_VENV) and installing $(WH_DBT_PKG)..."
	@python3 -m venv $(WH_VENV)
	@. $(WH_VENV)/bin/activate && python -m pip install --quiet --upgrade pip && pip install "$(WH_DBT_PKG)"
	@. $(WH_VENV)/bin/activate && dbt --version
	@echo "==> Done. Next: run 'make wh-portforward' (in another shell) then 'make wh-debug'."

# Expose ClickHouse HTTP on localhost:8123 for the host dev loop. Runs in the
# foreground (Ctrl-C to stop) - start it in a separate terminal.
wh-portforward:
	@echo "==> Port-forwarding svc/$(WH_CH_SVC) 8123 -> localhost:8123 (Ctrl-C to stop)..."
	@kubectl -n $(CH_NAMESPACE) port-forward svc/$(WH_CH_SVC) 8123:8123

# Verify dbt can reach ClickHouse as the scoped `dbt` user.
wh-debug:
	@$(DBT) debug --project-dir $(DBT_DIR) --profiles-dir $(DBT_PROFILES_DIR)

# Build all models (host, against the port-forwarded ClickHouse).
wh-build-local:
	@$(DBT) build --project-dir $(DBT_DIR) --profiles-dir $(DBT_PROFILES_DIR)

# Run tests only (host).
wh-test-local:
	@$(DBT) test --project-dir $(DBT_DIR) --profiles-dir $(DBT_PROFILES_DIR)
