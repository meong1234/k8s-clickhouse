# Warehouse (dbt + medallion) helpers
# Dev-loop targets for authoring and running the Nimbus dbt project against the
# in-cluster ClickHouse. Namespace / pod vars (CH_NAMESPACE, CH_POD_0/1) are
# defined in scripts/clickhouse.mk and shared via `include scripts/*`.

.PHONY: warehouse-help wh-setup wh-portforward wh-debug wh-build-local wh-test-local \
        wh-bronze wh-generate wh-seed wh-counts wh-drop \
        wh-image wh-build wh-test wh-logs wh-all

# Project layout
WH_DIR            ?= warehouse
DBT_DIR           ?= $(WH_DIR)/dbt
DBT_PROFILES_DIR  ?= $(DBT_DIR)/profiles
WH_VENV           ?= $(WH_DIR)/.venv
WH_LOADERS        ?= $(WH_DIR)/loaders
WH_GEN            ?= $(WH_DIR)/generator

# Pinned so host dev-loop and the (P3) in-cluster image use the same adapter.
WH_DBT_PKG        ?= dbt-clickhouse

# The operator-created CHI service that exposes HTTP 8123 (verify: make ch-status).
WH_CH_SVC         ?= clickhouse-clickhouse

# --- P3: in-cluster runtime (Mode B) ----------------------------------------------
# The dbt-runner image. The host builds/pushes to localhost:$(REG_HOST_PORT); the
# CronJob references the in-cluster registry name directly (k3d-local-dev-registry:5000)
# — see kubernetes/analytics/dbt/base/cronjob.yaml. REG_HOST_PORT is shared from
# scripts/k8s.mk via `include scripts/*`.
WH_IMAGE_REPO     ?= nimbus/dbt-runner
WH_IMAGE_TAG      ?= local
WH_IMAGE_LOCAL     = localhost:$(REG_HOST_PORT)/$(WH_IMAGE_REPO):$(WH_IMAGE_TAG)

# How long `make wh-build` / `wh-test` wait for the in-cluster Job before giving up.
WH_JOB_TIMEOUT_S  ?= 600

# --- Synthetic-data scale ---------------------------------------------------------
# SCALE drives both the Python generator (customer count, via --scale) and the
# ClickHouse-native loaders (app-event / card-auth row counts, substituted for __N__).
# medium == laptop-real (~5k customers, ~2M postings, ~10M app events).
SCALE ?= medium
ifeq ($(SCALE),small)
  N_APP_EVENTS ?= 1000000
  N_CARD_AUTHS ?= 100000
else ifeq ($(SCALE),large)
  N_APP_EVENTS ?= 50000000
  N_CARD_AUTHS ?= 5000000
else
  # medium (default)
  N_APP_EVENTS ?= 10000000
  N_CARD_AUTHS ?= 1000000
endif

GEN_SEED ?= 42

# Loading knobs. The pods are capped at 1.5Gi, so big loads are chunked/batched with a
# purge+retry (see warehouse/loaders/load_bronze.sh) — this is the P1 memory-risk
# mitigation. WH_BATCH = rows per native INSERT ... SELECT; WH_CSV_CHUNK = rows per CSV
# insert. Shrink them if you still hit memory pressure on a tighter machine.
WH_BATCH     ?= 1000000
WH_CSV_CHUNK ?= 250000

# The Python-generated (tier-1) bronze tables, loaded from CSV in dependency order.
WH_PY_TABLES = raw_customers raw_kyc_events raw_accounts raw_account_events raw_cards raw_ledger_postings
# All 8 bronze tables (for truncate/count).
WH_ALL_TABLES = $(WH_PY_TABLES) raw_app_events raw_card_authorizations

# clickhouse-client on replica 0 as admin (DDL/bulk-load bootstrap runs as admin).
# -c clickhouse pins the container (the pod also has a clickhouse-log sidecar), which
# keeps kubectl from printing a "Defaulted container ..." line to stderr on every call.
CH_EXEC   = kubectl -n $(CH_NAMESPACE) exec $(CH_POD_0) -c clickhouse -- clickhouse-client -u $(CH_USER) --password $(CH_PASSWORD)
CH_EXEC_I = kubectl -n $(CH_NAMESPACE) exec -i $(CH_POD_0) -c clickhouse -- clickhouse-client -u $(CH_USER) --password $(CH_PASSWORD)

# Run dbt inside the venv, always pointed at this project + profiles dir.
DBT = . $(WH_VENV)/bin/activate && dbt

# Create a Job from a suspended CronJob and wait for it (Complete or Failed), streaming
# logs at the end. $(1)=cronjob name, $(2)=job name prefix. Used by wh-build/wh-test.
# We poll for BOTH conditions (not `kubectl wait --for=complete`, which would block until
# the full timeout on a FAILED job) so failures surface promptly with a non-zero exit.
define wh_run_job
	set -e; \
	JOB=$(2)-$$(date +%s); \
	echo "==> Creating Job $$JOB from cronjob/$(1) (ns $(CH_NAMESPACE))..."; \
	kubectl -n $(CH_NAMESPACE) create job $$JOB --from=cronjob/$(1); \
	echo "==> Waiting up to $(WH_JOB_TIMEOUT_S)s for $$JOB..."; \
	for i in $$(seq 1 $$(( $(WH_JOB_TIMEOUT_S) / 3 )) ); do \
	  C=$$(kubectl -n $(CH_NAMESPACE) get job/$$JOB -o jsonpath='{.status.conditions[?(@.type=="Complete")].status}' 2>/dev/null); \
	  F=$$(kubectl -n $(CH_NAMESPACE) get job/$$JOB -o jsonpath='{.status.conditions[?(@.type=="Failed")].status}' 2>/dev/null); \
	  if [ "$$C" = "True" ]; then \
	    echo "==> $$JOB Complete. Logs:"; \
	    kubectl -n $(CH_NAMESPACE) logs job/$$JOB --tail=-1; \
	    exit 0; \
	  fi; \
	  if [ "$$F" = "True" ]; then \
	    echo "!! $$JOB Failed. Logs:"; \
	    kubectl -n $(CH_NAMESPACE) logs job/$$JOB --tail=-1 || true; \
	    kubectl -n $(CH_NAMESPACE) describe job/$$JOB | tail -n 20; \
	    exit 1; \
	  fi; \
	  sleep 3; \
	done; \
	echo "!! $$JOB did not finish within $(WH_JOB_TIMEOUT_S)s. Recent logs:"; \
	kubectl -n $(CH_NAMESPACE) logs job/$$JOB --tail=200 || true; \
	exit 1
endef

warehouse-help:
	@echo "Warehouse (dbt) Commands:"
	@echo "------------------------"
	@echo "  wh-setup         - Create warehouse/.venv and install $(WH_DBT_PKG)"
	@echo "  wh-portforward   - Port-forward the CHI HTTP port (svc/$(WH_CH_SVC) 8123) to localhost"
	@echo "  wh-debug         - dbt debug against the port-forwarded ClickHouse (spike helper)"
	@echo "  wh-build-local   - dbt build from the host (needs wh-portforward running)"
	@echo "  wh-test-local    - dbt test from the host (needs wh-portforward running)"
	@echo ""
	@echo "  In-cluster runtime, the v1 contract / 'Mode B' (P3):"
	@echo "  wh-image         - Build + push the dbt-runner image ($(WH_IMAGE_LOCAL))"
	@echo "  wh-build         - Run the dbt build in-cluster (Job from cronjob/dbt-runner)"
	@echo "  wh-test          - Run dbt test in-cluster (Job from cronjob/dbt-tester)"
	@echo "  wh-logs          - Tail the most recent dbt-runner pod (JOB=<name> to target one)"
	@echo "  wh-all           - Full: wh-image wh-bronze wh-generate wh-build wh-test"
	@echo ""
	@echo "  Bronze + synthetic data (P1):"
	@echo "  wh-bronze        - Apply bronze DDL ON CLUSTER (loaders/00_create_raw.sql, as admin)"
	@echo "  wh-generate      - Generate + load all bronze data (SCALE=$(SCALE)); prints row counts"
	@echo "  wh-seed          - dbt seed the static dimension CSVs (needs wh-portforward)"
	@echo "  wh-counts        - Show bronze row counts"
	@echo "  wh-drop          - TRUNCATE all bronze tables ON CLUSTER (keeps the schema)"
	@echo "                     Vars: SCALE={small|medium|large}, GEN_SEED=$(GEN_SEED)"
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

# --- P1: bronze DDL + synthetic data ---------------------------------------------

# Create the nimbus_* databases + the 8 bronze tables ON CLUSTER. Idempotent
# (everything is IF NOT EXISTS). Run as admin: this is DDL bootstrap, not a dbt model.
wh-bronze:
	@echo "==> Applying bronze DDL ON CLUSTER '$(CH_CLUSTER)' (as $(CH_USER))..."
	@cat $(WH_LOADERS)/00_create_raw.sql | $(CH_EXEC_I) --multiquery
	@echo "==> Bronze tables in nimbus_raw:"
	@$(CH_EXEC) -q "SELECT name, engine FROM system.tables WHERE database='nimbus_raw' ORDER BY name FORMAT PrettyCompact"

# Truncate all bronze tables (keeps schema) — makes wh-generate re-runnable and the
# Python-tier checksum deterministic across reloads.
wh-drop:
	@echo "==> Truncating bronze tables ON CLUSTER '$(CH_CLUSTER)'..."
	@for t in $(WH_ALL_TABLES); do \
		$(CH_EXEC) -q "TRUNCATE TABLE IF EXISTS nimbus_raw.$$t ON CLUSTER '{cluster}'" >/dev/null; \
	done
	@echo "    done."

# Full bronze populate: (1) Python tier -> CSV, then (2) load everything (CSV chunks +
# native batches) via loaders/load_bronze.sh, which truncates first (idempotent) and is
# memory-defensive. Row counts are printed at the end by the script.
wh-generate:
	@echo "==> [tier 1] Generating Python core (scale=$(SCALE), seed=$(GEN_SEED))..."
	@python3 $(WH_GEN)/generate.py --scale $(SCALE) --seed $(GEN_SEED) --out $(WH_GEN)/out
	@echo "==> Loading bronze (SCALE=$(SCALE): app=$(N_APP_EVENTS), auths=$(N_CARD_AUTHS))..."
	@CH_NS='$(CH_NAMESPACE)' CH_POD='$(CH_POD_0)' CH_USER='$(CH_USER)' CH_PW='$(CH_PASSWORD)' \
	 LOADERS_DIR='$(WH_LOADERS)' GEN_OUT='$(WH_GEN)/out' \
	 N_APP_EVENTS='$(N_APP_EVENTS)' N_CARD_AUTHS='$(N_CARD_AUTHS)' \
	 BATCH='$(WH_BATCH)' CSV_CHUNK='$(WH_CSV_CHUNK)' \
	 bash $(WH_LOADERS)/load_bronze.sh

# Row counts for all bronze tables (post-load sanity).
wh-counts:
	@echo "==> Bronze row counts (nimbus_raw):"
	@for t in $(WH_ALL_TABLES); do \
		printf '    %-28s ' "$$t"; \
		$(CH_EXEC) -q "SELECT count() FROM nimbus_raw.$$t"; \
	done

# Load the static dimension seeds via dbt (host; needs wh-portforward).
wh-seed:
	@$(DBT) seed --project-dir $(DBT_DIR) --profiles-dir $(DBT_PROFILES_DIR)

# --- P3: in-cluster runtime (Mode B, the v1 delivery contract) --------------------
# dbt runs INSIDE the cluster as a Flux-managed Job, talking to the CHI service directly
# (no host, no port-forward). The image bakes in the dbt project; credentials come from
# the dbt-credentials Secret. The host targets above (Mode A) remain a dev convenience.

# Build the dbt-runner image from the repo root (the Dockerfile COPYs warehouse/dbt) and
# push it to the local registry. Same clean single-platform push as images-push-all;
# nodes pull it via the k3d registry config (the manifest references the registry directly).
wh-image:
	@echo "==> Building dbt-runner image $(WH_IMAGE_LOCAL) (context: repo root)..."
	@if ! docker ps | grep -q $(REG_NAME); then \
		echo "Error: registry '$(REG_NAME)' is not running - run 'make cluster-create' first"; \
		exit 1; \
	fi
	@docker build -t $(WH_IMAGE_LOCAL) -f $(WH_DIR)/Dockerfile .
	@echo "==> Pushing $(WH_IMAGE_LOCAL)..."
	@docker push $(WH_IMAGE_LOCAL)
	@echo "==> Pushed. In-cluster ref: $(REG_NAME):$(REG_CLUSTER_PORT)/$(WH_IMAGE_REPO):$(WH_IMAGE_TAG)"
	@echo "    (CronJob pulls Always, so the next 'make wh-build' uses this build.)"

# Run the P2 DAG in-cluster: create a one-off Job from the suspended dbt-runner CronJob,
# wait for it, and stream its logs. Fails loudly (non-zero) on Job failure/timeout.
wh-build:
	@$(call wh_run_job,dbt-runner,dbt-build)

# Same, but from the dbt-tester CronJob (which runs `dbt test`). A separate CronJob is
# used because `kubectl create job --from=cronjob` cannot override the container command.
wh-test:
	@$(call wh_run_job,dbt-tester,dbt-test)

# Tail logs of the most recent dbt-runner pod(s). Override the selector with JOB=<name>
# to target a specific Job (e.g. a dbt-test-* run); label app=dbt-tester for the tester.
JOB ?=
wh-logs:
	@if [ -n "$(JOB)" ]; then \
		kubectl -n $(CH_NAMESPACE) logs -f job/$(JOB) --tail=200; \
	else \
		echo "==> Tailing most recent dbt-runner pod (app=dbt-runner). Override: make wh-logs JOB=<job-name>"; \
		kubectl -n $(CH_NAMESPACE) logs -l app=dbt-runner --tail=200 --prefix; \
	fi

# The full end-to-end from a clean state: build the image, (re)create bronze schema + data,
# then run the build and tests in-cluster. First-version convenience target.
wh-all: wh-image wh-bronze wh-generate wh-build wh-test
	@echo "==> wh-all complete: image built, bronze loaded, in-cluster build + tests green."
