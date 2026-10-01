# ClickHouse operational helpers
# Convenience targets for inspecting and exercising the ClickHouse cluster.

.PHONY: clickhouse-help ch-status keeper-status ch-client ch-demo ch-password crds-vendor

# Operator version whose CRDs are vendored under kubernetes/infra/crds
OPERATOR_VERSION ?= 0.27.1

# Namespaces / resource names (must match the manifests under kubernetes/analytics)
CH_NAMESPACE   ?= clickhouse
CHI_NAME       ?= clickhouse
CHK_NAME       ?= clickhouse-keeper
CH_CLUSTER     ?= default

# Admin credentials (local/dev only - see clickhouse-credentials.yaml)
CH_USER        ?= admin
CH_PASSWORD    ?= admin123

# A pod exec helper: run clickhouse-client on the first replica
CH_POD_0 = chi-$(CHI_NAME)-$(CH_CLUSTER)-0-0-0
CH_POD_1 = chi-$(CHI_NAME)-$(CH_CLUSTER)-0-1-0

clickhouse-help:
	@echo "ClickHouse Commands:"
	@echo "-------------------"
	@echo "  ch-status        - Show CHI, CHK, pods, PVCs and services"
	@echo "  keeper-status    - Check the 3-node Keeper quorum (ruok / mntr)"
	@echo "  ch-client        - Open an interactive clickhouse-client on replica 0"
	@echo "  ch-demo          - Create a ReplicatedMergeTree table ON CLUSTER, insert on"
	@echo "                     replica 0 and read it back from replica 1 (proves replication)"
	@echo "  ch-password      - Print the SHA256 hash for a PASSWORD=... value"
	@echo "  crds-vendor      - Re-download the Altinity CRDs (OPERATOR_VERSION=$(OPERATOR_VERSION))"
	@echo ""

# Show the state of the whole ClickHouse stack
ch-status:
	@echo "==> ClickHouseInstallation:"
	@kubectl -n $(CH_NAMESPACE) get chi $(CHI_NAME) -o wide 2>/dev/null || echo "  (not created yet)"
	@echo "==> ClickHouseKeeperInstallation:"
	@kubectl -n $(CH_NAMESPACE) get chk $(CHK_NAME) 2>/dev/null || echo "  (not created yet)"
	@echo "==> Pods:"
	@kubectl -n $(CH_NAMESPACE) get pods -o wide
	@echo "==> Persistent Volume Claims:"
	@kubectl -n $(CH_NAMESPACE) get pvc
	@echo "==> Services:"
	@kubectl -n $(CH_NAMESPACE) get svc

# Verify the Keeper quorum: one node must be leader, the rest followers.
# Uses a bash /dev/tcp probe to send the 4-letter-word 'mntr' command (the
# keeper image does not ship netcat), and falls back to reporting pod readiness.
keeper-status:
	@echo "==> Keeper pods:"
	@kubectl -n $(CH_NAMESPACE) get pods -l "clickhouse-keeper.altinity.com/chk=$(CHK_NAME)" -o wide 2>/dev/null \
		|| kubectl -n $(CH_NAMESPACE) get pods -o wide | grep chk-
	@echo "==> Keeper 'mntr' (expect exactly one 'leader' + two 'follower', synced_followers=2):"
	@for i in 0 1 2; do \
		POD="chk-$(CHK_NAME)-$(CH_CLUSTER)-0-$$i-0"; \
		echo "  --- $$POD ---"; \
		kubectl -n $(CH_NAMESPACE) exec $$POD -- bash -c \
			'exec 3<>/dev/tcp/127.0.0.1/2181; printf "mntr" >&3; timeout 2 cat <&3' 2>/dev/null \
			| grep -E "zk_server_state|zk_followers|zk_synced_followers" \
			|| echo "  (4lw probe failed - check readiness above / pod logs)"; \
	done
	@echo "==> Cross-check from ClickHouse (system.zookeeper root, proves CH<->Keeper):"
	@kubectl -n $(CH_NAMESPACE) exec $(CH_POD_0) -- clickhouse-client -u $(CH_USER) --password $(CH_PASSWORD) -q "\
		SELECT name FROM system.zookeeper WHERE path = '/' FORMAT PrettyCompact;" 2>/dev/null \
		|| echo "  (ClickHouse not ready yet)"

# Open an interactive clickhouse-client session on replica 0
ch-client:
	@kubectl -n $(CH_NAMESPACE) exec -it $(CH_POD_0) -- \
		clickhouse-client -u $(CH_USER) --password $(CH_PASSWORD)

# Prove replication end-to-end:
#   1. create a ReplicatedMergeTree table on every node of the cluster
#   2. insert rows on replica 0
#   3. read the same rows back from replica 1
ch-demo:
	@echo "==> Waiting for both replicas to register in cluster '$(CH_CLUSTER)' (avoids a partial ON CLUSTER)..."
	@for i in $$(seq 1 30); do \
		N=$$(kubectl -n $(CH_NAMESPACE) exec $(CH_POD_0) -- clickhouse-client -u $(CH_USER) --password $(CH_PASSWORD) -q \
			"SELECT count() FROM system.clusters WHERE cluster='$(CH_CLUSTER)'" 2>/dev/null || echo 0); \
		if [ "$${N:-0}" -ge 2 ]; then echo "  both replicas registered ($$N)"; break; fi; \
		echo "  waiting for replicas to register ($${N:-0}/2)..."; sleep 2; \
	done
	@echo "==> [replica 0] Creating replicated table ON CLUSTER '$(CH_CLUSTER)'..."
	@kubectl -n $(CH_NAMESPACE) exec $(CH_POD_0) -- clickhouse-client -u $(CH_USER) --password $(CH_PASSWORD) -q "\
		CREATE DATABASE IF NOT EXISTS demo ON CLUSTER '{cluster}';"
	@kubectl -n $(CH_NAMESPACE) exec $(CH_POD_0) -- clickhouse-client -u $(CH_USER) --password $(CH_PASSWORD) -q "\
		CREATE TABLE IF NOT EXISTS demo.events ON CLUSTER '{cluster}' (\
			id UInt64, ts DateTime DEFAULT now(), msg String\
		) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/demo/events', '{replica}')\
		ORDER BY id;"
	@echo "==> [replica 0] Inserting 5 rows..."
	@kubectl -n $(CH_NAMESPACE) exec $(CH_POD_0) -- clickhouse-client -u $(CH_USER) --password $(CH_PASSWORD) -q "\
		INSERT INTO demo.events (id, msg) SELECT number, 'hello-'||toString(number) FROM numbers(5);"
	@echo "==> [replica 0] Row count:"
	@kubectl -n $(CH_NAMESPACE) exec $(CH_POD_0) -- clickhouse-client -u $(CH_USER) --password $(CH_PASSWORD) -q "\
		SELECT hostName() AS replica, count() AS rows FROM demo.events;"
	@echo "==> [replica 1] Reading the SAME data back from the OTHER replica (proves replication):"
	@kubectl -n $(CH_NAMESPACE) exec $(CH_POD_1) -- clickhouse-client -u $(CH_USER) --password $(CH_PASSWORD) -q "\
		SELECT hostName() AS replica, count() AS rows FROM demo.events;"
	@echo "==> [replica 1] Rows:"
	@kubectl -n $(CH_NAMESPACE) exec $(CH_POD_1) -- clickhouse-client -u $(CH_USER) --password $(CH_PASSWORD) -q "\
		SELECT * FROM demo.events ORDER BY id FORMAT PrettyCompact;"

# Helper: compute the SHA256 hash to drop into clickhouse-credentials.yaml
#   make ch-password PASSWORD=mysecret
ch-password:
	@if [ -z "$(PASSWORD)" ]; then echo "Usage: make ch-password PASSWORD=yourpassword"; exit 1; fi
	@printf '%s' '$(PASSWORD)' | shasum -a 256 | awk '{print $$1}'

# Re-vendor the Altinity operator CRDs into kubernetes/infra/crds at a pinned
# operator version:  make crds-vendor OPERATOR_VERSION=0.27.1
crds-vendor:
	@echo "==> Vendoring Altinity CRDs for operator release-$(OPERATOR_VERSION)..."
	@API="https://api.github.com/repos/Altinity/clickhouse-operator/contents/deploy/helm/clickhouse-operator/crds"; \
	REF="release-$(OPERATOR_VERSION)"; \
	DEST="kubernetes/infra/crds"; \
	for pair in \
		"CustomResourceDefinition-clickhouseinstallations.clickhouse.altinity.com.yaml:clickhouseinstallations.yaml" \
		"CustomResourceDefinition-clickhouseinstallationtemplates.clickhouse.altinity.com.yaml:clickhouseinstallationtemplates.yaml" \
		"CustomResourceDefinition-clickhousekeeperinstallations.clickhouse-keeper.altinity.com.yaml:clickhousekeeperinstallations.yaml" \
		"CustomResourceDefinition-clickhouseoperatorconfigurations.clickhouse.altinity.com.yaml:clickhouseoperatorconfigurations.yaml" ; do \
		SRC=$${pair%%:*}; DST=$${pair##*:}; \
		echo "  - $$DST"; \
		curl -fsSL "$$API/$$SRC?ref=$$REF" \
			| python3 -c "import sys,json,base64; d=json.load(sys.stdin); sys.stdout.write(base64.b64decode(d['content']).decode())" \
			> "$$DEST/$$DST" || { echo "    failed to fetch $$SRC"; exit 1; }; \
	done
	@echo "==> Done. Review the diff and commit."
