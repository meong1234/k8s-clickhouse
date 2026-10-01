# CAS (Altinity content-addressed object storage) helpers
# The ClickHouse side of the experiment in docs/cas-local-s3-plan.md; the object-store
# side is scripts/minio.mk. Namespace / pod / credential vars (CH_NAMESPACE, CH_POD_0,
# CH_POD_1, CH_USER, CH_PASSWORD) come from scripts/clickhouse.mk via `include scripts/*`.

.PHONY: cas-help cas-status cas-demo cas-gc cas-drop-cache cas-evict-member

# Phase B fixture. Wide parts only (min_bytes_for_wide_part=100M would make every part
# compact, which is the opposite of what the plan asks for, so it is set per-table in
# cas-demo rather than here) — these two just size the load.
CAS_DEMO_ROWS  ?= 5000000
CAS_DEMO_TABLE ?= demo.cas_events

# REPLICA=0|1 selects which pod a single-replica target talks to.
REPLICA ?= 0
CAS_POD  = $(if $(filter 1,$(REPLICA)),$(CH_POD_1),$(CH_POD_0))

# Same shape as the CH_EXEC helpers in clickhouse.mk / warehouse.mk. A prefix variable
# rather than a $(call ...) function: every SQL statement below contains commas, and
# make's $(call) splits its arguments on them.
CAS_EXEC     = kubectl -n $(CH_NAMESPACE) exec $(CH_POD_0) -c clickhouse -- clickhouse-client -u $(CH_USER) --password $(CH_PASSWORD) -q
CAS_EXEC_1   = kubectl -n $(CH_NAMESPACE) exec $(CH_POD_1) -c clickhouse -- clickhouse-client -u $(CH_USER) --password $(CH_PASSWORD) -q
CAS_EXEC_SEL = kubectl -n $(CH_NAMESPACE) exec $(CAS_POD) -c clickhouse -- clickhouse-client -u $(CH_USER) --password $(CH_PASSWORD) -q

cas-help:
	@echo "CAS (compute-storage separation) Commands:"
	@echo "-----------------------------------------"
	@echo "  cas-status       - Mounts, disks, parts-by-disk on both replicas, recent cas_log"
	@echo "  cas-demo         - Phase B: create $(CAS_DEMO_TABLE) on the 'cas' policy,"
	@echo "                     insert $(CAS_DEMO_ROWS) rows on replica 0, prove the three claims"
	@echo "  cas-gc           - SYSTEM CAS GC RUN on replica 0, then tail system.cas_gc_log"
	@echo "  cas-drop-cache   - SYSTEM DROP FILESYSTEM CACHE (REPLICA=0|1, default $(REPLICA))"
	@echo "  cas-evict-member - Full replica-rebuild recovery after a lost data PVC: retire"
	@echo "                     the pool member, delete its tombstoned owner anchor, wait for"
	@echo "                     the replacement pod, drop its stale table + database replicas"
	@echo "                     and re-issue CREATE DATABASE. No manual DDL."
	@echo "                     (MEMBER=<cas_server_root_id>)"
	@echo ""

cas-status:
	@echo "==> system.cas_mounts (replica 0) - expect one live mount per pool member:"
	@$(CAS_EXEC) "SELECT disk, server_root_id, state, lifecycle, writer_epoch, renewal_sequence, expires_at, gc_fenced FROM system.cas_mounts ORDER BY disk, server_root_id FORMAT PrettyCompact"
	@echo "==> system.disks (replica 0):"
	@$(CAS_EXEC) "SELECT name, type, formatReadableSize(free_space) AS free, formatReadableSize(total_space) AS total FROM system.disks FORMAT PrettyCompact"
	@for r in 0 1; do \
		echo "==> active parts by disk [replica $$r] (demo.* + nimbus_raw.*):"; \
		POD=$$( [ "$$r" = "1" ] && echo $(CH_POD_1) || echo $(CH_POD_0) ); \
		kubectl -n $(CH_NAMESPACE) exec $$POD -c clickhouse -- clickhouse-client -u $(CH_USER) --password $(CH_PASSWORD) -q "\
			SELECT database, table, disk_name, count() AS parts, sum(rows) AS rows, \
			       formatReadableSize(sum(bytes_on_disk)) AS on_disk, sum(bytes_on_disk) AS bytes \
			FROM system.parts WHERE active AND database IN ('demo','nimbus_raw') \
			GROUP BY database, table, disk_name ORDER BY database, table, disk_name FORMAT PrettyCompact"; \
	done
	@echo "==> last 20 system.cas_log rows (replica 0):"
	@$(CAS_EXEC) "SELECT * FROM system.cas_log ORDER BY event_time_microseconds DESC LIMIT 20 FORMAT Vertical"

# Phase B end to end. Runs on replica 0 and then WAITS for replica 1's replication
# queue to drain: every claim below is about what replica 1 did to get the data, so
# measuring before it has finished would measure nothing.
cas-demo:
	@echo "==> [replica 0] Creating $(CAS_DEMO_TABLE) on storage_policy='cas'..."
	@# `demo` is a Replicated DATABASE, so its schema lives in Keeper and a replica that
	@# came back with an empty data PVC replays this table from the database's DDL log
	@# instead of needing a hand-written replay (plan Sec 5 "Replicated databases").
	@# That also dictates the two things missing from the statements below: no
	@# ON CLUSTER inside the database (Code: 80) and no explicit engine arguments
	@# (Code: 36) - the path comes from the table UUID, which the DDL log carries.
	@$(CAS_EXEC) "CREATE DATABASE IF NOT EXISTS demo ON CLUSTER '{cluster}' ENGINE = Replicated('/clickhouse/databases/{shard}/demo', '{shard}', '{replica}')"
	@# Dropped first, not IF NOT EXISTS: a re-run that appends a second identical batch
	@# would be deduplicated by content hash, and claim 3 would then compare a doubled
	@# row count against an unchanged bucket and read as a pass for the wrong reason.
	@$(CAS_EXEC) "DROP TABLE IF EXISTS $(CAS_DEMO_TABLE) SYNC"
	@$(CAS_EXEC) "CREATE TABLE IF NOT EXISTS $(CAS_DEMO_TABLE) (id UInt64, ts DateTime, payload String) ENGINE = ReplicatedMergeTree PARTITION BY toYYYYMM(ts) ORDER BY id SETTINGS storage_policy = 'cas', min_bytes_for_wide_part = 100000000, old_parts_lifetime = 30"
	@echo "==> [replica 0] Inserting $(CAS_DEMO_ROWS) rows over ~6 months (50-100 byte payloads)..."
	@$(CAS_EXEC) "INSERT INTO $(CAS_DEMO_TABLE) SELECT number AS id, toDateTime('2026-04-01 00:00:00') + toIntervalSecond(intDiv(number * 15552000, $(CAS_DEMO_ROWS))) AS ts, substring(hex(MD5(toString(number))) || hex(MD5(toString(number + 1))) || hex(MD5(toString(number + 2))), 1, 50 + (number % 51)) AS payload FROM numbers($(CAS_DEMO_ROWS)) SETTINGS max_insert_block_size = 500000, min_insert_block_size_rows = 500000"
	@echo "==> [replica 1] Waiting for the replication queue to drain..."
	@for i in $$(seq 1 60); do \
		N=$$(kubectl -n $(CH_NAMESPACE) exec $(CH_POD_1) -c clickhouse -- clickhouse-client -u $(CH_USER) --password $(CH_PASSWORD) -q \
			"SELECT count() FROM system.replication_queue WHERE database='demo' AND table='cas_events'" 2>/dev/null || echo 1); \
		if [ "$${N:-1}" = "0" ]; then echo "  queue empty"; break; fi; \
		echo "  $$N entries pending..."; sleep 5; \
	done; \
	echo "  (5 min cap: if it still says pending, the drill records 'not converged', it does not keep waiting)"
	@echo ""
	@echo "==> GATE B claim 1: same parts, same rows, disk 'cas_cache' on BOTH replicas"
	@for r in 0 1; do \
		POD=$$( [ "$$r" = "1" ] && echo $(CH_POD_1) || echo $(CH_POD_0) ); \
		echo "  --- replica $$r ---"; \
		kubectl -n $(CH_NAMESPACE) exec $$POD -c clickhouse -- clickhouse-client -u $(CH_USER) --password $(CH_PASSWORD) -q "\
			SELECT disk_name, count() AS parts, sum(rows) AS rows, sum(bytes_on_disk) AS bytes, \
			       formatReadableSize(sum(bytes_on_disk)) AS on_disk \
			FROM system.parts WHERE active AND database='demo' AND table='cas_events' \
			GROUP BY disk_name FORMAT PrettyCompact"; \
	done
	@echo ""
	@echo "==> GATE B claim 2: replica 1's cas_log for this table, by event type"
	@echo "    (expect relink-shaped events and NO blob_put for parts it received)"
	@# Unfiltered: cas_log's namespace is the server root + table UUID, not a readable
	@# name, and this pod has only ever written this one CAS table - so the absence of
	@# blob_put in the WHOLE log is a stronger statement than any filter on it.
	@$(CAS_EXEC_1) "SELECT event_type, outcome, count() AS n FROM system.cas_log GROUP BY event_type, outcome ORDER BY n DESC FORMAT PrettyCompact"
	@$(CAS_EXEC_1) "SELECT countIf(event_type = 'blob_put') AS blob_put_MUST_BE_0, countIf(event_type = 'blob_reuse_adopt') AS blob_reuse_adopt FROM system.cas_log FORMAT PrettyCompact"
	@echo ""
	@echo "==> GATE B claim 3: bucket bytes vs ONE replica's sum(bytes_on_disk)"
	@$(CAS_EXEC) "SELECT sum(bytes_on_disk) AS one_replica_bytes, formatReadableSize(sum(bytes_on_disk)) AS pretty FROM system.parts WHERE active AND database='demo' AND table='cas_events' FORMAT PrettyCompact"
	@$(MAKE) --no-print-directory minio-ls | tail -3
	@echo ""
	@echo "==> GATE B claim 4: identical contents on both replicas"
	@for r in 0 1; do \
		POD=$$( [ "$$r" = "1" ] && echo $(CH_POD_1) || echo $(CH_POD_0) ); \
		kubectl -n $(CH_NAMESPACE) exec $$POD -c clickhouse -- clickhouse-client -u $(CH_USER) --password $(CH_PASSWORD) -q "\
			SELECT hostName() AS replica, count() AS rows, sum(cityHash64(payload)) AS payload_hash \
			FROM demo.cas_events FORMAT PrettyCompact"; \
	done

# GC is reachability-based and runs in rounds (mark, then a recheck window before the
# delete), so one invocation may not free everything - run it again if cas_gc_log still
# shows pending work.
cas-gc:
	@echo "==> [replica 0] SYSTEM CAS GC RUN..."
	@$(CAS_EXEC) "SYSTEM CAS GC RUN"
	@echo "==> system.cas_gc_log (last 20):"
	@$(CAS_EXEC) "SELECT * FROM system.cas_gc_log ORDER BY event_time_microseconds DESC LIMIT 20 FORMAT Vertical"

# Drops the local read-through cache in front of CAS, so the next scan has to fetch
# blobs from the object store. REPLICA=0|1.
cas-drop-cache:
	@echo "==> [replica $(REPLICA)] SYSTEM DROP FILESYSTEM CACHE"
	@$(CAS_EXEC_SEL) "SYSTEM DROP FILESYSTEM CACHE"
	@$(CAS_EXEC_SEL) "SELECT cache_name, formatReadableSize(sum(size)) AS cached FROM system.filesystem_cache GROUP BY cache_name FORMAT PrettyCompact"

# Replica-rebuild recovery (Phase C) - the WHOLE recovery, in four phases:
#   1. decommission the lost pool member from a surviving replica,
#   2. delete its tombstoned `owner` anchor (guarded; see below),
#   3. wait for the replacement pod to claim the id and go Ready,
#   4. re-issue the CREATE DATABASE statements, which is the only DDL left to do.
#
# Why each phase exists. A pod that comes back without its data PVC has a new local
# server_uuid, so CAS refuses to let it re-claim its own server root; the decommission
# is what empties the member's subtree so the anchor can be removed safely instead of
# "identity lost over existing data". Two things the error messages make explicit and
# phase 1 therefore honours: there is no FORCE variant, so the victim's mount lease must
# actually lapse first (~60 s at the 30 s TTL), and the command is meant to be re-run
# until the slot is gone.
#
# Phase 2 used to be a manual `mc rm`. It is folded in here because the decommission is
# what makes it legal and because the guard below is a stronger precondition than an
# operator eyeballing a bucket listing: the subtree is listed first and the delete only
# happens if the member's prefix holds exactly ONE object, the `owner` anchor itself.
# Anything else (a live member, an interrupted decommission) leaves more than one object
# there and the target stops with the manual command printed instead. The tombstone's
# purpose - that a decommissioned root cannot SILENTLY resume - is kept: running this
# target is the deliberate operator act the tombstone is asking for.
#
# Phase 4 is what the `Replicated` database engine bought (plan Sec 5 "Replicated
# databases"): the TABLES replay themselves from each database's DDL log, so all that is
# left is the handful of CREATE DATABASE statements, whose own metadata lived on the lost
# PVC. They are idempotent and carry no data. The list mirrors the databases created by
# warehouse/loaders/00_create_raw.sql, warehouse/loaders/30_create_rt.sql and cas-demo.
#
# Phase 4 has to clear TWO stale Keeper registrations first, both left by the lost PVC and
# both reported as `Code: 253 … already exists`. They are the database-engine twin of the
# CAS owner anchor in phase 2 - same cause (a regenerated local identity over surviving
# shared state), three separate registries - and each has its own command:
#
#   a. the TABLE replicas, `/clickhouse/tables/<uuid>/<shard>/replicas/<replica>`. The DDL
#      log replays CREATE TABLE on the rebuilt pod, that CREATE tries to register the
#      replica, and the node is still there: `Error on initialization of <db>: Code: 253 …
#      Replica /clickhouse/tables/<uuid>/0/replicas/<member> already exists`. The database's
#      DDL worker then fails to initialize and NOTHING in that database replays - the
#      failure is one line in the rebuilt pod's log, not an error on any statement here, so
#      it is only visible as a database that stays empty. `SYSTEM DROP REPLICA … FROM
#      DATABASE <db>` on the survivor drops them all for that database in one statement.
#   b. the DATABASE replica, `/clickhouse/databases/<shard>/<db>/replicas/<shard>|<replica>`,
#      keyed by a host ID that carries the server uuid: CREATE DATABASE itself is refused
#      with `Replica host ID: '…:<old uuid>', current host ID: '…:<new uuid>'`.
#      `SYSTEM DROP DATABASE REPLICA` retires it.
#
# Both drops must happen BEFORE the CREATE DATABASE for that database - dropping the
# database replica out from under a database that the rebuilt pod has already attached
# leaves it with no log_ptr node, which is unrecoverable by SQL (the local DROP DATABASE
# needs the same node) and costs a DETACH plus a metadata-file removal on the pod.
MEMBER ?=
CAS_DISK ?= cas
CAS_POOL_PREFIX ?= cas/default
CAS_RECOVER_DBS ?= demo nimbus_raw nimbus_staging nimbus_intermediate nimbus_marts \
                   nimbus_metrics nimbus_stream nimbus_rt

# The member's pod. MEMBER is the cas_server_root_id, which the operator derives from the
# StatefulSet name, so the pod is the member plus the ordinal suffix.
CAS_MEMBER_POD = $(MEMBER)-0

# The member's prefix in the pool - the subtree the decommission empties, and the parent
# of the one `owner` object phase 2 removes.
CAS_MEMBER_ROOT = $(CAS_POOL_PREFIX)/gc/server-roots/$(MEMBER)

# mc against the CAS bucket, same ephemeral-pod pattern as minio-ls. $(1) = the mc
# argument string. MINIO_* come from scripts/minio.mk via `include scripts/*`.
cas_mc = kubectl -n $(MINIO_NS) run cas-mc-$$$$ --rm -i --restart=Never --quiet \
	--image=$(MINIO_MC_IMAGE) --env MC_CONFIG_DIR=/tmp/.mc --command -- \
	sh -c 'mc alias set local $(MINIO_ENDPOINT) $(MINIO_USER) $(MINIO_PASSWORD) > /dev/null && $(1)'

cas-evict-member:
	@test -n "$(MEMBER)" || { echo "ERROR: set MEMBER=<cas_server_root_id>, e.g. MEMBER=chi-clickhouse-default-0-1"; exit 2; }
	@echo "==> [1/4] [replica 0] Retiring pool member '$(MEMBER)' from disk '$(CAS_DISK)'"
	@echo "    (waiting for its mount lease to lapse; no FORCE variant exists)"
	@for i in $$(seq 1 24); do \
		if $(CAS_EXEC) "SYSTEM CAS DROP POOL MEMBER '$(MEMBER)' FROM DISK '$(CAS_DISK)'" 2>&1 | tee /tmp/cas-evict.$$$$ | grep -q "decommission underway"; then \
			echo "  decommission accepted; running GC to hand back its namespaces"; \
			rm -f /tmp/cas-evict.$$$$; break; \
		fi; \
		grep -q "alive or contended" /tmp/cas-evict.$$$$ || { cat /tmp/cas-evict.$$$$; rm -f /tmp/cas-evict.$$$$; break; }; \
		rm -f /tmp/cas-evict.$$$$; echo "  lease still held, retrying in 5s..."; sleep 5; \
	done
	@for i in 1 2 3 4 5 6; do \
		$(CAS_EXEC) "SYSTEM CAS GC RUN" > /dev/null 2>&1; \
		OUT=$$($(CAS_EXEC) "SYSTEM CAS DROP POOL MEMBER '$(MEMBER)' FROM DISK '$(CAS_DISK)'" 2>&1) || true; \
		echo "  round $$i: $$(echo "$$OUT" | tail -1 | cut -c1-140)"; \
		echo "$$OUT" | grep -q "unknown pool member\|tombstoned" && break; \
		sleep 5; \
	done
	@echo "==> system.cas_mounts (the member's slots should be gone):"
	@$(CAS_EXEC) "SELECT disk, server_root_id, state, lifecycle, lifecycle_reason FROM system.cas_mounts ORDER BY disk, server_root_id FORMAT PrettyCompact"
	@echo ""
	@echo "==> [2/4] Tombstoned owner anchor: listing the member's subtree first"
	@$(call cas_mc,mc ls --recursive local/$(MINIO_BUCKET)/$(CAS_MEMBER_ROOT)/) > /tmp/cas-owner.$$$$ 2>&1 || true; \
	 sed 's/^/    /' /tmp/cas-owner.$$$$; \
	 N=$$(grep -c . /tmp/cas-owner.$$$$ || true); rm -f /tmp/cas-owner.$$$$; \
	 if [ "$$N" != "1" ]; then \
	   echo "    STOP: expected exactly 1 object (the tombstoned owner), found $$N."; \
	   echo "    The decommission has not finished (or the member is alive). Re-run this"; \
	   echo "    target, or delete the anchor by hand once the subtree is empty:"; \
	   echo "      mc rm local/$(MINIO_BUCKET)/$(CAS_MEMBER_ROOT)/owner"; \
	   exit 1; \
	 fi; \
	 echo "    exactly 1 object - deleting the anchor"
	@$(call cas_mc,mc rm local/$(MINIO_BUCKET)/$(CAS_MEMBER_ROOT)/owner)
	@echo ""
	@echo "==> [3/4] Waiting for pod $(CAS_MEMBER_POD) to claim '$(MEMBER)' and go Ready"
	@for i in $$(seq 1 60); do \
		R=$$(kubectl -n $(CH_NAMESPACE) get pod $(CAS_MEMBER_POD) -o jsonpath='{.status.containerStatuses[?(@.name=="clickhouse")].ready}' 2>/dev/null); \
		if [ "$$R" = "true" ]; then echo "  Ready after ~$$((i*5))s"; break; fi; \
		echo "  not ready yet ($${R:-missing})..."; sleep 5; \
	done; \
	echo "  (5 min cap: past this the drill records 'not converged', it does not keep waiting)"
	@$(CAS_EXEC) "SELECT disk, server_root_id, state, lifecycle, writer_epoch FROM system.cas_mounts ORDER BY disk, server_root_id FORMAT PrettyCompact"
	@echo ""
	@echo "==> [4/4] Retiring the member's stale table + database replicas, then re-issuing"
	@echo "    CREATE DATABASE (the tables replay from each database's DDL log)"
	@# The shard name comes from the survivor's own macros rather than a hardcoded '0', so
	@# the target keeps working if the topology is ever renamed.
	@SHARD=$$($(CAS_EXEC) "SELECT substitution FROM system.macros WHERE macro = 'shard'" 2>/dev/null); \
	 for db in $(CAS_RECOVER_DBS); do \
		printf '    %-20s ' "$$db"; \
		$(CAS_EXEC) "SYSTEM DROP REPLICA '$(MEMBER)' FROM DATABASE $$db" > /dev/null 2>&1 \
			&& printf 'tables ok, ' || printf 'tables skipped, '; \
		$(CAS_EXEC) "SYSTEM DROP DATABASE REPLICA '$(MEMBER)' FROM SHARD '$$SHARD' FROM DATABASE $$db" > /dev/null 2>&1 \
			&& printf 'db replica ok, ' || printf 'db replica skipped, '; \
		$(CAS_EXEC) "CREATE DATABASE IF NOT EXISTS $$db ON CLUSTER '{cluster}' ENGINE = Replicated('/clickhouse/databases/{shard}/$$db', '{shard}', '{replica}')" > /dev/null 2>&1 \
			&& echo "created" || echo "FAILED (see docs/cas-local-s3-plan.md Sec 5)"; \
	 done
	@# The replay is asynchronous (each database's DDL worker walks its own log), so poll
	@# until the rebuilt replica holds as many objects as the survivor rather than printing
	@# a half-built snapshot. Same 5-minute cap as every other wait in this file.
	@echo "==> Waiting for the DDL log to replay on the rebuilt replica"
	@WANT=$$($(CAS_EXEC) "SELECT count() FROM system.tables t JOIN system.databases d ON d.name = t.database WHERE d.engine = 'Replicated'"); \
	 for i in $$(seq 1 60); do \
		HAVE=$$($(CAS_EXEC_1) "SELECT count() FROM system.tables t JOIN system.databases d ON d.name = t.database WHERE d.engine = 'Replicated'" 2>/dev/null || echo 0); \
		if [ "$${HAVE:-0}" = "$$WANT" ]; then echo "  $$HAVE/$$WANT objects after ~$$((i*5))s"; break; fi; \
		echo "  $${HAVE:-0}/$$WANT objects..."; sleep 5; \
	 done; \
	 echo "  (5 min cap: past this the drill records 'not converged'. A database that stays"; \
	 echo "   empty means its DDL worker failed to initialize - check the rebuilt pod's log"; \
	 echo "   for 'Error on initialization of <db>'.)"
	@echo "==> Objects recovered on the rebuilt replica:"
	@$(CAS_EXEC_1) "SELECT database, engine AS db_engine, count() AS objects FROM system.tables t JOIN system.databases d ON d.name = t.database WHERE d.engine = 'Replicated' GROUP BY database, db_engine ORDER BY database FORMAT PrettyCompact"
