# Plan: Altinity CAS (compute–storage separation) on local S3

Branch: `cas-local-s3`. Goal: run the existing 1-shard × 2-replica cluster with
MergeTree data on **CAS** (Altinity's content-addressed object storage backend),
backed by an **in-cluster S3** (MinIO), and prove the three claims of the
[Altinity blog post](https://altinity.com/blog/introducing-cas-drop-in-compute-storage-separation-for-clickhouse-mergetree-tables):
one copy of the bytes shared by both replicas, replication by *relink* instead of
re-upload, and reachability-based GC that actually deletes.

This is an **experiment**, not a production change. Nothing here merges to `main`
unless the gates below pass and we decide to keep it.

## 0. What CAS is (in one paragraph) and what it needs from us

CAS is a new `metadata_type` for `object_storage` disks in **Altinity Antalya 26.6+**
(experimental; the on-disk format changed in 26.6.4, so that is the *minimum*).
Blobs are addressed by content hash, so a part written by replica 0 is published
once; replica 1 gets it via Keeper's normal replication log but "relinks" the
existing blobs instead of fetching bytes. Keeper still does everything it does
today (replication log, part-set consensus) — CAS is not SharedMergeTree and does
**not** remove Keeper. Coordination for the storage side is done with **conditional
writes on the object store** (`If-None-Match`/`If-Match` PUTs), ranged reads,
resumable listing and stable object tokens. That last sentence is the whole risk of
this experiment: only AWS S3 and GCS are validated; a local S3 has to pass
ClickHouse's boot-time capability probe (`skip_access_check = false`, keep it that way).

Constraints the post is explicit about, which shape the plan:

- Insert path is not optimised yet ("do not write frequently to CAS disks"). So
  CAS is a **cold tier** here: local `default` volume first, `TTL … TO VOLUME 'cas'`
  moves parts down. Hot rollups and the 3-hourly rebuilt marts stay local.
- Raise `min_bytes_for_wide_part` (≥100M) / `min_level_for_wide_part` (3–4) on CAS
  tables so low-level parts stay compact (fewer objects).
- `http_keep_alive_timeout=30` + `http_keep_alive_max_requests=10000` on the disk
  are required, otherwise lease renewals get starved by TIME_WAIT churn.

## 1. Changes on this branch (what gets added / edited)

### 1.1 Image bump — server only

| File | Change |
| --- | --- |
| `scripts/images.mk` | `CLICKHOUSE_SERVER_IMAGE := altinity/clickhouse-server:26.6.4.20001.altinityantalya`; add `pgsty/minio` and `pgsty/mc` to `PROJECT_IMAGES`. |
| `kubernetes/analytics/clickhouse/base/clickhouse.yaml` | Same server tag in the pod template. |

Keeper stays on `26.3.16.10001.altinitystable`. The Keeper wire protocol is stable
across these lines and keeping it on LTS isolates the experiment to the server.
If the server refuses to talk to an older Keeper (unexpected), bump Keeper to
`altinity/clickhouse-keeper:26.6.4.20001.altinityantalya` in `keeper.yaml` + `images.mk`.

Operator `0.27.1` needs no change: everything below is plain `spec.configuration.files`
and `settings`, which it already renders.

### 1.2 In-cluster S3 — MinIO as a new Flux layer

New directory `kubernetes/analytics/minio/{base,local}`:

- `namespace.yaml` — `minio`.
- `secret.yaml` (local overlay only, like `clickhouse-credentials.yaml`) — root
  user/password and a dedicated `clickhouse` access key.
- `statefulset.yaml` — 1 replica, PVC `20Gi` on `local-path`, ports 9000 (S3) /
  9001 (console), `args: ["server", "/data", "--console-address", ":9001"]`.
  Single node is fine: we are testing CAS semantics, not MinIO durability.
- `service.yaml` — ClusterIP `minio.minio.svc.cluster.local:9000`.
- `bucket-job.yaml` — a `pgsty/mc` Job that creates bucket `clickhouse-cas`
  (idempotent `mc mb --ignore-existing`). ClickHouse does not create buckets.

Flux wiring in `kubernetes/clusters/local/analytics.yaml`: a new Kustomization
`minio` (`dependsOn: clickhouse-operator-ks`, `wait: true` — a StatefulSet has a
real Ready condition), and `clickhouse-chi` gains `dependsOn: minio`. Reconcile order
becomes `infra → operator → keeper + minio → chi → dbt`.

**Image source.** `minio/minio` and `minio/mc` no longer exist: MinIO stopped
publishing community images in Oct 2025 and the Docker Hub repos were removed in
Sep 2026. The binary that runs here is **`pgsty/silo:RELEASE.2026-09-16T00-00-00Z`**
— Silo, the renamed successor of the `pgsty/minio` community fork: same lineage,
same `server /data --console-address :9001` args, `pgsty/mc` still talks to it.

We started on `pgsty/minio:RELEASE.2026-08-04T00-00-00Z` and it passed the Phase A
probe as originally written — and ClickHouse still refused to open the pool. The
probe only covered conditional **PUT** and ranged reads; CAS also requires
`If-Match` on **DeleteObject**, because a mount lease is released by deleting the
token that holds it, and a backend that deletes unconditionally lets a fenced
server drop the live owner's lease. MinIO accepts the delete and the boot probe
says so: `CasProbe: remove with a stale incarnation was not rejected`. Silo ships
single-object conditional DELETE (412 on an ETag mismatch); its batch
`DeleteObjects` still ignores per-item ETags, which CAS probes separately and
falls back from. `make minio-probe` now covers all of it (checks e1–e3).

The directory, namespace and service stay named `minio`: the name identifies the
layer, not the binary, and the layer is swappable by design.

Why this family rather than something lighter: Altinity's own CAS test suite runs
against MinIO, so it is the only local backend with any upstream coverage. See §5
for the alternatives and why each was not chosen first.

### 1.3 CAS disk + policies on the CHI

Added to `spec.configuration.files` in `clickhouse.yaml` (base), credentials via
env from the MinIO secret using the same `from_env` idea as the admin password:

```xml
<!-- config.d/cas_disk.xml -->
<clickhouse>
  <storage_configuration>
    <disks>
      <cas>
        <type>object_storage</type>
        <object_storage_type>s3</object_storage_type>
        <metadata_type>cas</metadata_type>
        <cas_server_root_id>{replica}</cas_server_root_id>
        <endpoint>http://minio.minio.svc.cluster.local:9000/clickhouse-cas/cas/{cluster}/</endpoint>
        <access_key_id from_env="CAS_S3_ACCESS_KEY_ID"/>
        <secret_access_key from_env="CAS_S3_SECRET_ACCESS_KEY"/>
        <region>us-east-1</region>
        <http_keep_alive_timeout>30</http_keep_alive_timeout>
        <http_keep_alive_max_requests>10000</http_keep_alive_max_requests>
        <cas_blob_hash>xxh3-128</cas_blob_hash>   <!-- fixed at pool creation; pick deliberately -->
      </cas>
      <cas_cache>
        <type>cache</type>
        <disk>cas</disk>
        <path>/var/lib/clickhouse/cas_cache/</path>   <!-- lives on the existing 10Gi data PVC -->
        <max_size>3Gi</max_size>
      </cas_cache>
    </disks>
    <policies>
      <cas>               <!-- pure CAS: demo / drills -->
        <volumes><main><disk>cas_cache</disk></main></volumes>
      </cas>
      <cas_tiered>        <!-- local hot → CAS cold: the bronze tables -->
        <volumes>
          <hot><disk>default</disk><volume_priority>1</volume_priority></hot>
          <cas><disk>cas_cache</disk></cas>
        </volumes>
      </cas_tiered>
    </policies>
  </storage_configuration>
</clickhouse>
```

Pod template: two `env` entries with `secretKeyRef` into the MinIO secret. The
`{replica}` and `{cluster}` macros are already generated per pod by the operator.
`cas_server_root_id` must be unique **and stable** per pool member — the operator's
replica name (`chi-clickhouse-default-0-1`) satisfies both.

Leave every `cas_*` tuning knob at default except the hash. Mount-lease TTL /
renew period must be identical on all members and can only change with the whole
pool stopped, so do not touch them mid-experiment.

### 1.4 Which tables move

| Layer | Policy | Why |
| --- | --- | --- |
| `demo.*` (ch-demo, drills) | `cas` | Pure-CAS so every effect is visible immediately. |
| Bronze `nimbus_raw.raw_*` (`warehouse/loaders/00_create_raw.sql`) | `cas_tiered` + `TTL <ts> + INTERVAL 90 DAY TO VOLUME 'cas'` | Append-only, partitioned monthly over 18 months, the bulk of the bytes. Matches "local + TTL moves". |
| `nimbus_stream`, `nimbus_rt` | unchanged (local) | MV cascade writes on every insert — exactly what CAS says not to do. |
| `nimbus_marts` / `nimbus_metrics` | unchanged, optional later | Rebuilt every 3 h; heavy churn. Optionally try `fct_*` via `settings={'storage_policy':'cas_tiered'}` in dbt model config in Phase D. |

Bronze DDL edit: `SETTINGS storage_policy = 'cas_tiered', min_bytes_for_wide_part = 100000000, min_level_for_wide_part = 3`.
Table TTL uses the event timestamp column each table already has (`ingested_at`
for the replacing tables, `posting_ts` / `auth_ts` / `event_ts` for the event tables).

### 1.5 Make targets (`scripts/clickhouse.mk` or a new `scripts/cas.mk`)

- `cas-status` — `system.cas_mounts`, `system.disks`, parts-by-disk for `demo` and
  `nimbus_raw`, last 20 rows of `system.cas_log`.
- `cas-demo` — the Phase B script below, end to end.
- `cas-gc` — `SYSTEM CAS GC RUN` on replica 0 then tail `system.cas_gc_log`.
- `minio-console` — `kubectl port-forward svc/minio 9001` and print the creds.
- `minio-ls` — run `mc ls --recursive --summarize` in an ephemeral `minio/mc` pod
  (object count + bytes; the number we compare against `system.parts`).

## 2. Execution phases and gates

Each phase ends in a gate. A failed gate stops the plan and is written up in §4.

### Phase A — Does the object store satisfy CAS? (no ClickHouse changes yet)

1. Deploy MinIO layer, push artifacts, wait for the bucket Job.
2. From an `mc` pod, probe the two primitives CAS depends on:
   - `PUT` with `If-None-Match: *` twice → second must return **412**.
   - `PUT` with `If-Match: <etag>` using a stale ETag → **412**.
   - `HEAD`/`GET` with `Range:` → **206**, and ETag stable across reads.
   - `DELETE` with a stale `If-Match` → **412** *and the object still there*; with the
     current `If-Match` → **204**. This is the one the first pass missed, and it is the
     one CAS fails the whole pool over: a mount lease is released by deleting its token.
   - Batch `POST ?delete` carrying per-item `<ETag>`: informational. Neither MinIO nor
     Silo honours it; CAS probes bulk delete separately and falls back.
   Plain `curl --aws-sigv4` against the service is enough.
3. **Gate A:** every hard check behaves as S3 does. If not, swap the backend (candidates in
   §5) before touching the CHI. Never set `skip_access_check`.

### Phase B — Single-table smoke on the `demo` DB

1. Apply the image bump + `cas_disk.xml`, push artifacts, let the operator roll
   both replicas. Watch `clickhouse-server.err.log` for the CAS boot probe.
2. `SELECT * FROM system.cas_mounts` → two mounts (one per `{replica}`), leases renewing.
3. `CREATE TABLE demo.cas_events ON CLUSTER '{cluster}' … ReplicatedMergeTree
   SETTINGS storage_policy='cas', min_bytes_for_wide_part='100M'` then insert
   ~5M rows on replica 0. The fixture also sets `old_parts_lifetime = 30` — a
   **drill-speed knob, not a production setting**: CAS GC is reachability-based, so
   a merged-away part's blobs are correctly spared for as long as the outdated part
   is still in the server's part set, and the 8-minute default turns every GC drill
   into an 8-minute wait for an answer that is already decided.
4. **Gate B (the three claims):**
   - `system.parts WHERE active AND table='cas_events' GROUP BY disk_name` shows
     `cas_cache` on **both** replicas and the same rows.
   - `system.cas_log` on replica 1 shows relink events, not `blob_put`, for those parts.
   - `make minio-ls` bytes ≈ **one** replica's `sum(bytes_on_disk)`, not two.

### Phase C — Lifecycle drills (pure CAS, `demo`)

| Drill | Do | Expect |
| --- | --- | --- |
| Merge | `OPTIMIZE TABLE … FINAL` | New merged part published once; old blobs still present until GC. |
| GC | `ALTER … DROP PARTITION`, then `SYSTEM CAS GC RUN`, check `system.cas_gc_log` | Unreferenced blobs deleted after the mark/recheck rounds; bucket bytes drop. |
| Replica rebuild | Delete replica 1's pod **and** its `data` PVC; let the operator recreate it | Table comes back by relink; `cas_log` on the new pod shows no re-upload. |
| Cache cold | `SYSTEM DROP FILESYSTEM CACHE` on replica 1, run a full scan | Correct results; `system.cas_log` shows blob reads; cache refills. |
| Storage outage | Scale MinIO to 0, query cached vs uncached parts, insert | Cached reads work, uncached fail with a clear S3 error, insert fails (no local volume in `cas` policy); everything recovers when MinIO returns, leases re-established. |
| Restart storm | `kubectl rollout restart` the CHI StatefulSets | Mount leases re-acquired within `cas_mount_lease_ttl_ms` (30 s); no stale-mount fencing of the healthy peer. |

**Gate C:** every row's "Expect" holds. The outage row is allowed to be ugly but must
be *recoverable without manual cleanup*.

### Phase D — The real workload (bronze tiered)

1. `make wh-drop`, apply the edited `00_create_raw.sql`, `make wh-bronze` at
   `SCALE=medium`. Loads land on the local `default` volume (fast path).
2. Watch background moves: `system.parts` for `nimbus_raw` should shift older
   monthly partitions to `cas_cache` as the TTL move runs (or force with
   `ALTER TABLE … MOVE PARTITION … TO VOLUME 'cas'`).
3. `make wh-build` + `make wh-test` — full batch spine and the realtime deploy.
4. **Gate D:** all dbt tests still green; record wall-clock for `wh-bronze`,
   `wh-build` on this branch vs `main` (same SCALE, same laptop) in §4. A slowdown
   on batch reads over cold parts is expected and acceptable if bounded; a
   correctness difference is not.

### Phase E — Write-up and decision

Fill §4, then decide: keep as a documented drill branch, or fold the MinIO layer
and the CAS disk into `main` behind the `local` overlay only.

## 3. Order of work (concrete)

1. `scripts/images.mk` + CHI image tag. `make images-manage-all`.
2. MinIO layer + Flux Kustomization + bucket Job. `make fluxcd-push-artifacts`. Phase A.
3. `cas_disk.xml` + env in the CHI; `cas-*` make targets. Phase B, C.
4. Bronze DDL edit. Phase D.
5. README section "Trying CAS" + this doc's §4 filled in. Phase E.

## 4. Results log (fill in as phases run)

| Phase | Date | Outcome | Notes |
| --- | --- | --- | --- |
| A (MinIO) | 2026-10-01 | **REFUTED** — probe passed, CAS did not | MinIO `pgsty/minio:RELEASE.2026-08-04T00-00-00Z` (single node, 20Gi `local-path`), bucket `clickhouse-cas` created by the `mc` Job. `make minio-probe` (curl `--aws-sigv4` from an in-cluster pod) 10/10 PASS: `If-None-Match:*` 200 then **412**; `If-Match` stale **412** / current 200; ranged GET **206** twice with a stable ETag; DELETE 204. Cluster converged with all 7 Flux Kustomizations Ready, server on `26.6.4.20001.altinityantalya` on **both** replicas against the unchanged 26.3.16 Keeper (leader + 2 synced followers, `system.zookeeper` reachable) — no Keeper bump needed. Notes: the `minio` Kustomization is the only one with `wait: true` and it went Ready first try; `mc` ships no curl, so `curlimages/curl:8.18.0` was added to `PROJECT_IMAGES`. **Refuted the same day by the server itself:** replica 0 crash-looped on `Code: 48 … CasProbe: remove with a stale incarnation was not rejected (a mismatch was expected) — backend does not enforce conditional deletes. (NOT_IMPLEMENTED)`. The probe tested conditional PUT and Range but never conditional DELETE, so it passed falsely. Gate A was never actually cleared. |
| A (Silo) | 2026-10-01 | **PASS** — Gate A cleared | Backend swapped to `pgsty/silo:RELEASE.2026-09-16T00-00-00Z` (two lines: `scripts/images.mk` + the StatefulSet; directory/namespace/service unchanged). Drop-in over the same `/data` PVC — a rolling replace, bucket Job still `Complete`, no re-create. `make minio-probe` extended with the checks that were missing and now 13/13 hard PASS: **e1a** DELETE with a stale `If-Match` → **412**, **e1b** the object survives it (GET 200), **e2** DELETE with the current `If-Match` → **204**, on top of the original PUT/Range checks. **e3** (INFO only) POST `?delete` with a wrong per-item `<ETag>` → **200 / `<Deleted>`**, object gone: Silo's batch `DeleteObjects` ignores per-item ETags, as expected — CAS probes bulk delete separately and falls back. (e3 also needs a `Content-MD5`; curl's `--aws-sigv4` does not supply one, so the probe computes it with `md5sum \| xxd -r -p \| base64` — the curl image ships no `openssl`.) Both replicas then opened the pool clean: `system.cas_mounts` shows **2 live mounts** (`chi-clickhouse-default-0-0`, `-0-1`), `system.disks` has `cas` + `cas_cache`, `system.storage_policies` has `cas` and `cas_tiered`. No `CasProbe` error on either server after the swap. |
| B | 2026-10-01 | **PASS** — all four claims hold | `demo.cas_events`, `storage_policy='cas'`, 5,000,000 rows over 6 monthly partitions, 50–100 byte payloads, inserted on replica 0. **(1)** Both replicas: 15 active parts, 5,000,000 rows, 233,670,959 B (222.85 MiB), all on `cas_cache` — identical. **(2)** Replica 1's whole `system.cas_log`: `blob_put = 0`, `blob_reuse_adopt = 30` (plus `ref_resolve` 45, `build_publish` 30, `manifest_put` 30, `ref_drop` 15). It never uploaded a byte for a part it received. **(3)** `make minio-ls` = **223 MiB** vs ONE replica's 222.85 MiB — one copy, not two. **(4)** `count()` 5,000,000 and `sum(cityHash64(payload))` `4475074844160153721` equal on both replicas. |
| C | 2026-10-01 | **PASS — 6 of 6; replica rebuild re-run on `Replicated` databases, no hand-written DDL** | **Merge** PASS: `OPTIMIZE FINAL` → 15 parts to 6 (one per partition); bucket grew by exactly one copy (223 → 452 MiB for ~223 MiB of merged output) even though *both* replicas merged independently and each logged 6 `blob_put`s — the six content hashes are identical on both, so CAS stored the merged parts once. Old blobs stayed until GC (`blob_delete` unchanged). **GC** PASS: `DROP PARTITION '202604'` then `SYSTEM CAS GC RUN`; it takes **4 rounds**, not one — round *n* marks (`candidates_marked`/`entries_condemned`), *n+1* graduates, *n+2* deletes (`objects_deleted`/`entries_redeleted`), with `Deferred` (skip-unchanged) rounds in between; bucket 454 → 377 → 191 MiB against 188.69 MiB live. Bulk delete never appeared: deletes are counted as `manifests_deleted` + per-object `entries_redeleted`, consistent with the probe's e3 finding that batch delete cannot carry ETags. GC also correctly **spares** blobs of merged-away parts while the outdated parts are still in the part set — with the stock `old_parts_lifetime = 480` that is an 8-minute wait, so the drill fixture sets `old_parts_lifetime = 30`. **Replica rebuild PASS (re-run)** — failed on the first attempt, passes with the documented `SYSTEM CAS DROP POOL MEMBER` procedure now in §5. Drill: `delete pod` + `delete pvc data-chi-clickhouse-default-0-1-0`, then from replica 0 `SYSTEM CAS DROP POOL MEMBER 'chi-clickhouse-default-0-1' FROM DISK 'cas'` — refused for 60 s with `Code: 236 … pool member is alive or contended … (no FORCE variant exists; stop the server or wait for its lease to lapse). (ABORTED)`, then accepted: `10 namespace(s) are still owned by this member; upcoming GC rounds perform the final cleanup — re-run this command afterwards to retire the slot`. One `SYSTEM CAS GC RUN` + one re-run retired the slot (namespace count 0) and both its rows left `system.cas_mounts`. The member's whole subtree was then gone from the bucket — **1 of 666 objects named that root**, the tombstoned `owner` itself — which is the precondition the first attempt lacked. Deleting that one object and restarting the pod: **Ready in 51 s**, re-claiming `chi-clickhouse-default-0-1` with a fresh `server_uuid` at `writer_epoch = 1`. Data came back by relink: replica 1's whole `cas_log` is `blob_put = 0`, `blob_reuse_adopt = 294` (plus `ref_resolve` 441, `build_publish`/`precommit`/`manifest_put`/`build_start` 294 each, `ref_drop` 147); replication queue drained to 0 before the first poll. All 14 `demo` + `nimbus_raw` tables byte-identical to replica 0 (`demo.cas_events` 16 parts / 5,001,000 rows / 233,675,613 B on `cas_cache`; `sum(cityHash64(payload))` `273111313645302257` on both), and the bucket stayed at **248 MiB** — a full replica rebuild added zero bytes. Two costs, neither of them silent: one manual object deletion (step 3 in §5), and a DDL replay, because `demo`/`nimbus_raw` are `Atomic` databases whose schema lived on the lost PVC — that half is plain ClickHouse, not CAS, and needs `SYSTEM DROP REPLICA … FROM DATABASE` first (`Code: 253 … Replica … already exists`) plus explicit `UUID` for dbt's seed tables (`Code: 36 … Macro 'uuid' in engine arguments is only supported when the UUID is ex…`). `SYSTEM CAS FORGET`, `SYSTEM CAS FSCK`, `SYSTEM CAS GC REBUILD` and `cas_unsafe_remount_no_delay` were **not** needed and were not used. **Cache cold** PASS: `SYSTEM DROP FILESYSTEM CACHE` on replica 1, full scan returned the identical hash in 865 ms with 76 `S3GetObject` and 137,685,423 B read from source, cache refilled to 222.84 MiB. Note: blob *reads* are **not** in `system.cas_log` (it logs reference/GC decisions only) — the evidence is `system.query_log` ProfileEvents. **Storage outage** PASS: MinIO/Silo scaled to 0 — cached scan still correct; uncached scan failed with `Code: 499 … Connection refused … (S3_ERROR)`; insert failed with `Code: 210 … mount lease not held … TRANSIENT unavailability, not damage. (NETWORK_ERROR)`. Scaled back to 1: both mounts returned to `live` ~90 s later (replica 1 first, replica 0 at +55 s), reads and writes worked again, **no manual cleanup**. **Restart storm** PASS: `rollout restart` on both CHI StatefulSets — both pods Ready in 39 s, both mounts `live` with bumped `writer_epoch` (3 and 4), `gc_fenced = 0` on both, no fencing of the healthy peer, 5,001,000 rows on both. **Replica rebuild, re-run on `Replicated` databases (2026-10-01, after the conversion).** Same drill, nothing hand-written: `delete pod` + `delete pvc data-chi-clickhouse-default-0-1-0` (the PVC delete hangs on the `kubernetes.io/pvc-protection` finalizer until the pod is deleted a second time — the StatefulSet re-attaches it in between), the pod crash-looped on the expected `Code: 246 … CAS server-root 'chi-clickhouse-default-0-1' is owned by a different server (owner server_uuid=864160a9…, ours=2fc6448c…) — refusing to claim`, then one `make cas-evict-member MEMBER=chi-clickhouse-default-0-1`: decommission accepted on the first call (the lease had already lapsed during the crash-loop), two GC rounds retired the slot, the subtree held **exactly 1 object** so the guard let the target delete the tombstoned `owner` itself, and the pod was **Ready after ~65 s** at `writer_epoch = 1`. All four mounts `live`, `gc_fenced = 0`. The rebuilt replica came back to **all 8 `Replicated` databases and 64 objects — identical to replica 0** (demo 1, nimbus_raw 13, nimbus_rt 15, nimbus_marts 9, nimbus_staging 8, nimbus_stream 8, nimbus_intermediate 7, nimbus_metrics 3), byte-identical parts (`demo.cas_events` 15 parts / 5,000,000 rows / 233,670,959 B and `nimbus_raw` 131 parts / 1,300,293 rows / 25,498,200 B on `cas_cache`, plus 5 parts / 34 rows on `default`, the same on both), `sum(cityHash64(payload))` `4475074844160153721` on both, and the new pod's whole `cas_log` is **`blob_put = 0`**, `blob_reuse_adopt = 292` (`ref_resolve` 453, `build_publish`/`precommit`/`manifest_put`/`build_start` 292 each, `ref_drop` 146). `make minio-ls` **248 MiB before and after** — a full replica rebuild of both `demo` and the whole bronze layer added **zero bytes** (object count fell 1408 → 1092 as GC retired the evicted member's namespaces). **The DDL replay is gone, but it was replaced by two Keeper de-registrations, not by nothing** — see §5 "Replicated databases"; both are now inside `cas-evict-member`, so the drill is one make target plus two `kubectl delete`s. |
| D | 2026-10-01 | **PASS** — Gate D cleared, `SCALE=small` | Bronze DDL moved to `cas_tiered` + `TTL <event ts> + INTERVAL 90 DAY TO VOLUME 'cas'` + `min_bytes_for_wide_part=100000000` / `min_level_for_wide_part=3` (tables **dropped**, not truncated — those settings only apply at CREATE). `SCALE=small` (not medium) to keep every step inside the 5-minute cap: 500 customers, 193,642 postings, 1,000,000 app events, 103,031 auths = 1,300,293 bronze rows. Wall clock: `wh-bronze` 2 s, `wh-generate` 17 s, `wh-image` 44 s, `wh-deploy` 19 s, `wh-build` 29 s (dbt 16.1 s), `wh-test` 19 s (dbt 3.8 s). dbt: **build 183/183 PASS, test 151/151 PASS, 0 errors** — no storage-related failure. (The first `wh-build` failed 2/183 with `UNKNOWN_TABLE` on `nimbus_stream.slv_card_auths` / `nimbus_rt.rt_interchange_daily`; that is the P6 realtime plane not being deployed yet, unrelated to CAS — `make wh-deploy` then a clean 183/183.) Parts by disk: `nimbus_raw` 131 parts / 1,300,293 rows / 24.31 MiB on `cas_cache`, **identical on both replicas**; `nimbus_stream`, `nimbus_rt`, `nimbus_marts`, `nimbus_metrics`, `nimbus_intermediate` and the dbt seeds all on `default`, as intended. Bucket after the build: **248 MiB** = demo 222.85 MiB + bronze 24.31 MiB — one copy for two replicas. **Caveat on the tier demo:** the generator's newest event is `2026-06-30`, 93 days before today, so the *whole* corpus is already past the 90-day TTL and ClickHouse writes it straight to the CAS volume — no hot residue to watch move. Verified the split is real by inserting 1,000 rows at `now()`: they landed on `default` while every older partition stayed on `cas_cache`. No `MATERIALIZE TTL` or manual `MOVE PARTITION` was needed. |

### Resolved issue: a replica rebuild needs one operator step (Phase C)

Deleting replica 1's pod **and** its `data` PVC leaves the server unable to start on its
own. Wiping `/var/lib/clickhouse` regenerates the local server UUID, and CAS refuses to
reclaim a server-root whose write-once owner anchor names a different one:

```
Code: 246. DB::Exception: CAS server-root 'chi-clickhouse-default-0-1' is owned by a
different server (owner server_uuid=b3639bdd…, ours=864160a9…) — refusing to claim.
… Recover by restoring the old local uuid file; or configure a fresh <cas_server_root_id>
for this disk; or — only after verifying that NO server uses this root — manually delete
the owner object 'cas/default/gc/server-roots/chi-clickhouse-default-0-1/owner' and
restart. (CORRUPTED_DATA)
```

The first attempt took the third branch directly and got the refusal that made this look
unrecoverable — `has no owner anchor but its data subtree is non-empty (identity lost over
existing data)` — and we recovered by wiping the pool prefix. That was the wrong order, not
a missing capability. **`SYSTEM CAS DROP POOL MEMBER '<root>' FROM DISK '<disk>'` is the
documented eviction**, and it is what empties the subtree so the owner deletion becomes the
safe case instead of the refused one. Run from a surviving replica once the lost member's
mount lease has lapsed, alternated with `SYSTEM CAS GC RUN` until the slot retires, it
leaves exactly one object behind — a tombstoned `owner` — and deleting that one object lets
the replacement pod claim the same `cas_server_root_id` with its new uuid. Full sequence,
with what each command actually means, in §5 "Recovery procedure"; wrapped as
`make cas-evict-member MEMBER=<id>`.

So the drill passes, with one caveat worth stating plainly. **It is not self-healing**: the
owner deletion is a manual object-store step by design — the tombstone exists precisely so a
decommissioned root cannot "silently resume" — so a lost PVC on Kubernetes still needs an
operator, it just no longer needs surgery on a subtree or a wiped bucket. `make cas-evict-member`
is that operator act, wrapped and guarded.

The other half of the original cost — hand-replaying the schema from the survivor, because
`demo` and `nimbus_raw` were `Atomic` databases whose metadata lived on the lost PVC — **has
since been removed**: all seven analytics databases now use the `Replicated` engine, so the
tables replay themselves from each database's own DDL log. That did not make the rebuild
free, it made it *mechanical*: the replay needs two Keeper de-registrations first, both now
inside the same make target. See "Replicated databases" below.

### Decision

**What worked.** Everything the blog post claims, on a laptop, against a local S3: one
copy of the bytes for two replicas (Gate B claim 3, and again in Phase D at 248 MiB for
a two-replica cluster), replication by relink with `blob_put = 0` on the receiving
replica, GC that actually deletes and correctly spares what is still reachable, and a
storage outage that recovers on its own with a diagnostic (`TRANSIENT unavailability,
not damage`) that says exactly the right thing — and, unplanned, the same machinery
absorbing a laptop suspend/resume with no operator action (see "mount lease after laptop
sleep"). The full dbt spine — 183 models and 151 tests — is green with bronze on CAS and
shows no correctness difference, rebuilt from scratch on `Replicated` databases. And a
full replica rebuild after losing the data PVC now completes by relink alone, adding
**zero** bytes to the bucket (248 MiB before and after, for 259 MiB of parts on each of
two replicas) — the strongest version of the one-copy claim in the whole experiment — and
with the databases on the `Replicated` engine it needs **no hand-written DDL at all**: two
`kubectl delete`s and one `make cas-evict-member`.

**What did not.** (1) The Phase A probe as originally written was a false pass: it is worth
restating that `skip_access_check` must stay `false`, because the server's own capability
probe is what caught the backend that the make target had blessed. (2) GC needs several
manual rounds to converge, which is fine for a background process on a 60 s tick but makes
every drill — the member decommission included — a multi-round exercise. (3)
`system.cas_log` does not record blob reads, so the "cache cold" evidence has to come from
`system.query_log` ProfileEvents instead. (4) The rebuild is recoverable but still not
automatic, and the recovery is undocumented upstream: the config guide lists the
`SYSTEM CAS` command names with no semantics at all, and everything in §5 was reconstructed
from the servers' own error text and `system.cas_mounts`'s column comments. (5) The
`Replicated` database engine removed the hand-written schema replay but **not** the
identity problem it was part of: a rebuilt replica is still a new server uuid over
surviving shared state, and that now has to be retired in three registries, not one — the
CAS owner anchor, the table replicas, the database replica. Two of the three fail silently
or near-silently (a database whose DDL worker never initializes just stays empty), which is
why they belong in a target and not in a runbook step.

**Recommendation: keep this as a documented drill branch. Merging is defensible now — the
pre-conditions this section previously set have been met — but it is a judgement call, not
a conclusion the evidence forces.** The two items that were listed as prerequisites are
done: the analytics databases are on the `Replicated` engine, and the whole drill has been
re-run end to end on top of that (full rebuild at `SCALE=small`, build 183/183, test
151/151, replica rebuild with `blob_put = 0` and an unchanged bucket). What is on the other
side of the ledger, factually: the CAS on-disk format is still labelled experimental, every
`SYSTEM CAS` semantic in §5 is reconstructed from error strings rather than a published
contract, the object-store backend is a community fork chosen because the better-known one
fails the server's own probe, and the `Replicated` conversion is itself a one-way change
(the engine cannot be altered, so adopting it means a `DROP DATABASE` + rebuild on any
cluster that takes the merge). If it does merge, it should merge **behind the `local`
overlay only**, and the release notes should be re-read at that point rather than trusted
from this write-up.

## 5. Risks and fallbacks

### Recovery procedure: a pool member that lost its data PVC

Established by the Phase C re-run (2026-10-01) after reading the Antalya config guide and
the servers' own error text, which is far more specific than the guide. The guide lists
only the command names — `SYSTEM CAS GC RUN|STOP|START|REBUILD`, `SYSTEM CAS FSCK`,
`SYSTEM CAS FORGET`, and `SYSTEM CAS DROP POOL MEMBER '<server_root_id>' FROM DISK
'<disk>'` — with no semantics. The semantics are in `system.cas_mounts`'s own column
comments and in the exceptions:

- **`cas_server_root_id`** is "anchored in the pool by a write-once **owner claim** — a
  colliding identity is refused at mount". The anchor is a single object,
  `cas/<pool>/gc/server-roots/<root>/owner`, holding `{"server_uuid":…}`. A pod that comes
  back without `/var/lib/clickhouse` has a new local uuid, so the anchor no longer matches
  and the mount is refused (`CORRUPTED_DATA`). This is the whole failure.
- **`SYSTEM CAS DROP POOL MEMBER`** is a **decommission**, not a reset: it retires another
  member's slot from a surviving member. It is deliberately not forceable — "pool member is
  alive or contended … Refusing (no FORCE variant exists; stop the server or wait for its
  lease to lapse)" — so the victim's mount lease must actually lapse first (~60 s at the
  default 30 s TTL). It then runs in rounds: the first call answers "decommission underway:
  N namespace(s) are still owned by this member; upcoming GC rounds perform the final
  cleanup — re-run this command afterwards to retire the slot".
- **`SYSTEM CAS FORGET`** is the *self*-decommission (it takes an operand: `Expected one of:
  ON, string literal, identifier`). `system.cas_mounts.lifecycle_detail` documents its
  effect as "decommissioned by SYSTEM CAS FORGET at `<time>`", surfacing as
  `lifecycle = vanished`, `lifecycle_reason = forgotten`. It is the wrong end of the
  problem here: the member that needs removing is the one that is gone.
- **`lifecycle`** is the non-gated view of a member's own pool state — `live`, `not_live`,
  **`identity_lost`**, `vanished(replaced|forgotten)`, `constructing`, `shutdown`. A live
  peer's view of a crash-looping member never leaves the mount-slot columns, so the useful
  signal during a rebuild is the *disappearance* of the victim's rows from `cas_mounts`.
- **`cas_unsafe_remount_no_delay`** does not apply. It reclaims a slot "that carries **this
  server's own uuid**" without waiting out the TTL — it is a hard-restart accelerator for an
  unchanged identity, not an identity-change escape hatch. It is also explicitly "intended
  for test stands" only. It was therefore not used and is not set anywhere on this branch.

The working procedure — **the last step is manual on purpose**, and the decommission is
what makes it legal:

1. On a surviving replica, retire the lost member and wait out its lease:
   `SYSTEM CAS DROP POOL MEMBER 'chi-clickhouse-default-0-1' FROM DISK 'cas'`, retried
   until it stops answering `ABORTED … alive or contended`.
2. Alternate `SYSTEM CAS GC RUN` with a re-run of the same command until the member's
   namespace count reaches 0 and its rows vanish from `system.cas_mounts`. Both steps are
   `make cas-evict-member MEMBER=chi-clickhouse-default-0-1`.
3. Delete exactly one object: `cas/default/gc/server-roots/<root>/owner`. This is the same
   deletion the original error message recommends, and the reason it failed before is that
   it was done *without* step 2 — an owner anchor removed over a non-empty subtree gives
   "identity lost over existing data". After the decommission the subtree is empty (verified:
   one object left in the pool naming that root, the owner itself), so the deletion is the
   documented safe case rather than the refused one. It cannot be skipped either: the
   decommission **tombstones** the anchor rather than removing it, and a plain restart then
   fails with "was explicitly decommissioned by an operator (tombstoned at …) and is
   refusing to silently resume".
4. Restart the pod. It claims the same `cas_server_root_id` with its new `server_uuid` at
   `writer_epoch = 1`.
5. Let the DDL replay — but clear the member's two stale Keeper registrations first, then
   re-issue `CREATE DATABASE`. This step is **not CAS**, and since the databases moved to the
   `Replicated` engine it is no longer a hand-written schema replay: the TABLES come back from
   each database's own DDL log. What does not come back on its own is the member's identity in
   Keeper, in two places, both reported as `Code: 253 … already exists` and both keyed on the
   server uuid the lost PVC regenerated. **Order matters: both drops before the CREATE.**
   For each database, on the survivor:
   - `SYSTEM DROP REPLICA '<member>' FROM DATABASE <db>` — the table replicas under
     `/clickhouse/tables/<uuid>/<shard>/replicas/<member>`. Skipping this is the quiet failure
     mode: the DDL log replays `CREATE TABLE`, the CREATE cannot register the replica
     (`Error on initialization of <db>: Code: 253 … Replica /clickhouse/tables/<uuid>/0/replicas/<member>
     already exists`), the database's DDL worker never initializes and **nothing** in that
     database replays. No statement anywhere returns an error — the only symptom is a database
     that stays empty, and one line in the rebuilt pod's log.
   - `SYSTEM DROP DATABASE REPLICA '<member>' FROM SHARD '<shard>' FROM DATABASE <db>` — the
     database replica under `/clickhouse/databases/<shard>/<db>/replicas/<shard>|<member>`,
     whose host ID carries the uuid: without it `CREATE DATABASE` itself is refused with
     `Replica host ID: '…:<old uuid>', current host ID: '…:<new uuid>'`.
   - then `CREATE DATABASE IF NOT EXISTS <db> ON CLUSTER '{cluster}' ENGINE = Replicated(…)`.
   Do **not** drop the database replica after the rebuilt pod has already attached that
   database: it is then left with no `log_ptr` node, and the local `DROP DATABASE` needs the
   same node (`Code: 999 … No node, path /clickhouse/databases/0/<db>/replicas/0|<member>`), so
   recovery costs a `DETACH DATABASE` plus removing `/var/lib/clickhouse/metadata/<db>.sql` on
   the pod. All of step 5 is phase [4/4] of `make cas-evict-member`. The bytes then arrive by
   relink, and the target waits for the object count to match the survivor.

`SYSTEM CAS FSCK` and `SYSTEM CAS GC REBUILD` were never needed; the `BAD_ARGUMENTS` text
for an unknown member ("if victim objects linger without a slot, run cas-fsck") suggests
FSCK is the sweep for a decommission that was interrupted.

### Replicated databases

**Why.** A `Replicated` database keeps its own DDL log in Keeper, so a replica that comes
back with an empty data PVC replays every `CREATE TABLE`/`VIEW`/`DICTIONARY` itself instead
of needing a schema replayed by hand from the survivor. That is the single largest manual
cost the Phase C rebuild had, and it is independent of CAS: CAS brings the *bytes* back by
relink, the database engine brings the *schema* back. All seven analytics databases (`demo`
plus the six `nimbus_*`) are created with
`ENGINE = Replicated('/clickhouse/databases/{shard}/<db>', '{shard}', '{replica}')` in
`warehouse/loaders/00_create_raw.sql`, `warehouse/loaders/30_create_rt.sql`, `make ch-demo`
and `make cas-demo`. The engine cannot be altered: converting the live cluster meant
`DROP DATABASE … ON CLUSTER '{cluster}' SYNC` for all seven and a full rebuild.

**The adapter key.** dbt-clickhouse 1.10.1 learns this from one profile field,
`database_engine` (`warehouse/dbt/profiles/profiles.yml`). The string is matched
case-insensitively for `replicated` (`relation.py`), and the effect we need is that the
adapter then stops emitting `ON CLUSTER` on model DDL. `cluster` stays set — it still drives
dbt's `clusterAllReplicas()` introspection — it just no longer drives DDL.

**The `ON CLUSTER` rule.** Inside a `Replicated` database, `ON CLUSTER` is rejected
(`Code: 80`): the database replicates its own DDL, and a second distribution mechanism on
top of it is a conflict, not a redundancy. So `ON CLUSTER` survives on exactly one kind of
statement — `CREATE DATABASE`, which must reach every node directly because the database's
own log is what it is creating — and is gone from every table `CREATE`, `TRUNCATE` and
`DROP` inside one (`wh-drop`, `load_bronze.sh`, `wh-rt-backfill`, `ch-demo`, `cas-demo`).
Table engines go **bare** for the same reason: explicit `('<zk path>', '{replica}')`
arguments are refused inside a Replicated database, and the engine instead derives
`/clickhouse/tables/{uuid}/{shard}` from the table UUID, which the DDL log carries. A
version column is still an argument and still allowed (`ReplicatedReplacingMergeTree(ingested_at)`).

**Seeds.** dbt's seeds were the one place the old notes predicted trouble — the Phase C
hand-replay hit `Code: 36 … Macro 'uuid' in engine arguments is only supported when the
UUID is explicitly set`, because `SHOW CREATE` prints the server's
`default_replica_path = /clickhouse/tables/{uuid}/{shard}` and a hand-written `CREATE` has
no UUID to substitute. Inside a Replicated database that failure cannot occur, and the
adapter does not cause it either: `clickhouse__create_csv_table` builds the engine clause
from the plain `config.get('engine')` value, so the only thing that reaches the server is
what `dbt_project.yml` sets — `seeds: nimbus: +engine: ReplicatedMergeTree`, bare, no
arguments, no `{uuid}`. The UUID comes from the DDL log and both replicas agree on it.
Verified, not assumed: all five seeds built clean in the rebuild and all five came back on
the rebuilt replica (`nimbus_raw` = 8 bronze + 5 seeds = 13 objects on both). The rule to
keep is simply **never put explicit engine arguments on a seed or model**; the error
message is misleading if you do, because it blames the `uuid` macro rather than the
arguments.

**The `insert_quorum` decision.** The adapter sets two Replicated defaults when
`database_engine` is the *bare word* `Replicated`, and ours carries the zk path, so neither
applies automatically. One is wanted and is set explicitly in `custom_settings`:
`database_replicated_enforce_synchronous_settings = 1`, so a DDL entry is applied on every
replica before the next statement runs — dbt builds a model and immediately reads it back,
and an un-applied `CREATE` on the peer would surface as a flaky `UNKNOWN_TABLE`. The other,
`insert_quorum = auto`, is deliberately **not** set: on two replicas `auto` means a quorum
of two, so every insert would fail for as long as one replica is down — including the whole
window of the rebuild drill this branch exists to run. The trade is explicit: an insert
acknowledged by one replica can be lost if that replica dies before the peer fetches it,
which on a laptop drill branch is the right side of the trade and on anything real is not.

### Finding: mount lease after laptop sleep

Both pods had ~8 h of uptime and both CAS mounts read `live`, but the logs show the pool was
fenced and re-opened in between — the laptop slept. The renewer does not see a missed
deadline as a slow write; it sees that the deadline passed with **no attempt sent at all**,
which is exactly what a suspended process looks like from the inside:

```
02:59:12 <Warning> CasWriteRetryLater: CAS write could not be committed (CAS mount-lease
  renewal for key 'cas/default/gc/server-roots/chi-clickhouse-default-0-0/mount' did not
  retain the lease (external_lease_deadline, no attempt sent, last observation: nothing
  observed)); retrying later
02:59:12 <Warning> CasMountLeaseRenewer: CAS mount renewal 'chi-clickhouse-default-0-0'
  fenced after 0 physical attempts in 1702 ms (classification=external_lease_deadline,
  confirmed_deadline_boot_ms=133859839)
```

Replica 1 fenced the same way at 02:59:15. For the next ~30 minutes every background
`CleanupThread` on a CAS table logged the expected `Code: 210 … mount lease not held …
TRANSIENT unavailability, not damage. (NETWORK_ERROR)` — the same diagnostic the Phase C
storage-outage drill produced, and correct here too. Then the remount loop self-healed with
no operator action: replica 1 at `03:31:36` on attempt 1 (`writer_epoch=2`), replica 0 at
`03:31:39` on attempt 2 (`writer_epoch=4`), both `CasPool: CAS whole-chain remount attempt N
succeeded at step 'publish_live'`; replica 0's first attempt had stopped at
`pool_identity_probe` with `remount held TRANSIENT — pool-meta probe inconclusive`, which is
the loop declining to guess rather than a failure. `gc_fenced = 0` on both afterwards and no
data loss. Two things to take from it: a suspend/resume is handled by the same machinery as
a storage outage and needs nothing from an operator, and **`cas_mounts.state` alone is not a
history** — a clean `live` says nothing about whether the pool was fenced an hour ago, so the
err.log is the only place that fencing is visible after the fact.


- **The backend fails the conditional-write probe, or the `pgsty` images go away.**
  This already happened once: `pgsty/minio` ignores `If-Match` on DeleteObject and
  CAS's boot probe rejected it, so the backend is now `pgsty/silo` (see §1.2).
  Remaining fallbacks, in order (state as of 2026-09):
  - **SeaweedFS** (≥ 4.09): enforces `If-None-Match` / `If-Match` on PUT (before
    4.09 `If-Match` was silently ignored); single binary, `weed server -s3`; known
    bug when versioning + object lock are on, so keep the bucket unversioned.
  - **Ceph RGW** via Rook: full S3 semantics incl. conditional writes; far too
    heavy for a laptop k3d (needs OSDs, ~4Gi+), only if the others fail.
  - **RustFS 1.0.0** — Altinity's own CAS stress backend. Earlier RustFS builds had
    open issues on `If-None-Match` and ETag quoting; 1.0.0 is the orchestrator's
    call if Silo's conditional DELETE also proves incomplete.
  - **Not viable today:** **Garage** ignores `If-None-Match`/`If-Match` on PutObject
    (silent overwrite, issue open Sep 2026), so it would fail CAS's boot probe.
  The object-store layer is isolated in its own directory precisely so the backend
  can be swapped without touching the CHI — the Silo swap was two lines plus a probe.
- **Experimental format.** 26.6.4 changed the CAS layout; a later 26.6.x may again.
  Pin the tag, never roll the server forward with data in the pool without reading
  the release notes. Losing the pool = `make wh-drop` + reload; nothing here is precious.
- **Laptop memory.** The cache disk and the S3 client add RSS; the local overlay's
  4Gi limit should hold, but watch `MemoryTracking` during Phase D. MinIO itself
  needs ~512Mi.
- **Rate limiting (`503 Slow Down`)** is an AWS thing; MinIO on one node will
  instead show as latency. Keep `min_bytes_for_wide_part` high to limit object count.
- **Hash choice is permanent per pool.** `xxh3-128` chosen for speed with no known
  collision classes; changing it means a new bucket prefix.

## 6. References

- Blog: https://altinity.com/blog/introducing-cas-drop-in-compute-storage-separation-for-clickhouse-mergetree-tables
- Config guide (in-tree): https://github.com/Altinity/ClickHouse/blob/antalya-26.6/docs/en/antalya/cas/configuration.md
- Release notes: https://docs.altinity.com/releasenotes/altinity-antalya-release-notes/26.6/
- Images: `altinity/clickhouse-server:26.6.4.20001.altinityantalya` (2026-09-10)
