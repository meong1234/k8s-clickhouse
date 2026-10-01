# Kubernetes ClickHouse Deployment

A production-shaped, GitOps deployment of **ClickHouse** on a local Kubernetes cluster
(**k3d** — k3s-in-Docker), managed by the **Altinity ClickHouse Operator** and **FluxCD**:
a 3-node **Keeper** quorum, **replicated MergeTree** across two replicas, and
**compute–storage separation** — the replicas share **one copy of the data** in a local S3
through **Altinity CAS** — with a dbt warehouse on top.

It is the ClickHouse sibling of [`k8s-odoo`](https://github.com/meong1234/k8s-odoo) and
reuses the same conventions: a local cluster + a local OCI registry, FluxCD driven by **OCI
artifacts** (not a Git repo), and a `base` / `local` kustomize overlay layout.

> **Runtime note:** the cluster runs on **k3d/k3s** (`rancher/k3s:v1.33.13-k3s1`, containerd
> 2.2.5), not kind. kind v0.32's node images ship containerd 2.0.5, which SEGFAULTs during
> pod-sandbox creation on Docker Desktop / Apple Silicon; k3s's newer containerd is stable.
> Topology: 1 server (control plane) + 3 agents (3 dedicated worker nodes, one per Keeper /
> ClickHouse replica).

## What it deploys

A minimal but genuinely production-shaped topology:

| Component | Detail |
| --- | --- |
| **ClickHouse Keeper** | **3-node quorum** (standalone `ClickHouseKeeperInstallation`), spread across nodes via anti-affinity. Tolerates losing 1 node. |
| **ClickHouse** | 1 cluster, **1 shard × 2 replicas** (`ClickHouseInstallation`). Replicas kept on different nodes. Real `ReplicatedMergeTree` data replication. |
| **Object store** | In-cluster S3 (**Silo**, MinIO's community successor) holding the shared MergeTree data. One copy of the bytes for both replicas. |
| **CAS disk** | Altinity **CAS** (content-addressed storage) `object_storage` disk on every replica, with a local read-through cache in front. Policies: `cas` (pure) and `cas_tiered` (local hot → CAS cold). |
| **Operator** | Altinity `clickhouse-operator` `0.27.1` via HelmRelease. |
| **Images** | `clickhouse-server` on Altinity **Antalya** `26.6.4.20001.altinityantalya` (CAS is an Antalya 26.6+ feature); `clickhouse-keeper` on Altinity **Stable/LTS** `26.3.16.10001.altinitystable`. |

```
                    ┌─────────────────────────────────────────┐
                    │            ClickHouse Keeper             │
                    │  keeper-0     keeper-1     keeper-2       │   3-node Raft quorum
                    │  (leader)     (follower)   (follower)     │   (coordination: parts,
                    └──────▲──────────▲──────────▲──────────────┘    DDL, replication log)
                           │  Keeper protocol (2181)   │
              ┌────────────┴───────────┐   ┌───────────┴────────────┐
              │   ClickHouse replica 0 │◀─▶│   ClickHouse replica 1 │   ReplicatedMergeTree
              │   chi-...-0-0-0        │   │   chi-...-0-1-0        │   (which parts exist)
              │   [local cache]        │   │   [local cache]        │
              └────────────┬───────────┘   └───────────┬────────────┘
                           │  S3 (conditional writes)  │
                    ┌──────▼───────────────────────────▼──────┐
                    │        Object store (Silo, S3 API)       │   CAS: blobs keyed by
                    │   bucket clickhouse-cas — ONE copy of    │   content hash, shared
                    │   every part, referenced by both replicas│   by both replicas
                    └──────────────────────────────────────────┘
```

Keeper still decides *which* parts exist; CAS changes *where the bytes live*. A part
written by replica 0 is published to the object store once; replica 1 learns of it through
the normal replication log and **relinks** the existing blobs instead of fetching them.

## Why these choices (production best practices)

- **Standalone Keeper, 3 nodes.** Keeper is run in its own pods (a `ClickHouseKeeperInstallation`),
  *not* embedded inside the ClickHouse servers, so Raft consensus and query load never
  starve each other. Quorum needs an **odd** count that tolerates failures — **3** tolerates
  one node loss. (Never run 2: losing one breaks the quorum.)
- **Pod anti-affinity for node-spread.** In the **base** (production) manifests each Keeper pod
  and each ClickHouse replica must land on a distinct node
  (`requiredDuringScheduling`, `topologyKey: kubernetes.io/hostname`), so a single node failure
  costs at most one Keeper vote and one ClickHouse replica — never the quorum or the data.
  The **local** overlay relaxes this to *soft* (`preferredDuringScheduling`) so the stack still
  boots reliably on a small laptop cluster (see [Local vs production](#local-vs-production-profile)).
- **Real replication.** With 2 replicas per shard, `ReplicatedMergeTree` keeps both pods in
  sync through Keeper. The operator auto-generates `<remote_servers>`, per-pod `<macros>`
  (`{shard}`/`{replica}`/`{cluster}`) and `<zookeeper>`, so replicated DDL "just works"
  (see [the replication demo](#4-prove-replication)).
- **One copy of the data, not one per replica.** With CAS the two replicas reference the
  *same* blobs in the object store, so storage cost does not double with replica count and
  a new replica is a relink, not a copy. Chosen over **zero-copy replication** (deprecated
  upstream: mutable per-blob refcounts in Keeper, orphan files) and over a plain S3 disk
  (one tree per replica). The trade-off is stated plainly: CAS is **experimental**, its
  insert path is not optimised yet, so writes land on local disk and only aged partitions
  move to CAS (`cas_tiered` + TTL). See [the CAS demo](#5-prove-one-copy-of-the-data).
- **An object store that enforces conditional writes.** CAS coordinates through the S3 API
  itself — `If-None-Match`/`If-Match` on PUT **and on DELETE** — and refuses to open a pool
  on a backend that does not enforce them. MinIO fails that probe (it ignores `If-Match`
  on `DeleteObject`); Silo passes. `make minio-probe` checks every primitive from inside
  the cluster before ClickHouse ever sees the bucket.
- **Replicated database engine.** `demo` and all `nimbus_*` databases use
  `ENGINE = Replicated`, so the schema itself lives in Keeper. A replica that loses its
  data PVC replays every table from the database's DDL log — combined with CAS relink,
  a rebuild is two `kubectl delete`s and one make target, no hand-copied DDL. (Inside a
  Replicated database, DDL carries no `ON CLUSTER` and `ReplicatedMergeTree` takes no
  explicit path arguments.)
- **Keeper referenced by name.** The CHI points at the CHK via
  `spec.configuration.zookeeper.keeper.name` — the operator discovers the 3 endpoints and
  retries automatically, so nothing hardcodes a service DNS name.
- **Persistent storage.** Keeper, ClickHouse (local volume + CAS cache) and the object store
  each use PVCs (k3s's default `local-path` StorageClass).
- **Version-pinned, vendored CRDs.** The four operator CRDs are vendored under
  `kubernetes/infra/crds` (not installed by a Helm hook), so the bootstrap has no reconcile-time
  external dependency and the chart's `bitnami/kubectl` crd-job is disabled.
- **Secret-based credentials.** The admin password (SHA256) and the CAS disk's S3 keys come
  from Kubernetes Secrets, injected via `from_env` — never inline in the CHI.

## Prerequisites

- Docker
- Homebrew (macOS) — or install the tools below manually
- `kubectl`, `k3d`, `kustomize`, `flux` (≥ 2.9 needs k8s ≥ 1.33 — satisfied by k3s 1.33), and optionally `k9s`

Install everything with:

```bash
make brew-setup-all
```

> ⚠️ **Resource note:** this brings up 4 k3d nodes (1 server + 3 agents) running
> 3 Keeper pods + 2 ClickHouse pods + the object store + the operator + Flux. Give Docker
> Desktop **≥6 GB RAM / 4 CPUs** (Settings → Resources). If the Docker VM is starved by other
> running stacks, its kernel/OOM-killer can take down a node — free memory or raise the
> Docker allocation, then `make down && make up`.

## Quick Start

```bash
# 1. Create the cluster + registry, push images, install Flux, push artifacts, sync
make up

# 2. Watch it converge (operator → keeper + object store → clickhouse)
make ch-status          # repeat until 3 keeper + 2 clickhouse pods are Running
make minio-status       # object store Running, bucket Job Completed

# 3. Check the Keeper quorum (expect one leader + two followers)
make keeper-status

# 4. Prove replication end-to-end
make ch-demo

# 5. Prove one copy of the data: both replicas, one set of blobs
make minio-probe        # the object store honours CAS's conditional writes
make cas-demo           # 5M rows on CAS; relink on replica 1; bucket = one replica's bytes

# 6. Tear everything down
make down
```

### 4. Prove replication

`make ch-demo` creates a `ReplicatedMergeTree` table in a **`Replicated` database**, inserts
rows on **replica 0**, then reads the identical rows back from **replica 1** — data it never
received directly, only through Keeper-coordinated replication:

```sql
CREATE DATABASE demo ON CLUSTER '{cluster}'
ENGINE = Replicated('/clickhouse/databases/{shard}/demo', '{shard}', '{replica}');

CREATE TABLE demo.events (
    id UInt64, ts DateTime DEFAULT now(), msg String
) ENGINE = ReplicatedMergeTree
ORDER BY id;
```

Open an interactive session any time with `make ch-client` (user `admin`, password `admin123`).

### 5. Prove one copy of the data

`make cas-demo` creates a table on the pure `cas` storage policy, inserts 5M rows on
replica 0, waits for replica 1 to catch up, and prints the evidence for the three CAS claims:

```sql
CREATE TABLE demo.cas_events (id UInt64, ts DateTime, payload String)
ENGINE = ReplicatedMergeTree
PARTITION BY toYYYYMM(ts) ORDER BY id
SETTINGS storage_policy = 'cas', min_bytes_for_wide_part = 100000000;
```

| Claim | What the demo shows |
| --- | --- |
| Both replicas serve the data from CAS | `system.parts` on both: same parts, rows and bytes, all on the `cas_cache` disk |
| Replica 1 relinks, it does not re-upload | `system.cas_log` on replica 1: `blob_put = 0`, `blob_reuse_adopt > 0` |
| The bucket holds one copy | `make minio-ls` total ≈ **one** replica's `sum(bytes_on_disk)`, not two |

Everything that was measured — merges, GC, a replica rebuilt from a lost PVC, a cold cache,
an object-store outage, a restart storm, and the full dbt warehouse on tiered bronze — is in
[`docs/cas-local-s3-plan.md`](docs/cas-local-s3-plan.md):

| Gate | Result |
| --- | --- |
| A — does the object store satisfy CAS? | **PASS** on Silo (13/13 probes). MinIO **failed**: it ignores `If-Match` on `DeleteObject` |
| B — one copy, relink not re-upload | **PASS**: 223 MiB in the bucket for 2 × 222.85 MiB of parts; `blob_put = 0` on the receiving replica |
| C — merge / GC / rebuild / cold cache / outage / restart storm | **PASS** (6 of 6). Rebuild after losing the data PVC is two `kubectl delete`s plus one `make cas-evict-member`; `blob_put = 0`, bucket unchanged |
| D — the dbt warehouse on tiered bronze | **PASS**: 183 models + 151 tests green, bronze on CAS, one copy for two replicas |

## How it works (GitOps flow)

`make up` runs three stages:

1. **`cluster-create`** — creates the local registry (`k3d-local-dev-registry`) if needed, then
   the `local-dev` k3d cluster (1 server + 3 agents) from the declarative `k3d/cluster.yaml`.
2. **`images-manage-all`** — pulls the operator, metrics-exporter, ClickHouse server, Keeper,
   object-store and `mc` images and **pushes them into the local registry**. The cluster
   mirrors `docker.io` (and `ghcr.io`) to that registry, so nodes pull the pre-pushed images
   locally (with a fallback to the real upstreams). This is used instead of
   `k3d image import` / `kind load`, which break under Docker Desktop's containerd image
   store — see [Image preloading](#image-preloading).
3. **`fluxcd-setup`** — installs Flux, then **pushes each top-level `kubernetes/` directory as its
   own OCI artifact** to the local registry and points Flux at them:

   | Artifact | Source dir | Contains |
   | --- | --- | --- |
   | `cluster-sync` | `kubernetes/clusters` | the Flux `Kustomization`/`OCIRepository` graph |
   | `infra-sync` | `kubernetes/infra` | CRDs + the Altinity HelmRepository |
   | `operators-sync` | `kubernetes/operators` | the operator HelmRelease |
   | `analytics-sync` | `kubernetes/analytics` | Keeper (CHK) + object store + ClickHouse (CHI) + dbt runner |

Flux then reconciles them in dependency order:

```
infra-ks ─▶ clickhouse-operator-ks ─┬▶ keeper-chk ──┬▶ clickhouse-chi ─▶ dbt-runner
(CRDs +      (Altinity operator)    │  (3-node      │  (1 shard ×
 HelmRepo)                          │   Keeper)     │   2 replicas,
                                    └▶ minio ───────┘   CAS disk)
                                       (Silo S3 +
                                        bucket Job)
```

The object store sits *before* the CHI because the CAS disk probes its bucket at boot and
refuses to start without it. Because it is GitOps: edit a manifest, re-run
`make fluxcd-push-artifacts`, and Flux rolls out the change.

## Project Structure

```
k8s-clickhouse/
├── Makefile                       # entrypoint: `make up` / `make down` / `make help`
├── k3d/
│   └── cluster.yaml               # declarative k3d cluster (nodes, image, registry, mirrors)
├── scripts/                       # make modules
│   ├── setup.mk                   # brew tool install
│   ├── k8s.mk                     # k3d cluster + local registry lifecycle
│   ├── images.mk                  # pull + push images to the local registry
│   ├── fluxcd.mk                  # install Flux, push OCI artifacts, wire the sync
│   ├── clickhouse.mk              # ch-status / keeper-status / ch-client / ch-demo
│   ├── minio.mk                   # object store: status / console / ls / conditional-write probe
│   ├── cas.mk                     # CAS: cas-status / cas-demo / cas-gc / cas-drop-cache / cas-evict-member
│   └── warehouse.mk               # dbt + medallion dev loop
├── docs/
│   ├── architecture.md            # the Nimbus warehouse (batch + realtime planes)
│   └── cas-local-s3-plan.md       # CAS: method, gates, drills, recovery procedure
└── kubernetes/
    ├── clusters/local/            # Flux Kustomizations + dependency ordering
    │   ├── flux-system/           # bootstrap OCIRepository + root sync
    │   ├── infra.yaml             # infra-ks
    │   ├── operators.yaml         # clickhouse-operator-ks
    │   └── analytics.yaml         # keeper-chk + minio → clickhouse-chi → dbt-runner
    ├── infra/
    │   ├── crds/                  # vendored Altinity CRDs (release-0.27.1)
    │   └── helm_repository/       # Altinity Helm repo
    ├── operators/clickhouse-operator/{base,local}/
    └── analytics/
        ├── keeper/{base,local}/       # ClickHouseKeeperInstallation (3 nodes)
        ├── minio/{base,local}/        # object store (Silo, S3 API) + bucket Job
        ├── clickhouse/{base,local}/   # ClickHouseInstallation (1 shard × 2 replicas, CAS disk)
        └── dbt/{base,local}/          # in-cluster dbt runner CronJobs
```

## Available Commands

Run `make help` for the full list.

**High-level**

| Command | Description |
| --- | --- |
| `make up` | Create cluster + registry, push images, install Flux, deploy everything |
| `make down` | Delete the k3d cluster (keeps the registry image cache; `make registry-delete` to remove it) |

**ClickHouse operations** (`scripts/clickhouse.mk`)

| Command | Description |
| --- | --- |
| `make ch-status` | Show CHI/CHK, pods, PVCs, services |
| `make keeper-status` | Inspect the 3-node quorum (leader/followers) |
| `make ch-client` | Interactive `clickhouse-client` on replica 0 |
| `make ch-demo` | Create a replicated table, insert on one replica, read from the other |
| `make ch-password PASSWORD=…` | Compute a SHA256 hash for the admin secret |
| `make crds-vendor OPERATOR_VERSION=…` | Re-vendor the operator CRDs |

**Object store** (`scripts/minio.mk`)

| Command | Description |
| --- | --- |
| `make minio-status` | Pod, PVC, service and the bucket Job |
| `make minio-console` | Port-forward the web console on :9001 and print the creds |
| `make minio-ls` | Object count + total bytes in `clickhouse-cas` |
| `make minio-probe` | Prove conditional PUT/DELETE and ranged reads from inside the cluster — what CAS needs |

**CAS** (`scripts/cas.mk`)

| Command | Description |
| --- | --- |
| `make cas-status` | Mounts, disks, parts-by-disk on both replicas, recent `system.cas_log` |
| `make cas-demo` | Build `demo.cas_events` on CAS and print the one-copy evidence |
| `make cas-gc` | `SYSTEM CAS GC RUN` on replica 0, then tail `system.cas_gc_log` |
| `make cas-drop-cache REPLICA=0\|1` | Drop the local read-through cache in front of CAS |
| `make cas-evict-member MEMBER=<id>` | Rebuild a replica after a lost data PVC: retire the pool member, delete its tombstoned `owner` anchor, drop its stale table + database replicas, re-issue `CREATE DATABASE` |

## Configuration

Key settings:

- Cluster shape / k3s image / registry / mirrors: `k3d/cluster.yaml`
  (default `rancher/k3s:v1.33.13-k3s1`, 1 server + 3 agents)
- Cluster & registry names/ports: `scripts/k8s.mk` (`CLUSTER_NAME`, `REG_*`)
- Operator: chart `altinity-clickhouse-operator` `0.27.1`
- ClickHouse server image: `26.6.4.20001.altinityantalya` (Altinity Antalya — CAS needs 26.6.4+)
- Keeper image: `26.3.16.10001.altinitystable` (Altinity Stable LTS)
- Object store image: `pgsty/silo` (see `scripts/images.mk` for why not `minio/minio`)
- CAS disk, cache size and policies: `config.d/cas_disk.xml` inside
  `kubernetes/analytics/clickhouse/base/clickhouse.yaml`
- Admin credentials: user `admin`, password `admin123`
  (rotate via `make ch-password` + `kubernetes/analytics/clickhouse/local/clickhouse-credentials.yaml`)
- Object-store credentials: `kubernetes/analytics/minio/local/minio-credentials.yaml` and its
  copy for the CHI pods in `kubernetes/analytics/clickhouse/local/minio-credentials.yaml`
  (rotate both together)

To resize the cluster, change replica counts, or storage, edit:
- `kubernetes/analytics/keeper/base/keeper.yaml` — `layout.replicasCount`
- `kubernetes/analytics/clickhouse/base/clickhouse.yaml` — `layout.shardsCount` / `replicasCount`
- `kubernetes/analytics/minio/base/statefulset.yaml` — the object store's PVC size

### Local vs production profile

The **base** manifests (`kubernetes/analytics/*/base`) are production-shaped: hard
anti-affinity (one pod per node) and larger resource requests. The **local** overlays
(`kubernetes/analytics/*/local`) patch these for a laptop:

| | base (production) | local overlay |
| --- | --- | --- |
| Anti-affinity | `requiredDuringScheduling` (one pod per node) | `preferredDuringScheduling` (spread if possible) |
| Keeper resources | 256Mi / 1Gi | 128Mi / 512Mi |
| ClickHouse resources | 1Gi / 2Gi | 1Gi / 4Gi (room for merges + the CAS cache) |
| CAS endpoint | in-cluster Silo (a real deployment points this at a real bucket and a scoped IAM principal) | in-cluster Silo |

The default k3d cluster is **1 server + 3 agents** = 3 dedicated worker nodes, so even the
hard-anti-affinity base profile fits (one Keeper / replica per worker). The local overlay keeps
*soft* anti-affinity as a safety margin (it still schedules if you shrink the cluster) and trims
resources for laptops. To run the pure production profile, drop the two `patches:` entries from
the local kustomizations (and add more `agents` in `k3d/cluster.yaml` if you scale out).

## Image preloading

Images are preloaded by **pushing them to the local registry** (`k3d-local-dev-registry`), not
with `k3d image import` / `kind load`. Recent Docker Desktop enables the **containerd image
store**, under which a tag is a multi-platform OCI index (amd64 + arm64 + SBOM/provenance
attestations) with only your host platform's blobs present. Those import tools run
`--all-platforms` and die on the missing content (`content digest … not found`).

Pushing to a registry sidesteps this: `docker push` sends a clean single-platform image, and the
cluster is configured (in `k3d/cluster.yaml`) to mirror **`docker.io`** (project images) and
**`ghcr.io`** (Flux images) to the local registry, with the real upstreams as automatic fallback.
So pods pull the pre-pushed images locally, **regardless** of the Docker image-store setting.

```
docker push localhost:5050/…   ┌─────────────────────────┐   registries.yaml mirror
  (host)  ───────────────────▶ │ k3d-local-dev-registry │ ◀── docker.io / ghcr.io ── node pulls
                               └─────────────────────────┘   (fallback: real upstream, tried last)
```

Note the **port split**: push from the host to `localhost:5050`; in-cluster (mirror + FluxCD
OCIRepository) the registry is `k3d-local-dev-registry:5000`. The mirrors live in
`k3d/cluster.yaml`, so changing them requires recreating the cluster (`make down && make up`);
the registry itself persists across `make down` (image cache). Pushes: `images-push-all`
(project) and `fluxcd-push-images` (Flux).

## Common Issues

- **A node goes `NotReady` mid-run / containerd errors** — usually Docker Desktop VM memory
  pressure (often from other running stacks). Free memory or raise the Docker allocation, then
  `make down && make up`. Check `docker stats` and `kubectl get nodes -o wide`.
- **Pods `Pending`** — the default 3-node cluster satisfies the (soft) local profile and even the
  hard base profile. If you shrank the cluster below the replica count under hard anti-affinity,
  scale `agents` in `k3d/cluster.yaml`. Check `kubectl get nodes` / `kubectl describe pod <pod>`.
- **ClickHouse `CrashLoopBackOff` early on** — it will retry until Keeper is reachable; give the
  Keeper quorum a moment to elect a leader, then it recovers on its own.
- **ClickHouse `CrashLoopBackOff` with `CasProbe: … backend does not enforce conditional
  deletes`** — the object store does not honour `If-Match` on `DeleteObject`, so the CAS disk
  refuses to open the pool. Run `make minio-probe`; the backend must pass every check. Never
  set `skip_access_check` to get past this.
- **ClickHouse `CrashLoopBackOff` after a replica's data PVC was deleted, with `CAS server-root
  '…' is owned by a different server`** — expected: the pod has a fresh local UUID and the pool
  still holds the old identity. Run `make cas-evict-member MEMBER=<server-root-id>` from the
  survivor; it retires the member, clears the anchor, drops the stale replicas and lets the
  schema replay. Procedure and the trap to avoid are in `docs/cas-local-s3-plan.md` §5.
- **CAS writes fail with `mount lease not held … TRANSIENT unavailability, not damage`** —
  the object store is unreachable (scaled down, or the laptop slept and the lease lapsed).
  Nothing is lost; both mounts re-lease themselves within about a minute of the backend
  returning. `make cas-status` shows `state`/`lifecycle`.
- **Flux not syncing** — `kubectl get kustomizations -n flux-system` and
  `flux get kustomizations`; check `flux logs`.
- **`ImagePullBackOff`** — a node couldn't get an image. Images are preloaded into
  `k3d-local-dev-registry` and served via the `docker.io`/`ghcr.io` mirrors; if a mirror is
  missing the node falls back to the upstream (needs egress). Confirm the registry has the image:
  `curl -s http://localhost:5050/v2/altinity/clickhouse-server/tags/list`. Check
  `kubectl describe pod <pod>` for the exact reference.
- **Operator/CHK/CHI not appearing** — confirm CRDs installed:
  `kubectl get crd | grep altinity`.
- **CHK/CHI created but stuck with empty status and no StatefulSet** (operator seems to ignore it) —
  the operator only reconciles resources in its **watched** namespaces. An *empty*
  `watch.namespaces.include` does **not** mean "all namespaces" — the operator then watches only
  its **own** namespace (`clickhouse-operator`), so a CHK/CHI in `clickhouse` is silently never
  reconciled. This repo sets `watch.namespaces.include: [clickhouse, clickhouse-operator]` in the
  operator HelmRelease (`kubernetes/operators/clickhouse-operator/base/helmrelease.yaml`). If you
  deploy ClickHouse into a different namespace, add it there (or use `[".*"]` for true watch-all).

## Learn More

- [Altinity ClickHouse Operator](https://github.com/Altinity/clickhouse-operator)
- [Altinity Operator docs](https://docs.altinity.com/altinitykubernetesoperator/)
- [Altinity CAS: compute–storage separation for MergeTree](https://altinity.com/blog/introducing-cas-drop-in-compute-storage-separation-for-clickhouse-mergetree-tables)
- [CAS configuration guide (Antalya 26.6)](https://github.com/Altinity/ClickHouse/blob/antalya-26.6/docs/en/antalya/cas/configuration.md)
- [ClickHouse Keeper](https://clickhouse.com/docs/guides/sre/keeper/clickhouse-keeper)
- [ReplicatedMergeTree](https://clickhouse.com/docs/engines/table-engines/mergetree-family/replication)
- [Replicated database engine](https://clickhouse.com/docs/engines/database-engines/replicated)
- [FluxCD](https://fluxcd.io/)
