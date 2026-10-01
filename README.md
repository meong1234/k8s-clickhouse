# Kubernetes ClickHouse Deployment

A production-shaped, GitOps deployment of **ClickHouse** on a local Kubernetes cluster
(**k3d** — k3s-in-Docker), managed by the **Altinity ClickHouse Operator** and **FluxCD**.

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
| **Operator** | Altinity `clickhouse-operator` `0.27.1` via HelmRelease. |
| **Images** | Altinity **Stable/LTS** builds: `clickhouse-server` & `clickhouse-keeper` `26.3.16.10001.altinitystable`. |

```
                    ┌─────────────────────────────────────────┐
                    │            ClickHouse Keeper             │
                    │  keeper-0     keeper-1     keeper-2       │   3-node Raft quorum
                    │  (leader)     (follower)   (follower)     │   (coordination)
                    └──────▲──────────▲──────────▲──────────────┘
                           │  Keeper protocol (2181)   │
              ┌────────────┴───────────┐   ┌───────────┴────────────┐
              │   ClickHouse replica 0 │◀─▶│   ClickHouse replica 1 │   ReplicatedMergeTree
              │   chi-...-0-0-0        │   │   chi-...-0-1-0        │   (data replication)
              └────────────────────────┘   └────────────────────────┘
                        shard 0                     shard 0
```

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
- **Real replication.** With 2 replicas per shard, `ReplicatedMergeTree` keeps a full copy of
  the data on both pods and syncs through Keeper. The operator auto-generates `<remote_servers>`,
  per-pod `<macros>` (`{shard}`/`{replica}`/`{cluster}`) and `<zookeeper>`, so replicated DDL
  "just works" (see [the replication demo](#4-prove-replication)).
- **Keeper referenced by name.** The CHI points at the CHK via
  `spec.configuration.zookeeper.keeper.name` — the operator discovers the 3 endpoints and
  retries automatically, so nothing hardcodes a service DNS name.
- **Persistent storage.** Keeper and ClickHouse each use PVCs (k3s's default `local-path` StorageClass).
- **Version-pinned, vendored CRDs.** The four operator CRDs are vendored under
  `kubernetes/infra/crds` (not installed by a Helm hook), so the bootstrap has no reconcile-time
  external dependency and the chart's `bitnami/kubectl` crd-job is disabled.
- **Secret-based admin password.** The admin password (SHA256) comes from a Kubernetes Secret,
  injected via `from_env` — never inline in the CHI.

## Prerequisites

- Docker
- Homebrew (macOS) — or install the tools below manually
- `kubectl`, `k3d`, `kustomize`, `flux` (≥ 2.9 needs k8s ≥ 1.33 — satisfied by k3s 1.33), and optionally `k9s`

Install everything with:

```bash
make brew-setup-all
```

> ⚠️ **Resource note:** this brings up 4 k3d nodes (1 server + 3 agents) running
> 3 Keeper pods + 2 ClickHouse pods + the operator + Flux. Give Docker Desktop **≥6 GB RAM /
> 4 CPUs** (Settings → Resources). If the Docker VM is starved by other running stacks, its
> kernel/OOM-killer can take down a node — free memory or raise the Docker allocation, then
> `make down && make up`.

## Quick Start

```bash
# 1. Create the cluster + registry, push images, install Flux, push artifacts, sync
make up

# 2. Watch it converge (operator → keeper → clickhouse)
make ch-status          # repeat until 3 keeper + 2 clickhouse pods are Running

# 3. Check the Keeper quorum (expect one leader + two followers)
make keeper-status

# 4. Prove replication end-to-end
make ch-demo

# 5. Tear everything down
make down
```

### 4. Prove replication

`make ch-demo` creates a `ReplicatedMergeTree` table **on the whole cluster**, inserts rows on
**replica 0**, then reads the identical rows back from **replica 1** — data it never received
directly, only through Keeper-coordinated replication:

```sql
CREATE TABLE demo.events ON CLUSTER '{cluster}' (
    id UInt64, ts DateTime DEFAULT now(), msg String
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/demo/events', '{replica}')
ORDER BY id;
```

Open an interactive session any time with `make ch-client` (user `admin`, password `admin123`).

## How it works (GitOps flow)

`make up` runs three stages:

1. **`cluster-create`** — creates the local registry (`k3d-local-dev-registry`) if needed, then
   the `local-dev` k3d cluster (1 server + 3 agents) from the declarative `k3d/cluster.yaml`.
2. **`images-manage-all`** — pulls the operator, metrics-exporter, ClickHouse server and Keeper
   images and **pushes them into the local registry**. The cluster mirrors `docker.io` (and
   `ghcr.io`) to that registry, so nodes pull the pre-pushed images locally (with a fallback to
   the real upstreams). This is used instead of `k3d image import` / `kind load`, which break
   under Docker Desktop's containerd image store — see [Image preloading](#image-preloading).
3. **`fluxcd-setup`** — installs Flux, then **pushes each top-level `kubernetes/` directory as its
   own OCI artifact** to the local registry and points Flux at them:

   | Artifact | Source dir | Contains |
   | --- | --- | --- |
   | `cluster-sync` | `kubernetes/clusters` | the Flux `Kustomization`/`OCIRepository` graph |
   | `infra-sync` | `kubernetes/infra` | CRDs + the Altinity HelmRepository |
   | `operators-sync` | `kubernetes/operators` | the operator HelmRelease |
   | `analytics-sync` | `kubernetes/analytics` | Keeper (CHK) + ClickHouse (CHI) |

Flux then reconciles them in dependency order:

```
infra-ks ─▶ clickhouse-operator-ks ─▶ keeper-chk ─▶ clickhouse-chi
(CRDs +      (Altinity operator)       (3-node        (1 shard ×
 HelmRepo)                              Keeper)        2 replicas)
```

Because it is GitOps: edit a manifest, re-run `make fluxcd-push-artifacts`, and Flux rolls
out the change.

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
│   └── clickhouse.mk              # ch-status / keeper-status / ch-client / ch-demo
└── kubernetes/
    ├── clusters/local/            # Flux Kustomizations + dependency ordering
    │   ├── flux-system/           # bootstrap OCIRepository + root sync
    │   ├── infra.yaml             # infra-ks
    │   ├── operators.yaml         # clickhouse-operator-ks
    │   └── analytics.yaml         # keeper-chk → clickhouse-chi
    ├── infra/
    │   ├── crds/                  # vendored Altinity CRDs (release-0.27.1)
    │   └── helm_repository/       # Altinity Helm repo
    ├── operators/clickhouse-operator/{base,local}/
    └── analytics/
        ├── keeper/{base,local}/       # ClickHouseKeeperInstallation (3 nodes)
        └── clickhouse/{base,local}/   # ClickHouseInstallation (1 shard × 2 replicas)
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

## Configuration

Key settings:

- Cluster shape / k3s image / registry / mirrors: `k3d/cluster.yaml`
  (default `rancher/k3s:v1.33.13-k3s1`, 1 server + 3 agents)
- Cluster & registry names/ports: `scripts/k8s.mk` (`CLUSTER_NAME`, `REG_*`)
- Operator: chart `altinity-clickhouse-operator` `0.27.1`
- ClickHouse/Keeper image: `26.3.16.10001.altinitystable` (Altinity Stable LTS)
- Admin credentials: user `admin`, password `admin123`
  (rotate via `make ch-password` + `kubernetes/analytics/clickhouse/local/clickhouse-credentials.yaml`)

To resize the cluster, change replica counts, or storage, edit:
- `kubernetes/analytics/keeper/base/keeper.yaml` — `layout.replicasCount`
- `kubernetes/analytics/clickhouse/base/clickhouse.yaml` — `layout.shardsCount` / `replicasCount`

### Local vs production profile

The **base** manifests (`kubernetes/analytics/*/base`) are production-shaped: hard
anti-affinity (one pod per node) and larger resource requests. The **local** overlays
(`kubernetes/analytics/*/local`) patch these for a laptop:

| | base (production) | local overlay |
| --- | --- | --- |
| Anti-affinity | `requiredDuringScheduling` (one pod per node) | `preferredDuringScheduling` (spread if possible) |
| Keeper resources | 256Mi / 1Gi | 128Mi / 512Mi |
| ClickHouse resources | 1Gi / 2Gi | 512Mi / 1.5Gi |

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
- [ClickHouse Keeper](https://clickhouse.com/docs/guides/sre/keeper/clickhouse-keeper)
- [ReplicatedMergeTree](https://clickhouse.com/docs/engines/table-engines/mergetree-family/replication)
- [FluxCD](https://fluxcd.io/)
