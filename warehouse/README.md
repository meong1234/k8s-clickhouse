# Nimbus warehouse (dbt + medallion on ClickHouse)

A dbt-based analytics warehouse for a fictional neobank ("Nimbus"), built on the
replicated ClickHouse cluster this repo ships. See the design docs for the full picture:

- [`docs/warehouse-architecture.md`](../docs/warehouse-architecture.md) — scenario, schemas, layer design (the *what/why*).
- [`docs/warehouse-architecture-plan.md`](../docs/warehouse-architecture-plan.md) — the phased build roadmap (the *how/when*).

This README grows phase by phase. Right now it covers **P0** — the foundation and the
replication spike.

## Layout

```
warehouse/
├── dbt/
│   ├── dbt_project.yml               # project `nimbus`; layer -> database + engine config
│   ├── profiles/profiles.yml         # env-var driven; same file host + in-cluster
│   ├── macros/generate_schema_name.sql  # +schema: marts -> database nimbus_marts
│   └── models/marts/smoke_replication.sql  # P0 spike model
└── README.md
```

Make targets live in [`scripts/warehouse.mk`](../scripts/warehouse.mk) (`make warehouse-help`).

## Dev loop (host, Mode A)

```bash
make wh-setup          # one-time: create warehouse/.venv + install dbt-clickhouse
make wh-portforward    # in a second terminal: svc/clickhouse-clickhouse 8123 -> localhost
make wh-debug          # dbt debug (connection check)
make wh-build-local    # dbt build
make wh-test-local     # dbt test
```

The dbt project connects over HTTP 8123 as the scoped **`dbt`** ClickHouse user (password
`dbt123`, dev-only). Host vs in-cluster differ only by `NIMBUS_CH_HOST` (defaults to
`localhost`; the in-cluster P3 runner will set `clickhouse-clickhouse`).

---

## P0 spike findings — `dbt-clickhouse` + `ON CLUSTER` + replication

The whole point of P0 was to *validate* how the adapter drives a replicated cluster and how a
least-privilege user can be expressed GitOps-natively. Environment: server
`26.3.16.altinitystable`, `dbt-core 1.11.12`, `dbt-clickhouse 1.10.1`, cluster `default`
(1 shard × 2 replicas), Atomic databases.

### 1. Replicated engine is NOT automatic — name it explicitly

Setting `cluster:` in the profile makes dbt append `ON CLUSTER '<cluster>'` to DDL, but on
adapter 1.10.1 it does **not** auto-convert `MergeTree → ReplicatedMergeTree`. A model with
no `engine` config produced a plain `MergeTree` on both nodes (the table *object* is
distributed by `ON CLUSTER`, but data does **not** replicate — the second replica stayed
empty).

**Fix (encoded in `dbt_project.yml`):** table layers set `+engine: ReplicatedMergeTree`. A
*bare* `ReplicatedMergeTree` (no path args) is enough because the operator configures
server-level defaults:

```
default_replica_path = /clickhouse/tables/{uuid}/{shard}
default_replica_name = {replica}
```

With Atomic databases the `{uuid}` macro gives every table a unique keeper path, so
full-refreshes never collide. Verified: engine `ReplicatedMergeTree`, the row present on
**both** `chi-clickhouse-default-0-0-0` and `-0-1-0`, `system.replicas` → `total_replicas=2,
active_replicas=2, is_readonly=0, absolute_delay=0`.

### 2. Least-privilege `dbt` user — `allow_databases` is not enough; use config-native `<grants>`

The plan's first idea was a legacy `users.xml` `allow_databases` allow-list. Two hard blockers
surfaced:

- **`allow_databases` cannot grant global privileges.** With `cluster:` set, dbt introspects
  via `clusterAllReplicas(...)` (`list_relations_without_caching`), which requires the **global
  `REMOTE`** privilege. Creating models `ON CLUSTER` additionally requires the global
  **`CLUSTER`** privilege. Neither is expressible as a database allow-list.
- **XML users can't be SQL-granted.** `GRANT ... TO dbt` fails with
  `ACCESS_STORAGE_READONLY` — an operator-defined user lives in the read-only `users_xml`
  access storage. So even a "run GRANTs as admin" bootstrap script would not have worked for
  this user; the grants must come from the CHI config itself.

**Fix (GitOps-native, no SQL bootstrap):** the Altinity operator's `users:` block is a
pass-through to `<users>` in `users.xml`, which supports a `<grants>` element. We express the
grants declaratively in the CHI:

```yaml
dbt/grants/query:
  - "GRANT REMOTE ON *.*"      # clusterAllReplicas introspection
  - "GRANT CLUSTER ON *.*"     # run ON CLUSTER DDL
  - "GRANT SELECT ON system.*" # dbt introspection
  - "GRANT ALL ON nimbus_raw.*"
  - "GRANT ALL ON nimbus_staging.*"
  - "GRANT ALL ON nimbus_intermediate.*"
  - "GRANT ALL ON nimbus_marts.*"
  - "GRANT ALL ON nimbus_metrics.*"
```

`GRANT ALL` is scoped **per database**, so `dbt` has full DDL/DML on the `nimbus_*` databases
only and is denied everything else. Verified: as `dbt`, `SELECT`/`DROP` on `demo.events` →
`ACCESS_DENIED`; `admin` is unaffected (`make ch-demo` still passes). *(Possible future
tightening: replace `GRANT ALL` with the explicit verb list `CREATE …, DROP …, SELECT, INSERT,
ALTER, TRUNCATE, OPTIMIZE`; `ALL` also grants db-scoped role/user admin we don't use.)*

### 3. Two profile settings that matter

- `check_exchange`/`cluster_mode`: **`cluster_mode: false`**. We don't need dbt's distributed /
  exchange behaviors (this cluster is replicated, not sharded). `cluster:` alone drives the
  `ON CLUSTER` DDL.
- Leftover keeper znodes after a full-refresh are **not** a leak: Atomic databases defer drops
  by `database_atomic_delay_before_drop_table_sec` (480s) to allow `UNDROP`; the znodes GC when
  the deferred drop runs. `system.replicas` stays clean throughout.

### Net working configuration

| Where | Setting |
| --- | --- |
| `profiles.yml` | `cluster: default`, `cluster_mode: false` |
| `dbt_project.yml` | table layers `+engine: ReplicatedMergeTree` |
| CHI `users:` | `dbt/grants/query: [REMOTE, CLUSTER, SELECT system, ALL nimbus_*]` |

All acceptance criteria for P0 pass, entirely via Flux (no hand `kubectl apply` of the
CHI/Secret).
