# Nimbus warehouse (dbt + medallion on ClickHouse)

A dbt-based analytics warehouse for a fictional neobank ("Nimbus"), built on the
replicated ClickHouse cluster this repo ships. See the design docs for the full picture:

- [`docs/warehouse-architecture.md`](../docs/warehouse-architecture.md) — scenario, schemas, layer design (the *what/why*).
- [`docs/warehouse-architecture-plan.md`](../docs/warehouse-architecture-plan.md) — the phased build roadmap (the *how/when*).

This README grows phase by phase. Right now it covers **P0** (foundation + replication
spike) and **P1** (bronze tables + synthetic data).

## Layout

```
warehouse/
├── dbt/
│   ├── dbt_project.yml               # project `nimbus`; layer -> database + engine config
│   ├── profiles/profiles.yml         # env-var driven; same file host + in-cluster
│   ├── macros/generate_schema_name.sql  # +schema: marts -> database nimbus_marts
│   ├── seeds/                        # 5 static dimension CSVs + schema.yml (P1)
│   └── models/marts/smoke_replication.sql  # P0 spike model
├── generator/
│   ├── generate.py                   # tier-1 deterministic Python core -> CSV (P1)
│   └── requirements.txt              # (stdlib-only; no runtime deps)
├── loaders/
│   ├── 00_create_raw.sql             # bronze DDL — 8 raw_* tables ON CLUSTER (P1)
│   ├── 10_gen_app_events.sql         # tier-3 native: ~10M app events (P1)
│   └── 20_gen_card_auths.sql         # tier-3 native: ~1M auths + ~3% dupes (P1)
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

## P1 — bronze DDL + synthetic data

Bronze is the 8 raw source tables (`nimbus_raw.raw_*`) populated with deterministic,
referentially-consistent Nimbus data at laptop-real scale (~5k customers, ~1.9M ledger
postings, ~10M app events over 18 months). Three loading tiers, each also a teaching
example of a different technique:

```bash
make wh-bronze                 # create nimbus_* databases + 8 raw_* tables ON CLUSTER (admin)
make wh-generate               # populate everything (SCALE=medium default); prints row counts
make wh-generate SCALE=small   # ~1/10 scale for a quick run
make wh-counts                 # re-print bronze row counts
make wh-drop                   # TRUNCATE all bronze tables (keeps the schema)
# seeds are dbt's job — needs a port-forward (Mode A):
make wh-seed                   # load the 5 static dimension CSVs into nimbus_raw
```

**Tier 1 — Python generator (`generator/generate.py`), the referential backbone.**
Deterministic (`--seed 42`) and **stdlib-only** (runs from a bare `python3`, no venv —
determinism was the hard requirement, and `random` covers everything we need, so Faker
would only add install friction). Emits CSVs in dependency order — customers → KYC events →
accounts → account events → cards → **balanced double-entry ledger** — which
`wh-generate` streams into bronze with `INSERT ... FORMAT CSVWithNames`. Guarantees:

- Every `transaction_id`'s postings sum to zero (customer leg + a Nimbus internal/clearing
  leg). Debits are capped to the running balance, so no account ever goes negative.
- KYC transitions are strictly ordered (`submitted → pending → verified|rejected`, ~85%
  verified); accounts/cards only exist for verified customers.
- A fixed 18-month reference window (`2025-01-01 … 2026-06-30`), so regenerating yields an
  identical `raw_customers` checksum.

**Tier 2 — dbt seeds (`dbt/seeds/*.csv`).** The 5 static dimensions (transaction
categories, MCC codes, fee schedule, countries, risk tiers). They land in `nimbus_raw`
(no `+schema`, so they fall back to `target.schema`) with pinned column types, Replicated.

**Tier 3 — ClickHouse-native loaders (`loaders/10_*.sql`, `loaders/20_*.sql`).** The
high-volume, low-consistency streams, generated in pure SQL from `numbers()` × `rand()`:
`raw_app_events` (~10M) and `raw_card_authorizations` (~1M). Both are still referentially
valid — they pick a real customer / active card via an **array lookup** (a `groupArray`
held as a query-scalar, indexed per row). This is deliberate: `rand()` inside a `JOIN … ON`
gets constant-folded to a single value and would collapse every row onto one entity, so the
index must be materialized in a subquery instead. The card loader then **re-inserts a stable
~3% of rows** (same `auth_id`, later `ingested_at`) — the intentional duplicates that the
silver dedup demo (`argMax(ingested_at)`, P4) collapses back.

**Scale + memory.** `SCALE={small,medium,large}` sets customer count (generator) and app-
event / card-auth volumes (`N_APP_EVENTS` / `N_CARD_AUTHS`, substituted for `__COUNT__` in
the native SQL). The cluster's total memory is capped ~1.35 GiB (0.9 × the 1536Mi laptop
pod limit), which the laptop-real load has to live within. Two levers keep it under the cap
(the P1 memory-risk mitigation) — no extra RAM needed:

- **`loaders/load_bronze.sh`** streams every load in bounded 250k-row blocks (parallel
  parsing off), truncating first so it's re-runnable, with a purge+retry on transient
  memory blips. Because the three big tables are `PARTITION BY toYYYYMM` over 18 months,
  250k blocks give each of the 18 partitions a few large parts instead of hundreds of tiny
  ones — far less background merging.
- **The CHI local overlay** (`kubernetes/analytics/clickhouse/local/clickhouse-patch.yaml`)
  sets `merge_tree/merge_max_block_size: 1024` (default 8192), so each background merge holds
  ~8× fewer rows at once — the merge that would otherwise want 600+ MiB on a full server
  (→ `MEMORY_LIMIT_EXCEEDED`) now fits easily. Base/prod keeps ClickHouse defaults.

`medium` (~2M postings, 10M app events, 1M auths) completes in a couple of minutes on a
laptop and stays comfortably under the cap.

### Bronze invariants (P1 acceptance)

```sql
-- Ledger balances: every transaction's signed postings sum to zero  -> 0
SELECT count() FROM (
  SELECT transaction_id, sum(if(direction='debit', amount_minor, -amount_minor)) s
  FROM nimbus_raw.raw_ledger_postings GROUP BY transaction_id HAVING s != 0);

-- Dedup material present: dupes = rows - distinct auth_ids  -> ~3% of auths, > 0
SELECT count() - uniqExact(auth_id) FROM nimbus_raw.raw_card_authorizations;

-- KYC ordering: no 'verified' before 'submitted' per customer  -> 0
SELECT count() FROM (
  SELECT customer_id,
         minIf(event_ts, new_status='submitted') sub,
         minIf(event_ts, new_status='verified')  ver
  FROM nimbus_raw.raw_kyc_events GROUP BY customer_id
  HAVING ver != 0 AND ver < sub);
```

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
