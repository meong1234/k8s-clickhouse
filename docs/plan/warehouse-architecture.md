# Data Warehouse Architecture — "Nimbus" neobank on ClickHouse + dbt

> **Status:** finalized design, ready to build. No code written yet.
> **Scope:** adds a dbt-based analytics-engineering layer on top of the existing
> replicated ClickHouse cluster, demonstrating medallion architecture, a mart layer, and a
> metrics layer for a fictional neobank.
>
> **Locked decisions (see §13):** in-cluster dbt **Job/CronJob under Flux is required for v1**
> (GitOps-native, host-run kept only as a dev-loop convenience) · **laptop-real scale**
> (~5k customers, ~2M ledger postings, ~10M app events over 18 months) · **dedicated
> least-privilege `dbt` ClickHouse user** · **include the AggregatingMergeTree + materialized-view
> real-time metrics showcase**.

This document is the plan we review *before* building. It defines the scenario, the physical
and logical model, every dbt layer, the synthetic-data strategy, how dbt runs against the
in-cluster ClickHouse, the fintech-specific data tests, and a phased build roadmap.

---

## 1. Scenario: **Nimbus**, a mobile-first neobank

Nimbus offers a **checking + savings account**, a **debit card**, and **P2P / ACH transfers**.
Its revenue model is the real neobank model:

- **Interchange** — a cut (in basis points, varying by merchant category) of every approved card purchase.
- **Fees** — out-of-network ATM, expedited transfer, etc.
- **Net interest** — earned on customer deposits.

This gives us both a **revenue** story and an **engagement / activation** story, and it forces the
correctness-heavy modeling patterns that make fintech the best warehouse-discipline demo:

| Pattern | Where it shows up |
| --- | --- |
| **Double-entry ledger** (postings per transaction sum to zero) | balances, revenue, invariants |
| **Running / as-of-date balances** | `fct_account_daily_balance` (periodic-snapshot fact) |
| **SCD2** (state changes over time) | `dim_customers` (KYC/risk), `dim_accounts` (status/tier) |
| **Idempotent dedup** (retried events arrive multiple times) | card auth stream, silver layer |
| **Late / out-of-order events** | dedup via `argMax(ingested_at)` / `ReplacingMergeTree` |
| **Funnel stitching** | signup → KYC verified → funded → first transaction |

---

## 2. Medallion ↔ dbt layer map

```
BRONZE  ───────────  SILVER  ──────────────────────  GOLD  ───────────  METRICS
nimbus_raw           nimbus_staging /                 nimbus_marts       nimbus_metrics
(landed source)      nimbus_intermediate              (star schema)      (daily grain + cohorts)

raw_customers        → stg_customers        → int_customers_scd2      ─▶ dim_customers (SCD2) ─┐
raw_kyc_events       → stg_kyc_events        ┘                                                 │
raw_accounts         → stg_accounts         → int_accounts_scd2       ─▶ dim_accounts (SCD2)   │
raw_account_events   → stg_account_events    ┘                                                 │
raw_cards            → stg_cards            ─────────────────────────▶ dim_cards               ├─▶ metrics_finance_daily
raw_ledger_postings  → stg_ledger_postings  → int_ledger_categorized ─▶ fct_ledger_postings    ├─▶ metrics_growth_daily
                                            → int_account_daily_balance ▶ fct_account_daily_balance ─▶ metrics_risk_daily
raw_card_auths       → stg_card_auths(dedup)→ int_interchange_revenue ─▶ fct_card_authorizations └─▶ cohorts_retention
raw_app_events       → stg_app_events       → int_activation_funnel   ─▶ fct_app_events
                                            → int_daily_active_users     dim_date (spine)
```

- **Bronze (`nimbus_raw`)** — raw, append-only landing tables that mirror the source systems. Minimal
  typing. Physical dedup key present but *not yet applied*.
- **Silver / staging (`nimbus_staging`)** — typed, renamed, 1:1 with source. **Deduplication happens here**
  (idempotency keys on card auths, `argMax(ingested_at)`). Materialized as **views** (cheap, no storage).
- **Silver / intermediate (`nimbus_intermediate`)** — the hard, reusable logic: SCD2 construction, daily
  running balance against a date spine, transaction categorization, funnel stitching, DAU. Views or
  ephemeral, except the heavy daily-balance model (incremental table).
- **Gold / marts (`nimbus_marts`)** — a conformed **star schema**: SCD2 `dim_*` + `fct_*`, including the
  `fct_account_daily_balance` **periodic-snapshot fact** (grain = account × day).
- **Metrics (`nimbus_metrics`)** — curated daily-grain metric marts + retention cohorts, with documented
  definitions. (See §7 on why this is a metrics *mart*, not dbt MetricFlow.)

Each layer maps to its own **ClickHouse database** (ClickHouse "database" = schema). dbt's
`generate_schema_name` macro is overridden so a model in `models/marts/` lands in `nimbus_marts`,
not the default `target_schema + custom` concatenation.

---

## 3. Physical model — bronze (source tables)

Amounts are stored as **integer minor units** (`amount_minor`, cents) — never floats — per fintech
practice. `ingested_at` is present on every table to support dedup and incremental loads.

| Table | Grain | Key columns | ClickHouse engine |
| --- | --- | --- | --- |
| `raw_customers` | one customer | `customer_id`, `signup_ts`, `email`, `full_name`, `country`, `dob`, `risk_tier`, `referral_source`, `ingested_at` | `ReplacingMergeTree(ingested_at)` ORDER BY `customer_id` |
| `raw_kyc_events` | one KYC status change | `kyc_event_id`, `customer_id`, `event_ts`, `old_status`, `new_status`∈{submitted,pending,verified,rejected}, `reason`, `ingested_at` | `MergeTree` ORDER BY (`customer_id`,`event_ts`) |
| `raw_accounts` | one account (opening snapshot) | `account_id`, `customer_id`, `account_type`∈{checking,savings}, `opened_ts`, `interest_rate_bps`, `ingested_at` | `ReplacingMergeTree(ingested_at)` ORDER BY `account_id` |
| `raw_account_events` | one account state change | `account_event_id`, `account_id`, `event_ts`, `old_status`, `new_status`∈{active,frozen,closed}, `ingested_at` | `MergeTree` ORDER BY (`account_id`,`event_ts`) |
| `raw_cards` | one card | `card_id`, `account_id`, `customer_id`, `issued_ts`, `network`, `status`, `last4`, `ingested_at` | `ReplacingMergeTree(ingested_at)` ORDER BY `card_id` |
| `raw_ledger_postings` | one posting (double-entry leg) | `posting_id`, `transaction_id`, `account_id`, `posting_ts`, `direction`∈{debit,credit}, `amount_minor`, `currency`, `counterparty_account_id`, `category_code`, `mcc`, `description`, `idempotency_key`, `ingested_at` | `MergeTree` PARTITION BY toYYYYMM(`posting_ts`) ORDER BY (`account_id`,`posting_ts`) |
| `raw_card_authorizations` | one auth attempt (**intentionally duplicated**) | `auth_id`, `card_id`, `account_id`, `auth_ts`, `amount_minor`, `currency`, `mcc`, `merchant_name`, `approved`, `decline_reason`, `is_fraud`, `idempotency_key`, `ingested_at` | `MergeTree` PARTITION BY toYYYYMM(`auth_ts`) ORDER BY (`card_id`,`auth_ts`) |
| `raw_app_events` | one app event | `event_id`, `customer_id`, `event_ts`, `event_name`, `device`, `app_version`, `session_id`, `ingested_at` | `MergeTree` PARTITION BY toYYYYMM(`event_ts`) ORDER BY (`customer_id`,`event_ts`) |

**Static dimensions as dbt seeds (CSV):**

| Seed | Columns |
| --- | --- |
| `seed_transaction_categories` | `category_code`, `category`, `is_revenue`, `revenue_type`∈{interchange,fee,interest,none} |
| `seed_mcc_codes` | `mcc`, `merchant_category`, `interchange_rate_bps` |
| `seed_fee_schedule` | `fee_type`, `amount_minor` |
| `seed_countries` | `country`, `region`, `currency` |
| `seed_risk_tiers` | `risk_tier`, `description`, `daily_limit_minor` |

---

## 4. Gold — the star schema

**Dimensions**

- `dim_date` — date spine (day grain, with month/quarter/year, is_weekend, etc.). Generated from a
  `numbers()` range in ClickHouse.
- `dim_customers` — **SCD2**. `customer_key` (surrogate), `customer_id`, `valid_from`, `valid_to`,
  `is_current`, plus `kyc_status`, `risk_tier`, `country`, `region`.
- `dim_accounts` — **SCD2**. `account_key`, `account_id`, `customer_id`, `valid_from`, `valid_to`,
  `is_current`, `status`, `account_type`, `interest_rate_bps`.
- `dim_cards` — `card_key`, `card_id`, `account_id`, `network`, `status`, `last4`.

**Facts**

- `fct_ledger_postings` — grain: one posting. FKs to `dim_accounts`, `dim_date`. `amount_minor`,
  `direction`, `category`, `is_revenue`, `revenue_type`. **Incremental** by `posting_ts` day.
- `fct_account_daily_balance` — grain: **account × day** (periodic snapshot). `opening_balance`,
  `total_deposits`, `total_withdrawals`, `closing_balance`. **Incremental** (`delete+insert` by day).
- `fct_card_authorizations` — grain: one (deduped) auth. `approved`, `is_fraud`, `amount_minor`,
  `interchange_revenue_minor`. **Incremental** by `auth_ts` day.
- `fct_app_events` — grain: one app event. FKs to `dim_customers`, `dim_date`. **Incremental** by day.

**Why SCD2 is *derived*, not a dbt snapshot.** dbt snapshots rely on `UPDATE`-style change capture,
which maps poorly onto ClickHouse (mutations are async and heavy). Because we own the full change-event
history (`raw_kyc_events`, `raw_account_events`), we build SCD2 **deterministically** in the
intermediate layer with window functions (`leadInFrame` to derive `valid_to` from the next change).
This is cleaner, reproducible, and idiomatic for ClickHouse. dbt snapshots remain a documented
alternative with their caveats.

---

## 5. Intermediate models (the interesting logic)

| Model | What it does |
| --- | --- |
| `int_customers_scd2` | Fold `stg_customers` + `stg_kyc_events` into validity intervals via window functions. |
| `int_accounts_scd2` | Same, from `stg_accounts` + `stg_account_events`. |
| `int_ledger_categorized` | Join postings → `seed_transaction_categories` + `seed_mcc_codes`; flag `is_revenue`/`revenue_type`. |
| `int_account_daily_balance` | Cross accounts × `dim_date` spine, running `sum()` of signed postings → daily opening/closing. |
| `int_card_auths_deduped` | Dedup `stg_card_auths` by `auth_id` via `argMax(..., ingested_at)` (idempotency demo). |
| `int_interchange_revenue` | Approved auths × `interchange_rate_bps` (from MCC) → `interchange_revenue_minor`. |
| `int_activation_funnel` | Per customer: `signup_ts`, `kyc_verified_ts`, `first_funded_ts`, `first_txn_ts`. |
| `int_daily_active_users` | Distinct active customers per day from `stg_app_events` (feeds DAU/MAU). |

---

## 6. Metrics layer

Curated, documented, daily-grain marts in `nimbus_metrics`:

- **`metrics_finance_daily`** — `date`, `total_deposits`, `avg_balance_per_customer`,
  `interchange_revenue`, `fee_revenue`, `net_interest`, `total_revenue`, `arpu`.
- **`metrics_growth_daily`** — `date`, `signups`, `kyc_verified`, `accounts_funded`, `first_txn`,
  `activation_rate`, `dau`, `mau`, `stickiness` (DAU/MAU).
- **`metrics_risk_daily`** — `date`, `auth_count`, `decline_rate`, `fraud_rate`, `chargeback_rate`.
- **`cohorts_retention`** — `cohort_month`, `months_since_funding`, `retained_accounts`, `retention_rate`.

**Real-time showcase (included).** `metrics_finance_daily` is the batch, dbt-owned source of truth.
Alongside it we build a **near-real-time** rollup of interchange revenue using a ClickHouse
**`AggregatingMergeTree` target table + incremental `MATERIALIZED VIEW`** that updates on every insert
into the (deduped) card-auth stream — demonstrating a capability batch dbt can't offer. The two are
reconciled by a data test (§10) so the streaming rollup and the batch metric agree. The MV/target
table are created as first-class dbt models (`materialized_view` + a `table` with a custom
`AggregatingMergeTree` engine) so they live in the DAG and replicate `ON CLUSTER` like everything else.

---

## 7. The metrics-layer decision (honest tradeoff)

dbt's official Semantic Layer / **MetricFlow does not meaningfully support ClickHouse**. Options:

1. **Materialized metrics mart** (chosen) — dbt models at a documented daily grain. Production-shaped,
   fast to query, works today, versioned in git.
2. dbt `exposures` + hand-written metric SQL — documents lineage but no compute.
3. A third-party semantic layer (Cube, etc.) — out of scope; adds a service.

We go with **(1)** and treat the metric *definitions* (the SQL + `description` in schema.yml) as the
contract. If we later want a queryable semantic API, Cube-over-ClickHouse is the natural add-on.

---

## 8. Synthetic data — hybrid strategy

Fintech data must be **referentially consistent**: the ledger must balance, KYC transitions must be
ordered, balances must be plausible. Three tiers, each also demonstrating a different loading technique:

1. **Python generator** (`warehouse/generator/`) — deterministic (seeded), builds the coherent core:
   customers → KYC event sequences → accounts → account events → a **balanced double-entry ledger**
   (every `transaction_id`'s postings sum to zero) → cards. Emits CSV/Parquet, loaded into bronze via
   `clickhouse-client`. This is the referential backbone.
2. **dbt seeds** (CSV) — the small static dimensions in §3.
3. **ClickHouse-native** (`warehouse/loaders/*.sql`) — `generateRandom` / `numbers` / `rand` to bulk-
   synthesize the **high-volume, low-consistency** streams (`raw_app_events`, and the
   intentionally-**duplicated** `raw_card_authorizations`) at millions of rows. Shows off ClickHouse
   generation and gives the dedup/DAU models something big to chew on.

Tunable scale (e.g. `SCALE=small|medium|large` → N customers, date range, events/day). Determinism via
a fixed seed so every `make wh-generate` reproduces the same warehouse.

---

## 9. How dbt runs against the in-cluster ClickHouse

**Adapter:** `dbt-clickhouse` (uses `clickhouse-connect` over **HTTP 8123**).

**Canonical runtime — in-cluster Job (Mode B, required for v1).** The dbt project is baked into a
`dbt-runner` image, pushed to the local registry (same pattern as the other images), and run as:

- an on-demand **`Job`** (`make wh-build` / `make wh-test` trigger a fresh Job) for builds, and
- a **`CronJob`** for scheduled refreshes,

both reconciled by **Flux** under `kubernetes/analytics/dbt/{base,local}/`. This keeps the warehouse
truly *on Kubernetes* and GitOps-managed — consistent with how the operator, Keeper, and ClickHouse
are already delivered. The Job connects to the CHI service in-cluster (no port-forward needed) and
reads the `dbt` credentials from a Kubernetes Secret.

**Dev-loop convenience — host-run (Mode A).** For fast iteration while authoring models, `make
wh-portforward` exposes ClickHouse on localhost and dbt runs from a host venv against it. Same project,
same `profiles.yml` (host vs in-cluster host is the only difference, via env var). This is a
convenience, **not** the v1 delivery contract — the Job is.

**Dedicated least-privilege `dbt` user.** Rather than reuse `admin`, we add a scoped `dbt` ClickHouse
user via the operator's `spec.configuration.users` in the CHI, with its password (SHA256) sourced from
a new `dbt-credentials` Secret (mirroring how the admin password is handled today). Grants: `CREATE`,
`DROP`, `INSERT`, `SELECT`, `ALTER` limited to the `nimbus_*` databases (plus `SELECT` on `system` for
dbt introspection). Both the Job and host-run use this user.

**Replication-aware materializations.** The cluster is 1 shard × 2 replicas with `ReplicatedMergeTree`.
Models should materialize `ON CLUSTER '{cluster}'` using **Replicated** engines so dbt-built tables
replicate to both ClickHouse pods — tying the warehouse layer back to the infra the repo showcases.
dbt-clickhouse exposes a `cluster` profile setting + per-model `engine`; **exact adapter behavior to be
validated in P0** and encoded as project defaults.

---

## 10. Data tests (the fintech highlight)

Generic dbt tests (`not_null`, `unique`, `relationships`, `accepted_values`) throughout, **plus**
custom data tests encoding fintech invariants:

- **Ledger balances to zero** — every `transaction_id`'s signed postings sum to 0 (double-entry holds).
- **No unexplained negative balances** — `closing_balance ≥ -overdraft_limit` in `fct_account_daily_balance`.
- **SCD2 integrity** — per entity, no overlapping validity intervals, no gaps, exactly one `is_current`.
- **Funnel monotonicity** — `signup_ts ≤ kyc_verified_ts ≤ first_funded_ts ≤ first_txn_ts`.
- **Dedup effectiveness** — `auth_id` unique in `int_card_auths_deduped` despite dupes in bronze.
- **Revenue reconciliation** — `metrics_finance_daily.interchange_revenue` == sum of per-auth interchange.

Plus `dbt docs` (lineage graph) and `exposures` pointing at the metrics marts.

---

## 11. Repository layout (proposed additions)

```
warehouse/
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles/profiles.yml          # templated; points at port-forwarded ClickHouse
│   ├── models/
│   │   ├── staging/                   # → nimbus_staging (views)
│   │   ├── intermediate/              # → nimbus_intermediate
│   │   ├── marts/                     # → nimbus_marts (dim_*, fct_*)
│   │   └── metrics/                   # → nimbus_metrics
│   ├── seeds/                         # static dimension CSVs
│   ├── macros/                        # generate_schema_name override, SCD2 helpers, tests
│   └── tests/                         # custom fintech data tests
├── generator/                        # Python synthetic-data generator (tier 1)
│   ├── generate.py
│   └── requirements.txt
├── loaders/                          # ClickHouse-native bulk generation (tier 3) + bronze DDL
│   ├── 00_create_raw.sql
│   ├── 10_gen_app_events.sql
│   └── 20_gen_card_auths.sql
├── Dockerfile                        # dbt-runner image (dbt project baked in)
└── README.md

scripts/warehouse.mk                  # make targets (below)

kubernetes/analytics/dbt/             # dbt-runner Job + CronJob, Flux-reconciled (v1, required)
├── base/
│   ├── namespace.yaml
│   ├── job.yaml                       # on-demand `dbt build` / `dbt test`
│   ├── cronjob.yaml                   # scheduled refresh
│   └── kustomization.yaml
└── local/                            # overlay: image tag, schedule, resources

kubernetes/analytics/clickhouse/local/dbt-credentials.yaml  # scoped `dbt` user secret (SHA256)
```

The new `dbt` artifact is added to the Flux dependency chain after `clickhouse-chi`:
`clickhouse-chi ─▶ dbt-runner` (a Kustomization `dependsOn` the ClickHouse being Ready), and the
`analytics` OCI artifact already covers `kubernetes/analytics/`, so pushing artifacts picks it up.

**Make targets (`scripts/warehouse.mk`):**

| Target | Does |
| --- | --- |
| `make wh-image` | Build + push the `dbt-runner` image to the local registry. |
| `make wh-bronze` | Apply bronze DDL (`loaders/00_create_raw.sql`) `ON CLUSTER`. |
| `make wh-generate` | Run the Python generator + ClickHouse-native loaders → populate bronze. |
| `make wh-build` | Trigger an in-cluster **Job**: `dbt seed && dbt run` (silver → gold → metrics). |
| `make wh-test` | Trigger an in-cluster **Job**: `dbt test` (generic + fintech invariants). |
| `make wh-all` | image → bronze → generate → build → test, end to end. |
| `make wh-logs` | Tail the most recent dbt Job's logs. |
| **dev-loop (Mode A)** | |
| `make wh-setup` | Host venv + `dbt-clickhouse` + generator deps + `dbt deps`. |
| `make wh-portforward` | Port-forward the CHI service (HTTP 8123) to localhost. |
| `make wh-build-local` | `dbt build` from the host against the port-forwarded ClickHouse. |
| `make wh-docs` | `dbt docs generate && dbt docs serve` (host). |

---

## 12. Phased build roadmap

| Phase | Deliverable |
| --- | --- |
| **P0** | Scoped `dbt` user + `dbt-credentials` secret in the CHI; scaffold `warehouse/dbt`; wire dbt→ClickHouse via **host-run** (fast dev loop); validate `ON CLUSTER`/Replicated behavior; one trivial model builds & replicates. |
| **P1** | Bronze DDL + hybrid generator (Python core + native streams + seeds) at **laptop-real scale** (~5k customers, ~2M postings, ~10M app events, 18 months); bronze populated. |
| **P2** | **Vertical slice:** ledger → `stg_ledger_postings` → `int_account_daily_balance` → `fct_account_daily_balance` → one finance metric. Whole stack proven end-to-end. |
| **P3** | **In-cluster runtime:** `dbt-runner` image + `make wh-image`; Job + CronJob under `kubernetes/analytics/dbt/`, wired into Flux (`dependsOn` clickhouse-chi). `make wh-build`/`wh-test` run in-cluster. This is the v1 delivery contract. |
| **P4** | Full silver: all staging (with dedup) + intermediate (SCD2, funnel, DAU, interchange). |
| **P5** | Full gold star schema: all `dim_*` (SCD2) + `fct_*`. |
| **P6** | **Superseded — real-time restructure (lambda)** per [`realtime-warehouse-architecture.md`](realtime-warehouse-architecture.md): the medallion runs continuously as a ClickHouse MV cascade (`nimbus_stream`/`nimbus_rt`), dbt splits into control plane + batch spine, batch-truth metrics land here. Replaces the single-rollup showcase originally planned. |
| **P7** | Data tests (generic + fintech invariants incl. streaming-vs-batch reconciliation), retention cohorts, `dbt docs`, exposures, README. |

> The in-cluster Job (P3) lands right after the vertical slice so the GitOps runtime is proven early on
> a small DAG, then the model library grows underneath a runtime that already works. Host-run (P0)
> stays available throughout as the authoring dev-loop.

---

## 13. Decisions (locked)

1. **dbt run location** — **In-cluster Job/CronJob under Flux is the v1 contract** (Mode B). Host-run
   (Mode A) is retained as a dev-loop convenience only. → §9, P0/P3.
2. **Scale** — **laptop-real**: ~5k customers, ~2M ledger postings, ~10M app events over 18 months.
   Enough to exercise incremental models and feel real; fits the 3-node k3d cluster. → §8, P1.
3. **dbt user** — **dedicated least-privilege `dbt`** ClickHouse user + grants on `nimbus_*`, password
   from a `dbt-credentials` Secret. Not `admin`. → §9, P0.
4. **Real-time metrics** — **superseded and expanded** (2026-07): the single showcase rollup grew
   into a full real-time serving plane — see
   [`realtime-warehouse-architecture.md`](realtime-warehouse-architecture.md) (lambda: MV-cascade
   serving + this batch layer as the correctness spine, bound by reconciliation tests). → P6.
