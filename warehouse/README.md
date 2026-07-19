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

## P2 — vertical slice (ledger → daily balance → finance metric)

P2 proves the whole modeling stack on **one thread** and establishes the patterns every later
phase copies: the incremental-model shape, the per-account date spine, and the first singular
tests. Build it host-side (Mode A — needs a port-forward):

```bash
make wh-build-local   # seeds + stg → int → fct → metric, then all tests
make wh-test-local    # tests only
```

The DAG (all `ON CLUSTER`, replicated to both pods):

```
raw_ledger_postings ─▶ stg_ledger_postings (view)  ─┐
raw_accounts ──────────────────────────────────────┴▶ int_account_daily_balance (incremental)
                                                        └▶ fct_account_daily_balance (incremental)
                                                             └▶ metrics_finance_daily (table)
```

**Sign convention (defined once, in `stg_ledger_postings`).** Bronze stores `amount_minor` as a
positive magnitude with the sign in `direction`. Staging folds it into
`signed_amount_minor = if(direction='credit', +amount, -amount)` — from the **account's**
perspective: credit = money in (deposit), debit = money out (withdrawal). Everything downstream
reads the signed column, never the raw magnitude.

**Real accounts only.** The ledger is balanced double-entry, so every transaction also has an
internal `NIMBUS-*` clearing leg. Daily balances join to `raw_accounts` to keep only real
customer accounts; the internal legs are excluded (but still make each transaction sum to zero,
which `tests/assert_ledger_balances.sql` verifies).

**The incremental running-balance pattern (`int_account_daily_balance`).** Grain = account × day,
with a row for **every** day each account exists (a per-account date spine from `opened_ts` to the
max posting day, via `ARRAY JOIN range(...)`), so `closing_balance` carries across zero-posting
days. `closing_balance` is a cumulative `sum(daily_net) OVER (PARTITION BY account ORDER BY day)`.
That is inherently full-history, so incrementality uses a **carried-opening window**
(`incremental_strategy: delete_insert`, `partition_by: toYYYYMM(day)`):

- each run recomputes only the trailing window `[max(stored day) − daily_balance_lookback_days,
  max]` (the var defaults to 3 — set in `dbt_project.yml`);
- the window's opening balance is **seeded** from the `closing_balance` already stored in
  `{{ this }}` on the last day before the window, so the running sum stays correct without
  recomputing all of history;
- `delete_insert` replaces exactly the recomputed `(account_id, day)` tuples → only recent
  `toYYYYMM` partitions are touched.

**Caveats to know (they apply to every incremental model built after this one):**

1. **Late data older than the window is not reflected.** A posting dated before
   `max(day) − daily_balance_lookback_days` is neither re-aggregated nor propagated into the
   already-frozen later balances. Fix with a targeted rebuild —
   `dbt run --full-refresh --select int_account_daily_balance` (seconds at this scale). The
   lookback var only sizes the slack for *slightly*-late data.
2. **The window tracks `max(day)` in the table, not wall-clock** — a future-dated posting shifts
   it.
3. **The `delete_insert` DELETE/INSERT is emitted without `ON CLUSTER`.** Correctness on 1 shard ×
   2 replicas relies on ReplicatedMergeTree propagating via Keeper — fine here, **not**
   sharded-safe. Don't copy this model into a multi-shard context expecting `ON CLUSTER` fan-out.

`fct_account_daily_balance` is a thin incremental projection of the intermediate model at the mart
grain (P5 adds the SCD2 `account_key` + `dim_date` FK). `metrics_finance_daily` is the v1 finance
metric mart — `date`, `total_deposits`, `avg_balance_per_customer` (revenue columns arrive in P6);
the metric definitions in its `schema.yml` are the contract.

### Balance invariants (P2 acceptance)

```sql
-- Grain: (account_id, day) is unique                                    -> tests/assert_account_daily_balance_unique.sql
-- Continuity: closing_balance = opening_balance + daily_net on every row -> tests/assert_balance_continuity.sql
-- Ledger still balances through staging                                  -> tests/assert_ledger_balances.sql

-- Spot check: fct closing equals a hand-computed running sum off bronze (-> 0 mismatches)
WITH raw_run AS (
  SELECT account_id, toDate(posting_ts) d,
         sum(sum(if(direction='credit',amount_minor,-amount_minor)))
             OVER (PARTITION BY account_id ORDER BY toDate(posting_ts)) exp
  FROM nimbus_raw.raw_ledger_postings
  WHERE account_id GLOBAL IN (SELECT account_id FROM nimbus_raw.raw_accounts)
  GROUP BY account_id, d)
SELECT count() FROM raw_run r
JOIN nimbus_marts.fct_account_daily_balance f ON f.account_id=r.account_id AND f.date=r.d
WHERE r.exp != f.closing_balance;
```

> **Memory note (P1 mitigation still applies).** The pods are capped ~1.35 GiB. If a `dbt build`
> fails with `MEMORY_LIMIT_EXCEEDED` at connect/introspect time, the server RSS (cgroup memory,
> incl. page cache from earlier heavy reads) has crept over the cap — restart the pods
> (`kubectl -n clickhouse delete pod chi-clickhouse-default-0-0-0 chi-clickhouse-default-0-1-0`)
> and build against the fresh, cold-cache server. P2's working set (the ~2M-row ledger + spine)
> fits comfortably once RSS is reset.

---

## P3 — in-cluster runtime (the v1 contract, "Mode B")

Everything above runs **Mode A**: dbt on your laptop (`warehouse/.venv`) reaching ClickHouse
through `make wh-portforward`. Handy for authoring, but it is not how the warehouse ships. The
locked v1 delivery contract is **Mode B** — dbt runs **inside the cluster**, as a Flux-managed
Job, talking to the CHI service directly (no host, no port-forward), reading its password from a
Secret. P3 lands that runtime now, on the small P2 DAG, so every later phase grows under a GitOps
runtime that already works. Mode A stays as a dev-loop convenience.

### The runner image

`warehouse/Dockerfile` bakes the dbt project + a pinned `dbt-clickhouse==1.10.1` (the same adapter
the host venv uses) into `python:3.12-slim`, with `DBT_PROFILES_DIR` preset and a default
`CMD ["dbt","build"]`. **No secrets are baked in** — the committed `profiles/profiles.yml` already
resolves the host and password from `NIMBUS_CH_HOST` / `NIMBUS_CH_PASSWORD` via `env_var()`, and
the CronJob supplies both. The build context is the **repo root** (the `COPY` is repo-relative),
so `.dockerignore` keeps `.venv` / generated data / `target` out of it.

```bash
make wh-image     # docker build -f warehouse/Dockerfile . -> push localhost:5050/nimbus/dbt-runner:local
```

The manifest references the image by the **in-cluster** registry name
(`k3d-local-dev-registry:5000/nimbus/dbt-runner:local`) directly: `nimbus/dbt-runner` is not a
docker.io path, so it does not resolve through the docker.io→registry mirror; k3s resolves the
registry host via the k3d registry config. The `:local` tag is mutable and `imagePullPolicy:
Always`, so each `wh-image` rebuild is what the next `wh-build` runs.

### Manifests — `kubernetes/analytics/dbt/{base,local}`

Two CronJobs in the existing `clickhouse` namespace (where the `dbt-credentials` Secret already
lives — no cross-namespace copying):

- **`dbt-runner`** — `dbt build`; **`dbt-tester`** — `dbt test`. Why two? `kubectl create job
  --from=cronjob` copies the Job spec verbatim and **cannot override the container command**, so a
  second CronJob is the zero-`yq` way to give `wh-test` a clean source.
- Both are `suspend: true` with `schedule: "0 */3 * * *"`. The schedule stays **suspended even
  locally** — the drive path is a manual Job (`make wh-build` / `wh-test`), and a laptop cluster
  should not fire unattended builds. The schedule is there so the runtime is a real CronJob a
  future environment can simply un-suspend.
- Flux wiring: a `dbt-runner` Kustomization in `kubernetes/clusters/local/analytics.yaml`
  (`dependsOn: clickhouse-chi`, `path: ./dbt/local`), so the CHI + Secret exist first.

**Secret — one password, two forms.** The CHI stores the `dbt` user by SHA256 hash, but dbt sends
the real password over HTTP. So `dbt-credentials` now carries **both** keys for the same dev value
(`dbt123`): `dbt_password_sha256_hex` (CHI) and the new plaintext `dbt_password` (the runner env).
To rotate: `make ch-password PASSWORD=…` for the hash, update the plaintext, and update the
`profiles.yml` default.

### Run it

```bash
# One-time / after any model or Dockerfile change:
make wh-image                 # build + push the runner image
make fluxcd-push-artifacts    # re-push analytics-sync (new dbt/ + secret key) AND cluster-sync
flux reconcile source oci analytics-source   # or wait for the 1m interval
flux get kustomizations       # -> dbt-runner Ready
kubectl -n clickhouse get cronjob             # -> dbt-runner, dbt-tester (SUSPEND=True)

# The actual in-cluster runtime:
make wh-build                 # Job from cronjob/dbt-runner; waits, streams logs, fails loud
make wh-test                  # Job from cronjob/dbt-tester (dbt test)
make wh-logs                  # tail the latest dbt-runner pod (JOB=<name> to target one)
make wh-all                   # image -> bronze -> generate -> build -> test, end to end
```

### P3 acceptance

- `flux get kustomizations` shows `dbt-runner` **Ready**; both CronJobs exist, suspended.
- `make wh-build` runs the **P2 DAG in-cluster** to `Complete` (exit 0); `make wh-logs` shows dbt's
  model-by-model output; `make wh-test` is green.
- **Self-sufficient, no host dbt:** `DROP DATABASE nimbus_marts ON CLUSTER '{cluster}'`
  (`make ch-client`), then `make wh-build` rebuilds `fct_account_daily_balance` on both replicas.
- The host dev-loop (`make wh-build-local`) still works unchanged.

> **Not yet (future extension).** Scheduling-policy tuning and alerting on Job failure are out of
> scope for P3. Today a failed run surfaces via `make wh-build`'s non-zero exit and `wh-logs`; a
> later phase can wire a Flux `Alert`/`Provider` (notification-controller is already installed) to
> push CronJob failures somewhere.

---

## P4 — full silver (staging + intermediate)

P4 fills out the entire silver layer on top of the P3 runtime: **7 staging views** (`stg_customers`,
`stg_kyc_events`, `stg_accounts`, `stg_account_events`, `stg_cards`, `stg_app_events`,
`stg_card_auths` — joining the P2 `stg_ledger_postings`) and **6 intermediate models** carrying the
teaching-value logic. `sources.yml` now declares all 8 `raw_*` tables.

- **Staging = typed/renamed 1:1 views.** The three `ReplacingMergeTree` sources
  (customers/accounts/cards) and the duplicate-bearing `raw_card_authorizations` are collapsed to
  one row per key with the new `dedupe_latest()` macro (`argMax(col, ingested_at) GROUP BY key`) —
  the same idiom P2 used inline, now reusable. Append-only event streams pass straight through.
- **`int_customers_scd2` / `int_accounts_scd2`** — SCD2 histories derived from the change-event logs
  with `leadInFrame` windowing; the open interval carries a far-future sentinel `valid_to`
  (`2999-12-31`) plus `is_current`, which keeps contiguity tests and P5 as-of joins NULL-free.
- **`int_ledger_categorized`** joins the category + MCC seeds; **`int_interchange_revenue`** computes
  interchange with integer bps math (`intDiv(amount_minor * rate_bps, 10000)`, per-auth so P6's
  reconciliation holds); **`int_activation_funnel`** is one wide row per customer; and
  **`int_daily_active_users`** is a `uniqExact` day rollup materialized as a table (the one heavy
  scan), not a view.

**Dedup folds into staging (note vs architecture §5).** The `docs/warehouse-architecture.md` §5
lineage shows a separate `int_card_auths_deduped` node. We deliberately **fold that dedup into
`stg_card_auths`** instead — staging is where §2 says dedup belongs, and a passthrough intermediate
model would add nothing. `int_interchange_revenue` (and P5's `fct_card_authorizations`) read the
already-deduped staging view directly. The `unique` test on `stg_card_auths.auth_id` is the
dedup-effectiveness check: it passes while `nimbus_raw.raw_card_authorizations` still holds the ~3%
duplicates.

The P2 `int_account_daily_balance` and `metrics_finance_daily` now read `stg_accounts` instead of
re-deduping `raw_accounts` inline (the TODO their P2 comments left for P4).

### P4 acceptance

- `make wh-build && make wh-test` green in-cluster (full DAG ≈ 25+ models).
- **Dedup:** `count() = uniqExact(auth_id)` in `nimbus_staging.stg_card_auths`, while bronze
  `raw_card_authorizations` still has `count() > uniqExact(auth_id)`.
- **SCD2 spot check** (a customer with ≥2 KYC events): intervals contiguous (each `valid_to` = next
  `valid_from`), no overlap, exactly one `is_current`.
- **Funnel monotonicity** (`signup ≤ verified ≤ funded ≤ txn`) holds for all customers (spot query;
  formal test in P7).
- **DAU** covers the 18-month window with plausible values, no spurious zero-gaps.

---

## P5 — full gold (star schema)

P5 builds the conformed **star schema** in `nimbus_marts` (§4): **4 dimensions** + **4 incremental
facts**, wired together with `relationships` tests and a documented column on every mart — this layer
is the contract.

- **Dimensions (tables).** `dim_date` is a gap-free `numbers()` spine (2024-01-01..2027-12-31).
  `dim_customers` / `dim_accounts` are thin SCD2 projections of the P4 intermediates carrying **all**
  versions (never `where is_current`), with a surrogate `*_key = cityHash64(natural_key, valid_from)`.
  `dim_cards` is type-1 (`cityHash64(card_id)`). Each non-date dim carries an **unknown-member
  (key 0)** row so an ASOF no-match (`join_use_nulls=0` → default 0, not NULL) stays
  relationships-valid.
- **Surrogate-key discipline.** The key is **computed in the dim only**; every fact **pulls** it from
  the ASOF-matched dim row and never recomputes `cityHash64` — a recompute with a differently-typed
  argument would silently mint a non-matching key. This makes `relationships` pass by construction.
- **As-of FK resolution (the teaching moment).** Facts attribute each event to the dim **version in
  effect at event time** via `ASOF LEFT JOIN … AND event_ts >= valid_from` (equality first, one
  non-strict inequality **last**). Because SCD2 intervals are contiguous, the greatest
  `valid_from ≤ event_ts` is exactly the containing version, so `valid_to` is never tested.
  `fct_account_daily_balance` is the one Date-grain fact: it resolves **end-of-day**
  (`toDateTime(date)+86400-1 >= valid_from`) to avoid silent midnight coercion. `fct_card_authorizations`
  stages its two ASOF joins **one-per-CTE** (multi-ASOF-in-one-SELECT is fragile) and carries both
  `account_key` and `customer_key`.
- **Incremental facts.** `delete_insert` keyed on the event's natural id over a trailing
  `event_lookback_days` (1) window — idempotent by construction, `>=` (not `>`) so a straggler on the
  boundary day isn't skipped forever. `fct_app_events` (largest) also accepts `backfill_lo`/`backfill_hi`
  vars for a month-window build if memory is tight.

**Data fix — auth temporal validity (a P1 defect the P5 tests surfaced).** The gold relationships
tests exposed that `loaders/20_gen_card_auths.sql` drew `auth_ts` uniformly over the whole 18-month
window, independent of the card — leaving **~47% of auths dated before their account was opened**, so
they orphaned onto the unknown member. The loader now draws `auth_ts` from `[card issued_ts,
window_end]` (issue ≥ account open ≥ signup), so every auth lands inside a live `dim_accounts` /
`dim_customers` interval. This is exactly the cross-cutting referential bug a tested star schema is
meant to catch.

### P5 acceptance

- `make wh-build && make wh-test` green — `dbt build --select marts` builds 5 tables + 4 incremental
  facts and passes **62 tests** (all facts→dims `relationships`, `unique`/`not_null` on surrogate
  keys, `accepted_values`).
- **Star-join smoke query** (monthly card spend by region × account_type over
  `fct_card_authorizations` × `dim_customers` × `dim_accounts` × `dim_date`) returns sane,
  unknown-member-free results.
- **As-of correctness** (on the SCD2-varying `kyc_status`; `risk_tier` is static here): a customer's
  app events resolve to the `submitted`/`pending` (non-current) versions valid *at event time*, not
  the current `verified` status.
- **Incremental double-run** leaves counts unchanged; a simulated late app event lands in the right
  partition (`toYYYYMM=202606`) with an as-of-resolved `customer_key`.
- `system.replicas` clean; all marts present on both replicas.

---

## P6 — real-time restructure (lambda)

P6 turns the streamable slice of the medallion into a **materialized-view cascade** that runs
continuously *inside* ClickHouse — bronze → `nimbus_stream` (silver) → `nimbus_rt` (gold) — fresh
within seconds of a bronze insert and with **zero dbt at runtime**. dbt becomes the *control plane*
that deploys/versions the streaming objects (`dbt run --select tag:rt`); the existing batch layer is
retained untouched as the correctness/history spine and a watermarked reconciliation test binds the
two. Full design: [`../docs/realtime-warehouse-architecture.md`]; task breakdown:
[`../docs/warehouse-architecture-plan.md`] §P6. Built as sub-phases A–F.

### P6-A keystone spike findings (`models/rt/rt_smoke.sql`)

The three questions that gate everything after, validated on ClickHouse `26.3.16` / adapter
`1.10.1` before any real model was built. `rt_smoke` is a standard-mode dbt `materialized_view`:
dbt creates a target table `nimbus_rt.rt_smoke` (`ReplicatedAggregatingMergeTree`, `auth_count
AggregateFunction(count)` inferred from `countState()`) **and** the view `nimbus_rt.rt_smoke_mv`
(`TO nimbus_rt.rt_smoke`), both `ON CLUSTER`, on both pods.

**(a) Single-fire replication keystone — CONFIRMED.** With the target empty (`catchup=false`),
inserting 1000 auths **on pod-0 only** left *exactly* 1000 in the rollup on **both** replicas (not
2000), and another 500 through the service left *exactly* 1500 on both. An MV fires on the receiving
replica's `INSERT`; the bronze source part **and** the MV-output part then replicate as ordinary
parts, and the other replica does **not** re-fire the MV on a fetched part → **each event is
aggregated exactly once**, HA preserved. This is the correctness foundation for the whole cascade on
this 1-shard × 2-replica topology.

**(b) In-place evolution — CONFIRMED (`MODIFY QUERY`, no drop).** Re-deploying `rt_smoke` with a
changed `SELECT` took the `ALTER TABLE rt_smoke_mv MODIFY QUERY …` path: the target table's `uuid`
was **unchanged**, its rows were **preserved**, and the live view definition picked up the new
predicate. So a routine `dbt run` on a changed MV evolves it in place with no gap — the basis for the
"deploy on change is safe / an unchanged deploy is a no-op" contract (P6-F). (`catchup=true`, used by
the real rollups in P6-B+, backfills the target from bronze at create time via `CREATE TABLE … AS
SELECT` — never `POPULATE`.)

**(c) Refreshable MV on a Replicated target — NOT VIABLE here → micro-batch fallback selected.**
`CREATE MATERIALIZED VIEW … REFRESH EVERY … TO <ReplicatedMergeTree>` is **refused by the server**:

> `Code: 36 … This combination doesn't work: refreshable materialized view, no APPEND,
> non-replicated database, replicated table. Each refresh would replace the replicated table
> locally, but other replicas wouldn't see it. Refusing to create.`

Our databases are `Atomic` (non-replicated) with `Replicated` tables, so a REPLACE-semantics RMV
can't coordinate across replicas (the essence of upstream #84134). Making it work would require
either a `Replicated`-engine database (not how the operator provisions this cluster) or a
non-replicated target (loses HA). **Verdict:** the T2 layer (`rt_activation_funnel`, P6-E) uses the
**micro-batch dbt CronJob** fallback — same minutes-tier freshness, HA-preserving, zero new
mechanism.

### Run modes — deploy (control plane) vs build (batch spine)

P6 splits dbt into two run modes off the **same** `dbt-runner` image (four suspended CronJobs, all
driven by `make` — the P3 zero-templating pattern):

| Mode | CronJob / make | Command | Scope |
| --- | --- | --- | --- |
| **Deploy** streaming objects | `dbt-deployer` / `make wh-deploy` | `dbt run --select tag:rt` | `nimbus_stream` + `nimbus_rt` MVs, dicts, targets — on change |
| **Build** batch spine | `dbt-runner` / `make wh-build` | `dbt build --exclude tag:rt` | `nimbus_staging/_intermediate/_marts/_metrics` + reconciliation tests |
| **Test** batch spine | `dbt-tester` / `make wh-test` | `dbt test --exclude tag:rt` | batch tests incl. the watermarked reconciliation |
| **Refresh** T2 | `dbt-refresher` / `make wh-refresh` | `dbt run --select rt_activation_funnel` | the micro-batch funnel (un-suspend for the 5-min cadence) |

The `tag:rt` selector is the seam: **plane isolation** means `make wh-build` never touches a
`nimbus_stream`/`nimbus_rt` object, and a `make wh-deploy` on an unchanged project is a **no-op**
(every MV takes the in-place `MODIFY QUERY` path — nothing is dropped, live MVs keep firing).

Backfill of the fan-in-fed targets (interchange / risk / deduped silver, which read the Null
`auth_fanin` and so start empty) is a controlled replay of bronze **through** the fan-in:
`make wh-rt-backfill` (never `POPULATE`). The silver-fed rollups (finance/dau/balance) self-backfill
via the adapter's `catchup` at deploy.

### ⚠️ `--full-refresh` is destructive to a live MV — maintenance windows only

A normal deploy uses `ALTER TABLE … MODIFY QUERY` (in place, no gap). **`dbt run --full-refresh
--select tag:rt` DROPS and recreates** each MV and its target table — any bronze insert that lands
during that window is **silently lost** (no MV exists to fan it out), and an AggregatingMergeTree
target is rebuilt from scratch. Only ever `--full-refresh` the streaming plane in a **maintenance
window with ingestion paused**. Routine change → plain `make wh-deploy` (MODIFY QUERY). The
`dbt-deployer` CronJob deliberately runs `dbt run` (never `dbt build --full-refresh`) for this reason.

### Real-time demo & observability

- `make wh-demo-realtime` — insert a handful of approved auths into **bronze only**; the interchange
  rollup advances within ~1s with **zero dbt runs** (printed before/after/delta proof).
- `make wh-debug` — prints `system.query_views_log` (which MVs fired, timing, exceptions),
  `system.view_refreshes` (empty — the T2 fallback uses no refreshable MVs), and stream/rt replica
  health.

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
