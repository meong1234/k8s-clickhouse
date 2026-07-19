# Warehouse Build Plan — detailed phased roadmap

> **Status:** execution plan, ready to implement phase by phase.
> **Companion doc:** [`warehouse-architecture.md`](warehouse-architecture.md) is the **what/why**
> (scenario, schemas, layer design, locked decisions). This document is the **how/when**: each
> phase from §12 of the architecture doc expanded into concrete tasks, files, commands, and
> acceptance criteria. Section references (§N) below point at the architecture doc.
>
> Checkboxes are intentional — tick them as work lands, so this doc doubles as the progress
> tracker.

## Phase overview

| Phase | Goal | Key deliverable | Depends on |
| --- | --- | --- | --- |
| ✅ [P0](#p0--dbt-user--dbt-scaffold--replication-spike) | dbt can build a replicated table as a scoped user | `dbt` user + `warehouse/dbt` scaffold + validated `ON CLUSTER` behavior | running cluster (`make up`) |
| ✅ [P1](#p1--bronze-ddl--synthetic-data) | Bronze populated with laptop-real Nimbus data | DDL + Python generator + native loaders + seeds | P0 |
| ✅ [P2](#p2--vertical-slice) | One metric flows bronze→metrics end-to-end | ledger → daily balance → finance metric slice | P1 |
| ✅ [P3](#p3--in-cluster-runtime-v1-contract) | dbt runs **in-cluster** under Flux (v1 contract) | `dbt-runner` image + Job/CronJob + Flux wiring | P2 |
| ✅ [P4](#p4--full-silver) | All staging + intermediate models | dedup, SCD2, funnel, DAU, interchange | P3 |
| ✅ [P5](#p5--full-gold-star-schema) | Complete star schema | SCD2 dims + 4 facts + `dim_date` | P4 |
| [P6](#p6--real-time-restructure-lambda) | **Real-time restructure** per [`realtime-warehouse-architecture.md`](realtime-warehouse-architecture.md) — MV-cascade serving plane + batch spine (lambda) | `nimbus_stream` + `nimbus_rt` via dbt-owned MVs/dictionaries, batch-truth metrics, reconciliation, control-plane split | P5 |
| [P7](#p7--data-tests-docs-readme) | Trustworthy + documented | fintech invariant tests, cohorts, `dbt docs`, exposures, READMEs | P6 |

**Conventions used throughout** (from the existing repo):

- Namespace `clickhouse`; replica pods `chi-clickhouse-default-0-0-0` / `chi-clickhouse-default-0-1-0`
  (variables `CH_POD_0`/`CH_POD_1` in `scripts/clickhouse.mk`).
- Cluster name `default`; replicated DDL uses `ON CLUSTER '{cluster}'` (see `ch-demo`).
- Secrets: SHA256-hex password in a Secret consumed by the CHI `users:` block via `secretKeyRef`
  (`kubernetes/analytics/clickhouse/base/clickhouse.yaml`), hash computed with `make ch-password`.
- Make: `Makefile` does `include scripts/*` — any new `scripts/warehouse.mk` is picked up
  automatically; add `warehouse-help` to the `help` target's prerequisites.
- Flux: `kubernetes/analytics/` is pushed as the `analytics-sync` OCI artifact
  (`scripts/fluxcd.mk`, `ARTIFACTS_TO_PUSH`); the Kustomization chain lives in
  `kubernetes/clusters/local/analytics.yaml` (`keeper-chk → clickhouse-chi`, `wait: false`).
  Changing `kubernetes/clusters/**` requires re-pushing the **cluster-sync** artifact too —
  `make fluxcd-push-artifacts` pushes all four, so just use that.
- Registry split: host pushes to `localhost:5050`, in-cluster references use
  `k3d-local-dev-registry:5000` (mirrors for `docker.io`/`ghcr.io` configured in `k3d/cluster.yaml`).
- Deploy loop for any manifest change: edit → `make fluxcd-push-artifacts` → `flux reconcile` /
  wait for the 1m `OCIRepository` interval.

---

## P0 — `dbt` user + dbt scaffold + replication spike

**Goal:** a scoped `dbt` ClickHouse user exists via GitOps, a minimal dbt project connects from
the host, and we have **validated** exactly how `dbt-clickhouse` does `ON CLUSTER` + replicated
engines (the one open unknown flagged in §9). Everything later builds on the settings proven here.

**Prerequisites:** cluster up and healthy (`make up`, `make ch-status`, `make ch-demo` passes).

### Tasks

**A. Scoped `dbt` user (GitOps)**

- [x] Generate a password hash: `make ch-password PASSWORD=dbt123` (dev-only default, documented).
- [x] Create `kubernetes/analytics/clickhouse/local/dbt-credentials.yaml` — Secret
      `dbt-credentials` in ns `clickhouse` with key `dbt_password_sha256_hex`, mirroring
      `clickhouse-credentials.yaml` (including the rotation comment).
- [x] Add the secret to `kubernetes/analytics/clickhouse/local/kustomization.yaml` resources.
- [x] Add the user to the CHI **base** (`kubernetes/analytics/clickhouse/base/clickhouse.yaml`,
      `spec.configuration.users`): `dbt/password_sha256_hex` (via `secretKeyRef`),
      `dbt/profile: default`, `dbt/quota: default`, `dbt/networks/ip`.
- [x] **Research item — grants. RESOLVED.** Both planned options failed and a third,
      still-GitOps-native mechanism was used instead (full write-up in `warehouse/README.md`):
      1. `allow_databases` (option 1) is **insufficient** — with `cluster:` set, dbt introspects
         via `clusterAllReplicas()`, needing the **global** `REMOTE` privilege (and `CLUSTER` to
         run `ON CLUSTER` DDL); a DB allow-list can't grant global privileges.
      2. The SQL-bootstrap fallback (option 2) is **impossible** for this user — an
         operator-defined user lives in read-only `users_xml` storage, so `GRANT … TO dbt` fails
         with `ACCESS_STORAGE_READONLY`. (No `warehouse/loaders/01_bootstrap_dbt_user.sql` shipped.)
      3. **Used:** a config-native `<grants>` block in the CHI (`dbt/grants/query: […]`) — the
         operator renders it into `users.xml`. Grants: `REMOTE`, `CLUSTER`, `SELECT ON system.*`,
         and `ALL ON nimbus_*.*` (per-database → `demo` etc. denied). Outcome recorded as a
         comment in the CHI manifest.
- [x] Deploy: `make fluxcd-push-artifacts`, wait for reconcile, verify
      `SELECT currentUser()` as `dbt` on pod 0 (and pod 1).

**B. dbt project scaffold**

- [x] `warehouse/dbt/dbt_project.yml` — project `nimbus`; model path config mapping
      `staging/ → schema: staging`, `intermediate/ → intermediate`, `marts/ → marts`,
      `metrics/ → metrics`; `+materialized` defaults (views for staging/intermediate,
      tables for marts/metrics). Also `+engine: ReplicatedMergeTree` on the table layers
      (see D).
- [x] `warehouse/dbt/profiles/profiles.yml` — target `dev`:
      `host: "{{ env_var('NIMBUS_CH_HOST', 'localhost') }}"`, `port: 8123`, `user: dbt`,
      `password: "{{ env_var('NIMBUS_CH_PASSWORD', 'dbt123') }}"`,
      `schema: nimbus_raw` (base schema; real placement via macro below),
      `cluster: "{{ env_var('NIMBUS_CH_CLUSTER', 'default') }}"`, plus
      `cluster_mode`/engine defaults as determined by the spike (D).
      Same profile works host-side and in-cluster — only `NIMBUS_CH_HOST` differs.
- [x] `warehouse/dbt/macros/generate_schema_name.sql` — override so `+schema: marts` →
      database `nimbus_marts` (prefix `nimbus_`, drop the default `target.schema + '_'`
      concatenation) (§2).
- [x] One trivial model `models/marts/smoke_replication.sql`
      (`select 1 as id, now() as built_at`, materialized `table`).
- [x] `warehouse/dbt/.gitignore` (`target/`, `dbt_packages/`, `logs/`); zero packages (no
      `packages.yml`).

**C. Dev-loop make targets (`scripts/warehouse.mk`)**

- [x] `wh-setup` — `python3 -m venv warehouse/.venv && pip install dbt-clickhouse`. Added
      `warehouse/.venv` to root `.gitignore`. (No `dbt deps` — zero packages.)
- [x] `wh-portforward` — `kubectl -n clickhouse port-forward svc/clickhouse-clickhouse 8123:8123`
      (service name confirmed via `make ch-status`). Also added `wh-debug`.
- [x] `wh-build-local` / `wh-test-local` — `dbt build`/`dbt test` with
      `--project-dir`/`--profiles-dir`, venv-activated.
- [x] `warehouse-help` section, wired into the root `help` target.

**D. Replication spike (the research deliverable)**

- [x] With the port-forward up, run `dbt debug` then build `smoke_replication` and validate:
      - `cluster` profile setting emits `ON CLUSTER` DDL — confirmed;
      - engine is `ReplicatedMergeTree` on **both** replicas. **Finding:** adapter 1.10.1 does
        **not** auto-convert `MergeTree → Replicated` from the `cluster` setting on server 26.3 —
        a bare model became plain `MergeTree` (no replication). Fixed by setting
        `+engine: ReplicatedMergeTree` explicitly; a bare engine works because the operator sets
        `default_replica_path=/clickhouse/tables/{uuid}/{shard}` on Atomic DBs (unique path/table);
      - table visible and identical from `CH_POD_0` **and** `CH_POD_1` — confirmed;
      - `dbt run --full-refresh` drops/recreates cleanly; `system.replicas` clean (leftover
        keeper znodes are Atomic deferred-drops, not orphans).
- [x] Encode the working settings as project-level defaults in `dbt_project.yml` /
      `profiles.yml` (`cluster`, `cluster_mode: false`, `+engine`), and write the findings into
      `warehouse/README.md` (started here, grown in P7).

### Acceptance criteria

- [x] `kubectl -n clickhouse exec chi-clickhouse-default-0-0-0 -- clickhouse-client -u dbt --password dbt123 -q "SELECT currentUser()"` → `dbt`; same on replica 1.
- [x] `dbt` **cannot** drop `demo.events` or read outside its scope (via the CHI `<grants>` block); `admin` unaffected (`make ch-demo` still passes).
- [x] `dbt debug` green from the host through the port-forward.
- [x] `nimbus_marts.smoke_replication` exists with the same row on **both** replicas, engine `Replicated*`.
- [x] All of the above achieved via Flux (no `kubectl apply` of CHI/Secret by hand).

**Out of scope:** any real models, bronze tables, images.

---

## P1 — Bronze DDL + synthetic data

**Goal:** the 8 bronze tables exist `ON CLUSTER` and are populated with deterministic,
referentially-consistent Nimbus data at laptop-real scale (~5k customers, ~2M ledger postings,
~10M app events, 18 months — §8/§13).

**Prerequisites:** P0 (dbt user + databases exist; `wh-*` plumbing works).

### Tasks

**A. Bronze DDL — `warehouse/loaders/00_create_raw.sql`**

- [x] `CREATE DATABASE IF NOT EXISTS nimbus_raw ON CLUSTER '{cluster}'` (+ the other `nimbus_*`
      databases if not already created by the P0 bootstrap).
- [x] The 8 `raw_*` tables exactly per §3 (engines, `PARTITION BY toYYYYMM(...)`, `ORDER BY`),
      each `ON CLUSTER '{cluster}'` with `Replicated*` engines and explicit keeper paths
      (`/clickhouse/tables/{shard}/nimbus_raw/<table>`, `{replica}`) — same pattern as `ch-demo`.
- [x] Idempotent: `CREATE TABLE IF NOT EXISTS`; re-runnable without error.
- [x] `make wh-bronze` — pipe the file through `clickhouse-client` on `CH_POD_0` (as `admin`,
      since it's DDL bootstrap), one statement at a time (`clickhouse-client` has no multi-stmt:
      use `--multiquery`).

**B. Python generator — `warehouse/generator/`**

- [x] `generate.py` + `requirements.txt` (stdlib + `faker` optional — prefer stdlib-only for
      zero-friction; decide at implementation, document choice).
- [x] Deterministic: `--seed 42` default; `--scale small|medium|large` presets
      (medium = the laptop-real numbers; small ≈ 1/10 for quick CI-style runs).
- [x] Generates, in dependency order, as CSV files into `warehouse/generator/out/`:
      1. **customers** (~5k) — signup dates over 18 months, weighted countries/risk tiers/referral sources;
      2. **kyc_events** — ordered transitions per customer (`submitted → pending → verified|rejected`),
         realistic conversion (~85% verified), timestamps strictly increasing;
      3. **accounts** (~1.3/customer, verified customers only) + **account_events**
         (open → occasional frozen/closed);
      4. **cards** (1 per checking account, most `active`);
      5. **ledger_postings** (~2M) — **balanced double-entry**: every `transaction_id` emits
         postings summing to zero (customer leg + Nimbus internal/settlement account leg);
         transaction mix: payroll deposits (biweekly), card settlements (linked to auth MCCs),
         P2P, ATM+fee, monthly interest accrual; balances never dip below −overdraft;
         `category_code` values match `seed_transaction_categories`.
- [x] Loader step: `cat out/<t>.csv | kubectl exec -i $(CH_POD_0) -- clickhouse-client -u admin … -q "INSERT INTO nimbus_raw.<t> FORMAT CSVWithNames"` (streaming, no temp copy in the pod).
- [x] `make wh-generate` — runs generator + load + the native loaders below; prints row counts.

**C. ClickHouse-native loaders (tier 3)**

- [x] `warehouse/loaders/10_gen_app_events.sql` — ~10M `raw_app_events` via
      `numbers()` + `rand()`-derived customer_id (skewed toward active customers), event-name
      distribution, session ids, 18-month spread. Pure SQL `INSERT … SELECT`.
- [x] `warehouse/loaders/20_gen_card_auths.sql` — ~1M auth attempts derived from active cards,
      ~92% approved, decline reasons, ~0.3% `is_fraud`, MCC distribution matching
      `seed_mcc_codes`; **then a second INSERT re-inserting ~3% of rows with a later
      `ingested_at`** (same `auth_id`/`idempotency_key`) — the deliberate duplicates for the
      dedup demo (§1, §5).
- [x] Both parameterized by scale via a `-- {SCALE}` substitution or separate small/medium variants
      (keep it simple: `sed`-style envsubst in the make target).

**D. Seeds — `warehouse/dbt/seeds/`**

- [x] The 5 CSVs per §3 (`seed_transaction_categories`, `seed_mcc_codes`, `seed_fee_schedule`,
      `seed_countries`, `seed_risk_tiers`) + `seeds/schema.yml` with column docs.
- [x] `dbt seed` works host-side (seeds land in `nimbus_raw` or a `nimbus_seeds` schema — decide
      and encode in `generate_schema_name`; the architecture doc treats them as static dims, so
      `nimbus_raw` is fine).

### Acceptance criteria

- [x] `make wh-bronze && make wh-generate` from scratch completes < ~10 min on the laptop.
- [x] Row counts (medium scale): `raw_customers` ≈ 5k; `raw_ledger_postings` ≈ 2M;
      `raw_app_events` ≈ 10M; `raw_card_authorizations` ≈ 1.03M (incl. dupes).
- [x] **Ledger balances:** `SELECT count() FROM (SELECT transaction_id, sum(if(direction='debit', amount_minor, -amount_minor)) s FROM nimbus_raw.raw_ledger_postings GROUP BY transaction_id HAVING s != 0)` → **0**.
- [x] **Dupes present:** `SELECT count() - uniqExact(auth_id) FROM nimbus_raw.raw_card_authorizations` → ~3% of auths, > 0.
- [x] **KYC ordering:** no customer has `verified` before `submitted` (spot query).
- [x] Determinism: dropping + regenerating yields identical `raw_customers` checksum
      (`SELECT sum(cityHash64(*)) …`) for the Python-generated tables.
- [x] Data visible from **both** replicas (replication of bronze inserts).

**Out of scope:** any dbt models over this data (P2+).

---

## P2 — Vertical slice

**Goal:** prove the whole modeling stack on ONE thread: ledger → staging → daily balance →
snapshot fact → a finance metric. Establishes the incremental-model pattern, the date spine, and
the testing pattern every later phase copies.

**Prerequisites:** P1 (bronze populated).

### Tasks

- [x] `models/staging/stg_ledger_postings.sql` (view) — typed/renamed 1:1, `amount_minor` signed
      convention decided here (`signed_amount_minor = if(direction='credit', +, -)` from the
      account's perspective), plus `posting_date`.
- [x] `models/staging/sources.yml` — declare `nimbus_raw` source with freshness off (static demo).
- [x] `models/intermediate/int_account_daily_balance.sql` — **incremental table**
      (`incremental_strategy: delete+insert`, partition/day key): date spine via
      `range`/`numbers()` from first posting to max date, cross-joined to accounts, daily net
      via `sum(signed_amount_minor)`, running `sum() OVER (PARTITION BY account ORDER BY day)` →
      opening/closing (§5).
- [x] `models/marts/fct_account_daily_balance.sql` — grain account × day; opening/closing,
      deposits, withdrawals; incremental by day (§4).
- [x] `models/metrics/metrics_finance_daily.sql` — minimal v1: `date`, `total_deposits`,
      `avg_balance_per_customer` only (revenue columns arrive in P6).
- [x] `schema.yml` per layer: `not_null`/`unique` on keys;
      `unique` on `(account_id, date)` for the snapshot (via `dbt_utils`-free surrogate:
      `unique` on a concat column or a singular test — keep zero packages).
- [x] First **singular test**: `tests/assert_ledger_balances.sql` (transaction sums = 0) — the
      pattern for P7's invariant suite.

### Acceptance criteria

- [x] `make wh-build-local && make wh-test-local` green.
- [x] Spot check: for one sampled account, `closing_balance` on 3 dates equals a hand-computed
      running sum straight off `raw_ledger_postings`.
- [x] Sum of all customer-account closing balances is plausible (positive, stable day-over-day
      magnitude — no runaway drift).
- [x] Incremental behavior: second `dbt run` (no new data) is fast and idempotent (same row
      counts, same checksums); after inserting one extra posting dated *yesterday*, only recent
      partitions rebuild.
- [x] All built tables exist on both replicas.

**Out of scope:** other staging models, SCD2, revenue metrics.

---

## P3 — In-cluster runtime (v1 contract)

**Goal:** dbt runs **inside the cluster** as a Flux-managed Job/CronJob using the `dbt-runner`
image — the locked v1 delivery contract (§9/§13). Landed now, on a small DAG, so every later
phase grows under a working GitOps runtime.

**Prerequisites:** P2 (a real DAG to run).

### Tasks

**A. Image**

- [x] `warehouse/Dockerfile` — `python:3.12-slim`, `pip install "dbt-clickhouse==1.10.1"`
      (pinned to the host-venv version), `COPY warehouse/dbt /app/dbt`, `WORKDIR /app/dbt`,
      `DBT_PROFILES_DIR=/app/dbt/profiles`, default cmd `dbt build`. No secrets baked in
      (host/password from env). Added a repo-root `.dockerignore` (context is the repo root).
- [x] `make wh-image` — `docker build -t localhost:5050/nimbus/dbt-runner:local -f warehouse/Dockerfile . && docker push …`.
      **Decided:** reference the registry name **directly** in the manifest
      (`k3d-local-dev-registry:5000/nimbus/dbt-runner:local`) — `nimbus/dbt-runner` is not a
      docker.io path so it does NOT resolve via the docker.io mirror; k3s resolves the registry
      host via the k3d registry config. `imagePullPolicy: Always` (mutable `:local` tag).

**B. Manifests — `kubernetes/analytics/dbt/{base,local}`**

- [x] Reuse namespace `clickhouse` (the `dbt-credentials` Secret already lives there — no
      cross-ns secret copying).
- [x] `base/cronjob.yaml` — CronJob `dbt-runner`, `suspend: true` in base,
      **`schedule: "0 */3 * * *"`** (every 3h — user choice, not the daily `0 2 * * *` first
      sketched), env `NIMBUS_CH_HOST=clickhouse-clickhouse` + `NIMBUS_CH_CLUSTER=default`,
      `NIMBUS_CH_PASSWORD` from Secret `dbt-credentials` key **`dbt_password`**,
      `restartPolicy: Never`, `backoffLimit: 1`, resources 128Mi/256Mi.
      Secret change applied to `kubernetes/analytics/clickhouse/local/dbt-credentials.yaml`:
      plaintext `dbt_password` added alongside `dbt_password_sha256_hex`, both documented.
- [x] `base/kustomization.yaml` (runner + tester); `local/` overlay is a documented pass-through
      (base is already laptop-sized; schedule **stays suspended locally** per the user — manual
      `wh-build` is the drive path, so no `suspend: false` override).
- [x] Flux wiring: appended Kustomization `dbt-runner` to
      `kubernetes/clusters/local/analytics.yaml` — `dependsOn: [clickhouse-chi]`,
      `path: ./dbt/local`, `sourceRef: analytics-source`, `wait: false`, `prune: true`.

**C. Make targets (in-cluster contract)**

- [x] `wh-build` — `kubectl -n clickhouse create job dbt-build-$$(date +%s) --from=cronjob/dbt-runner`
      then poll for **Complete or Failed** (bounded by `WH_JOB_TIMEOUT_S`, streams logs, non-zero
      exit on failure/timeout). Shared canned recipe `wh_run_job` (also used by `wh-test`).
- [x] `wh-test` — **Decided: second suspended CronJob `dbt-tester`** (`command: [dbt, test]`),
      zero `yq`/`sed` dependency, since `kubectl create job --from` can't override the command.
- [x] `wh-logs` — `kubectl -n clickhouse logs -l app=dbt-runner --tail=200` (pods carry
      `app=dbt-runner`); `JOB=<name>` override to follow a specific Job.
- [x] `wh-all` (first version) — `wh-image wh-bronze wh-generate wh-build wh-test`.

### Acceptance criteria

> Static validation done (kustomize build of `dbt/{base,local}` + `clickhouse/local`; rendered
> manifests inspected; `make -n` dry-runs of `wh-build`/`wh-test`/`wh-image`/`wh-all`). The
> runtime checks below need a live cluster (`make up`) and are pending that run.

- [x] `make wh-image && make fluxcd-push-artifacts` → `flux get kustomizations` shows
      `dbt-runner` Ready, CronJob exists.
- [x] `make wh-build` runs the **P2 DAG in-cluster** to completion (Job `Complete`, exit 0);
      `make wh-logs` shows dbt's model-by-model output.
- [x] `make wh-test` green in-cluster.
- [x] Deleting the `nimbus_marts` database and re-running `make wh-build` rebuilds it (runtime
      is self-sufficient, no host dbt needed).
- [x] Host dev-loop (`wh-build-local`) still works unchanged.

**Out of scope:** scheduling policy tuning, alerting/notification on Job failure (noted as a
future extension in the README — notification-controller is already installed).

---

## P4 — Full silver

**Goal:** every staging view and every intermediate model from §2/§5 — the layer with the
teaching-value logic (dedup, SCD2, funnel, DAU, interchange).

**Prerequisites:** P3 (all builds now run via `make wh-build`; author host-side with
`wh-build-local`).

### Tasks

**A. Staging (`models/staging/`, all views, 1:1, typed/renamed)**

- [x] `stg_customers`, `stg_kyc_events`, `stg_accounts`, `stg_account_events`, `stg_cards`,
      `stg_app_events` — mechanical.
- [x] `stg_card_auths` — **the dedup showcase**: `GROUP BY auth_id` +
      `argMax(<every column>, ingested_at)` (latest ingest wins). Consider a small
      `dedupe_latest()` macro in `macros/` to keep it readable and reusable.
- [x] Extend `sources.yml` to all 8 raw tables with descriptions.

**B. Intermediate (`models/intermediate/`)**

- [x] `int_customers_scd2` — from `stg_customers` × `stg_kyc_events`: one row per state
      interval; `valid_from = event_ts`, `valid_to = leadInFrame(event_ts) OVER (PARTITION BY
      customer_id ORDER BY event_ts …)` with `NULL`/max-date for current; initial interval from
      signup with `kyc_status='none'` (§4 "derived, not snapshot").
- [x] `int_accounts_scd2` — same pattern from `stg_accounts` × `stg_account_events`.
- [x] `int_ledger_categorized` — join `stg_ledger_postings` → `seed_transaction_categories`
      (+ `seed_mcc_codes` where `mcc` present); flags `is_revenue`, `revenue_type`.
- [x] `int_card_auths_deduped` — thin passthrough of `stg_card_auths` if dedup fully lives in
      staging (keep the model for DAG-shape parity with §5, or fold it — decide; default: fold
      into staging and document, updating §5's table via a footnote in the README).
- [x] `int_interchange_revenue` — approved, deduped auths × `interchange_rate_bps` →
      `interchange_revenue_minor` (integer bps math, no floats).
- [x] `int_activation_funnel` — per customer: `signup_ts`, min verified ts, first funding
      posting ts, first outbound txn ts (from ledger), as one wide row.
- [x] `int_daily_active_users` — `uniqExact(customer_id)` per day from `stg_app_events`;
      materialize as **table** (10M-row scan feeding multiple metrics).

**C. Tests-as-you-go**

- [x] `unique`/`not_null` keys on every model; `accepted_values` on statuses;
      dedup uniqueness (`auth_id` unique post-staging) as a schema test now (formalized as the
      invariant suite in P7).

### Acceptance criteria

- [x] `make wh-build && make wh-test` green in-cluster (full DAG ≈ 25+ models).
- [x] **SCD2 spot check** (one sampled customer with ≥2 KYC events): intervals contiguous
      (each `valid_to` = next `valid_from`), no overlap, exactly one open interval.
- [x] **Dedup:** `count() = uniqExact(auth_id)` in the deduped relation, while bronze still has
      the ~3% dupes.
- [x] **Funnel monotonicity** holds for all customers (spot query; formal test in P7).
- [x] `int_daily_active_users` covers every day in the 18-month window with plausible DAU
      (no zero-gaps unless genuinely quiet days exist at small scale).

---

## P5 — Full gold (star schema)

**Goal:** the complete conformed star schema of §4 — SCD2 dims + `dim_date` + 4 incremental facts.

**Prerequisites:** P4.

### Tasks

- [x] `models/marts/dim_date.sql` — spine from `numbers()`: day, week, month, quarter, year,
      `is_weekend`, month_name; table, small. (2024-01-01..2027-12-31, 1461 days.)
- [x] `dim_customers` — from `int_customers_scd2` (region already joined upstream);
      surrogate `customer_key = cityHash64(customer_id, valid_from)`; `is_current` flag;
      unknown-member (key 0) row for ASOF no-match.
- [x] `dim_accounts` — same from `int_accounts_scd2` (+ `interest_rate_bps`).
- [x] `dim_cards` — type-1, from `stg_cards`.
- [x] `fct_ledger_postings` — from `int_ledger_categorized`; FK `account_key` resolved by
      `ASOF LEFT JOIN` (posting_ts within the dim's validity interval — commented teaching
      moment: `posting_ts >= valid_from`, greatest match == the containing version because
      SCD2 intervals are contiguous); incremental by posting day.
- [x] `fct_account_daily_balance` — promoted the P2 model (split already present; added the
      `account_key` FK resolved **end-of-day** to avoid Date→midnight coercion).
- [x] `fct_card_authorizations` — from deduped auths + `int_interchange_revenue`;
      `interchange_revenue_minor` carried on the fact; `card_key`/`account_key`/`customer_key`
      resolved via staged one-ASOF-per-CTE; incremental by auth day.
- [x] `fct_app_events` — thin fact over `stg_app_events` with `customer_key` as-of join +
      date FK; incremental by day (full 1M/10M build fits; `backfill_lo`/`backfill_hi`
      month-window fallback documented in the model header).
- [x] `schema.yml` — per-model `.yml`: `relationships` tests facts→dims, `unique` surrogate
      keys, docs on every column of every mart (this layer is the contract).
- [x] **Data fix (P1 defect surfaced by the P5 relationships tests):**
      `loaders/20_gen_card_auths.sql` drew `auth_ts` uniformly over the whole window, leaving
      ~47% of auths dated before their account existed → orphaned onto the unknown member.
      Now gated to `[card issued_ts, window_end]`, so every auth falls in a live dim interval.

### Acceptance criteria

- [x] `make wh-build && make wh-test` green (relationships tests included). Verified host
      dev-loop: `dbt build --select marts` → 5 tables + 4 incremental facts + 62 tests, PASS.
- [x] Star-join smoke query returns sane results: monthly card spend by region × account_type
      joining `fct_card_authorizations` × `dim_customers` × `dim_accounts` × `dim_date`
      (0 unknown-member rows after the data fix).
- [x] **As-of correctness spot check:** demonstrated on the SCD2-varying attribute `kyc_status`
      (note: `risk_tier` is static in this data model). A customer's app events resolve to the
      `submitted`/`pending` (non-current) versions valid at event time, not the current
      `verified` status.
- [x] Incremental double-run: unchanged counts; simulated late app event landed in the right
      partition (`toYYYYMM = 202606`) with an as-of-resolved `customer_key`.
- [x] `system.replicas` clean (no readonly/broken replicas); all marts present on both replicas.

---

## P6 — Real-time restructure (lambda)

**Goal:** restructure the warehouse to follow
[`realtime-warehouse-architecture.md`](realtime-warehouse-architecture.md) — the medallion
transformation runs **continuously inside ClickHouse** as a materialized-view cascade
(`nimbus_stream` silver + `nimbus_rt` gold), dbt splits into a **control plane** (deploys/versions
the streaming objects) and the **batch spine** (SCD2, cohorts, as-of facts — unchanged), and a
watermarked reconciliation test binds the two planes. This phase folds the realtime doc's
P-RT0…P-RT5 into six sub-phases A–F, and absorbs the parts of the old P6 (batch metric marts)
that now serve as the **batch truth** side of the lambda. Section references (§N) below point at
the realtime doc.

**Decisions resolved from the realtime doc's §10 open questions (recorded here):**

1. **Scope** — full build through reconciliation (all six sub-phases), not just the flagship slice.
2. **Serving proof** — `make wh-demo-realtime` (insert → `sumMerge` advances, zero dbt). A Grafana
   dashboard is a documented future extension, not P6.
3. **T2 mechanism** — refreshable MV *if* the sub-phase A spike proves it on Replicated targets on
   26.3; otherwise the **micro-batch dbt CronJob** fallback (HA-preserving, zero new mechanism).

**Prerequisites:** P5. Existing `nimbus_staging/_intermediate/_marts/_metrics` and the P3 runtime
are untouched throughout — `make ch-demo` and `make wh-build` stay green as canaries.

### Tasks

**A. Foundations + keystone spike (≙ P-RT0)**

- [x] **Grants gap (must land first):** the CHI `dbt/grants/query` block enumerates databases
      *explicitly* — `nimbus_stream`/`nimbus_rt` are **not** covered by any prefix wildcard. Add
      `GRANT ALL ON nimbus_stream.*` + `GRANT ALL ON nimbus_rt.*` to
      `kubernetes/analytics/clickhouse/base/clickhouse.yaml`; deploy via
      `make fluxcd-push-artifacts`.
- [x] `warehouse/loaders/30_create_rt.sql` — `CREATE DATABASE IF NOT EXISTS nimbus_stream|nimbus_rt
      ON CLUSTER '{cluster}'` (admin bootstrap, same pattern as `00_create_raw.sql`); wire into
      `wh-bronze` or a new `wh-rt-init`.
- [x] `generate_schema_name` mapping: `models/stream/` → `nimbus_stream`, `models/rt/` →
      `nimbus_rt`; all P6 models carry `tags: ['rt']` (the control-plane selector, sub-phase F).
- [x] **Spike model** `models/rt/rt_smoke.sql` — dbt `materialized_view` materialization
      (adapter 1.10.1) over a trivial source, target `ReplicatedAggregatingMergeTree` `ON CLUSTER`.
      Validate and record in `warehouse/README.md`:
      - **(a) single-fire keystone (§2):** insert a block on pod 0 → MV output part appears on
        *both* pods with the event counted **exactly once** (fetched parts don't re-fire MVs);
        repeat with an insert through the service (either replica may receive it);
      - **(b) `catchup` semantics:** what the adapter's backfill-on-create does on 26.3, and
        whether `dbt run` on a changed MV takes the in-place `MODIFY QUERY` path (no drop);
      - **(c) T2 verdict:** does a refreshable MV work against a Replicated target on 26.3
        (upstream issue #84134)? Record the verdict; it selects sub-phase E's mechanism.

**B. Flagship slice — live interchange revenue (≙ P-RT1)**

- [x] `models/rt/mcc_dict.sql` + `models/rt/category_dict.sql` — dbt `dictionary`
      materializations over `seed_mcc_codes` / `seed_transaction_categories` (**dictionaries, not
      joins** — an MV only fires off its left-most table, §3/§6).
- [x] `models/rt/rt_interchange_daily.sql` — target `ReplicatedAggregatingMergeTree`,
      `ORDER BY (day, merchant_category)`, columns `interchange_minor AggregateFunction(sum, Int64)`,
      `auth_count AggregateFunction(count)`; MV over `nimbus_raw.raw_card_authorizations`
      (`WHERE approved = 1`): `sumState(amount_minor * dictGet(..., 'interchange_rate_bps', mcc)
      div 10000)` — integer bps math, evaluated per inserted block.
- [x] **Controlled backfill** of the ~1M historical auths already in bronze (MVs never fire
      retroactively): prefer the adapter's `catchup`; fallback manual
      `INSERT INTO target SELECT ... -State ... FROM bronze` (never `POPULATE` — it drops
      concurrent inserts).
- [x] `wh-demo-realtime` make target — insert a handful of fresh approved auths for *today* into
      **bronze only**, query `sumMerge(interchange_minor)` for today before/after: the rollup
      advances **with zero dbt involvement**; prints the comparison + elapsed time.

**C. Silver stream + honest dedup (≙ P-RT2)**

- [x] `models/stream/auth_fanin.sql` — `Null`-engine fan-in: one MV `bronze → auth_fanin`, then
      parallel MVs `auth_fanin → {rt_interchange_daily, rt_risk_daily, slv_card_auths}` — cascade
      depth stays ≤2, no deep chain in the insert path (§6); re-point B's MV accordingly.
- [x] `models/stream/slv_card_auths.sql` — `ReplicatedReplacingMergeTree(ingested_at)`
      `ORDER BY auth_id`: the **corrected** (deduped) auth stream; reads via `argMax`/`FINAL`,
      never `FINAL` on the hot path. Backfill from bronze.
- [x] `models/stream/slv_ledger_postings.sql` — typed/signed 1:1 + `dictGet(category_dict, …)`
      enrichment at insert time; `models/stream/slv_app_events.sql` — typed/renamed. Backfills.
- [x] The ~3% bronze dupes now demonstrate the **at-least-once tradeoff** (§6): the fast T0
      rollup double-counts them *by design*; the corrected path resolves them. Both numbers ship.

**D. Gold rollups (≙ P-RT3)**

- [x] `models/rt/rt_risk_daily.sql` — `countState` auths / declines / fraud by day (via fan-in).
- [x] `models/rt/rt_finance_daily.sql` — `sumState` deposits by day from `slv_ledger_postings`'s
      insert stream.
- [x] `models/rt/rt_dau_daily.sql` — `uniqState(customer_id)` by day over the app-event stream.
- [x] `models/rt/rt_balance_delta_daily.sql` — `sumState(signed_amount_minor)` by account × day;
      the **cumulative** balance is a *query-time* window over the deltas (a window over all
      history cannot stream, §3) — the query documented in the model's yml.
- [x] Backfills month-wise if the 1.5 GiB `max_memory_usage` bites (same mitigation as P1).

**E. T2 layer + batch truth (≙ P-RT4, absorbs old-P6 metrics)**

- [x] `models/rt/rt_activation_funnel.sql` — per sub-phase A's verdict: **refreshable MV**
      (`refreshable={"interval": "EVERY 5 MINUTE"}`) or the **micro-batch CronJob** fallback (a
      third suspended CronJob running `dbt run --select rt_activation_funnel` every 5 min).
- [x] Complete the **batch-truth metrics** (the lambda's source-of-truth side, old P6-A):
      `metrics_finance_daily` full (interchange + fee + interest revenue, `total_revenue`, `arpu`),
      `metrics_growth_daily` (activation, dau/mau/stickiness), `metrics_risk_daily` — formulas
      documented per column; these are what the stream reconciles against.
      (`cohorts_retention` moves to P7 — batch-only, no realtime counterpart.)

**F. Lambda contract — reconciliation + control-plane split (≙ P-RT5)**

- [x] **Run-mode split (§5):** batch CronJobs run `dbt build --exclude tag:rt` (the spine);
      a new suspended CronJob `dbt-deployer` runs `dbt run --select tag:rt` (deploy streaming
      objects, **on change only** — triggered by `make wh-deploy`, same zero-tooling pattern as
      `dbt-tester`). Verify a deploy run on an unchanged project is a no-op (`MODIFY QUERY`
      path, MVs keep running — no drop/recreate).
- [x] **`--full-refresh` guard** documented in `warehouse/README.md`: it drops/recreates live MVs
      (inserts during the window are silently lost) — maintenance windows only, ingestion paused.
- [x] `tests/assert_rt_batch_reconciliation.sql` — **watermarked to closed days** (`day < today`;
      you can't test a moving target): `|corrected_stream(d) − batch(d)| == 0` (error), and the
      `fast(d) − batch(d)` dup-drift **reported** via a warn-severity test (surfaced, not hidden).
- [x] Observability: `system.query_views_log` (MV fires/timing/exceptions) +
      `system.view_refreshes` (RMV status) queries added to `wh-debug`; Prometheus scrape already
      in place.

### Acceptance criteria

- [x] **Keystone:** sub-phase A validations recorded; identical `sumMerge` totals on both
      replicas after inserts on either pod.
- [x] `make wh-demo-realtime` — fresh bronze insert visible in `rt_interchange_daily` within
      seconds, zero dbt runs, printed proof.
- [x] **Dedup honesty:** corrected stream unique on `auth_id`; bronze still holds the ~3% dupes;
      fast-vs-corrected drift ≈ the dupe rate on affected days.
- [x] **Reconciliation green:** corrected stream == batch metrics on all closed days (interchange,
      risk counts, DAU); drift report shows fast-path over-count only where dupes exist.
- [x] **Plane isolation:** `make wh-build` (batch spine) touches no `nimbus_stream`/`nimbus_rt`
      object; `make wh-deploy` on an unchanged project is a no-op; both green in-cluster.
- [x] Old-P6 sanity retained: `mau ≥ dau`, `0 < stickiness ≤ 1`, activation matches funnel counts
      on a sampled week.
- [x] `system.replicas` clean; all stream/rt tables present on both replicas.

**Out of scope:** ingestion changes (bronze stays the boundary, §0), Grafana serving (documented
extension), `cohorts_retention` (P7).

---

## P7 — Data tests, docs, README

**Goal:** the fintech invariant suite (§10), lineage docs, exposures, and user-facing
documentation; `make wh-all` proves the whole thing from a fresh cluster.

**Prerequisites:** P6.

### Tasks

**A. Invariant test suite (`warehouse/dbt/tests/`)**

- [ ] `assert_ledger_balances_to_zero.sql` (promote from P2).
- [ ] `assert_no_unexplained_negative_balances.sql` — closing ≥ −overdraft limit.
- [ ] `assert_scd2_integrity.sql` — per entity: no overlaps, no gaps, exactly one `is_current`
      (generic test macro parameterized by model + entity key → applied to both SCD2 dims).
- [ ] `assert_funnel_monotonicity.sql`.
- [ ] `assert_dedup_effective.sql` — deduped relation unique on `auth_id` **and** bronze
      dupe-count > 0 (proves the demo is real, not vacuous).
- [ ] Harden `assert_rt_batch_reconciliation.sql` (created in P6-F) into the **three-way** check:
      corrected stream == batch metric == direct aggregate off `fct_card_authorizations`,
      per closed day.

**A2. Cohorts (moved from old P6 — batch-only, no realtime counterpart)**

- [ ] `models/metrics/cohorts_retention.sql` — cohort_month = first funding month; activity from
      ledger/app events; months_since × retained_accounts × rate.
- [ ] Cohort matrix monotonically non-increasing along `months_since_funding` (test).

**B. Docs & lineage**

- [ ] `exposures.yml` — the four metric marts as exposures (owner, description, depends_on).
- [ ] Descriptions filled for every model/column still missing them; `dbt docs generate` clean;
      `make wh-docs` serves the lineage graph (bronze → metrics visible end-to-end).

**C. READMEs & final glue**

- [ ] `warehouse/README.md` — the full walkthrough: scenario recap, layer map (link to both
      architecture docs), how to run (in-cluster contract + dev loop + the P6 deploy/build
      run-mode split), the P0 **and P6-A** spike findings, the invariants, the real-time demo,
      the `--full-refresh` guard, known simplifications.
- [ ] Root `README.md` — new "Data warehouse (dbt + medallion + realtime lambda)" section: what
      it adds, the 3-command demo (`make wh-all`, `make wh-demo-realtime`, `make wh-docs`),
      link to all three docs.
- [ ] `make wh-all` finalized: `wh-image → wh-bronze → wh-generate → wh-build → wh-test`,
      idempotent, with a closing summary (row counts, test results).
- [ ] Update `docs/warehouse-architecture.md` status line → "implemented"; tick all boxes here.

### Acceptance criteria

- [ ] **The gauntlet:** `make down && make up && make wh-all` on a clean machine state passes
      end-to-end; total wall-clock documented in the README.
- [ ] `dbt test` fully green: generic + 6 invariant tests, in-cluster.
- [ ] `make wh-docs` renders full lineage bronze→metrics with no undocumented models.
- [ ] A newcomer can follow the root README's warehouse section without reading this plan.

---

## Cross-phase notes

**Git hygiene.** One commit (or short branch) per phase, message prefixed `wh(P<n>):` — each
phase leaves `main` in a demonstrably working state (its acceptance criteria are the gate).

**Risk register.**

| Risk | Phase | Mitigation / fallback |
| --- | --- | --- |
| `dbt-clickhouse` `ON CLUSTER`/Replicated behavior differs from assumption | P0 | That's why P0 is a spike; fallback = explicit per-model `engine` + keeper path via project vars |
| Operator 0.27.1 can't express DB-scoped grants in CHI `users:` | P0 | SQL bootstrap script (`01_bootstrap_dbt_user.sql`) run by `wh-bronze` — still deterministic, just not pure-CHI |
| Laptop memory during 10M-row generation / `fct_app_events` build | P1/P5 | **Hit in P1** (background merges OOM'd the ~1.35 GiB server, not the inserts). Fixed without adding RAM: `merge_tree/merge_max_block_size: 1024` in the CHI local overlay (~8× less memory per merge) + 250k-row insert blocks in `load_bronze.sh` (fewer parts across the 18 monthly partitions → less merging). `SCALE=small` and month-wise backfill remain fallbacks |
| `kubectl create job --from=cronjob` can't override args for `wh-test` | P3 | Second suspended CronJob (`dbt-tester`) — zero extra tooling |
| dbt-clickhouse `materialized_view` materialization quirks | P6 | Fallback = target table as dbt model + MV via a `run_operation`/pre-hook `CREATE MATERIALIZED VIEW` |
| Registry mirror path mismatch for the runner image | P3 | Reference `k3d-local-dev-registry:5000/...` directly in the manifest |
| Refreshable MV broken on Replicated targets on 26.3 (upstream #84134) | P6-A/E | Spike verdict gates it; fallback = micro-batch CronJob (`dbt run --select rt_activation_funnel` every 5 min) — same T2 freshness, HA-preserving |
| CHI `<grants>` doesn't cover the new DBs (explicit per-DB list, no prefix wildcard) | P6-A | Land the two new `GRANT ALL` lines *before* any dbt deploy touches `nimbus_stream`/`nimbus_rt` |
| `--full-refresh` drops a live MV → silent data gap during the window | P6-F | Documented guard: maintenance windows only, ingestion paused; normal deploys use the `MODIFY QUERY` path |
| Adapter `catchup` backfill semantics differ from assumption | P6-A/B | Spike validates; fallback = manual `INSERT INTO target SELECT …-State… FROM bronze` (never `POPULATE`) |
| Backfill memory vs 1.5 GiB `max_memory_usage` (10M app events → `uniqState`) | P6-D | Month-wise backfill loop (same mitigation as P1/P5) |

**Rollback.** Every phase is purely additive to the existing stack. Full teardown of the
warehouse without touching ClickHouse/Keeper/Flux:
`DROP DATABASE nimbus_raw|nimbus_staging|nimbus_intermediate|nimbus_marts|nimbus_metrics|nimbus_stream|nimbus_rt ON CLUSTER '{cluster}'`,
plus removing the `dbt-runner` Kustomization. (Dropping a database also drops its MVs — the
streaming plane tears down with it; bronze inserts simply stop fanning out.) The existing `make ch-demo` path stays untouched
throughout and doubles as the canary that the base stack is unharmed.
