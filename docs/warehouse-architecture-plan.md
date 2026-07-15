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
| [P2](#p2--vertical-slice) | One metric flows bronze→metrics end-to-end | ledger → daily balance → finance metric slice | P1 |
| [P3](#p3--in-cluster-runtime-v1-contract) | dbt runs **in-cluster** under Flux (v1 contract) | `dbt-runner` image + Job/CronJob + Flux wiring | P2 |
| [P4](#p4--full-silver) | All staging + intermediate models | dedup, SCD2, funnel, DAU, interchange | P3 |
| [P5](#p5--full-gold-star-schema) | Complete star schema | SCD2 dims + 4 facts + `dim_date` | P4 |
| [P6](#p6--metrics-layer--real-time-showcase) | Metrics marts + streaming rollup | 3 metric marts + cohorts + AggregatingMergeTree/MV | P5 |
| [P7](#p7--data-tests-docs-readme) | Trustworthy + documented | fintech invariant tests, `dbt docs`, exposures, READMEs | P6 |

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

- [ ] `models/staging/stg_ledger_postings.sql` (view) — typed/renamed 1:1, `amount_minor` signed
      convention decided here (`signed_amount_minor = if(direction='credit', +, -)` from the
      account's perspective), plus `posting_date`.
- [ ] `models/staging/sources.yml` — declare `nimbus_raw` source with freshness off (static demo).
- [ ] `models/intermediate/int_account_daily_balance.sql` — **incremental table**
      (`incremental_strategy: delete+insert`, partition/day key): date spine via
      `range`/`numbers()` from first posting to max date, cross-joined to accounts, daily net
      via `sum(signed_amount_minor)`, running `sum() OVER (PARTITION BY account ORDER BY day)` →
      opening/closing (§5).
- [ ] `models/marts/fct_account_daily_balance.sql` — grain account × day; opening/closing,
      deposits, withdrawals; incremental by day (§4).
- [ ] `models/metrics/metrics_finance_daily.sql` — minimal v1: `date`, `total_deposits`,
      `avg_balance_per_customer` only (revenue columns arrive in P6).
- [ ] `schema.yml` per layer: `not_null`/`unique` on keys;
      `unique` on `(account_id, date)` for the snapshot (via `dbt_utils`-free surrogate:
      `unique` on a concat column or a singular test — keep zero packages).
- [ ] First **singular test**: `tests/assert_ledger_balances.sql` (transaction sums = 0) — the
      pattern for P7's invariant suite.

### Acceptance criteria

- [ ] `make wh-build-local && make wh-test-local` green.
- [ ] Spot check: for one sampled account, `closing_balance` on 3 dates equals a hand-computed
      running sum straight off `raw_ledger_postings`.
- [ ] Sum of all customer-account closing balances is plausible (positive, stable day-over-day
      magnitude — no runaway drift).
- [ ] Incremental behavior: second `dbt run` (no new data) is fast and idempotent (same row
      counts, same checksums); after inserting one extra posting dated *yesterday*, only recent
      partitions rebuild.
- [ ] All built tables exist on both replicas.

**Out of scope:** other staging models, SCD2, revenue metrics.

---

## P3 — In-cluster runtime (v1 contract)

**Goal:** dbt runs **inside the cluster** as a Flux-managed Job/CronJob using the `dbt-runner`
image — the locked v1 delivery contract (§9/§13). Landed now, on a small DAG, so every later
phase grows under a working GitOps runtime.

**Prerequisites:** P2 (a real DAG to run).

### Tasks

**A. Image**

- [ ] `warehouse/Dockerfile` — `python:3.12-slim`, `pip install dbt-clickhouse` (pin versions),
      `COPY warehouse/dbt /app/dbt`, `WORKDIR /app/dbt`, `DBT_PROFILES_DIR=/app/dbt/profiles`,
      default cmd `dbt build`. No secrets baked in (password from env).
- [ ] `make wh-image` — `docker build -t localhost:5050/nimbus/dbt-runner:local -f warehouse/Dockerfile . && docker push …`
      (same push pattern as `fluxcd-push-images`; in-cluster ref
      `k3d-local-dev-registry:5000/nimbus/dbt-runner:local` via the docker.io mirror **only if**
      the repo path matches the mirror rules — verify; otherwise reference the registry name
      directly in the manifest, which k3s resolves via the registry config).

**B. Manifests — `kubernetes/analytics/dbt/{base,local}`**

- [ ] Reuse namespace `clickhouse` (decision: the runner is part of the analytics stack; the
      `dbt-credentials` Secret already lives there — no cross-ns secret copying).
- [ ] `base/cronjob.yaml` — CronJob `dbt-runner`, `suspend: true` in base (schedule is an
      overlay concern), `schedule: "0 2 * * *"`, container from the runner image, env:
      `NIMBUS_CH_HOST=clickhouse-clickhouse` (the CHI service — confirm exact name from
      `make ch-status`), `NIMBUS_CH_PASSWORD` from Secret `dbt-credentials`
      (requires adding the **plaintext** key `dbt_password` to the secret alongside the hash —
      dbt needs the real password, the CHI needs the hash; document both keys in the secret),
      `restartPolicy: Never`, `backoffLimit: 1`, resources ~256Mi/500m.
- [ ] `base/kustomization.yaml`; `local/` overlay: image tag, `suspend: false` if we want the
      schedule live locally, trimmed resources.
- [ ] Flux wiring: append Kustomization `dbt-runner` to
      `kubernetes/clusters/local/analytics.yaml` — `dependsOn: [clickhouse-chi]`,
      `path: ./dbt/local`, `sourceRef: analytics-source`, `wait: false`, `prune: true`.

**C. Make targets (in-cluster contract)**

- [ ] `wh-build` — `kubectl -n clickhouse create job dbt-build-$$(date +%s) --from=cronjob/dbt-runner`
      then wait on `condition=complete` (with timeout + failure surface).
- [ ] `wh-test` — same, overriding the command to `dbt test` (`kubectl create job --from` can't
      override args → template a Job manifest via `kubectl create … --dry-run=client -o yaml |
      yq/sed` or keep a second suspended CronJob `dbt-tester`; **decide at implementation,
      prefer the second CronJob for zero yq dependency**).
- [ ] `wh-logs` — `kubectl -n clickhouse logs -l job-name --tail=…` of the most recent dbt job
      (label jobs `app=dbt-runner` for a clean selector).
- [ ] `wh-all` (first version) — `wh-image wh-bronze wh-generate wh-build wh-test`.

### Acceptance criteria

- [ ] `make wh-image && make fluxcd-push-artifacts` → `flux get kustomizations` shows
      `dbt-runner` Ready, CronJob exists.
- [ ] `make wh-build` runs the **P2 DAG in-cluster** to completion (Job `Complete`, exit 0);
      `make wh-logs` shows dbt's model-by-model output.
- [ ] `make wh-test` green in-cluster.
- [ ] Deleting the `nimbus_marts` database and re-running `make wh-build` rebuilds it (runtime
      is self-sufficient, no host dbt needed).
- [ ] Host dev-loop (`wh-build-local`) still works unchanged.

**Out of scope:** scheduling policy tuning, alerting/notification on Job failure (mention as a
future extension in the README).

---

## P4 — Full silver

**Goal:** every staging view and every intermediate model from §2/§5 — the layer with the
teaching-value logic (dedup, SCD2, funnel, DAU, interchange).

**Prerequisites:** P3 (all builds now run via `make wh-build`; author host-side with
`wh-build-local`).

### Tasks

**A. Staging (`models/staging/`, all views, 1:1, typed/renamed)**

- [ ] `stg_customers`, `stg_kyc_events`, `stg_accounts`, `stg_account_events`, `stg_cards`,
      `stg_app_events` — mechanical.
- [ ] `stg_card_auths` — **the dedup showcase**: `GROUP BY auth_id` +
      `argMax(<every column>, ingested_at)` (latest ingest wins). Consider a small
      `dedupe_latest()` macro in `macros/` to keep it readable and reusable.
- [ ] Extend `sources.yml` to all 8 raw tables with descriptions.

**B. Intermediate (`models/intermediate/`)**

- [ ] `int_customers_scd2` — from `stg_customers` × `stg_kyc_events`: one row per state
      interval; `valid_from = event_ts`, `valid_to = leadInFrame(event_ts) OVER (PARTITION BY
      customer_id ORDER BY event_ts …)` with `NULL`/max-date for current; initial interval from
      signup with `kyc_status='none'` (§4 "derived, not snapshot").
- [ ] `int_accounts_scd2` — same pattern from `stg_accounts` × `stg_account_events`.
- [ ] `int_ledger_categorized` — join `stg_ledger_postings` → `seed_transaction_categories`
      (+ `seed_mcc_codes` where `mcc` present); flags `is_revenue`, `revenue_type`.
- [ ] `int_card_auths_deduped` — thin passthrough of `stg_card_auths` if dedup fully lives in
      staging (keep the model for DAG-shape parity with §5, or fold it — decide; default: fold
      into staging and document, updating §5's table via a footnote in the README).
- [ ] `int_interchange_revenue` — approved, deduped auths × `interchange_rate_bps` →
      `interchange_revenue_minor` (integer bps math, no floats).
- [ ] `int_activation_funnel` — per customer: `signup_ts`, min verified ts, first funding
      posting ts, first outbound txn ts (from ledger), as one wide row.
- [ ] `int_daily_active_users` — `uniqExact(customer_id)` per day from `stg_app_events`;
      materialize as **table** (10M-row scan feeding multiple metrics).

**C. Tests-as-you-go**

- [ ] `unique`/`not_null` keys on every model; `accepted_values` on statuses;
      dedup uniqueness (`auth_id` unique post-staging) as a schema test now (formalized as the
      invariant suite in P7).

### Acceptance criteria

- [ ] `make wh-build && make wh-test` green in-cluster (full DAG ≈ 25+ models).
- [ ] **SCD2 spot check** (one sampled customer with ≥2 KYC events): intervals contiguous
      (each `valid_to` = next `valid_from`), no overlap, exactly one open interval.
- [ ] **Dedup:** `count() = uniqExact(auth_id)` in the deduped relation, while bronze still has
      the ~3% dupes.
- [ ] **Funnel monotonicity** holds for all customers (spot query; formal test in P7).
- [ ] `int_daily_active_users` covers every day in the 18-month window with plausible DAU
      (no zero-gaps unless genuinely quiet days exist at small scale).

---

## P5 — Full gold (star schema)

**Goal:** the complete conformed star schema of §4 — SCD2 dims + `dim_date` + 4 incremental facts.

**Prerequisites:** P4.

### Tasks

- [ ] `models/marts/dim_date.sql` — spine from `numbers()`: day, week, month, quarter, year,
      `is_weekend`, month_name; table, small.
- [ ] `dim_customers` — from `int_customers_scd2` + seed joins (country → region);
      surrogate `customer_key = cityHash64(customer_id, valid_from)`; `is_current` flag.
- [ ] `dim_accounts` — same from `int_accounts_scd2` (+ `interest_rate_bps`).
- [ ] `dim_cards` — type-1, from `stg_cards`.
- [ ] `fct_ledger_postings` — from `int_ledger_categorized`; FK `account_key` resolved by
      as-of join (posting_ts within the dim's validity interval — implement via
      `ASOF JOIN` or interval-range join; this is a teaching moment, comment it); incremental
      by posting day.
- [ ] `fct_account_daily_balance` — promote/finish the P2 model (add
      `total_deposits`/`total_withdrawals` split, account_key FK).
- [ ] `fct_card_authorizations` — from deduped auths + `int_interchange_revenue`;
      `interchange_revenue_minor` carried on the fact; incremental by auth day.
- [ ] `fct_app_events` — thin fact over `stg_app_events` with `customer_key` as-of join +
      date FK; incremental by day (largest table — validate build memory fits the 1.5G limit;
      if tight, build month-by-month via incremental backfill loop documented in the README).
- [ ] `schema.yml` — `relationships` tests facts→dims, `unique` surrogate keys, docs on every
      column of every mart (this layer is the contract).

### Acceptance criteria

- [ ] `make wh-build && make wh-test` green (relationships tests included).
- [ ] Star-join smoke query returns sane results: monthly card spend by region × account_type
      joining `fct_card_authorizations` × `dim_customers` × `dim_accounts` × `dim_date`.
- [ ] **As-of correctness spot check:** a customer whose risk_tier changed mid-history has
      auths attributed to the tier valid *at auth time* (not the current one).
- [ ] Incremental double-run: unchanged checksums; simulated late event lands in the right
      partition.
- [ ] `system.replicas` clean (no readonly/broken replicas) after full builds.

---

## P6 — Metrics layer + real-time showcase

**Goal:** the four metric marts of §6 plus the locked AggregatingMergeTree + MATERIALIZED VIEW
near-real-time interchange rollup, reconciled against batch.

**Prerequisites:** P5.

### Tasks

**A. Batch metrics (`models/metrics/`)**

- [ ] `metrics_finance_daily` — completed per §6: deposits, avg balance/customer,
      interchange + fee + interest revenue, `total_revenue`, `arpu`. Grain: day. Documented
      formula per column in `schema.yml` (**the metric definitions are the contract**, §7).
- [ ] `metrics_growth_daily` — signups, kyc_verified, funded, first_txn, `activation_rate`,
      `dau`, `mau` (28-day rolling `uniq` over app events — window or self-join over
      `int_daily_active_users`' source), `stickiness`.
- [ ] `metrics_risk_daily` — auth_count, decline_rate, fraud_rate, chargeback_rate (chargebacks
      approximated from a ledger category — note the simplification).
- [ ] `cohorts_retention` — cohort_month = first funding month; activity from ledger/app
      events; months_since × retained_accounts × rate.

**B. Real-time showcase**

- [ ] `models/metrics/rt_interchange_daily_target.sql` — dbt `table` with explicit engine
      `AggregatingMergeTree`, `ORDER BY date`, columns `date`,
      `interchange_minor AggregateFunction(sum, UInt64)`, `auth_count AggregateFunction(count)`.
- [ ] `models/metrics/rt_interchange_mv.sql` — dbt `materialized_view` materialization
      (dbt-clickhouse supports it — validate version) reading from the **deduped card-auth
      relation's physical table** with `sumState(...)`/`countState()` group by day, writing into
      the target. Note the honest caveat in comments: the MV fires on *new inserts* only —
      which is exactly the demo.
- [ ] `wh-demo-realtime` make target — inserts a handful of fresh approved auths for *today*
      into bronze **and** the deduped physical table path the MV watches, then queries
      `sumMerge(interchange_minor)` for today twice (before/after) showing the rollup advance
      **without any dbt run**; prints the comparison.

### Acceptance criteria

- [ ] `make wh-build && make wh-test` green.
- [ ] **Reconciliation:** `metrics_finance_daily.interchange_revenue` per day ==
      `sumMerge` from the RT target for fully-loaded days == direct aggregate off
      `fct_card_authorizations` (three-way check; becomes a P7 test).
- [ ] `make wh-demo-realtime` visibly bumps today's rollup with zero dbt involvement.
- [ ] `metrics_growth_daily` sanity: `mau ≥ dau`, `0 < stickiness ≤ 1`, activation_rate matches
      funnel counts on a sampled week.
- [ ] Cohort matrix is monotonically non-increasing along `months_since_funding`.

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
- [ ] `assert_rt_batch_reconciliation.sql` — the three-way interchange check from P6.

**B. Docs & lineage**

- [ ] `exposures.yml` — the four metric marts as exposures (owner, description, depends_on).
- [ ] Descriptions filled for every model/column still missing them; `dbt docs generate` clean;
      `make wh-docs` serves the lineage graph (bronze → metrics visible end-to-end).

**C. READMEs & final glue**

- [ ] `warehouse/README.md` — the full walkthrough: scenario recap, layer map (link to
      architecture doc), how to run (in-cluster contract + dev loop), the P0 spike findings,
      the invariants, the real-time demo, known simplifications.
- [ ] Root `README.md` — new "Data warehouse (dbt + medallion)" section: what it adds, the
      3-command demo (`make wh-all`, `make wh-demo-realtime`, `make wh-docs`), link to both docs.
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

**Rollback.** Every phase is purely additive to the existing stack. Full teardown of the
warehouse without touching ClickHouse/Keeper/Flux:
`DROP DATABASE nimbus_raw|nimbus_staging|nimbus_intermediate|nimbus_marts|nimbus_metrics ON CLUSTER '{cluster}'`,
plus removing the `dbt-runner` Kustomization. The existing `make ch-demo` path stays untouched
throughout and doubles as the canary that the base stack is unharmed.
