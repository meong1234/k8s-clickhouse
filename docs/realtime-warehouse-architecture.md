# Real-Time Warehouse Architecture — Nimbus, event-driven medallion on ClickHouse

> **Status:** design, ready to review. Companion to
> [`warehouse-architecture.md`](warehouse-architecture.md) (the batch medallion, P0–P2 built)
> and [`warehouse-architecture-plan.md`](warehouse-architecture-plan.md).
> **Thesis:** the medallion transformation *bronze → silver → gold* should run **continuously
> inside ClickHouse** (materialized-view cascade), not on a scheduled `dbt build`. dbt stops being
> the *runtime* and becomes the *control plane* that deploys and versions the streaming objects —
> while a **batch spine** (the existing dbt layer) is retained for the models that genuinely
> cannot stream, and reconciles the two. This is a **lambda** architecture, native to ClickHouse.

---

## 0. What "real-time" means here (the reframe)

Real-time is **not** an ingestion question. Ingestion always lands raw events in **bronze**
(`nimbus_raw.*`) — whether that is today's bulk CSV load, or a future Kafka / CDC / `async_insert`
stream is an orthogonal decision that does not change anything below. Bronze is the boundary.

Real-time here is about the **internal warehouse**: the moment a row lands in bronze, how long
until it is reflected in a **queryable gold metric**? In the batch design that latency equals the
**CronJob period** (the whole `bronze → metrics` DAG only advances when `dbt build` runs). The
real-time design collapses that to **seconds** for the streamable models by making every bronze
insert *trigger* the transformation, with **zero dbt involvement at runtime**.

```
BATCH (today)          bronze ──[ dbt build, every N min/hours ]──▶ silver ─▶ gold ─▶ metrics
                       latency = CronJob period

REAL-TIME (this doc)   bronze ──[ ClickHouse MV, fires on every INSERT ]──▶ silver ─▶ gold
                       latency = insert cadence + MV cascade depth  (seconds)
                              + batch spine (dbt) for the un-streamable models
```

The honest bar (from the research): a genuinely tuned streaming path lands at **low single-digit
seconds** end-to-end (Mux: 2–6s), *not* sub-second, once real merge/dedup machinery is in the loop.
We design to the **seconds tier** and are explicit about what each tier costs (§4).

---

## 1. Core idea — the medallion becomes a materialized-view cascade

A ClickHouse **incremental materialized view** is not a view; it is an **`AFTER INSERT` trigger**
that runs its `SELECT` against *only the just-inserted block* and writes the result into a target
table. Chain them and the medallion runs itself:

```
                          nimbus_raw (bronze, unchanged)
                                    │  every INSERT fires the MV below
                                    ▼
   ┌──────────────────────── SILVER (nimbus_stream) ───────────────────────┐
   │  mv_stg_ledger      → slv_ledger_postings   (typed/renamed, signed)    │
   │  mv_dedup_auths     → slv_card_auths        ReplacingMergeTree(ing_at) │  dictGet on
   │  mv_stg_app_events  → slv_app_events                                   │  seed dictionaries
   └───────────────────────────────┬───────────────────────────────────────┘  (mcc, category)
                                    │  MVs on the silver inserts
                                    ▼
   ┌──────────────────────── GOLD  (nimbus_rt) ────────────────────────────┐
   │  AggregatingMergeTree rollups, one MV each:                            │
   │    rt_interchange_daily     sumState / countState  by (day, region)   │
   │    rt_finance_daily         sumState deposits / avg-balance state     │
   │    rt_risk_daily            countState auths / declines / fraud       │
   │    rt_dau_daily             uniqState(customer_id) by day             │
   └───────────────────────────────────────────────────────────────────────┘
                                    │  query time: -Merge combinators
                                    ▼
                          dashboards  (queryable within seconds of the bronze insert)
```

Every gold table is an **`AggregatingMergeTree`** holding partial aggregate *states*
(`sumState`, `countState`, `uniqState`, `argMaxState`); queries read them with the matching
`-Merge` combinators. This is the "shift compute from query-time to insert-time" pattern — a
published example is ~33× faster reads (a 238M-row scan vs. a 5.7k-row pre-aggregate).

**Two ClickHouse databases are added, leaving the batch schemas untouched:**

| Database | Role | Engines |
| --- | --- | --- |
| `nimbus_stream` | real-time **silver** (deduped/typed continuous tables) | `ReplicatedReplacingMergeTree`, `ReplicatedMergeTree` |
| `nimbus_rt` | real-time **gold** (continuous rollups) | `ReplicatedAggregatingMergeTree` |

The existing `nimbus_staging / _intermediate / _marts / _metrics` stay exactly as they are — they
become the **batch spine** (§5). Nothing in the built P0–P2 work is removed.

---

## 2. The replication-correctness keystone (why this fits the 1×2 cluster)

This cluster is **1 shard × 2 replicas** on `Replicated*MergeTree` + Keeper. The single most
important real-time correctness fact for this topology:

> **A materialized view fires on `INSERT`, never on replication.** When a client inserts into a
> bronze table on replica A, the MV fires **on A only** and writes to the (Replicated) gold target
> on A. Then **both** parts — the bronze source part *and* the gold MV-output part — replicate to
> replica B as ordinary data parts. Replica B **does not re-fire** the MV (it received a fetched
> part, not an `INSERT`). Result: **each event is aggregated exactly once**, and both replicas
> converge to identical gold state.

So the entire cascade is built with **Replicated engines end-to-end** (source, silver, gold) and
`ON CLUSTER '{cluster}'` DDL — the same pattern the repo already showcases (`ch-demo`, bronze
DDL). No double-counting, HA preserved, and the real-time layer *reinforces* the repo's
replication story instead of side-stepping it. This is validated in P-RT0 (§8) exactly as the
batch P0 validated `ON CLUSTER` for dbt.

---

## 3. What streams vs. what stays batch (the boundary, mapped to real Nimbus models)

An MV can only compute what is expressible **on a single inserted block**, optionally enriched by
**dictionaries** (not joins — an MV only triggers off its left-most `FROM` table; a joined
dimension that changes is invisible to it). Everything requiring a **window over full history**,
a **mutable multi-table join**, or **as-of temporal logic** stays batch. Mapping the existing
Nimbus DAG:

| Nimbus model (from the batch design) | Real-time? | How |
| --- | --- | --- |
| `stg_ledger_postings` (type/rename/sign) | ✅ stream | incremental MV → `slv_ledger_postings` |
| card-auth **dedup** (`int_card_auths_deduped`) | ✅ stream | MV → `slv_card_auths` `ReplacingMergeTree(ingested_at)`; collapse at merge/`FINAL`/`argMax` |
| **interchange revenue** rollup | ✅ stream | MV `dictGet(mcc_dict,'interchange_rate_bps',mcc)` × approved amount → `rt_interchange_daily` |
| `metrics_finance_daily` (deposits, avg bal) | ✅ stream | MV `sumState`/`avgState` by day → `rt_finance_daily` |
| `metrics_risk_daily` (decline/fraud rate) | ✅ stream | MV `countState`/`sumState` by day → `rt_risk_daily` |
| DAU (`int_daily_active_users`) | ✅ stream | MV `uniqState(customer_id)` by day → `rt_dau_daily` |
| **account running balance** (cumulative) | ⚠️ hybrid | *daily delta* streams (`sumState` by account×day); the **cumulative** `sum() OVER (…)` is a **window over all history** → computed at **query time** from the delta rollup, or batch-snapshotted |
| `int_activation_funnel` (signup→…→first-txn) | ⚠️ refreshable | multi-source per-customer min/first → **refreshable MV** (§6) or batch |
| `dim_customers` / `dim_accounts` **SCD2** | ❌ batch | `leadInFrame` window over full change history — dbt spine |
| `cohorts_retention` | ❌ batch | full-history cohort matrix — dbt spine |
| star-schema **as-of joins** (`fct_*` → SCD2 dim) | ❌ batch | `ASOF JOIN` over a mutable dim — dbt spine |

The rule, stated once: **single-table + insert-scoped + dictionary-enriched → stream. Window /
mutable-join / as-of → batch.** The interesting real-time work is the top block; the interesting
*correctness* work is the reconciliation between the two (§7).

---

## 4. Latency tiers (design to seconds, be honest about each)

| Tier | Mechanism | Freshness | Used for |
| --- | --- | --- | --- |
| **T0 — continuous** | incremental MV → `AggregatingMergeTree` | **seconds** (insert cadence + cascade depth) | interchange/finance/risk/DAU rollups |
| **T1 — merge-bounded** | `ReplacingMergeTree` dedup resolving at merge, or `FINAL`/`argMax` at query | seconds–minutes (or exact at query with `FINAL`) | deduped silver card-auths |
| **T2 — refreshable** | `REFRESH EVERY 1–5 MINUTE` full recompute | minutes (bounded by interval) | funnel, complex-join gold |
| **T3 — scheduled batch** | dbt CronJob `dbt build` | the CronJob period | SCD2, cohorts, star as-of, reconciliation |

What dominates T0 latency, in order: **MV cascade depth** (each chained MV is *synchronous in the
insert path* — throughput drops ~55% at 1 chained MV, ~90% at 10, per Altinity), then background
**merges** for anything Replacing/Aggregating (no SLA; async), then **`FINAL`** if used at query
time. Design consequences baked in below: **keep the cascade shallow (≤2 hops)**, use the
**Null-table fan-in** so one bronze insert feeds many rollups in parallel rather than a deep chain,
and **avoid `FINAL`** on the hot path (prefer `argMax`/`-Merge`).

---

## 5. dbt's new job — control plane, not runtime (two modes)

dbt is, by its own docs, batch: invoke-driven, ~5–15 min floor, "better replaced by materialized
views … for the ultra-low-latency read path." So dbt stops *computing* the streamable models and
instead **owns their definitions**. `dbt-clickhouse` (v1.8–1.10) supports exactly this:

- **`materialized_view` materialization** (v1.6+; in-place `ALTER TABLE … MODIFY QUERY` since
  v1.8.8) — a dbt model *is* the MV; `dbt run` deploys/evolves the `CREATE MATERIALIZED VIEW … TO
  <target>` DDL. The MV then runs continuously with no dbt involvement.
- **`dictionary` materialization** (v1.7.4+) — the seeds (`seed_mcc_codes`,
  `seed_transaction_categories`) become **dictionaries** so MVs can `dictGet` them at insert time
  instead of joining. Incremental dictionary refresh (`update_field`) since v1.10.
- **`refreshable` config** (v1.8.7+) — `refreshable={"interval":"EVERY 5 MINUTE", …}` for T2.
- Explicit **target-table** models + **many-MVs → one target** fan-in (v1.8.5+).

This yields **two dbt run modes**, both from the *same existing `dbt-runner` image* (P3):

| Mode | Trigger | Scope | Cadence |
| --- | --- | --- | --- |
| **Deploy** streaming objects | on change (GitOps / CI) | `nimbus_stream` + `nimbus_rt` MVs, targets, dictionaries | **not scheduled** — runs when the SQL changes |
| **Build** batch spine | scheduled CronJob (the P3 runtime) | `nimbus_staging/_intermediate/_marts/_metrics` (SCD2, cohorts, reconciliation tests) | e.g. hourly/daily |

dbt still delivers the streaming layer via **Flux + the in-cluster Job**, so the real-time objects
are GitOps-versioned, `ON CLUSTER`, tested, and lineage-documented — the repo's whole delivery
ethos — while ClickHouse does the continuous execution. **`--full-refresh` is destructive to a
live MV** (drops/recreates; inserts during the window are lost), so deploy-mode uses in-place
`MODIFY QUERY` and reserves full-refresh for maintenance windows with ingestion paused (documented
guard, mirrors the P6 note).

---

## 6. Physical design details (the non-obvious parts)

**Null-table fan-in (shallow, parallel cascade).** Rather than chain silver→gold deeply, land the
hot **card-auth** stream through a `Null` staging table whose *only* purpose is to fan one insert
out to several independent rollup MVs (interchange, risk, per-region) in parallel — the raw block
is never re-stored, and no single deep chain throttles inserts. (Bronze itself stays a real table;
the Null table is an internal fan-out node in `nimbus_stream`.)

**Dictionaries, not joins.** `mcc_dict` (mcc → `interchange_rate_bps`, `merchant_category`) and
`category_dict` (category_code → `is_revenue`, `revenue_type`) are dbt-owned dictionaries. The
interchange MV computes, per inserted approved auth, `amount_minor *
dictGetUInt16('nimbus_rt.mcc_dict','interchange_rate_bps',mcc) / 10000` — integer bps math, no
floats, no join, evaluated at insert time.

**AggregatingMergeTree target shape.** `GROUP BY` in the MV must match the target `ORDER BY`, e.g.
`rt_interchange_daily ORDER BY (day, region)` with columns
`interchange_minor AggregateFunction(sum, Int64)`, `auth_count AggregateFunction(count)`. Query:
`SELECT day, sumMerge(interchange_minor), countMerge(auth_count) … GROUP BY day, region`.

**Dedup honesty (the intentional 3% duplicate auths).** An incremental MV fires on *every* bronze
insert, **including the duplicate re-inserts** → a naive streaming interchange rollup **double-
counts** them. This is the real, teachable at-least-once property. Two answers, both shipped:
1. **T0 fast/approximate** — the incremental-MV rollup accepts the dupes; it is the *fast* number.
2. **T1/T2 corrected** — dedup into `slv_card_auths` (`ReplacingMergeTree(ingested_at)`, one row
   per `auth_id`), and compute the corrected rollup either at query time (`argMax`/`FINAL`) or via
   a **refreshable MV** over the deduped silver.

The gap between (1) and (2) is exactly what the reconciliation test measures (§7) — the design
*teaches* that streaming rollups over an at-least-once stream are approximate and batch reconciles.

**Refreshable-MV caveat on this cluster.** RMVs on `ReplicatedMergeTree` targets have open issues
(e.g. ClickHouse #84134) — **must be validated on 26.3 in the spike**. If they don't work cleanly
replicated, the fallback is a **short-interval dbt micro-batch** (a second CronJob at `EVERY 1–5
MIN` over just the funnel/complex-gold models) — same T2 freshness, zero new mechanism. Decision
deferred to P-RT0's validation, with the batch fallback as the safe default.

---

## 7. Reconciliation — the lambda correctness contract

The batch metric is the **source of truth**; the stream is the **fast, approximate** copy. A dbt
**data test** (extending the existing `assert_*` suite) asserts they agree on **closed days**:

```
for every day d < today (watermarked, so no in-flight inserts):
    rt:    sumMerge(interchange_minor) from nimbus_rt.rt_interchange_daily      where day = d
    batch: interchange_revenue         from nimbus_metrics.metrics_finance_daily where date = d
    assert  |rt_corrected − batch| == 0      (deduped stream must match exactly)
    report   rt_fast − batch                 (the dup-driven drift, surfaced not asserted)
```

Watermarking (`day < today`) is essential: you cannot test a moving target — only closed windows
are stable. This is the same principle as the batch design's revenue-reconciliation test, now
doing double duty as the **stream-vs-batch** guarantee. Observability rides on
`system.query_views_log` (which MVs fired, timing, exceptions) and `system.view_refreshes` (RMV
status) — surfaced in the existing Prometheus scrape.

---

## 8. Phased roadmap (additive; mirrors the batch P-numbering)

| Phase | Deliverable |
| --- | --- |
| **P-RT0** | **Spike + keystone.** Validate on 26.3: (a) incremental MV over Replicated bronze → Replicated `AggregatingMergeTree` gold fires **once** and both parts replicate to both pods (the §2 keystone); (b) whether refreshable MVs work on Replicated targets (→ picks T2 mechanism, §6). One trivial `rt_smoke` rollup, deployed via dbt `materialized_view`. |
| **P-RT1** | **Flagship vertical slice — live interchange revenue.** bronze `raw_card_authorizations` → `mcc_dict` (dbt dictionary) → incremental MV → `rt_interchange_daily` (`AggregatingMergeTree`). Insert a fresh auth, query `sumMerge` before/after → rollup advances **with no dbt run**. The real-time analog of batch P2. |
| **P-RT2** | **Silver + dedup.** `slv_card_auths` (`ReplacingMergeTree`) via MV; `slv_ledger_postings`, `slv_app_events`; the Null-table fan-in for the auth stream. |
| **P-RT3** | **Gold rollups.** `rt_finance_daily`, `rt_risk_daily`, `rt_dau_daily` via MVs. Running-balance daily-delta rollup + query-time cumulative. |
| **P-RT4** | **T2 layer.** `rt_activation_funnel` as refreshable MV (or micro-batch fallback per P-RT0). |
| **P-RT5** | **Reconciliation + control-plane split.** Stream-vs-batch reconciliation test; split dbt into *deploy* (streaming objects, on-change Flux Job) vs *build* (batch-spine CronJob); `system.query_views_log` observability; READMEs; `make wh-demo-realtime` shows insert→queryable in seconds. |

Each phase is purely additive — the batch warehouse and `ch-demo` stay working throughout as the
canary, exactly as in the batch plan's risk model.

---

## 9. Decisions (proposed — for review)

1. **Real-time = internal warehouse, not ingestion.** Bronze is the fixed boundary; ingestion path
   is out of scope. → §0.
2. **Lambda.** Real-time MV cascade for serving + the existing batch dbt layer as the correctness/
   history spine; a reconciliation test binds them. → §5, §7.
3. **Event-driven medallion via MV cascade** into two new DBs (`nimbus_stream`, `nimbus_rt`),
   Replicated end-to-end, `ON CLUSTER`; the §2 single-fire replication keystone is the correctness
   foundation. → §1, §2.
4. **dbt as control plane** (deploy/version MVs, dictionaries, refreshable MVs; batch-build the
   spine) — same in-cluster runner image, two run modes. → §5.
5. **Honest dedup + tiers.** Fast approximate T0 rollup *and* corrected T1/T2 rollup; the design
   teaches the at-least-once tradeoff instead of hiding it. → §4, §6, §7.
6. **Refreshable-MV-on-Replicated is a validated unknown** (P-RT0); micro-batch dbt is the safe
   fallback. → §6, §8.

---

## 10. Open questions for you

- **T2 mechanism preference** if the P-RT0 spike shows refreshable MVs are flaky on Replicated
  targets on 26.3 — accept a **non-replicated RMV target** (simpler, loses HA on that one table)
  or the **micro-batch dbt CronJob** fallback (HA-preserving, one more CronJob)?
- **Scope of the first cut** — build only the flagship slice (P-RT0→P-RT1) to prove the paradigm,
  or the full gold set (through P-RT3) in one go?
- **Serving** — is the target a live dashboard (Grafana over ClickHouse) as the visible proof, or
  is `make wh-demo-realtime` (query before/after an insert) enough to demonstrate the seconds-tier
  freshness?
