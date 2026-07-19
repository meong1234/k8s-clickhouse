-- slv_card_auths — the CORRECTED (deduped) real-time silver card-auth stream (P6-C).
--
-- The streaming analog of batch stg_card_auths. Bronze re-inserts ~3% of auths (same
-- auth_id, later ingested_at) to simulate at-least-once delivery. The fast gold rollup
-- (rt_interchange_daily) double-counts those by design — that is the honest T0 number.
-- This table is the T1 CORRECTED path: a ReplicatedReplacingMergeTree keyed on auth_id
-- with ingested_at as the version, so a background merge (or argMax / FINAL at read time)
-- collapses each auth_id to its latest ingest — exactly one row per auth.
--
-- Fed off the fan-in (auth_fanin), so it backfills in the same replay as the gold
-- rollups. Read the deduped truth WITHOUT FINAL on the hot path:
--   SELECT argMax(approved, ingested_at) ... FROM slv_card_auths GROUP BY auth_id
-- or `... FROM slv_card_auths FINAL` for ad-hoc correctness.
--
-- Standard-mode materialized_view: creates the target table slv_card_auths
-- (ReplicatedReplacingMergeTree(ingested_at), ORDER BY auth_id) + the MV
-- slv_card_auths_mv (auth_fanin -> slv_card_auths). catchup=false (backfill via replay).
{{ config(
    materialized='materialized_view',
    engine='ReplicatedReplacingMergeTree(ingested_at)',
    order_by='auth_id',
    catchup=false
) }}

select
    auth_id,
    card_id,
    account_id,
    auth_ts,
    toDate(auth_ts) as auth_date,
    amount_minor,
    currency,
    mcc,
    merchant_name,
    approved,
    decline_reason,
    is_fraud,
    idempotency_key,
    ingested_at
from {{ ref('auth_fanin') }}
