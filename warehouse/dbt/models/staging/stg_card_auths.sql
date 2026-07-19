-- Staging: card authorizations — THE dedup showcase.
--
-- Bronze raw_card_authorizations is a plain MergeTree into which the loader
-- deliberately re-inserts ~3% of rows (same auth_id, later ingested_at) to simulate
-- at-least-once idempotency-key delivery. This view collapses those duplicates back to
-- one row per auth_id, keeping the LATEST ingest via dedupe_latest()
-- (argMax(col, ingested_at)).
--
-- This is where dedup lives for the whole card-auth thread: int_interchange_revenue
-- (and, in P5, fct_card_authorizations) read this already-deduped relation. There is no
-- separate int_card_auths_deduped model — that node from architecture §5 is folded here
-- (see warehouse/README.md P4 note). The `unique` test on auth_id in the paired .yml is
-- the dedup-effectiveness check.
--
--   auth_date — Date bucket of auth_ts.
--
-- Materialized as a view (staging default).
with deduped as (
    {{ dedupe_latest(
        source('nimbus_raw', 'raw_card_authorizations'),
        key='auth_id',
        order_col='ingested_at',
        value_columns=['card_id', 'account_id', 'auth_ts', 'amount_minor', 'currency',
                       'mcc', 'merchant_name', 'approved', 'decline_reason', 'is_fraud',
                       'idempotency_key']
    ) }}
)

select
    auth_id,
    card_id,
    account_id,
    auth_ts,
    toDate(auth_ts)   as auth_date,
    amount_minor,
    currency,
    mcc,
    merchant_name,
    approved,          -- UInt8 (1 = approved)
    decline_reason,
    is_fraud,          -- UInt8
    idempotency_key
from deduped
