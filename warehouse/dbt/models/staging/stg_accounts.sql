-- Staging: accounts — one row per account, deduped and typed/renamed 1:1.
--
-- raw_accounts is a ReplacingMergeTree(ingested_at); we collapse to the latest row per
-- account_id via dedupe_latest() so downstream models (int_accounts_scd2, the daily
-- balance spine, metrics customer attribution) read a clean 1-row-per-account relation.
--
--   opened_date — the Date bucket of opened_ts, the spine start for daily balances.
--
-- Materialized as a view (staging default).
with deduped as (
    {{ dedupe_latest(
        source('nimbus_raw', 'raw_accounts'),
        key='account_id',
        order_col='ingested_at',
        value_columns=['customer_id', 'account_type', 'opened_ts', 'interest_rate_bps']
    ) }}
)

select
    account_id,
    customer_id,
    account_type,              -- checking | savings
    opened_ts,
    toDate(opened_ts)   as opened_date,
    interest_rate_bps
from deduped
