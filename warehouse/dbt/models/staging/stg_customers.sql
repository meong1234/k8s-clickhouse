-- Staging: customers — one row per customer, deduped and typed/renamed 1:1.
--
-- raw_customers is a ReplacingMergeTree(ingested_at); reading it without FINAL can
-- surface un-merged duplicates, so we collapse to the latest row per customer_id via
-- the dedupe_latest() macro (argMax(col, ingested_at)). No filtering or business logic.
--
-- Materialized as a view (staging default): cheap, no storage, always fresh.
with deduped as (
    {{ dedupe_latest(
        source('nimbus_raw', 'raw_customers'),
        key='customer_id',
        order_col='ingested_at',
        value_columns=['signup_ts', 'email', 'full_name', 'country', 'dob',
                       'risk_tier', 'referral_source']
    ) }}
)

select
    customer_id,
    signup_ts,
    email,
    full_name,
    country,
    dob,
    risk_tier,
    referral_source
from deduped
