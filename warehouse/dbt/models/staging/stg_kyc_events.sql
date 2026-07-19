-- Staging: KYC events — typed/renamed 1:1 over the append-only bronze event log.
--
-- raw_kyc_events is a plain MergeTree (append-only, no dedup needed): one row per KYC
-- status change, strictly ordered per customer. Feeds int_customers_scd2 (the KYC
-- status history) and int_activation_funnel (first verified timestamp).
--
-- Materialized as a view (staging default).
select
    kyc_event_id,
    customer_id,
    event_ts,
    old_status,
    new_status,   -- submitted | pending | verified | rejected
    reason,
    ingested_at
from {{ source('nimbus_raw', 'raw_kyc_events') }}
