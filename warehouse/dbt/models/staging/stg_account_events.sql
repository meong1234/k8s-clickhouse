-- Staging: account events — typed/renamed 1:1 over the append-only bronze event log.
--
-- raw_account_events is a plain MergeTree (append-only): one row per account state
-- change. Feeds int_accounts_scd2 (the account status history).
--
-- Materialized as a view (staging default).
select
    account_event_id,
    account_id,
    event_ts,
    old_status,
    new_status,   -- active | frozen | closed
    ingested_at
from {{ source('nimbus_raw', 'raw_account_events') }}
