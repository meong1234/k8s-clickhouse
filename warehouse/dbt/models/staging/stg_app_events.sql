-- Staging: app events — typed/renamed 1:1 over the high-volume bronze event stream.
--
-- raw_app_events is a plain MergeTree (append-only, ~millions of rows). No dedup; we
-- only add the day bucket the DAU rollup groups on.
--
--   event_date — Date bucket of event_ts, the grain for int_daily_active_users.
--
-- Materialized as a view (staging default): the heavy uniqExact scan happens in the
-- DAU table model, not here.
select
    event_id,
    customer_id,
    event_ts,
    toDate(event_ts)   as event_date,
    event_name,
    device,
    app_version,
    session_id,
    ingested_at
from {{ source('nimbus_raw', 'raw_app_events') }}
