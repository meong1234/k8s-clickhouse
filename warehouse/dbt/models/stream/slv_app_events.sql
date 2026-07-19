-- slv_app_events — real-time silver app-event stream (P6-C).
--
-- Streaming analog of batch stg_app_events: typed/renamed 1:1 over the high-volume bronze
-- event stream, plus the event_date day bucket the DAU rollup (rt_dau_daily, P6-D) groups
-- on. A single MV bronze raw_app_events -> slv_app_events (append-only, no dedup — app
-- events have no idempotency dupes).
--
-- catchup=true backfills the ~1M historical events from bronze at deploy time via
-- CREATE TABLE ... AS SELECT (a straight typed copy — no aggregation, so cheap). Engine
-- ReplicatedMergeTree, ORDER BY (customer_id, event_ts) like the bronze source.
{{ config(
    materialized='materialized_view',
    engine='ReplicatedMergeTree',
    order_by='(customer_id, event_ts)',
    catchup=true
) }}

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
