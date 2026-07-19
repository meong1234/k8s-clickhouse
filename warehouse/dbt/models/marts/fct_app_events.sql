-- fct_app_events — engagement fact, grain one app event. The largest fact (~10M rows).
--
-- Thin projection of stg_app_events with the customer version resolved as-of event_ts
-- (customer_key from dim_customers) and date = event_date (dim_date FK). The ASOF is
-- `event_ts >= valid_from` (equality first, one inequality LAST, non-strict); customer_key
-- is PULLED from the matched row, never recomputed. Both sides are DateTime — no coercion.
--
-- Memory: the ASOF loads only the small dim_customers into memory; the 10M event side
-- streams, and the insert-side sort spills at max_bytes_before_external_sort (384 MiB), so
-- the full build fits the ~1.5Gi pod under threads=1. If a full build is ever tight, use
-- the month-window backfill: set backfill_lo/backfill_hi to build one month at a time
-- (first chunk with --full-refresh to create the table, the rest incremental — delete_insert
-- dedups by event_id so re-running a chunk is safe):
--
--   dbt run --full-refresh -s fct_app_events --vars '{backfill_lo: "2025-01-01", backfill_hi: "2025-02-01"}'
--   dbt run              -s fct_app_events --vars '{backfill_lo: "2025-02-01", backfill_hi: "2025-03-01"}'
--   ... one call per month across the data window ...
--
-- Incremental (delete_insert) keyed on event_id over the trailing event_lookback_days
-- window; idempotent. Engine inherited from the marts default.

{{
  config(
    materialized='incremental',
    incremental_strategy='delete_insert',
    unique_key=['event_id'],
    order_by=['customer_id', 'event_ts'],
    partition_by='toYYYYMM(date)'
  )
}}

{%- set bf_lo = var('backfill_lo', none) -%}
{%- set bf_hi = var('backfill_hi', none) -%}

with events as (
    select
        event_id,
        customer_id,
        event_ts,
        event_date      as date,
        event_name,
        device,
        app_version,
        session_id
    from {{ ref('stg_app_events') }}
    {% if bf_lo is not none and bf_hi is not none %}
    -- Explicit month-window backfill (overrides the incremental predicate).
    where event_date >= toDate('{{ bf_lo }}') and event_date < toDate('{{ bf_hi }}')
    {% elif is_incremental() %}
    where event_date >= (select max(date) - {{ var('event_lookback_days', 1) }} from {{ this }})
    {% endif %}
)

select
    e.event_id,
    e.customer_id,
    dcu.customer_key,
    e.date,
    e.event_ts,
    e.event_name,
    e.device,
    e.app_version,
    e.session_id
from events e
asof left join {{ ref('dim_customers') }} dcu
  on e.customer_id = dcu.customer_id   -- equality first
 and e.event_ts    >= dcu.valid_from   -- single inequality, LAST, non-strict
