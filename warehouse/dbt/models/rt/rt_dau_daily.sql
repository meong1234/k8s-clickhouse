-- rt_dau_daily — real-time daily-active-users rollup (P6-D).
--
-- uniqState(customer_id) by day over the silver app-event stream (slv_app_events). The
-- streaming analog of batch int_daily_active_users: the expensive distinct-count is a
-- partial HyperLogLog state maintained at insert time, so the DAU read is a cheap
-- uniqMerge instead of a full uniqExact scan over ~1M events. A single MV (no fan-in).
--
-- catchup=true backfills from the already-populated slv_app_events. uniqState is HLL, so
-- the backfill over 1M rows is memory-light. mau/stickiness are query-time rollups over
-- these daily states (a 30-day uniqMerge window) — documented in the .yml, computed batch
-- in metrics_growth_daily (P6-E) as the reconciled truth.
--
-- Read: SELECT day, uniqMerge(dau) FROM nimbus_rt.rt_dau_daily GROUP BY day;
{{ config(
    materialized='materialized_view',
    engine='ReplicatedAggregatingMergeTree',
    order_by='(day)',
    catchup=true
) }}

select
    event_date              as day,
    uniqState(customer_id)  as dau
from {{ ref('slv_app_events') }}
group by day
