-- int_daily_active_users — distinct active customers per calendar day.
--
-- Unlike the rest of the intermediate layer (views), this is materialized as a TABLE:
-- it collapses the ~10M-row stg_app_events scan into one row per day once, so the
-- multiple P6 growth metrics that read DAU don't re-scan the event stream each time.
-- Engine is named explicitly (dbt-clickhouse 1.10.1 does not auto-replicate from the
-- cluster profile — same rule as the marts/metrics layer defaults).
--
-- Grain: one row per activity_date. dau = uniqExact(customer_id) (exact, not approximate
-- — small enough scale to afford it).
{{
  config(
    materialized='table',
    engine='ReplicatedMergeTree',
    order_by=['activity_date']
  )
}}

select
    event_date              as activity_date,
    uniqExact(customer_id)  as dau
from {{ ref('stg_app_events') }}
group by activity_date
order by activity_date
