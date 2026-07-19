-- rt_risk_daily — real-time risk rollup (P6-D): auths / approvals / declines / fraud by
-- day. Fed off the Null fan-in (auth_fanin) alongside rt_interchange_daily, so one bronze
-- auth insert updates both in parallel (cascade depth 2). This is the FAST (T0) number —
-- it counts every bronze auth including the ~3% dupes; the corrected counts come from
-- slv_card_auths at reconciliation time (P6-F).
--
-- Counts are kept as sumState over UInt64 flags (not countIf) so the -Merge read is a
-- plain sumMerge and the intent of each column is explicit. catchup=false — backfilled by
-- the fan-in replay (`make wh-rt-backfill`).
--
-- Read: SELECT day, countMerge(auth_count), sumMerge(approved_count), sumMerge(decline_count),
--       sumMerge(fraud_count) FROM nimbus_rt.rt_risk_daily GROUP BY day;
{{ config(
    materialized='materialized_view',
    engine='ReplicatedAggregatingMergeTree',
    order_by='(day)',
    catchup=false
) }}

select
    toDate(auth_ts)                     as day,
    countState()                        as auth_count,
    sumState(toUInt64(approved = 1))    as approved_count,
    sumState(toUInt64(approved = 0))    as decline_count,
    sumState(toUInt64(is_fraud = 1))    as fraud_count
from {{ ref('auth_fanin') }}
group by day
