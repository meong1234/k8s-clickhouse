-- rt_interchange_daily — the flagship real-time rollup (P6-B, ≙ P-RT1).
--
-- Live interchange revenue per (day, merchant_category), computed continuously by a
-- materialized view firing on every INSERT into bronze raw_card_authorizations — no dbt
-- at runtime. The batch analog is int_interchange_revenue -> metrics_finance_daily; here
-- the SAME integer basis-point math runs at insert time and lands as a partial aggregate
-- state, read back with -Merge in single-digit seconds of the bronze insert.
--
-- Standard-mode `materialized_view`: dbt creates the target table
-- nimbus_rt.rt_interchange_daily (ReplicatedAggregatingMergeTree, ORDER BY (day,
-- merchant_category)) with columns inferred from the -State expressions —
--   interchange_minor  AggregateFunction(sum, Int64)
--   auth_count         AggregateFunction(count)
-- plus the view rt_interchange_daily_mv (TO the target). GROUP BY matches ORDER BY so
-- states collapse on the right key.
--
-- Fed off the Null fan-in (auth_fanin), not bronze directly (P6-C re-point): one bronze
-- insert fans out to this rollup, rt_risk_daily, and slv_card_auths in parallel, cascade
-- depth 2. catchup=false — auth_fanin holds no history, so the historical backfill is a
-- controlled replay through the fan-in (`make wh-rt-backfill`), which fires this MV over
-- the ~1M existing bronze auths. NOT POPULATE (which would drop concurrent inserts).
--
-- Enrichment is via the mcc_dict DICTIONARY, not a join (an MV only fires off its
-- left-most FROM table). The depends_on orders the deploy (mcc_dict before this) since
-- dictGet takes a string literal, not a ref().
--
-- Read pattern (see the .yml):
--   SELECT day, merchant_category,
--          sumMerge(interchange_minor)  AS interchange_minor,
--          countMerge(auth_count)       AS auth_count
--   FROM nimbus_rt.rt_interchange_daily GROUP BY day, merchant_category;
--
-- depends_on: {{ ref('mcc_dict') }}
{{ config(
    materialized='materialized_view',
    engine='ReplicatedAggregatingMergeTree',
    order_by='(day, merchant_category)',
    catchup=false
) }}

select
    toDate(auth_ts)                                                            as day,
    dictGetString('nimbus_rt.mcc_dict', 'merchant_category', toUInt64(mcc))    as merchant_category,
    sumState(
        intDiv(
            amount_minor * dictGetUInt16('nimbus_rt.mcc_dict', 'interchange_rate_bps', toUInt64(mcc)),
            10000
        )
    )                                                                          as interchange_minor,
    countState()                                                               as auth_count
from {{ ref('auth_fanin') }}
where approved = 1
group by day, merchant_category
