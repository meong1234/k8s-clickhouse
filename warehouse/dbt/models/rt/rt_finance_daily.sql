-- rt_finance_daily — real-time finance rollup (P6-D): deposits / withdrawals by day.
--
-- Fed off the silver ledger insert stream (slv_ledger_postings), NOT bronze — a single MV
-- (ledger has one consumer, so no fan-in). Deposits are credits (signed_amount_minor > 0),
-- withdrawals the magnitude of debits; both as sumState partial aggregates.
--
-- catchup=true: the target is created via CREATE TABLE ... AS SELECT ...State... FROM
-- slv_ledger_postings, which is already populated (its own catchup) by the time this model
-- runs (dbt DAG order via ref), so this backfills the ~194k historical postings directly.
--
-- Read: SELECT day, sumMerge(deposits_minor), sumMerge(withdrawals_minor),
--       countMerge(posting_count) FROM nimbus_rt.rt_finance_daily GROUP BY day;
{{ config(
    materialized='materialized_view',
    engine='ReplicatedAggregatingMergeTree',
    order_by='(day)',
    catchup=true
) }}

select
    posting_date                                                        as day,
    sumState(if(signed_amount_minor > 0, signed_amount_minor, toInt64(0)))   as deposits_minor,
    sumState(if(signed_amount_minor < 0, -signed_amount_minor, toInt64(0)))  as withdrawals_minor,
    countState()                                                        as posting_count
from {{ ref('slv_ledger_postings') }}
group by day
