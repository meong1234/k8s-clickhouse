-- rt_balance_delta_daily — real-time per-account daily balance DELTA (P6-D).
--
-- The account running balance is a cumulative sum over ALL history — a window that cannot
-- stream (an MV sees only the just-inserted block, never the full past). So we stream the
-- DAILY DELTA (sumState of signed_amount_minor by account x day), and compute the
-- CUMULATIVE balance at QUERY TIME with a window over the small daily-delta rollup:
--
--   SELECT account_id, day,
--          sum(sumMerge(delta_minor)) OVER (PARTITION BY account_id ORDER BY day)
--            AS running_balance_minor
--   FROM nimbus_rt.rt_balance_delta_daily
--   GROUP BY account_id, day;
--
-- This is the §3 "window / as-of -> batch" boundary made concrete: the delta is
-- insert-scoped (streams), the cumulative is a full-history window (query-time or batch).
-- Fed off slv_ledger_postings (single MV). catchup=true backfills from the populated
-- silver.
{{ config(
    materialized='materialized_view',
    engine='ReplicatedAggregatingMergeTree',
    order_by='(account_id, day)',
    catchup=true
) }}

select
    account_id,
    posting_date                    as day,
    sumState(signed_amount_minor)   as delta_minor,
    countState()                    as posting_count
from {{ ref('slv_ledger_postings') }}
group by account_id, day
