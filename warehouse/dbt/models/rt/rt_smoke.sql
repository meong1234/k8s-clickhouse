-- P6-A keystone spike (≙ P-RT0). The smallest possible incremental materialized-view
-- rollup, used to validate the whole real-time path on ClickHouse 26.3 / adapter 1.10.1
-- BEFORE any real model is built. Deployed with `dbt run --select rt_smoke`; validations
-- and verdicts are recorded in warehouse/README.md.
--
-- Standard-mode `materialized_view`: dbt creates TWO objects here —
--   1. nimbus_rt.rt_smoke      — the target table (ReplicatedAggregatingMergeTree),
--   2. nimbus_rt.rt_smoke_mv   — the materialized view that fires on every INSERT into
--                                the source (nimbus_raw.raw_card_authorizations) and
--                                writes partial states into the target.
-- Both are created ON CLUSTER (profile `cluster` setting), so they exist on both pods.
--
-- catchup=false: the target table is created EMPTY (no historical backfill), so the
-- keystone test starts from zero and a single controlled INSERT proves exactly-once
-- firing. (The flagship rollup in P6-B exercises the catchup=true backfill path.)
--
-- Query the rollup with the matching -Merge combinator:
--   SELECT auth_day, countMerge(auth_count) FROM nimbus_rt.rt_smoke GROUP BY auth_day;
{{ config(
    materialized='materialized_view',
    engine='ReplicatedAggregatingMergeTree',
    order_by='(auth_day)',
    catchup=false
) }}

select
    toDate(auth_ts) as auth_day,
    countState()    as auth_count
from {{ source('nimbus_raw', 'raw_card_authorizations') }}
group by auth_day
