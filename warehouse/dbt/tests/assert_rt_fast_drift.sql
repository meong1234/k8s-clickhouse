-- Fast-path dup-drift SURFACING (P6-F, §7) — the honest half of the lambda contract.
--
-- The fast T0 rollup (rt_interchange_daily) aggregates every bronze auth INCLUDING the
-- ~3% at-least-once duplicate re-inserts, so on days with dupes it OVER-counts the batch
-- truth. That drift is expected and by design — this test SURFACES it (warn severity, not
-- hidden) rather than asserting it away. It returns one row per closed day where the fast
-- interchange differs from batch, with the signed drift; `dbt test` shows these as WARN.
--
-- It also encodes the one hard invariant of an at-least-once stream: the fast path must
-- never UNDER-count (fast_minor >= batch_minor on every day). A negative drift would mean
-- lost data, not a dupe — a real bug. (Reconciliation "green" is the error-severity
-- assert_rt_batch_reconciliation passing; this warn is informational drift reporting.)
-- rt_interchange_daily is referenced by LITERAL name (not ref()) so `dbt build
-- --exclude tag:rt` keeps this batch-spine test instead of dropping it as an rt dependent.
{{ config(severity='warn') }}

with fast as (
    select day, toInt64(sumMerge(interchange_minor)) as fast_minor
    from nimbus_rt.rt_interchange_daily
    where day < today()
    group by day
),
batch as (
    select date as day, toInt64(interchange_revenue_minor) as batch_minor
    from {{ ref('metrics_finance_daily') }}
    where date < today()
)

select
    f.day                       as day,
    f.fast_minor                as fast_minor,
    b.batch_minor               as batch_minor,
    f.fast_minor - b.batch_minor as dup_drift_minor
from fast f
inner join batch b using (day)
where f.fast_minor != b.batch_minor
order by day
