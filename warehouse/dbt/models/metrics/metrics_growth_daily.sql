-- metrics_growth_daily — the curated daily growth metric mart (P6-E).
--
-- Batch-truth growth for the lambda: DAU reconciles against the streaming rt_dau_daily on
-- closed days (P6-F); mau/stickiness/activation are the completeness metrics. Grain: day.
--
--   dau         — distinct customers active that day (uniqExact over stg_app_events).
--   mau         — distinct customers active in the trailing 28 days (inclusive). Computed
--                 with a windowed groupArray of each day's customer set, flattened and
--                 de-duplicated — uniq is not additive, so it can't be a rolling sum.
--   stickiness  — dau / mau (0 < stickiness <= 1 by construction).
--   signups     — customers whose signup_ts falls on that day.
--   activations — customers whose FIRST money-out posting (int_activation_funnel.
--                 first_txn_ts) falls on that day; sum over all days == the funnel's
--                 transacted count (the P6-F activation sanity check).
with daily_events as (
    select
        event_date,
        groupUniqArray(customer_id)  as day_customers,
        uniqExact(customer_id)       as dau
    from {{ ref('stg_app_events') }}
    group by event_date
),

growth as (
    select
        event_date  as date,
        dau,
        length(
            arrayDistinct(arrayFlatten(
                groupArray(day_customers) over (
                    order by event_date rows between 27 preceding and current row
                )
            ))
        )           as mau
    from daily_events
),

signups as (
    select toDate(signup_ts) as date, count() as signups
    from {{ ref('stg_customers') }}
    group by date
),

activations as (
    select toDate(first_txn_ts) as date, count() as activations
    from {{ ref('int_activation_funnel') }}
    where first_txn_ts is not null
    group by date
)

select
    g.date                                  as date,
    g.dau                                   as dau,
    g.mau                                   as mau,
    round(g.dau / nullif(g.mau, 0), 4)      as stickiness,
    ifNull(s.signups, 0)                    as signups,
    ifNull(a.activations, 0)                as activations
from growth g
left join signups s     on g.date = s.date
left join activations a on g.date = a.date
order by date
