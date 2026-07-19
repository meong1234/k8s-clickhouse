-- int_customers_scd2 — the customer KYC-status history as a Slowly-Changing Dimension
-- Type 2, DERIVED (not a dbt snapshot).
--
-- Why derived: we own the full change-event log (stg_kyc_events), so SCD2 is built
-- deterministically with window functions rather than dbt snapshots (which lean on
-- UPDATE-style change capture that maps poorly onto ClickHouse mutations). See
-- docs/warehouse-architecture.md §4.
--
-- Grain: one row per (customer_id, valid_from) — a contiguous validity interval during
-- which the customer's kyc_status was constant.
--
--   * change points = one synthetic INITIAL point at signup_ts with kyc_status='none'
--     (pre-KYC), UNION every KYC event (event_ts, new_status).
--   * valid_from = the change ts; valid_to = leadInFrame(next change ts) over the
--     per-customer timeline, defaulting to a far-future sentinel for the open interval.
--   * is_current = the interval whose valid_to is the sentinel (exactly one per customer).
--
-- Static attributes (risk_tier, country) come from stg_customers; region is joined from
-- seed_countries. Materialized as a view (intermediate default).
--
-- Sentinel note: source timestamps are 32-bit DateTime (max 2106-02-07), so the open
-- interval's valid_to is 2106-01-01 — "far future" relative to the 2025-2026 data window,
-- and safely in range. Defined once so the leadInFrame default and the is_current
-- comparison can never drift apart.

{% set far_future = "toDateTime('2106-01-01 00:00:00')" %}

with kyc_points as (
    -- Initial pre-KYC interval from signup. priority 0 so a real event at the same
    -- timestamp wins the collapse below.
    select
        customer_id,
        signup_ts   as change_ts,
        'none'      as kyc_status,
        0           as priority
    from {{ ref('stg_customers') }}

    union all

    select
        customer_id,
        event_ts    as change_ts,
        new_status  as kyc_status,
        1           as priority
    from {{ ref('stg_kyc_events') }}
),

-- One row per (customer_id, change_ts): if signup coincides with the first event, the
-- event's status wins. Guarantees valid_from is unique per customer (the SCD2 key test).
collapsed as (
    select
        customer_id,
        change_ts,
        argMax(kyc_status, priority) as kyc_status
    from kyc_points
    group by customer_id, change_ts
),

intervals as (
    select
        customer_id,
        kyc_status,
        change_ts as valid_from,
        leadInFrame(change_ts, 1, {{ far_future }}) over (
            partition by customer_id
            order by change_ts
            rows between current row and unbounded following
        ) as valid_to
    from collapsed
)

select
    i.customer_id                  as customer_id,   -- alias: join-ambiguous names, else CH keeps `i.`/`c.` prefix
    i.valid_from                   as valid_from,
    i.valid_to                     as valid_to,
    i.valid_to = {{ far_future }}  as is_current,
    i.kyc_status                   as kyc_status,
    c.risk_tier                    as risk_tier,
    c.country                      as country,
    co.region                      as region
from intervals i
inner join {{ ref('stg_customers') }} c on i.customer_id = c.customer_id
left join {{ ref('seed_countries') }} co on c.country = co.country
