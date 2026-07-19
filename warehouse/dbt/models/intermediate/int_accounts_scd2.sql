-- int_accounts_scd2 — the account status history as SCD2, derived from the account
-- event log (same pattern and rationale as int_customers_scd2; see that model's header).
--
-- Grain: one row per (account_id, valid_from). Change points = one synthetic INITIAL
-- point at opened_ts with status='active', UNION every account event (event_ts,
-- new_status). valid_to via leadInFrame with a far-future sentinel; is_current on the
-- open interval. Static attrs (customer_id, account_type, interest_rate_bps) from
-- stg_accounts. Materialized as a view (intermediate default).
--
-- Sentinel note: source timestamps are 32-bit DateTime (max 2106-02-07), so the open
-- interval's valid_to is 2106-01-01 — "far future" for the 2025-2026 data window and in
-- range. Defined once so the leadInFrame default and is_current can't drift apart.

{% set far_future = "toDateTime('2106-01-01 00:00:00')" %}

with account_points as (
    -- Initial interval from account opening. priority 0 so a same-timestamp event wins.
    select
        account_id,
        opened_ts   as change_ts,
        'active'    as status,
        0           as priority
    from {{ ref('stg_accounts') }}

    union all

    select
        account_id,
        event_ts    as change_ts,
        new_status  as status,
        1           as priority
    from {{ ref('stg_account_events') }}
),

collapsed as (
    select
        account_id,
        change_ts,
        argMax(status, priority) as status
    from account_points
    group by account_id, change_ts
),

intervals as (
    select
        account_id,
        status,
        change_ts as valid_from,
        leadInFrame(change_ts, 1, {{ far_future }}) over (
            partition by account_id
            order by change_ts
            rows between current row and unbounded following
        ) as valid_to
    from collapsed
)

select
    i.account_id                   as account_id,    -- alias: join-ambiguous names, else CH keeps `i.`/`a.` prefix
    a.customer_id                  as customer_id,
    i.valid_from                   as valid_from,
    i.valid_to                     as valid_to,
    i.valid_to = {{ far_future }}  as is_current,
    i.status                       as status,
    a.account_type                 as account_type,
    a.interest_rate_bps            as interest_rate_bps
from intervals i
inner join {{ ref('stg_accounts') }} a on i.account_id = a.account_id
