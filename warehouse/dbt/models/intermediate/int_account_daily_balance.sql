-- int_account_daily_balance — the P2 incremental teaching model.
--
-- Grain: account_id x day. One row for EVERY day each REAL customer account exists
-- (a date spine from the account's opened day to the global max posting day), so the
-- running closing_balance carries forward across days with no postings.
--
-- Running balance is inherently full-history: day N's closing = opening + sum of all
-- daily_net up to N. We keep it incremental anyway (this is the pattern every later
-- incremental model copies) using a CARRIED-OPENING window:
--
--   * Each run recomputes only the trailing window [window_start, max_day], where
--     window_start = max(day already stored) - daily_balance_lookback_days.
--   * The opening balance for that window is SEEDED from the closing_balance already
--     stored in {{ this }} on the last day strictly before window_start, so the
--     running sum stays correct without recomputing all of history.
--   * delete_insert removes exactly the recomputed (account_id, day) tuples and
--     re-inserts them, so only recent toYYYYMM partitions are touched.
--
-- Only REAL customer accounts get balances: the balanced double-entry ledger also has
-- NIMBUS-* internal/settlement legs, which we exclude by joining to raw_accounts.
--
-- CAVEATS (see warehouse/README.md):
--   1. Late data dated BEFORE window_start is NOT reflected (its downstream closings
--      are frozen). Fix with: dbt run --full-refresh --select int_account_daily_balance
--      (cheap at this scale). daily_balance_lookback_days sizes the slightly-late slack.
--   2. window_start tracks max(day) IN THE TABLE, not wall-clock; a future-dated posting
--      shifts the window.
--   3. The delete_insert DELETE/INSERT is emitted without ON CLUSTER; correctness on
--      1 shard x 2 replicas relies on ReplicatedMergeTree propagating via Keeper. Fine
--      here, NOT sharded-safe.

{{
  config(
    materialized='incremental',
    incremental_strategy='delete_insert',
    unique_key=['account_id', 'day'],
    engine='ReplicatedMergeTree',
    order_by=['account_id', 'day'],
    partition_by='toYYYYMM(day)'
  )
}}

{%- set lookback = var('daily_balance_lookback_days', 3) -%}

{#- Resolve the rebuild-window lower bound once, from what is already stored. -#}
{%- if is_incremental() -%}
  {%- set win_query -%}
    select toString(max(day) - {{ lookback }}) from {{ this }}
  {%- endset -%}
  {%- set window_start = run_query(win_query).columns[0].values()[0] -%}
{%- endif -%}

with accounts as (
    -- Dedup the ReplacingMergeTree source: one opened_day per real account.
    -- (P4 will repoint this at stg_accounts.)
    select
        account_id,
        toDate(argMax(opened_ts, ingested_at)) as opened_day
    from {{ source('nimbus_raw', 'raw_accounts') }}
    group by account_id
),

bounds as (
    select max(posting_date) as max_day
    from {{ ref('stg_ledger_postings') }}
),

{% if is_incremental() %}
seed as (
    -- Carried opening: closing_balance on the last stored day strictly before the window.
    select
        account_id,
        argMax(closing_balance, day) as opening_seed
    from {{ this }}
    where day < toDate('{{ window_start }}')
    group by account_id
),
{% endif %}

-- Per-account date spine via ARRAY JOIN range() — bounded per account (opened_day..max_day),
-- so no N-accounts x N-days cross-join blow-up. In incremental mode the spine start is
-- clamped to the rebuild window.
spine as (
    select
        a.account_id,
        {% if is_incremental() -%}
        greatest(a.opened_day, toDate('{{ window_start }}')) + n as day
        {%- else -%}
        a.opened_day + n as day
        {%- endif %}
    from accounts a
    cross join bounds b
    array join range(
        toUInt32(greatest(
            b.max_day - {% if is_incremental() %}greatest(a.opened_day, toDate('{{ window_start }}')){% else %}a.opened_day{% endif %},
            0
        )) + 1
    ) as n
),

daily as (
    select
        p.account_id,
        p.posting_date                                    as day,
        sumIf(p.amount_minor, p.direction = 'credit')     as daily_deposits,
        sumIf(p.amount_minor, p.direction = 'debit')      as daily_withdrawals,
        sum(p.signed_amount_minor)                        as daily_net
    from {{ ref('stg_ledger_postings') }} p
    where p.account_id in (select account_id from accounts)   -- real customer legs only
      {% if is_incremental() %}
      and p.posting_date >= toDate('{{ window_start }}')
      {% endif %}
    group by p.account_id, day
),

joined as (
    select
        s.account_id,
        s.day,
        coalesce(d.daily_deposits, 0)    as daily_deposits,
        coalesce(d.daily_withdrawals, 0) as daily_withdrawals,
        coalesce(d.daily_net, 0)         as daily_net
    from spine s
    left join daily d on s.account_id = d.account_id and s.day = d.day
),

running as (
    select
        j.account_id,
        j.day,
        j.daily_deposits,
        j.daily_withdrawals,
        j.daily_net,
        {% if is_incremental() %}coalesce(sd.opening_seed, toInt64(0)){% else %}toInt64(0){% endif %}
          + sum(j.daily_net) over (
                partition by j.account_id
                order by j.day
                rows between unbounded preceding and current row
            ) as closing_balance
    from joined j
    {% if is_incremental() %}
    left join seed sd on j.account_id = sd.account_id
    {% endif %}
)

select
    account_id,
    day,
    daily_deposits,
    daily_withdrawals,
    daily_net,
    closing_balance - daily_net as opening_balance,   -- = prior day's closing
    closing_balance
from running
