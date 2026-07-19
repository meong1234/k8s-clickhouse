-- fct_ledger_postings — transaction fact, grain one ledger posting (double-entry leg).
--
-- Projects int_ledger_categorized to the mart contract and attaches the star-schema FKs:
-- account_key (dim_accounts, as-of posting_ts) and date (dim_date, = posting_date).
--
-- As-of FK (the teaching moment): a posting's account status can change over time, so we
-- attribute the leg to the account VERSION in effect at posting_ts via an ASOF LEFT JOIN.
-- ClickHouse ASOF requires the equality condition(s) first and exactly one inequality
-- LAST; `posting_ts >= valid_from` (non-strict) selects the row with the greatest
-- valid_from <= posting_ts per account. Because dim_accounts intervals are contiguous and
-- non-overlapping, that is exactly the version whose [valid_from, valid_to) contains
-- posting_ts — so we never test valid_to. account_key is PULLED from the matched row, not
-- recomputed, so fct->dim relationships hold by construction. Both sides are DateTime, so
-- there is no Date/DateTime coercion here (unlike fct_account_daily_balance).
--
-- Note on internal legs: a transaction's counterparty leg may post to a NIMBUS-* internal
-- clearing account that is not in raw_accounts (hence not in dim_accounts). Those legs are
-- kept (the grain is "one posting", and keeping them lets the ledger sum-to-zero invariant
-- be checked at the fact) and resolve to the unknown member account_key = 0.
--
-- Incremental (delete_insert) keyed on posting_id, so re-scanning the trailing
-- event_lookback_days window is idempotent. Engine inherited from the marts default.

{{
  config(
    materialized='incremental',
    incremental_strategy='delete_insert',
    unique_key=['posting_id'],
    order_by=['account_id', 'posting_ts'],
    partition_by='toYYYYMM(date)'
  )
}}

with postings as (
    select
        posting_id,
        transaction_id,
        account_id,
        posting_ts,
        posting_date        as date,
        direction,
        amount_minor,
        signed_amount_minor,
        category,
        is_revenue,
        revenue_type
    from {{ ref('int_ledger_categorized') }}
    {% if is_incremental() %}
    where posting_date >= (select max(date) - {{ var('event_lookback_days', 1) }} from {{ this }})
    {% endif %}
)

select
    p.posting_id,
    p.transaction_id,
    p.account_id,
    da.account_key,
    p.date,
    p.posting_ts,
    p.direction,
    p.amount_minor,
    p.signed_amount_minor,
    p.category,
    p.is_revenue,
    p.revenue_type
from postings p
asof left join {{ ref('dim_accounts') }} da
  on p.account_id = da.account_id      -- equality first
 and p.posting_ts >= da.valid_from     -- single inequality, LAST, non-strict
