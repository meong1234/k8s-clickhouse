-- fct_account_daily_balance — periodic-snapshot fact, grain account x day.
--
-- A thin incremental projection of int_account_daily_balance at the mart grain: the
-- running-balance window logic already happened upstream, so here we rename to the mart's
-- column contract and attach the star-schema FKs. Incremental (delete_insert) by day
-- mirrors the intermediate model's rebuild window, so a second run touches only recent
-- partitions. The `date` column is the dim_date FK.
--
-- P5 promotes this model with account_key (the SCD2 dim_accounts FK). Note the as-of
-- subtlety unique to this fact: the grain is a Date, but dim_accounts.valid_from is a
-- DateTime, and `date >= valid_from` would coerce the Date to MIDNIGHT — so an account
-- that flipped status mid-day would resolve to the pre-change version for that whole day.
-- We deliberately resolve the END-OF-DAY status by comparing 23:59:59 of the snapshot day
-- (toDateTime(date) + 86400 - 1) against valid_from. ASOF picks the version with the
-- greatest valid_from <= that instant; because SCD2 intervals are contiguous and
-- non-overlapping we do not also test valid_to. account_key is PULLED from the matched dim
-- row (never recomputed) so the relationships test matches by construction.
--
-- Engine (ReplicatedMergeTree) is inherited from the marts layer default in dbt_project.yml.

{{
  config(
    materialized='incremental',
    incremental_strategy='delete_insert',
    unique_key=['account_id', 'date'],
    order_by=['account_id', 'date'],
    partition_by='toYYYYMM(date)'
  )
}}

with balance as (
    select
        account_id,
        day               as date,
        opening_balance,
        closing_balance,
        daily_deposits    as total_deposits,
        daily_withdrawals as total_withdrawals
    from {{ ref('int_account_daily_balance') }}
    {% if is_incremental() %}
    where day >= (select max(date) - {{ var('daily_balance_lookback_days', 3) }} from {{ this }})
    {% endif %}
)

select
    b.account_id,
    da.account_key,
    b.date,
    b.opening_balance,
    b.closing_balance,
    b.total_deposits,
    b.total_withdrawals
from balance b
asof left join {{ ref('dim_accounts') }} da
  on b.account_id = da.account_id                          -- equality first
 and (toDateTime(b.date) + 86400 - 1) >= da.valid_from     -- end-of-day, single inequality LAST
