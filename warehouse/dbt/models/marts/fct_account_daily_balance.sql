-- fct_account_daily_balance — periodic-snapshot fact, grain account x day.
--
-- A thin incremental projection of int_account_daily_balance at the mart grain: the
-- running-balance window logic already happened upstream, so here we only rename to the
-- mart's column contract and re-present. Incremental (delete_insert) by day mirrors the
-- intermediate model's rebuild window, so a second run touches only recent partitions.
--
-- P5 promotes this model: adds account_key (SCD2 FK) and a dim_date FK. For P2 it is the
-- account x day snapshot with opening/closing + the deposit/withdrawal split.
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
