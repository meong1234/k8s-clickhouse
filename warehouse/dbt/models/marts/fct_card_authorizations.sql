-- fct_card_authorizations — card-authorization fact, grain one deduped auth.
--
-- Reads the already-deduped stg_card_auths (the idempotency showcase — ~3% of bronze auths
-- are intentional dupes, collapsed by auth_id upstream) and attaches the star-schema FKs
-- plus interchange revenue. Carries BOTH account_key and customer_key (resolved as-of
-- auth_ts) so the acceptance smoke query (monthly spend by region x account_type) and the
-- as-of correctness check both join straight off this fact.
--
-- Why one ASOF per CTE: ClickHouse multi-ASOF-in-one-SELECT is fragile (the two dims both
-- expose valid_from, and multi-join column resolution is pairwise), so each ASOF gets its
-- own SELECT scope. stg_card_auths has no customer_id, so the customer for the auth comes
-- from dim_cards (card_id -> customer_id), then dim_customers is resolved as-of auth_ts.
-- Every ASOF is `auth_ts >= valid_from` (equality first, one inequality LAST, non-strict);
-- keys are PULLED from the matched dim rows, never recomputed.
--
-- interchange_revenue_minor comes from int_interchange_revenue (approved auths only, one
-- row per auth_id — a plain equi-join, no fan-out); declined/unmatched auths coalesce to 0.
--
-- Incremental (delete_insert) keyed on auth_id over the trailing event_lookback_days
-- window; idempotent. Engine inherited from the marts default.

{{
  config(
    materialized='incremental',
    incremental_strategy='delete_insert',
    unique_key=['auth_id'],
    order_by=['card_id', 'auth_ts'],
    partition_by='toYYYYMM(date)'
  )
}}

with auths as (
    select
        auth_id,
        card_id,
        account_id,
        auth_ts,
        auth_date,
        amount_minor,
        currency,
        mcc,
        merchant_name,
        approved,
        is_fraud
    from {{ ref('stg_card_auths') }}
    {% if is_incremental() %}
    where auth_date >= (select max(date) - {{ var('event_lookback_days', 1) }} from {{ this }})
    {% endif %}
),

-- Type-1 card join: pulls card_key AND the customer_id the customer ASOF needs.
with_card as (
    select
        a.*,
        dc.card_key,
        dc.customer_id
    from auths a
    left join {{ ref('dim_cards') }} dc on a.card_id = dc.card_id
),

-- ASOF #1: account version in effect at auth_ts.
with_acct as (
    select
        wc.*,
        da.account_key
    from with_card wc
    asof left join {{ ref('dim_accounts') }} da
      on wc.account_id = da.account_id
     and wc.auth_ts    >= da.valid_from
),

-- ASOF #2: customer version in effect at auth_ts.
with_cust as (
    select
        wa.*,
        dcu.customer_key
    from with_acct wa
    asof left join {{ ref('dim_customers') }} dcu
      on wa.customer_id = dcu.customer_id
     and wa.auth_ts     >= dcu.valid_from
)

select
    wc.auth_id,
    wc.card_id,
    wc.account_id,
    wc.card_key,
    wc.account_key,
    wc.customer_key,
    wc.auth_date                              as date,
    wc.auth_ts,
    wc.amount_minor,
    wc.currency,
    wc.mcc,
    wc.merchant_name,
    wc.approved,
    wc.is_fraud,
    coalesce(ir.interchange_revenue_minor, 0) as interchange_revenue_minor
from with_cust wc
left join {{ ref('int_interchange_revenue') }} ir on wc.auth_id = ir.auth_id
