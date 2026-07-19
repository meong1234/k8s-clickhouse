-- rt_activation_funnel — the T2 activation funnel (P6-E, ≙ P-RT4).
--
-- The funnel (per-customer min/first across four sources) is a multi-table join with
-- history-spanning mins — the §3 "not a single insert-scoped block" case, so it can't be
-- a T0 incremental MV. The P6-A spike proved the T2 alternative (a refreshable MV) is
-- REFUSED on this cluster (Replicated target + non-replicated database — see the README
-- P6-A findings), so this is the MICRO-BATCH fallback: an ordinary dbt table model in
-- nimbus_rt, tagged 'rt', that a short-interval CronJob (dbt-refresher, P6-F) re-runs
-- every few minutes. Same minutes-tier freshness as a refreshable MV, HA-preserving, zero
-- new mechanism.
--
-- Reads bronze + the silver ledger stream directly (not the batch staging views), so the
-- refresher is independent of the batch spine. Milestone definitions mirror the batch
-- int_activation_funnel exactly, so the two reconcile: transacted == funnel transacted.
-- One row: the current cumulative funnel + a refreshed_at stamp to make freshness visible.
{{ config(
    materialized='table',
    engine='ReplicatedMergeTree',
    order_by='tuple()'
) }}

with cust as (
    select customer_id, argMax(signup_ts, ingested_at) as signup_ts
    from {{ source('nimbus_raw', 'raw_customers') }}
    group by customer_id
),

verified as (
    select customer_id, toNullable(min(event_ts)) as kyc_verified_ts
    from {{ source('nimbus_raw', 'raw_kyc_events') }}
    where new_status = 'verified'
    group by customer_id
),

acct as (
    select account_id, argMax(customer_id, ingested_at) as customer_id
    from {{ source('nimbus_raw', 'raw_accounts') }}
    group by account_id
),

postings as (
    select
        a.customer_id,
        nullIf(minIf(p.posting_ts, p.signed_amount_minor > 0), toDateTime(0)) as first_funded_ts,
        nullIf(minIf(p.posting_ts, p.signed_amount_minor < 0), toDateTime(0)) as first_txn_ts
    from {{ ref('slv_ledger_postings') }} p
    inner join acct a on p.account_id = a.account_id
    group by a.customer_id
)

select
    count()                                     as signups,
    countIf(v.kyc_verified_ts is not null)      as kyc_verified,
    countIf(p.first_funded_ts is not null)      as funded,
    countIf(p.first_txn_ts is not null)         as transacted,
    now()                                       as refreshed_at
from cust c
left join verified v  on c.customer_id = v.customer_id
left join postings p  on c.customer_id = p.customer_id
