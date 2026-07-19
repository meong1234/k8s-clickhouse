-- int_activation_funnel — one wide row per customer with the four activation milestones,
-- in the order the funnel invariant expects them (P7 formalizes it):
--
--   signup_ts <= kyc_verified_ts <= first_funded_ts <= first_txn_ts
--
--   * signup_ts       — from stg_customers.
--   * kyc_verified_ts — earliest 'verified' KYC event.
--   * first_funded_ts — earliest money-IN posting on any of the customer's accounts
--                       (signed_amount_minor > 0).
--   * first_txn_ts    — earliest money-OUT posting (signed_amount_minor < 0).
--
-- Ledger is attributed to a customer via stg_accounts (account_id -> customer_id), which
-- also drops the NIMBUS-* internal/settlement legs (they have no customer account).
-- Milestones not yet reached are NULL: minIf() returns the epoch default (0) when no row
-- matches, which nullIf() normalizes to NULL so "not reached" is unambiguous.
--
-- Materialized as a view (intermediate default).
with verified as (
    -- toNullable so customers who never verified (e.g. rejected) resolve to NULL after
    -- the LEFT JOIN below. ClickHouse's default join (join_use_nulls=0) fills an
    -- unmatched non-Nullable DateTime with the epoch (1970-01-01), which would look like
    -- a "verified before signup" milestone; a Nullable column fills with NULL instead.
    select
        customer_id,
        toNullable(min(event_ts)) as kyc_verified_ts
    from {{ ref('stg_kyc_events') }}
    where new_status = 'verified'
    group by customer_id
),

customer_postings as (
    select
        a.customer_id,
        nullIf(minIf(p.posting_ts, p.signed_amount_minor > 0), toDateTime(0)) as first_funded_ts,
        nullIf(minIf(p.posting_ts, p.signed_amount_minor < 0), toDateTime(0)) as first_txn_ts
    from {{ ref('stg_ledger_postings') }} p
    inner join {{ ref('stg_accounts') }} a on p.account_id = a.account_id
    group by a.customer_id
)

select
    c.customer_id       as customer_id,   -- alias: customer_id is join-ambiguous, else CH keeps `c.customer_id`
    c.signup_ts         as signup_ts,
    v.kyc_verified_ts   as kyc_verified_ts,
    cp.first_funded_ts  as first_funded_ts,
    cp.first_txn_ts     as first_txn_ts
from {{ ref('stg_customers') }} c
left join verified v          on c.customer_id = v.customer_id
left join customer_postings cp on c.customer_id = cp.customer_id
