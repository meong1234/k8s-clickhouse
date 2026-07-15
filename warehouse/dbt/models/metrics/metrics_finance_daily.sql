-- metrics_finance_daily — the curated daily finance metric mart (v1, minimal).
--
-- P2 ships only the two metrics the vertical slice can support end to end:
--   total_deposits          — total credits across all accounts that day (minor units).
--   avg_balance_per_customer— total closing balance that day / distinct customers holding
--                             an account (minor units per customer).
-- Revenue columns (interchange/fee/interest, total_revenue, arpu) arrive in P6.
--
-- The metric DEFINITIONS are the contract (see the column docs in the .yml). Grain: day.
-- Small (~18 months of days), so a full-rebuild table; engine inherited from the metrics
-- layer default. Customer attribution comes from the deduped raw_accounts (account->customer).
with account_customer as (
    select
        account_id,
        argMax(customer_id, ingested_at) as customer_id
    from {{ source('nimbus_raw', 'raw_accounts') }}
    group by account_id
)

select
    b.date,
    sum(b.total_deposits)                                        as total_deposits,
    intDiv(sum(b.closing_balance), uniqExact(ac.customer_id))    as avg_balance_per_customer
from {{ ref('fct_account_daily_balance') }} b
inner join account_customer ac on b.account_id = ac.account_id
group by b.date
order by b.date
