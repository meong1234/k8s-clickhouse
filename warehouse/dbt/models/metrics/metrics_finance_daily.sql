-- metrics_finance_daily — the curated daily finance metric mart (P6-E, full).
--
-- The batch-truth (source-of-truth) finance metric for the lambda: the real-time
-- rt_interchange_daily / rt_finance_daily rollups reconcile against these numbers on
-- closed days (P6-F). Grain: day. The metric DEFINITIONS are the contract (see the .yml).
--
--   total_deposits            — total credits across all accounts that day (minor units).
--   avg_balance_per_customer  — total closing balance / distinct account-holding customers.
--   interchange_revenue_minor — interchange on approved auths that day, from the DEDUPED
--                               int_interchange_revenue (round-then-sum integer bps math).
--   fee_revenue_minor         — ledger postings classified revenue_type='fee' that day.
--   interest_revenue_minor    — ledger postings classified revenue_type='interest'.
--   total_revenue_minor       — interchange + fee + interest.
--   arpu_minor                — total_revenue / distinct active-account customers that day.
--
-- Revenue is attributed by its own event date (interchange by auth_date, fee/interest by
-- posting_date), left-joined onto the daily balance spine so every active day is present.
with account_customer as (
    select account_id, customer_id
    from {{ ref('stg_accounts') }}
),

balances as (
    select
        b.date                                                       as date,
        sum(b.total_deposits)                                        as total_deposits,
        intDiv(sum(b.closing_balance), uniqExact(ac.customer_id))    as avg_balance_per_customer,
        uniqExact(ac.customer_id)                                    as active_customers
    from {{ ref('fct_account_daily_balance') }} b
    inner join account_customer ac on b.account_id = ac.account_id
    group by b.date
),

interchange as (
    select
        auth_date                       as date,
        sum(interchange_revenue_minor)  as interchange_revenue_minor
    from {{ ref('int_interchange_revenue') }}
    group by auth_date
),

fees_interest as (
    select
        posting_date                                            as date,
        sumIf(amount_minor, revenue_type = 'fee')               as fee_revenue_minor,
        sumIf(amount_minor, revenue_type = 'interest')          as interest_revenue_minor
    from {{ ref('int_ledger_categorized') }}
    group by posting_date
)

select
    b.date                                          as date,
    b.total_deposits                                as total_deposits,
    b.avg_balance_per_customer                      as avg_balance_per_customer,
    ifNull(i.interchange_revenue_minor, 0)          as interchange_revenue_minor,
    ifNull(fi.fee_revenue_minor, 0)                 as fee_revenue_minor,
    ifNull(fi.interest_revenue_minor, 0)            as interest_revenue_minor,
    ifNull(i.interchange_revenue_minor, 0)
      + ifNull(fi.fee_revenue_minor, 0)
      + ifNull(fi.interest_revenue_minor, 0)        as total_revenue_minor,
    intDiv(
        ifNull(i.interchange_revenue_minor, 0)
          + ifNull(fi.fee_revenue_minor, 0)
          + ifNull(fi.interest_revenue_minor, 0),
        b.active_customers
    )                                               as arpu_minor
from balances b
left join interchange i     on b.date = i.date
left join fees_interest fi  on b.date = fi.date
order by date
