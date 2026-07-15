-- Grain guard: (account_id, day) must be unique in the daily-balance snapshot. A
-- duplicate would mean the spine or the incremental delete_insert double-counted a day.
-- Kept as a singular test (generic `unique` takes a single column, and we stay
-- package-free — no dbt_utils surrogate_key). Fails if any pair repeats.
select
    account_id,
    day
from {{ ref('int_account_daily_balance') }}
group by account_id, day
having count() > 1
