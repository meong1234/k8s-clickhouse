-- SCD2 key uniqueness for accounts: at most one interval per (account_id, valid_from).
-- Fails if any row is returned. (Full integrity suite lands in P7.)
select
    account_id,
    valid_from,
    count() as n
from {{ ref('int_accounts_scd2') }}
group by account_id, valid_from
having n > 1
