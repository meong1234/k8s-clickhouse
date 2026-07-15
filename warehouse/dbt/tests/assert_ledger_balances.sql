-- Fintech invariant #1 (the seed of the P7 invariant suite): the ledger balances.
-- Every double-entry transaction's signed legs must sum to exactly zero. Any
-- transaction_id with a non-zero net is a broken entry — the test fails if any row
-- is returned.
select
    transaction_id,
    sum(signed_amount_minor) as net
from {{ ref('stg_ledger_postings') }}
group by transaction_id
having net != 0
