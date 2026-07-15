-- Continuity invariant: on every row, closing_balance must equal
-- opening_balance + daily_net. A cheap check that catches window/seed regressions in
-- the incremental running-balance logic (e.g. a mis-carried opening seed). Fails if any
-- row breaks the identity.
select
    account_id,
    day,
    opening_balance,
    daily_net,
    closing_balance
from {{ ref('int_account_daily_balance') }}
where closing_balance != opening_balance + daily_net
