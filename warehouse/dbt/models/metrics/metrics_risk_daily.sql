-- metrics_risk_daily — the curated daily risk metric mart (P6-E).
--
-- Batch-truth risk for the lambda: computed from the DEDUPED stg_card_auths (one row per
-- auth_id), so it is the CORRECTED number the streaming rt_risk_daily (fast, over bronze
-- incl. the ~3% dupes) reconciles against on closed days (P6-F). Grain: day.
--
--   auth_count     — deduped auth attempts that day.
--   approved_count — auths with approved = 1.
--   decline_count  — auths with approved = 0.
--   fraud_count    — auths flagged is_fraud = 1.
--   decline_rate   — decline_count / auth_count (4 dp).
--   fraud_rate     — fraud_count / auth_count (4 dp).
select
    auth_date                                           as date,
    count()                                             as auth_count,
    countIf(approved = 1)                               as approved_count,
    countIf(approved = 0)                               as decline_count,
    countIf(is_fraud = 1)                               as fraud_count,
    round(countIf(approved = 0) / count(), 4)           as decline_rate,
    round(countIf(is_fraud = 1) / count(), 4)           as fraud_rate
from {{ ref('stg_card_auths') }}
group by auth_date
order by date
