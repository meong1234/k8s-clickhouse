-- int_interchange_revenue — interchange revenue per approved card authorization.
--
-- Reads stg_card_auths (already deduped by auth_id) filtered to approved auths, joins
-- the MCC's interchange rate, and computes revenue with INTEGER basis-point math:
--
--   interchange_revenue_minor = intDiv(amount_minor * interchange_rate_bps, 10000)
--
-- (1 bps = 0.01%, so bps/10000 is the fraction; intDiv truncates to whole minor units —
-- no floats, ever, in money math.) Computed per auth and summed downstream (round-then-
-- sum), so the P6 reconciliation invariant — daily interchange == sum of per-auth
-- interchange — holds exactly.
--
-- INNER join to seed_mcc_codes: every card-auth MCC is drawn from the seed set by the
-- loader, so every approved auth has a rate. Grain: one row per approved auth_id.
--
-- Materialized as a view (intermediate default).
select
    a.auth_id,
    a.card_id,
    a.account_id,
    a.auth_ts,
    a.auth_date,
    a.amount_minor,
    a.mcc,
    m.interchange_rate_bps,
    intDiv(a.amount_minor * m.interchange_rate_bps, 10000) as interchange_revenue_minor
from {{ ref('stg_card_auths') }} a
inner join {{ ref('seed_mcc_codes') }} m on a.mcc = m.mcc
where a.approved = 1
