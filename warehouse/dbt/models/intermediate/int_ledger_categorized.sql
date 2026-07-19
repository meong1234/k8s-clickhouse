-- int_ledger_categorized — ledger postings enriched with their revenue classification.
--
-- Joins stg_ledger_postings to the two static dimension seeds:
--   * seed_transaction_categories (on category_code) → category label + is_revenue flag
--     + revenue_type (interchange | fee | interest | none).
--   * seed_mcc_codes (on mcc, present only on card-settlement legs) → merchant_category
--     + interchange_rate_bps.
-- LEFT joins so no posting is dropped when a code is unmapped; unmapped rows carry
-- is_revenue = 0 / revenue_type = 'none'. Grain unchanged: one row per posting.
--
-- Materialized as a view (intermediate default).
select
    p.posting_id,
    p.transaction_id,
    p.account_id,
    p.counterparty_account_id,
    p.posting_ts,
    p.posting_date,
    p.direction,
    p.amount_minor,
    p.signed_amount_minor,
    p.currency,
    p.category_code                 as category_code,   -- alias: also in seed_transaction_categories
    c.category,
    coalesce(c.is_revenue, false)   as is_revenue,
    coalesce(c.revenue_type, 'none') as revenue_type,
    p.mcc                           as mcc,              -- alias: also in seed_mcc_codes
    m.merchant_category,
    m.interchange_rate_bps,
    p.description,
    p.idempotency_key,
    p.ingested_at
from {{ ref('stg_ledger_postings') }} p
left join {{ ref('seed_transaction_categories') }} c on p.category_code = c.category_code
left join {{ ref('seed_mcc_codes') }} m on p.mcc = m.mcc
