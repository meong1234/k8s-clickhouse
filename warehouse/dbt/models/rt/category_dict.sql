-- category_dict — the ledger-category → revenue-treatment dictionary.
--
-- P6-C companion to mcc_dict. The seed_transaction_categories seed becomes a dictionary
-- keyed by category_code so the silver ledger MV (slv_ledger_postings) can classify each
-- posting's revenue treatment with dictGet(...) at insert time instead of a join — the
-- streaming analog of int_ledger_categorized's LEFT JOIN to the seed.
--
-- Read pattern:
--   dictGetString('nimbus_rt.category_dict', 'revenue_type', category_code)
--   dictGetUInt8 ('nimbus_rt.category_dict', 'is_revenue',   category_code)
--
-- Keyed by a String category_code (COMPLEX_KEY_HASHED layout is required for non-UInt64
-- keys). LIFETIME(MIN 0 MAX 0): static reference data, reloaded only on re-deploy.
{{ config(
    materialized='dictionary',
    fields=[
      ('category_code', 'String'),
      ('category', 'String'),
      ('is_revenue', 'UInt8'),
      ('revenue_type', 'String')
    ],
    primary_key='category_code',
    layout='COMPLEX_KEY_HASHED()',
    lifetime='MIN 0 MAX 0'
) }}

select
    category_code,
    category,
    toUInt8(is_revenue) as is_revenue,
    revenue_type
from {{ ref('seed_transaction_categories') }}
