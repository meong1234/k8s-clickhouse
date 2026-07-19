-- slv_ledger_postings — real-time silver ledger stream (P6-C).
--
-- Streaming analog of batch stg_ledger_postings + int_ledger_categorized, folded into one
-- insert-time transform: typed/renamed 1:1 over bronze, the signed amount derived once
-- (credit = +, debit = -), and the revenue treatment enriched via the category_dict
-- DICTIONARY (dictGet at insert time — not a join, so the MV fires cleanly off its single
-- source). A single MV bronze raw_ledger_postings -> slv_ledger_postings; no fan-in
-- needed (ledger has one consumer, unlike the hot auth stream).
--
-- catchup=true: the target table is created via CREATE TABLE ... AS SELECT, backfilling
-- the ~194k historical postings from bronze at deploy time. Engine ReplicatedMergeTree
-- (append-only; ledger postings are immutable, no dedup).
--
-- category_dict has a String (complex) key, so dictGet takes tuple(category_code).
-- depends_on orders the deploy since dictGet references the dict by string literal.
--
-- depends_on: {{ ref('category_dict') }}
{{ config(
    materialized='materialized_view',
    engine='ReplicatedMergeTree',
    order_by='(account_id, posting_ts)',
    catchup=true
) }}

select
    posting_id,
    transaction_id,
    account_id,
    counterparty_account_id,
    posting_ts,
    toDate(posting_ts)                                     as posting_date,
    direction,
    amount_minor,
    if(direction = 'credit', amount_minor, -amount_minor)  as signed_amount_minor,
    currency,
    category_code,
    dictGetUInt8('nimbus_rt.category_dict', 'is_revenue', tuple(category_code))     as is_revenue,
    dictGetString('nimbus_rt.category_dict', 'revenue_type', tuple(category_code))  as revenue_type,
    mcc,
    description,
    idempotency_key,
    ingested_at
from {{ source('nimbus_raw', 'raw_ledger_postings') }}
