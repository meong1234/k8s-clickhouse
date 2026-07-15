-- Staging: ledger postings — typed/renamed 1:1 over the bronze source, plus the
-- two derived columns the whole finance thread depends on:
--
--   signed_amount_minor — the amount from the ACCOUNT's perspective. Bronze stores
--     amount_minor as a positive magnitude with the sign carried by `direction`;
--     here we fold it in: credit = money in (+), debit = money out (-). Every
--     downstream balance/deposit/withdrawal calculation reads this, never the raw
--     magnitude, so the sign convention is defined in exactly one place.
--   posting_date — the day bucket (Date) for the daily-grain snapshot + spine.
--
-- Materialized as a view (staging default): cheap, no storage, always fresh.
select
    posting_id,
    transaction_id,
    account_id,
    counterparty_account_id,
    posting_ts,
    toDate(posting_ts)                                       as posting_date,
    direction,
    amount_minor,
    if(direction = 'credit', amount_minor, -amount_minor)   as signed_amount_minor,
    currency,
    category_code,
    mcc,
    description,
    idempotency_key,
    ingested_at
from {{ source('nimbus_raw', 'raw_ledger_postings') }}
