-- Staging: cards — one row per card, deduped and typed/renamed 1:1.
--
-- raw_cards is a ReplacingMergeTree(ingested_at); we collapse to the latest row per
-- card_id via dedupe_latest(). last4 is a FixedString(4) in bronze; kept as-is.
--
-- Materialized as a view (staging default).
with deduped as (
    {{ dedupe_latest(
        source('nimbus_raw', 'raw_cards'),
        key='card_id',
        order_col='ingested_at',
        value_columns=['account_id', 'customer_id', 'issued_ts',
                       'network', 'status', 'last4']
    ) }}
)

select
    card_id,
    account_id,
    customer_id,
    issued_ts,
    network,    -- visa | mastercard
    status,     -- active | blocked | expired
    last4
from deduped
