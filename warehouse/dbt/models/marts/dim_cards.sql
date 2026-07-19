-- dim_cards — conformed type-1 card dimension (no history; one row per card).
--
-- Cards have no change-event log in the source, so this is a plain type-1 dimension over
-- the deduped stg_cards view — the current attributes overwrite in place. Because there is
-- no validity interval, the surrogate is keyed on the natural id alone:
-- card_key = cityHash64(card_id). Facts join dim_cards on card_id (a plain equi-join, not
-- ASOF) and PULL card_key from it, so the key is defined here only.
--
-- Unknown member: card_key = 0 row so a fact left-join miss stays relationships-valid.
--
-- Materialized as a table (marts default), ReplicatedMergeTree, ordered by card_id.

{{ config(order_by=['card_id']) }}

select
    cityHash64(card_id) as card_key,
    card_id,
    account_id,
    customer_id,
    issued_ts,
    network,    -- visa | mastercard
    status,     -- active | blocked | expired
    last4
from {{ ref('stg_cards') }}

union all

-- Unknown member (key 0). Sentinel attributes.
select
    toUInt64(0)                          as card_key,
    'UNKNOWN'                            as card_id,
    'UNKNOWN'                            as account_id,
    'UNKNOWN'                            as customer_id,
    toDateTime('1970-01-01 00:00:00')    as issued_ts,
    'unknown'                            as network,
    'unknown'                            as status,
    toFixedString('0000', 4)             as last4
