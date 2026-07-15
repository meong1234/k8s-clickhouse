-- Tier-3 loader: bulk-synthesize card authorization attempts in ClickHouse — the
-- BASE auth stream. The intentional ~3% duplicates are a separate step
-- (21_gen_card_auth_dupes.sql), run once after all base batches.
--
-- Referentially valid: every auth points at a real ACTIVE card (and its account)
-- from raw_cards, picked by an ARRAY lookup (not a JOIN — rand() in a JOIN ... ON is
-- constant-folded and would collapse every auth onto one card). The pick index is
-- materialized in an inner subquery so card_id and account_id agree per row.
--
-- Distribution matches the design: ~92% approved, decline reasons on the rest,
-- ~0.3% is_fraud, MCCs drawn from the set that exists in seed_mcc_codes.
--
-- One BATCH of `__COUNT__` rows from offset `__OFFSET__` (substituted + looped by the
-- wh-generate make target). `number` is globally unique across batches, so auth_id /
-- idempotency_key stay unique (before the deliberate dupe step). Run with --multiquery.
INSERT INTO nimbus_raw.raw_card_authorizations
WITH
    (SELECT groupArray(card_id)    FROM (SELECT card_id, account_id FROM nimbus_raw.raw_cards WHERE status = 'active' ORDER BY card_id)) AS card_ids,
    (SELECT groupArray(account_id) FROM (SELECT card_id, account_id FROM nimbus_raw.raw_cards WHERE status = 'active' ORDER BY card_id)) AS acct_ids,
    length(card_ids) AS n_cards,
    toDateTime('2025-01-01 00:00:00') AS window_start,
    toUInt64(dateDiff('second', toDateTime('2025-01-01 00:00:00'), toDateTime('2026-06-30 23:59:59'))) AS window_secs,
    -- MCCs and matching merchant names (index-aligned) — all present in seed_mcc_codes.
    [5411, 5812, 5541, 5732, 5999, 4111, 5921, 7011, 4899, 5691] AS mccs,
    ['Whole Foods', 'Chipotle', 'Shell', 'Best Buy', 'Amazon', 'Uber',
     'Total Wine', 'Marriott', 'Netflix', 'Zara'] AS merchants,
    ['insufficient_funds', 'suspected_fraud', 'card_expired', 'limit_exceeded', 'do_not_honor'] AS decline_reasons
SELECT
    concat('AUTH-', leftPad(toString(number + 1), 12, '0')) AS auth_id,
    card_ids[p] AS card_id,
    acct_ids[p] AS account_id,
    window_start + toIntervalSecond(rand64() % window_secs) AS auth_ts,
    toInt64(100 + (rand() % 45000)) AS amount_minor,
    'USD' AS currency,
    mcc,
    merchants[indexOf(mccs, mcc)] AS merchant_name,
    approved,
    if(approved, '', decline_reasons[1 + (rand() % length(decline_reasons))]) AS decline_reason,
    (rand() % 1000 < 3) AS is_fraud,     -- ~0.3%
    concat('IDEM-', leftPad(toString(number + 1), 12, '0')) AS idempotency_key,
    now() AS ingested_at
FROM
(
    -- Materialize the per-row picks so card/mcc/approved stay consistent across
    -- every reference below.
    SELECT
        number,
        1 + (rand64() % n_cards) AS p,                   -- uniform over active cards
        mccs[1 + (rand() % length(mccs))] AS mcc,
        (rand() % 100 < 92) AS approved
    FROM numbers(__OFFSET__, __COUNT__)
);
