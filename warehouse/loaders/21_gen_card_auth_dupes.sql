-- Tier-3 loader, step 2: the DELIBERATE duplicates for the silver dedup demo (§1, §5).
--
-- Re-inserts a stable ~3% of the base auth stream (same auth_id / idempotency_key) but
-- with a LATER ingested_at — a retried/redelivered event. Silver's stg_card_auths
-- (P4) collapses these back with argMax(<cols>, ingested_at), keeping the latest.
--
-- The subset is chosen by cityHash64(auth_id) % 100 < 3 (STABLE — not rand()), so it
-- is idempotent-ish and doesn't depend on generation order. Run once, after all base
-- batches, with --multiquery as `admin`. Small (~3% of rows), so it needs no batching.

INSERT INTO nimbus_raw.raw_card_authorizations
SELECT
    auth_id, card_id, account_id, auth_ts, amount_minor, currency, mcc, merchant_name,
    approved, decline_reason, is_fraud, idempotency_key,
    ingested_at + toIntervalHour(1 + (rand() % 48)) AS ingested_at
FROM nimbus_raw.raw_card_authorizations
WHERE cityHash64(auth_id) % 100 < 3;
