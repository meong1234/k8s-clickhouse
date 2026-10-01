-- Bronze DDL — the 8 Nimbus source ("landed") tables + the medallion databases.
--
-- Run as `admin` (DDL bootstrap) via `make wh-bronze`, one statement at a time
-- (clickhouse-client --multiquery). Idempotent: every object is IF NOT EXISTS, so
-- re-running is a no-op. Data is loaded separately (make wh-generate).
--
-- Replication: every table uses a Replicated* engine with an EXPLICIT keeper path
--   /clickhouse/tables/{shard}/nimbus_raw/<table>   (replica '{replica}')
-- — the same teaching pattern as `make ch-demo`. {shard}/{replica} are the
-- operator-provided macros; the path is static (not {uuid}) so the DDL reads
-- self-explanatory. These tables are created once and TRUNCATE'd (not dropped) on
-- reload, so static paths never collide with Atomic deferred-drops.
--
-- Amounts are always integer minor units (cents) — never floats (fintech practice).
-- `ingested_at` is on every table to drive dedup + incremental loads downstream.

-- Medallion layer databases (one ClickHouse database == one dbt schema).
CREATE DATABASE IF NOT EXISTS nimbus_raw          ON CLUSTER '{cluster}';
CREATE DATABASE IF NOT EXISTS nimbus_staging      ON CLUSTER '{cluster}';
CREATE DATABASE IF NOT EXISTS nimbus_intermediate ON CLUSTER '{cluster}';
CREATE DATABASE IF NOT EXISTS nimbus_marts        ON CLUSTER '{cluster}';
CREATE DATABASE IF NOT EXISTS nimbus_metrics      ON CLUSTER '{cluster}';

-- 1. customers — one row per customer (opening snapshot). ReplacingMergeTree keeps
--    the latest row per customer_id by ingested_at.
CREATE TABLE IF NOT EXISTS nimbus_raw.raw_customers ON CLUSTER '{cluster}'
(
    customer_id     String,
    signup_ts       DateTime,
    email           String,
    full_name       String,
    country         LowCardinality(String),
    dob             Date,
    risk_tier       LowCardinality(String),
    referral_source LowCardinality(String),
    ingested_at     DateTime
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/nimbus_raw/raw_customers', '{replica}', ingested_at)
ORDER BY customer_id;

-- 2. kyc_events — one row per KYC status change (append-only event log).
CREATE TABLE IF NOT EXISTS nimbus_raw.raw_kyc_events ON CLUSTER '{cluster}'
(
    kyc_event_id String,
    customer_id  String,
    event_ts     DateTime,
    old_status   LowCardinality(String),
    new_status   LowCardinality(String),  -- submitted | pending | verified | rejected
    reason       String,
    ingested_at  DateTime
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/nimbus_raw/raw_kyc_events', '{replica}')
ORDER BY (customer_id, event_ts);

-- 3. accounts — one row per account (opening snapshot).
CREATE TABLE IF NOT EXISTS nimbus_raw.raw_accounts ON CLUSTER '{cluster}'
(
    account_id        String,
    customer_id       String,
    account_type      LowCardinality(String),  -- checking | savings
    opened_ts         DateTime,
    interest_rate_bps UInt16,
    ingested_at       DateTime
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/nimbus_raw/raw_accounts', '{replica}', ingested_at)
ORDER BY account_id;

-- 4. account_events — one row per account state change (active/frozen/closed).
CREATE TABLE IF NOT EXISTS nimbus_raw.raw_account_events ON CLUSTER '{cluster}'
(
    account_event_id String,
    account_id       String,
    event_ts         DateTime,
    old_status       LowCardinality(String),
    new_status       LowCardinality(String),  -- active | frozen | closed
    ingested_at      DateTime
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/nimbus_raw/raw_account_events', '{replica}')
ORDER BY (account_id, event_ts);

-- 5. cards — one row per card.
CREATE TABLE IF NOT EXISTS nimbus_raw.raw_cards ON CLUSTER '{cluster}'
(
    card_id     String,
    account_id  String,
    customer_id String,
    issued_ts   DateTime,
    network     LowCardinality(String),  -- visa | mastercard
    status      LowCardinality(String),  -- active | blocked | expired
    last4       FixedString(4),
    ingested_at DateTime
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/nimbus_raw/raw_cards', '{replica}', ingested_at)
ORDER BY card_id;

-- 6. ledger_postings — one double-entry leg. Postings for a transaction_id sum to
--    zero (customer leg + Nimbus internal/settlement leg). Partitioned by month.
CREATE TABLE IF NOT EXISTS nimbus_raw.raw_ledger_postings ON CLUSTER '{cluster}'
(
    posting_id              String,
    transaction_id          String,
    account_id              String,
    posting_ts              DateTime,
    direction               LowCardinality(String),  -- debit | credit
    amount_minor            Int64,                    -- positive magnitude; sign via `direction`
    currency                LowCardinality(String),
    counterparty_account_id String,
    category_code           LowCardinality(String),
    mcc                     Nullable(UInt16),         -- present on card settlements only
    description             String,
    idempotency_key         String,
    ingested_at             DateTime
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/nimbus_raw/raw_ledger_postings', '{replica}')
PARTITION BY toYYYYMM(posting_ts)
ORDER BY (account_id, posting_ts);

-- 7. card_authorizations — one auth attempt. INTENTIONALLY duplicated (~3% of rows
--    re-inserted with a later ingested_at) to drive the silver dedup demo.
CREATE TABLE IF NOT EXISTS nimbus_raw.raw_card_authorizations ON CLUSTER '{cluster}'
(
    auth_id         String,
    card_id         String,
    account_id      String,
    auth_ts         DateTime,
    amount_minor    Int64,
    currency        LowCardinality(String),
    mcc             UInt16,
    merchant_name   String,
    approved        UInt8,
    decline_reason  LowCardinality(String),
    is_fraud        UInt8,
    idempotency_key String,
    ingested_at     DateTime
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/nimbus_raw/raw_card_authorizations', '{replica}')
PARTITION BY toYYYYMM(auth_ts)
ORDER BY (card_id, auth_ts);

-- 8. app_events — one mobile-app event. High volume, low consistency.
CREATE TABLE IF NOT EXISTS nimbus_raw.raw_app_events ON CLUSTER '{cluster}'
(
    event_id    String,
    customer_id String,
    event_ts    DateTime,
    event_name  LowCardinality(String),
    device      LowCardinality(String),
    app_version LowCardinality(String),
    session_id  String,
    ingested_at DateTime
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/nimbus_raw/raw_app_events', '{replica}')
PARTITION BY toYYYYMM(event_ts)
ORDER BY (customer_id, event_ts);
