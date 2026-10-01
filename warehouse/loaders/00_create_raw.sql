-- Bronze DDL — the 8 Nimbus source ("landed") tables + the medallion databases.
--
-- Run as `admin` (DDL bootstrap) via `make wh-bronze`, one statement at a time
-- (clickhouse-client --multiquery). Idempotent: every object is IF NOT EXISTS, so
-- re-running is a no-op. Data is loaded separately (make wh-generate).
--
-- Replication, two levels (branch cas-local-s3, docs/cas-local-s3-plan.md §5
-- "Replicated databases"):
--
--   1. the DATABASES use the `Replicated` engine, so the schema itself lives in Keeper
--      and a replica that comes back with an empty data PVC replays every CREATE from
--      the database's own DDL log instead of needing a hand-written replay. The CREATE
--      DATABASE statements below are the only DDL a rebuilt replica still needs, and
--      they are what `make wh-bronze` re-applies.
--   2. the TABLES use a Replicated* engine with NO engine arguments. Inside a
--      Replicated database explicit arguments are rejected outright
--      (`Code: 36 … not allowed to specify explicit zookeeper_path and replica_name …
--      in Replicated database`); the engine derives /clickhouse/tables/{uuid}/{shard}
--      from the table UUID, which the database's DDL log carries, so both replicas —
--      and any later rebuild — agree on the path without a static name.
--
-- For the same reason there is no `ON CLUSTER` on any statement INSIDE these databases
-- (`Code: 80 … ON CLUSTER is not allowed for Replicated database`): the database
-- replicates its own DDL. `ON CLUSTER` stays on CREATE DATABASE, which is the one
-- statement that must reach every node directly.
--
-- Amounts are always integer minor units (cents) — never floats (fintech practice).
-- `ingested_at` is on every table to drive dedup + incremental loads downstream.
--
-- Storage (branch cas-local-s3, docs/cas-local-s3-plan.md §1.4): bronze is the bulk of
-- the bytes and is append-only, so it runs on the `cas_tiered` policy — inserts land on
-- the local `default` volume (CAS's insert path is not optimised yet) and a table TTL
-- moves partitions older than 90 days down to the CAS volume, where both replicas share
-- one copy of the blobs. The TTL column is the table's own EVENT time where it has one
-- (that is the age the tier is about); the three Replacing snapshot tables have no event
-- time, so they age by `ingested_at`, which is also their version column.
-- `min_bytes_for_wide_part` / `min_level_for_wide_part` are raised per the CAS guidance:
-- low-level parts stay compact, so a partition becomes a handful of objects, not
-- thousands. These settings only apply at CREATE — an existing table must be dropped
-- (not truncated) to pick them up.

-- Medallion layer databases (one ClickHouse database == one dbt schema). dbt never
-- creates these: its profile declares the same engine, but dbt-clickhouse 1.10.1 drops
-- ON CLUSTER once `database_engine` is Replicated, so a dbt-created database would land
-- on one replica only. They are created here, up front, for all six dbt schemas.
CREATE DATABASE IF NOT EXISTS nimbus_raw          ON CLUSTER '{cluster}'
    ENGINE = Replicated('/clickhouse/databases/{shard}/nimbus_raw', '{shard}', '{replica}');
CREATE DATABASE IF NOT EXISTS nimbus_staging      ON CLUSTER '{cluster}'
    ENGINE = Replicated('/clickhouse/databases/{shard}/nimbus_staging', '{shard}', '{replica}');
CREATE DATABASE IF NOT EXISTS nimbus_intermediate ON CLUSTER '{cluster}'
    ENGINE = Replicated('/clickhouse/databases/{shard}/nimbus_intermediate', '{shard}', '{replica}');
CREATE DATABASE IF NOT EXISTS nimbus_marts        ON CLUSTER '{cluster}'
    ENGINE = Replicated('/clickhouse/databases/{shard}/nimbus_marts', '{shard}', '{replica}');
CREATE DATABASE IF NOT EXISTS nimbus_metrics      ON CLUSTER '{cluster}'
    ENGINE = Replicated('/clickhouse/databases/{shard}/nimbus_metrics', '{shard}', '{replica}');

-- 1. customers — one row per customer (opening snapshot). ReplacingMergeTree keeps
--    the latest row per customer_id by ingested_at.
CREATE TABLE IF NOT EXISTS nimbus_raw.raw_customers
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
ENGINE = ReplicatedReplacingMergeTree(ingested_at)
ORDER BY customer_id
TTL ingested_at + INTERVAL 90 DAY TO VOLUME 'cas'
SETTINGS storage_policy = 'cas_tiered', min_bytes_for_wide_part = 100000000, min_level_for_wide_part = 3;

-- 2. kyc_events — one row per KYC status change (append-only event log).
CREATE TABLE IF NOT EXISTS nimbus_raw.raw_kyc_events
(
    kyc_event_id String,
    customer_id  String,
    event_ts     DateTime,
    old_status   LowCardinality(String),
    new_status   LowCardinality(String),  -- submitted | pending | verified | rejected
    reason       String,
    ingested_at  DateTime
)
ENGINE = ReplicatedMergeTree
ORDER BY (customer_id, event_ts)
TTL event_ts + INTERVAL 90 DAY TO VOLUME 'cas'
SETTINGS storage_policy = 'cas_tiered', min_bytes_for_wide_part = 100000000, min_level_for_wide_part = 3;

-- 3. accounts — one row per account (opening snapshot).
CREATE TABLE IF NOT EXISTS nimbus_raw.raw_accounts
(
    account_id        String,
    customer_id       String,
    account_type      LowCardinality(String),  -- checking | savings
    opened_ts         DateTime,
    interest_rate_bps UInt16,
    ingested_at       DateTime
)
ENGINE = ReplicatedReplacingMergeTree(ingested_at)
ORDER BY account_id
TTL ingested_at + INTERVAL 90 DAY TO VOLUME 'cas'
SETTINGS storage_policy = 'cas_tiered', min_bytes_for_wide_part = 100000000, min_level_for_wide_part = 3;

-- 4. account_events — one row per account state change (active/frozen/closed).
CREATE TABLE IF NOT EXISTS nimbus_raw.raw_account_events
(
    account_event_id String,
    account_id       String,
    event_ts         DateTime,
    old_status       LowCardinality(String),
    new_status       LowCardinality(String),  -- active | frozen | closed
    ingested_at      DateTime
)
ENGINE = ReplicatedMergeTree
ORDER BY (account_id, event_ts)
TTL event_ts + INTERVAL 90 DAY TO VOLUME 'cas'
SETTINGS storage_policy = 'cas_tiered', min_bytes_for_wide_part = 100000000, min_level_for_wide_part = 3;

-- 5. cards — one row per card.
CREATE TABLE IF NOT EXISTS nimbus_raw.raw_cards
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
ENGINE = ReplicatedReplacingMergeTree(ingested_at)
ORDER BY card_id
TTL ingested_at + INTERVAL 90 DAY TO VOLUME 'cas'
SETTINGS storage_policy = 'cas_tiered', min_bytes_for_wide_part = 100000000, min_level_for_wide_part = 3;

-- 6. ledger_postings — one double-entry leg. Postings for a transaction_id sum to
--    zero (customer leg + Nimbus internal/settlement leg). Partitioned by month.
CREATE TABLE IF NOT EXISTS nimbus_raw.raw_ledger_postings
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
ENGINE = ReplicatedMergeTree
PARTITION BY toYYYYMM(posting_ts)
ORDER BY (account_id, posting_ts)
TTL posting_ts + INTERVAL 90 DAY TO VOLUME 'cas'
SETTINGS storage_policy = 'cas_tiered', min_bytes_for_wide_part = 100000000, min_level_for_wide_part = 3;

-- 7. card_authorizations — one auth attempt. INTENTIONALLY duplicated (~3% of rows
--    re-inserted with a later ingested_at) to drive the silver dedup demo.
CREATE TABLE IF NOT EXISTS nimbus_raw.raw_card_authorizations
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
ENGINE = ReplicatedMergeTree
PARTITION BY toYYYYMM(auth_ts)
ORDER BY (card_id, auth_ts)
TTL auth_ts + INTERVAL 90 DAY TO VOLUME 'cas'
SETTINGS storage_policy = 'cas_tiered', min_bytes_for_wide_part = 100000000, min_level_for_wide_part = 3;

-- 8. app_events — one mobile-app event. High volume, low consistency.
CREATE TABLE IF NOT EXISTS nimbus_raw.raw_app_events
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
ENGINE = ReplicatedMergeTree
PARTITION BY toYYYYMM(event_ts)
ORDER BY (customer_id, event_ts)
TTL event_ts + INTERVAL 90 DAY TO VOLUME 'cas'
SETTINGS storage_policy = 'cas_tiered', min_bytes_for_wide_part = 100000000, min_level_for_wide_part = 3;
