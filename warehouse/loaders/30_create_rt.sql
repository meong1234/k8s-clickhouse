-- P6 real-time DDL — the two streaming-plane databases (lambda "speed layer").
--
-- Run as `admin` (DDL bootstrap, same as 00_create_raw.sql) via `make wh-rt-init`,
-- one statement at a time (clickhouse-client --multiquery). Idempotent: every object is
-- IF NOT EXISTS, so re-running is a no-op.
--
-- These two databases hold ONLY objects that dbt deploys as the control plane
-- (materialized views, their AggregatingMergeTree / ReplacingMergeTree target tables,
-- and dictionaries) — no admin-created tables live here. Creating the databases as an
-- admin bootstrap (not a dbt model) keeps the same split as the batch medallion, where
-- 00_create_raw.sql owns the database DDL and dbt owns the objects inside.
--
--   nimbus_stream — real-time SILVER: deduped/typed continuous tables
--                   (ReplicatedReplacingMergeTree / ReplicatedMergeTree) + a Null
--                   fan-in node. Fed by MVs firing on bronze inserts.
--   nimbus_rt     — real-time GOLD: continuous rollups holding partial aggregate
--                   states (ReplicatedAggregatingMergeTree), read with -Merge at
--                   query time; plus the seed-backed dictionaries.
--
-- Both are created ON CLUSTER so they exist on both replicas, and both use the
-- `Replicated` database engine (branch cas-local-s3, docs/cas-local-s3-plan.md §5
-- "Replicated databases"): the schema then lives in Keeper, so the MV cascade survives
-- a replica that loses its data PVC without a hand-written DDL replay. The objects dbt
-- creates inside them carry NO `ON CLUSTER` — a Replicated database replicates its own
-- DDL and rejects the clause (`Code: 80`); the profile's `database_engine` key is what
-- makes dbt drop it. The single-fire-on-INSERT replication keystone (see
-- realtime-warehouse-architecture.md §2) makes the Replicated cascade correct on this
-- 1-shard x 2-replica topology.

CREATE DATABASE IF NOT EXISTS nimbus_stream ON CLUSTER '{cluster}'
    ENGINE = Replicated('/clickhouse/databases/{shard}/nimbus_stream', '{shard}', '{replica}');
CREATE DATABASE IF NOT EXISTS nimbus_rt     ON CLUSTER '{cluster}'
    ENGINE = Replicated('/clickhouse/databases/{shard}/nimbus_rt', '{shard}', '{replica}');
