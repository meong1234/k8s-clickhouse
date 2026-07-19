-- auth_fanin — the Null-table fan-in node for the hot card-auth stream (P6-C, §6).
--
-- A ClickHouse `Null` table stores nothing; its only job is to be an insert target that
-- fans ONE bronze card-auth insert out to SEVERAL independent rollup/silver MVs in
-- parallel (rt_interchange_daily, rt_risk_daily, slv_card_auths) — instead of chaining
-- them into a deep synchronous cascade (each chained MV is synchronous in the insert
-- path; throughput drops ~55% at 1 chained MV, ~90% at 10). With the fan-in, cascade
-- depth stays at 2: bronze -> auth_fanin -> {the auth rollups}, all downstream MVs firing
-- off the single fan-in insert, and the raw block is never re-stored.
--
-- Standard-mode `materialized_view` with engine=Null: dbt creates the Null table
-- `auth_fanin` AND the view `auth_fanin_mv` (bronze -> auth_fanin), both ON CLUSTER.
-- catchup=false — a Null table holds no history; the historical backfill is a controlled
-- replay through this table (`make wh-rt-backfill`), which fires every downstream MV once
-- over the ~1M existing bronze auths.
--
-- Columns mirror bronze raw_card_authorizations so each downstream MV can select what it
-- needs (interchange: mcc/amount, risk: approved/decline/fraud, silver: everything).
{{ config(
    materialized='materialized_view',
    engine='Null',
    catchup=false
) }}

select
    auth_id,
    card_id,
    account_id,
    auth_ts,
    amount_minor,
    currency,
    mcc,
    merchant_name,
    approved,
    decline_reason,
    is_fraud,
    idempotency_key,
    ingested_at
from {{ source('nimbus_raw', 'raw_card_authorizations') }}
