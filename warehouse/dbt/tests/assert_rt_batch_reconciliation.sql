-- The lambda correctness contract (P6-F, §7): on every CLOSED day (day < today, so no
-- in-flight inserts skew a moving target), the CORRECTED real-time stream must EXACTLY
-- equal the batch-truth metric. This is the binding that makes the speed layer trustworthy
-- — the fast T0 rollups are approximate (they double-count the ~3% at-least-once dupes),
-- but the deduped stream must agree with the batch spine to the cent.
--
-- Fails (returns rows) if any closed day disagrees on: interchange revenue, the deduped
-- approved/decline/fraud counts, or DAU. The CORRECTED stream reads slv_card_auths FINAL
-- (dedup resolved) with the same integer-bps math as batch; DAU reads the rt_dau_daily
-- HLL states (app events have no dupes, so fast == corrected there). The fast-path dup
-- drift is expected and is NOT asserted here — it is surfaced by assert_rt_fast_drift.
--
-- The streaming relations are referenced by LITERAL name (nimbus_stream.* / nimbus_rt.*),
-- not ref(): this is a batch-spine test that reconciles against the SEPARATELY-deployed
-- streaming plane, so it must NOT create a build-DAG dependency on tag:rt models (that
-- would make `dbt build --exclude tag:rt` drop the test). Only the batch metrics are ref()d.
{{ config(severity='error') }}

with
stream_auths as (
    select
        toDate(auth_ts)                     as day,
        countIf(approved = 1)               as approved_count,
        countIf(approved = 0)               as decline_count,
        countIf(is_fraud = 1)               as fraud_count,
        sumIf(
            intDiv(amount_minor * dictGetUInt16('nimbus_rt.mcc_dict', 'interchange_rate_bps', toUInt64(mcc)), 10000),
            approved = 1
        )                                   as interchange_minor
    from nimbus_stream.slv_card_auths final
    where toDate(auth_ts) < today()
    group by day
),

stream_dau as (
    select day, toInt64(uniqMerge(dau)) as dau
    from nimbus_rt.rt_dau_daily
    where day < today()
    group by day
),

batch_fin as (
    select date as day, toInt64(interchange_revenue_minor) as interchange_minor
    from {{ ref('metrics_finance_daily') }} where date < today()
),
batch_risk as (
    select date as day, toInt64(approved_count) approved_count, toInt64(decline_count) decline_count, toInt64(fraud_count) fraud_count
    from {{ ref('metrics_risk_daily') }} where date < today()
),
batch_growth as (
    select date as day, toInt64(dau) as dau from {{ ref('metrics_growth_daily') }} where date < today()
),

mismatches as (
    select 'interchange_minor' as metric, s.day as day, toInt64(s.interchange_minor) as stream_value, b.interchange_minor as batch_value
    from stream_auths s inner join batch_fin b using (day) where toInt64(s.interchange_minor) != b.interchange_minor

    union all
    select 'approved_count', s.day, toInt64(s.approved_count), b.approved_count
    from stream_auths s inner join batch_risk b using (day) where toInt64(s.approved_count) != b.approved_count

    union all
    select 'decline_count', s.day, toInt64(s.decline_count), b.decline_count
    from stream_auths s inner join batch_risk b using (day) where toInt64(s.decline_count) != b.decline_count

    union all
    select 'fraud_count', s.day, toInt64(s.fraud_count), b.fraud_count
    from stream_auths s inner join batch_risk b using (day) where toInt64(s.fraud_count) != b.fraud_count

    union all
    select 'dau', s.day, s.dau, b.dau
    from stream_dau s inner join batch_growth b using (day) where s.dau != b.dau
)

select * from mismatches
