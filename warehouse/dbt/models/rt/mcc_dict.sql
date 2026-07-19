-- mcc_dict — the MCC → interchange-rate dictionary (real-time enrichment).
--
-- P6-B. An incremental materialized view can only enrich an inserted block via a
-- DICTIONARY, never a JOIN (an MV fires off its left-most FROM table only; a joined
-- dimension that changes is invisible to it). So the seed_mcc_codes seed becomes a
-- ClickHouse dictionary that the interchange MV reads with dictGet(...) at insert time.
--
-- Deployed by the control plane (`dbt run --select tag:rt`) as
-- `CREATE OR REPLACE DICTIONARY nimbus_rt.mcc_dict ON CLUSTER ...` (exists on both pods).
-- LIFETIME(MIN 0 MAX 0) = never auto-refresh: the MCC table is static reference data, so
-- the dict is only reloaded when the control plane re-deploys it. HASHED layout: tiny
-- key space (~10 MCCs), in-memory hash lookup.
--
-- Read pattern (used by rt_interchange_daily):
--   dictGetUInt16('nimbus_rt.mcc_dict', 'interchange_rate_bps', toUInt64(mcc))
--   dictGetString('nimbus_rt.mcc_dict', 'merchant_category',   toUInt64(mcc))
{{ config(
    materialized='dictionary',
    fields=[
      ('mcc', 'UInt16'),
      ('merchant_category', 'String'),
      ('interchange_rate_bps', 'UInt16')
    ],
    primary_key='mcc',
    layout='HASHED()',
    lifetime='MIN 0 MAX 0'
) }}

select
    mcc,
    merchant_category,
    interchange_rate_bps
from {{ ref('seed_mcc_codes') }}
