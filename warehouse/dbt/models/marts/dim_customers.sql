-- dim_customers — conformed SCD2 customer dimension.
--
-- A thin projection of int_customers_scd2 (which already folds the KYC change log into
-- contiguous validity intervals and joins region) plus the surrogate key. ALL versions
-- are kept — never filter `where is_current` — because facts resolve the version in
-- effect at event time via an ASOF join over the full interval history.
--
-- Surrogate key: customer_key = cityHash64(customer_id, valid_from). The key is COMPUTED
-- here and only here; facts PULL it from the ASOF-matched row and never recompute it (a
-- recompute with a differently-typed valid_from would silently mint a non-matching key).
-- cityHash64 is a pure, deterministic function of the argument bytes — identical on both
-- replicas. valid_from is a DateTime, so the fact-side ASOF must also compare a DateTime.
--
-- Unknown member: a synthetic customer_key = 0 row. With join_use_nulls = 0 (profile
-- default), an ASOF LEFT no-match yields key 0 rather than NULL; this row gives that 0 a
-- home in the dimension so fact->dim relationships tests stay green for any edge event
-- that predates its customer's first interval. (The generator emits every event at/after
-- signup, so genuine orphans should not occur — this is honest-star-schema insurance.)
--
-- SCD2 stability caveat: customer_key is stable only while (customer_id, valid_from) is
-- stable. The dim is a full rebuild each run while facts are incremental, so a backdated
-- KYC event that shifted an existing interval's valid_from would remint that version's key
-- and dangle out-of-window fact rows. Safe here because raw_kyc_events is append-only; the
-- remedy if that ever changes is a periodic `dbt run --full-refresh -s fct_*`.
--
-- Materialized as a table (marts default), ReplicatedMergeTree, ordered by
-- (customer_id, valid_from) — the natural probe order for the ASOF joins downstream.

{{ config(order_by=['customer_id', 'valid_from']) }}

{% set far_future = "toDateTime('2106-01-01 00:00:00')" %}

select
    cityHash64(customer_id, valid_from) as customer_key,
    customer_id,
    valid_from,
    valid_to,
    is_current,
    kyc_status,
    risk_tier,
    country,
    region
from {{ ref('int_customers_scd2') }}

union all

-- Unknown member (key 0). Sentinel attributes; excluded from is_current.
select
    toUInt64(0)                          as customer_key,
    'UNKNOWN'                            as customer_id,
    toDateTime('1970-01-01 00:00:00')    as valid_from,
    {{ far_future }}                     as valid_to,
    toUInt8(0)                           as is_current,
    'unknown'                            as kyc_status,
    'unknown'                            as risk_tier,
    'unknown'                            as country,
    'unknown'                            as region
