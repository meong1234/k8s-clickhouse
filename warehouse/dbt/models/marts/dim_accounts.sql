-- dim_accounts — conformed SCD2 account dimension.
--
-- Thin projection of int_accounts_scd2 (account status folded into contiguous validity
-- intervals) plus the surrogate key. ALL versions kept — never `where is_current` — since
-- facts resolve the version in effect at event time via ASOF over the full history.
--
-- Surrogate key: account_key = cityHash64(account_id, valid_from), computed here only;
-- facts PULL it from the ASOF-matched row (see dim_customers header for the full rationale
-- on why facts must never recompute the key, and the append-only-history stability caveat).
--
-- Unknown member: account_key = 0 row so an ASOF LEFT no-match (key 0 under
-- join_use_nulls = 0) stays referentially valid against fct->dim relationships tests.
--
-- Materialized as a table (marts default), ReplicatedMergeTree, ordered by
-- (account_id, valid_from) — the probe order for the downstream ASOF joins.

{{ config(order_by=['account_id', 'valid_from']) }}

{% set far_future = "toDateTime('2106-01-01 00:00:00')" %}

select
    cityHash64(account_id, valid_from) as account_key,
    account_id,
    customer_id,
    valid_from,
    valid_to,
    is_current,
    status,
    account_type,
    interest_rate_bps
from {{ ref('int_accounts_scd2') }}

union all

-- Unknown member (key 0). Sentinel attributes; excluded from is_current.
select
    toUInt64(0)                          as account_key,
    'UNKNOWN'                            as account_id,
    'UNKNOWN'                            as customer_id,
    toDateTime('1970-01-01 00:00:00')    as valid_from,
    {{ far_future }}                     as valid_to,
    toUInt8(0)                           as is_current,
    'unknown'                            as status,
    'unknown'                            as account_type,
    toUInt16(0)                          as interest_rate_bps
