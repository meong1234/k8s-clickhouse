{#
    dedupe_latest — collapse a source relation to one row per business key, keeping
    the LATEST version of every value column by an ordering column (usually
    `ingested_at`). This is the `argMax(col, ingested_at) … GROUP BY key` idiom that
    already appears inline in the P2 models (metrics_finance_daily, the balance model),
    lifted into one reusable, readable place.

    Two shapes of source it handles:
      * ReplacingMergeTree tables (raw_customers/raw_accounts/raw_cards) — the engine
        dedups on merge, but reading WITHOUT `FINAL` can still surface un-merged dupes,
        so we dedup deterministically at read time.
      * plain MergeTree with intentional duplicates (raw_card_authorizations) — the
        ~3% re-inserted rows (same auth_id, later ingested_at) collapse to the latest.

    Emits a full SELECT (not a CTE), so wrap it in a CTE if you need to add derived
    columns downstream:

        with deduped as (
            {{ dedupe_latest(source('nimbus_raw','raw_customers'),
                             key='customer_id',
                             order_col='ingested_at',
                             value_columns=['signup_ts','email', ...]) }}
        )
        select *, toDate(signup_ts) as signup_date from deduped

    Package-free by design: `value_columns` is passed explicitly rather than introspected
    via dbt_utils, keeping the demo dependency-light.

    Args:
      relation      — a Relation (source()/ref()) or table name.
      key           — business key column (string) or list of columns for a composite key.
      order_col     — column whose max picks the surviving row (e.g. 'ingested_at').
      value_columns — list of the remaining columns to carry through via argMax.
#}
{% macro dedupe_latest(relation, key, order_col, value_columns) %}
{#- Emit `key, argMax(col, order_col) as col, …  GROUP BY key`.

    Two columns are deliberately never argMax'd and are filtered out of value_columns:
      * the key itself — it's fixed by GROUP BY;
      * the order_col — the ordering timestamp is a dedup artifact, not carried into the
        output. (Carrying it would force either argMax(order_col, order_col) — a self-
        reference — or max(order_col) alongside the other argMax(…, order_col) calls;
        ClickHouse's analyzer rejects BOTH shapes with ILLEGAL_AGGREGATION, seeing the
        shared order_col as an aggregate nested inside another aggregate.)

    Callers may safely pass the key and/or order_col in value_columns; they're dropped
    here. Downstream models don't need ingested_at once a relation is deduped. -#}
{%- set key_cols = key if key is not string else [key] -%}
{%- set exclude = key_cols + [order_col] -%}
{%- set carried = value_columns | reject('in', exclude) | list -%}
select
    {%- for k in key_cols %}
    {{ k }},
    {%- endfor %}
    {%- for col in carried %}
    argMax({{ col }}, {{ order_col }}) as {{ col }}{{ "," if not loop.last }}
    {%- endfor %}
from {{ relation }}
group by {{ key_cols | join(', ') }}
{%- endmacro %}
