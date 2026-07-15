{#
    Override dbt's default schema naming.

    Default dbt behavior concatenates target.schema + '_' + custom_schema_name
    (e.g. nimbus_raw_marts). We instead map each layer's `+schema` config directly
    to its own ClickHouse database with a `nimbus_` prefix:

        +schema: marts   ->  nimbus_marts
        +schema: staging ->  nimbus_staging

    Models with no custom schema fall back to target.schema (nimbus_raw).
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema | trim }}
    {%- else -%}
        nimbus_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
