-- dim_date — the conformed date-spine dimension (grain: one row per calendar day).
--
-- Generated from a ClickHouse numbers() range rather than sourced from data, so the
-- calendar is complete with no gaps regardless of which days actually carry facts. The
-- Date column itself is the join key — every fact carries a `date` (Date) column that
-- references dim_date.date, so this spine MUST span every fact's date range or the
-- fact->dim_date relationships test fails.
--
-- Range: 2024-01-01 .. 2027-12-31 = 1461 days (2024 is a leap year). The synthetic data
-- window is 2025-01-01 .. 2026-06-30, so this brackets it with a year of slack on each
-- side. Widen `spine_days` (and/or the start) if the data window ever grows.
--
-- Materialized as a table (marts default), ReplicatedMergeTree, ordered by date.

{{ config(order_by=['date']) }}

{% set spine_start = "toDate('2024-01-01')" %}
{% set spine_days = 1461 %}

select
    d.date                                    as date,
    toYear(d.date)                            as year,
    toQuarter(d.date)                         as quarter,
    toMonth(d.date)                           as month,
    -- Full month name (January, February, ...). dateName is stable across CH versions.
    dateName('month', d.date)                 as month_name,
    -- ISO-8601 week (mode 3): weeks start Monday, week 1 contains the first Thursday.
    toISOWeek(d.date)                          as week,
    toDayOfMonth(d.date)                       as day_of_month,
    -- 1 = Monday .. 7 = Sunday (ClickHouse toDayOfWeek default mode).
    toDayOfWeek(d.date)                        as day_of_week,
    dateName('weekday', d.date)                as day_name,
    toDayOfWeek(d.date) in (6, 7)              as is_weekend
from (
    select {{ spine_start }} + number as date
    from numbers({{ spine_days }})
) d
