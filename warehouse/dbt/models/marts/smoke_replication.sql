-- P0 spike model: the smallest possible table that proves the whole path works —
-- dbt connects as the scoped `dbt` user, creates nimbus_marts ON CLUSTER, and the
-- resulting table replicates to BOTH ClickHouse pods with a Replicated* engine.
-- Materialized as a table (inherits the marts default in dbt_project.yml).
select
    1        as id,
    now()    as built_at
