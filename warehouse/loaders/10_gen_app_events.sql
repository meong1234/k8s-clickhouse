-- Tier-3 loader: bulk-synthesize ~N mobile-app events entirely in ClickHouse.
--
-- This is the "high-volume, low-consistency" stream: no per-row referential logic,
-- just numbers() x rand() at millions of rows. It IS referentially valid on
-- customer_id, though — every event points at a real raw_customers row and
-- event_ts is always >= that customer's signup_ts.
--
-- Customer picking uses an ARRAY lookup (groupArray of customer_ids/signups held as
-- query-scalars), not a JOIN: `rand()` inside a JOIN ... ON gets constant-folded to a
-- single value, which would collapse every event onto one customer. The pick index
-- is materialized in an inner subquery so all references to it agree per row.
--
-- One BATCH of `__COUNT__` rows starting at offset `__OFFSET__` (both substituted by
-- the wh-generate make target, which loops over the SCALE preset's total in batches).
-- Batching keeps each insert's memory + connection short so the ~1.35 GiB cluster
-- memory cap and the k3d exec-stream timeout are never tripped. Run with --multiquery
-- as `admin`. `number` is globally unique across batches (offset..offset+count-1), so
-- event_id / session ids stay unique.

INSERT INTO nimbus_raw.raw_app_events
WITH
    -- Customers ordered by signup (index 0 == earliest adopter). Two aligned arrays
    -- so a single picked index gives both the id and the signup timestamp.
    (SELECT groupArray(customer_id) FROM (SELECT customer_id FROM nimbus_raw.raw_customers ORDER BY signup_ts, customer_id)) AS cust_ids,
    (SELECT groupArray(signup_ts)   FROM (SELECT signup_ts   FROM nimbus_raw.raw_customers ORDER BY signup_ts, customer_id)) AS cust_sig,
    length(cust_ids) AS n_cust,
    toDateTime('2026-06-30 23:59:59') AS window_end,
    ['app_open', 'view_balance', 'view_transactions', 'send_p2p', 'card_settings',
     'deposit_check', 'support_chat', 'update_profile', 'view_statement', 'toggle_card_lock'] AS event_names,
    ['ios', 'android', 'web'] AS devices,
    ['1.0.0', '1.1.0', '1.2.0', '2.0.0', '2.1.0'] AS app_versions
SELECT
    concat('EVT-', leftPad(toString(number + 1), 12, '0')) AS event_id,
    customer_id,
    event_ts,
    event_names[1 + (rand() % length(event_names))] AS event_name,
    devices[1 + (rand() % length(devices))] AS device,
    app_versions[1 + (rand() % length(app_versions))] AS app_version,
    -- Same customer + same day == same session (a usable session grain downstream).
    concat('SESS-', customer_id, '-', toString(toDate(event_ts))) AS session_id,
    now() AS ingested_at
FROM
(
    SELECT
        number,
        cust_ids[p] AS customer_id,
        -- Uniform in [signup_ts, window_end]: events never precede signup.
        cust_sig[p] + toIntervalSecond(rand64() % greatest(1, toUInt64(dateDiff('second', cust_sig[p], window_end)))) AS event_ts
    FROM
    (
        -- Squaring a uniform biases the pick toward index 0 (early adopters == more
        -- active users). 1-based for ClickHouse array indexing.
        SELECT
            number,
            1 + least(toUInt64(n_cust - 1), toUInt64(n_cust * pow(rand64() / 18446744073709551615.0, 2))) AS p
        FROM numbers(__OFFSET__, __COUNT__)
    )
);
