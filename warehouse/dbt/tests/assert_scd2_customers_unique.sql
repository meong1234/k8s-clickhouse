-- SCD2 key uniqueness: each customer has at most one interval starting on a given
-- valid_from. A duplicate (customer_id, valid_from) would mean two overlapping histories
-- for the same instant — a broken SCD2. Fails if any row is returned.
-- (The fuller no-gaps/no-overlap/one-current integrity suite is formalized in P7.)
select
    customer_id,
    valid_from,
    count() as n
from {{ ref('int_customers_scd2') }}
group by customer_id, valid_from
having n > 1
