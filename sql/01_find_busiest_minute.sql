-- 01. Find your busiest minute (for box 6 in poc_run_test)
--
-- The 10 busiest minutes on the warehouse your dashboards use, over the last 90 days.
-- Use the top value for "6. Busiest minute". It sets the load-test rate.
-- Replace <warehouse ID>: open Compute, SQL warehouses, the warehouse, then Connection details.
-- The ID is the last part of the HTTP path (/sql/1.0/warehouses/<ID>).
-- Run it on any SQL warehouse except poc-rt.

SELECT date_trunc('MINUTE', start_time) AS minute,
       count(*)                         AS queries
FROM system.query.history
WHERE compute.warehouse_id = '<warehouse ID>'
  AND start_time >= current_date() - INTERVAL 90 DAYS
GROUP BY 1
ORDER BY queries DESC
LIMIT 10;
