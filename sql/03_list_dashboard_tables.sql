-- 03. List the tables your dashboards read
--
-- Every table and view read by queries on the warehouse your dashboards use, over the last 7 days,
-- from Unity Catalog lineage. The POC tests these tables, and every later check runs on this list.
-- Replace <warehouse ID> (see 01_find_busiest_minute.sql). Run it in any workspace in the same region (the system tables cover them all), on any SQL warehouse except poc-rt.

SELECT l.source_table_full_name          AS table_name,
       any_value(l.source_type)          AS source_type,
       count(DISTINCT l.statement_id)    AS statements,
       max(l.event_time)                 AS last_read
FROM system.access.table_lineage AS l
JOIN system.query.history AS q
  ON q.statement_id = l.statement_id
WHERE q.compute.warehouse_id = '<warehouse ID>'
  AND q.start_time >= current_date() - INTERVAL 7 DAYS
  AND l.event_date >= current_date() - INTERVAL 7 DAYS
  AND l.source_table_full_name IS NOT NULL
GROUP BY l.source_table_full_name
ORDER BY statements DESC;
