-- 02. Find your top dashboard query shapes (for the query file)
--
-- The 25 query patterns that used the most time on the warehouse your dashboards use, over the last 30 days.
-- Numbers and quoted values are masked, so the same dashboard query with different parameters counts as one shape.
-- Pick about 10 for poc_queries.sql: the slowest, the most frequent and the ones that time out.
-- Look at sample_sql for the columns each shape filters on. They're the candidates for clustering keys.
-- Replace <warehouse ID> (see 01_find_busiest_minute.sql). Run it on any SQL warehouse except poc-rt.

WITH q AS (
  SELECT *,
    regexp_replace(
      regexp_replace(statement_text, concat(chr(39), '[^', chr(39), ']*', chr(39)), '?'),
      '[0-9]+', '#') AS shape
  FROM system.query.history
  WHERE compute.warehouse_id = '<warehouse ID>'
    AND start_time >= current_date() - INTERVAL 30 DAYS
    AND statement_type = 'SELECT'
)
SELECT
  substr(sha2(shape, 256), 1, 12)                         AS shape_id,
  count(*)                                                AS runs,
  count_if(execution_status = 'CANCELED')                 AS canceled,
  percentile_approx(total_duration_ms, 0.5)               AS p50_ms,
  percentile_approx(total_duration_ms, 0.95)              AS p95_ms,
  percentile_approx(coalesce(waiting_at_capacity_duration_ms, 0), 0.95) AS queue_wait_p95_ms,
  round(avg(read_bytes) / power(1024, 2))                 AS avg_read_mb,
  round(max(coalesce(spilled_local_bytes, 0)) / power(1024, 3), 1) AS max_spill_gib,
  round(sum(total_duration_ms) / 3600000.0, 1)            AS total_query_hours,
  any_value(statement_id)                                 AS example_statement_id,
  substr(any_value(statement_text), 1, 400)               AS sample_sql
FROM q
GROUP BY shape
ORDER BY total_query_hours DESC
LIMIT 25;
