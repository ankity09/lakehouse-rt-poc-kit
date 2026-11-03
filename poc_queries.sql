-- Lakehouse//RT POC: query file
-- ============================================================================================================
-- This file holds the dashboard queries the POC runs, on both warehouses, exactly as written.
-- The poc_run_test notebook reads it from the same folder. Keep the file name: poc_queries.sql
--
-- HOW TO WRITE IT
-- 1. Pick the 10 or so query shapes that matter most to your dashboards: the slowest, the most frequent,
--    and the ones that time out. The README in the kit has SQL that finds them in your query history.
-- 2. For each shape, write 4 blocks: 2 time windows x 2 variants.
--      window "typical": the time range most users pick (for example the last 24 hours)
--      window "large"  : the largest range users can pick (for example the last 30 days)
--      variant t1, t2  : the same query for a different tenant or filter value
-- 3. Each block starts with a label line, then the query:
--
--      -- @Q01 typical t1
--      SELECT ...
--
--    The label line is "-- @" followed by three words separated by spaces:
--      query ID (Q01, Q02, ...), window (typical or large), variant (t1, t2, ...)
--
-- RULES (the check mode in poc_run_test finds mistakes and tells you what to fix)
-- * Only read queries: each query starts with SELECT or WITH. No SET, CREATE, INSERT, UPDATE, DELETE or MERGE.
-- * One query per label. Ending semicolons are optional.
-- * Use real values in the query, not parameter markers such as :tenant or ?.
-- * Use fixed dates and times, not now() or current_timestamp(), so every run reads exactly the same rows.
--   Pick windows that have already ended (for example ending yesterday at midnight UTC).
-- * Give every output column its own name (use AS), and use full table names: catalog.schema.table.
-- * Lines that start with -- are comments, except label lines (-- @). Comments inside a query are fine.
-- * Delete the two example blocks below, or replace them with your first real query.
-- ============================================================================================================

-- @Q01 typical t1
-- Example: top 20 services by requests for one tenant over one day. Replace the table, columns and values.
SELECT service_name, count(*) AS requests, avg(latency_ms) AS avg_latency_ms
FROM <catalog>.<schema>.<table>
WHERE tenant_id = 'tenant-a'
  AND event_time >= TIMESTAMP '2026-10-25 00:00:00' AND event_time < TIMESTAMP '2026-10-26 00:00:00'
GROUP BY service_name
ORDER BY requests DESC
LIMIT 20;

-- @Q01 typical t2
-- Example: the same query for a second tenant.
SELECT service_name, count(*) AS requests, avg(latency_ms) AS avg_latency_ms
FROM <catalog>.<schema>.<table>
WHERE tenant_id = 'tenant-b'
  AND event_time >= TIMESTAMP '2026-10-25 00:00:00' AND event_time < TIMESTAMP '2026-10-26 00:00:00'
GROUP BY service_name
ORDER BY requests DESC
LIMIT 20;
