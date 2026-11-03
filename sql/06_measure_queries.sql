-- 06. Measure your slowest queries, before and after tuning
--
-- Take the 5 slowest shapes from 02_find_query_shapes.sql and one example statement ID for each.
-- Run this before tuning and again after (with new example statements), and compare.
-- The "after" numbers are your tuned baseline. Also save each statement's Query Profile: a screenshot is fine.
-- Run it on any SQL warehouse except poc-rt.

SELECT statement_id, execution_status, total_duration_ms, execution_duration_ms,
       read_files, pruned_files,
       round(100 * pruned_files / nullif(read_files + pruned_files, 0), 1) AS pct_files_skipped,
       round(read_bytes / power(1024, 3), 2)          AS read_gib,
       round(spilled_local_bytes / power(1024, 3), 2) AS spill_gib,
       produced_rows
FROM system.query.history
WHERE statement_id IN ('<id1>', '<id2>', '<id3>', '<id4>', '<id5>');

-- The full text of one statement, for example to test it on poc-rt (08_smoke_test_on_lakehouse_rt.sql).
SELECT statement_text
FROM system.query.history
WHERE statement_id = '<example_statement_id>';
