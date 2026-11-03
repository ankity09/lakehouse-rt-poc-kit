-- 05. Check each table's layout and maintenance
--
-- How each table is stored today decides how to tune it (07_tune_tables.sql).
-- Run these for each large table the top query shapes read. Small lookup tables don't need clustering.
-- Run them on any SQL warehouse except poc-rt.

-- Layout. Look at:
--   partitionColumns   whether the table is partitioned
--   clusteringColumns  its liquid clustering keys; empty means it isn't clustered
--   numFiles, sizeInBytes   average file size = sizeInBytes / numFiles; many small files means OPTIMIZE will help
DESCRIBE DETAIL <catalog>.<schema>.<table>;

-- Settings. Look at Type (MANAGED or EXTERNAL) and the Predictive Optimization row:
-- whether it's on, and whether it's inherited from the schema or catalog.
DESCRIBE TABLE EXTENDED <catalog>.<schema>.<table>;

-- History. operation shows how the table is written (WRITE, MERGE, STREAMING UPDATE, CREATE OR REPLACE TABLE AS SELECT),
-- and when OPTIMIZE last ran. The table's Lineage tab in Catalog Explorer shows the job or pipeline that writes it.
-- For a dbt model, also check whether its OPTIMIZE after each run is turned off (skip_optimize, or DATABRICKS_SKIP_OPTIMIZE).
DESCRIBE HISTORY <catalog>.<schema>.<table> LIMIT 20;

-- What predictive optimization did in the last 30 days (empty if it's off for these tables).
-- If ANALYZE doesn't appear for a table, refresh its statistics before the baseline (07_tune_tables.sql).
SELECT table_name, operation_type, operation_status,
       count(*) AS operations, max(start_time) AS last_run
FROM system.storage.predictive_optimization_operations_history
WHERE catalog_name = '<catalog>' AND schema_name = '<schema>'
  AND start_time >= current_date() - INTERVAL 30 DAYS
GROUP BY ALL
ORDER BY table_name, operation_type;
