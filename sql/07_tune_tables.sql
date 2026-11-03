-- 07. Tune the tables (before the baseline)
--
-- These change how a table is stored, never what's in it. Before you run them:
--   * get the table owner's sign-off for any change to how a table is built, and try it on a copy first if you can
--   * run them off-peak, from a Databricks SQL warehouse: not poc-rt (Lakehouse//RT is read-only), and not the
--     warehouse that serves your dashboards
--   * measure before and after each step (06_measure_queries.sql)
-- Both POC warehouses read the same tables, so tuning helps both engines and keeps the comparison fair.
-- Don't run this file as a whole: pick the steps that fit each table, using what 05_check_table_layout.sql showed.

-- Step 1. Refresh statistics, if predictive optimization hasn't run ANALYZE recently.
-- Statistics help Databricks choose fast plans, such as broadcasting a small lookup table. It reads the whole table once.
ANALYZE TABLE <catalog>.<schema>.<table> COMPUTE STATISTICS FOR ALL COLUMNS;
-- For a very wide table, only the filter and join columns:
ANALYZE TABLE <catalog>.<schema>.<table> COMPUTE STATISTICS FOR COLUMNS <time_col>, <tenant_col>;

-- Step 2. Turn on liquid clustering.
-- Keys: the columns your top query shapes filter on most, usually a time column and a tenant ID. Use up to 4 keys,
-- and fewer for tables under 10 TB. If two columns move together, such as a date and a timestamp, keep only one.
-- Choose the keys yourself for a POC. CLUSTER BY AUTO lets Databricks pick them from your query history,
-- on its own schedule: a good option after the POC (managed tables with predictive optimization).

-- A table that isn't partitioned:
ALTER TABLE <catalog>.<schema>.<table> CLUSTER BY (<time_col>, <tenant_col>);

-- A partitioned table (Databricks Runtime 18.1 or above). Converts it in place. If the partition column is a timestamp,
-- follow the extra step in the liquid clustering documentation first.
ALTER TABLE <catalog>.<schema>.<table> REPLACE PARTITIONED BY WITH CLUSTER BY (<partition_col>, <tenant_col>);

-- A table built by a pipeline or dbt: change the definition there, or the next run undoes the change.
--   Lakeflow pipeline (streaming table or materialized view): change PARTITIONED BY to CLUSTER BY in the definition.
--   dbt model: set liquid_clustered_by in the model config, and remove partition_by (they can't be used together).
--   A job that rebuilds the table with CREATE OR REPLACE: add CLUSTER BY to that statement.

-- Step 3. Reorganize the data already in the table. Changing the keys doesn't re-sort existing data until this runs.
-- FULL rewrites a lot of data on a big table, so run it off-peak.
OPTIMIZE <catalog>.<schema>.<table> FULL;

-- Step 4. Keep it maintained. If predictive optimization is off, turn it on (managed tables only), so OPTIMIZE and
-- ANALYZE keep running during and after the POC:
ALTER SCHEMA <catalog>.<schema> ENABLE PREDICTIVE OPTIMIZATION;

-- During the test weeks: run OPTIMIZE on these tables before each test day, so both warehouses read the same
-- compacted data, and don't change clustering keys or table definitions.
OPTIMIZE <catalog>.<schema>.<table>;
