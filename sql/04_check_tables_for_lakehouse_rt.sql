-- 04. Check that Lakehouse//RT can read each table
--
-- The Lakehouse//RT Beta reads Unity Catalog managed tables (Delta or Iceberg), materialized views, streaming tables
-- and metric views, with no row filters, column masks or ABAC policies.
-- Put every table from 03_list_dashboard_tables.sql in the lists below. Checks 1 to 3 cover all of them at once.
-- Run these on any SQL warehouse except poc-rt. Lakehouse//RT can't read system tables or run SHOW commands.
-- The information schema views only show tables you have permission on, so run them as someone who can see every table.

-- Check 1. Each table is managed, in Delta or Iceberg format.
-- Good: table_type is MANAGED (or MATERIALIZED_VIEW or STREAMING_TABLE), and data_source_format is DELTA or ICEBERG.
-- Not supported in the Beta: EXTERNAL tables, tables in hive_metastore, and FOREIGN tables.
-- A VIEW isn't listed as supported: note it, and check the tables under it.
SELECT table_catalog, table_schema, table_name, table_type, data_source_format
FROM system.information_schema.tables
WHERE concat_ws('.', table_catalog, table_schema, table_name)
      IN ('<catalog>.<schema>.<table_1>', '<catalog>.<schema>.<table_2>');

-- Check 2. No row filters. Good: no rows.
SELECT table_catalog, table_schema, table_name, filter_name
FROM system.information_schema.row_filters
WHERE concat_ws('.', table_catalog, table_schema, table_name)
      IN ('<catalog>.<schema>.<table_1>', '<catalog>.<schema>.<table_2>');

-- Check 3. No column masks. Good: no rows.
SELECT table_catalog, table_schema, table_name, column_name, mask_name
FROM system.information_schema.column_masks
WHERE concat_ws('.', table_catalog, table_schema, table_name)
      IN ('<catalog>.<schema>.<table_1>', '<catalog>.<schema>.<table_2>');

-- Check 4. No ABAC policies. Run once per schema. Good: no rows.
-- EFFECTIVE also lists policies set on the catalog above the schema. ABAC policies are usually set at the catalog
-- or schema level. One set on a single table doesn't show here; 08_smoke_test_on_lakehouse_rt.sql catches it.
SHOW EFFECTIVE POLICIES ON SCHEMA <catalog>.<schema>;

-- To check one table directly:
SHOW EFFECTIVE POLICIES ON TABLE <catalog>.<schema>.<table_1>;
