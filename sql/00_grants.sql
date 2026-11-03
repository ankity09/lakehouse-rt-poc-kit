-- 00. Grants (a metastore admin runs this once)
--
-- Gives the POC engineer read access to the system tables the checks and the tests use,
-- and a schema for the kit's log table.
-- Replace <engineer@example.com> and <your_catalog>. Run it on any SQL warehouse except poc-rt.

-- Needed by the notebooks: timing and cost
GRANT USE CATALOG ON CATALOG system TO `<engineer@example.com>`;
GRANT USE SCHEMA ON SCHEMA system.query TO `<engineer@example.com>`;
GRANT SELECT ON TABLE system.query.history TO `<engineer@example.com>`;
GRANT USE SCHEMA ON SCHEMA system.billing TO `<engineer@example.com>`;
GRANT SELECT ON TABLE system.billing.usage TO `<engineer@example.com>`;
GRANT SELECT ON TABLE system.billing.list_prices TO `<engineer@example.com>`;

-- Needed by the table checks: 03_list_dashboard_tables.sql and 05_check_table_layout.sql
GRANT USE SCHEMA ON SCHEMA system.access TO `<engineer@example.com>`;
GRANT SELECT ON TABLE system.access.table_lineage TO `<engineer@example.com>`;
GRANT USE SCHEMA ON SCHEMA system.storage TO `<engineer@example.com>`;
GRANT SELECT ON TABLE system.storage.predictive_optimization_operations_history TO `<engineer@example.com>`;

-- A schema for the log table. The notebooks create the table in it.
CREATE SCHEMA IF NOT EXISTS <your_catalog>.lakehouse_rt_poc COMMENT 'Lakehouse//RT POC test log';
GRANT USE CATALOG ON CATALOG <your_catalog> TO `<engineer@example.com>`;
GRANT USE SCHEMA, CREATE TABLE, SELECT, MODIFY ON SCHEMA <your_catalog>.lakehouse_rt_poc TO `<engineer@example.com>`;
