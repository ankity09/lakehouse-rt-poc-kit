# Security

## Reporting vulnerabilities

If you find a security vulnerability in this code, report it to Databricks at bugbounty@databricks.com. Databricks acknowledges receipt and sends regular updates on the investigation.

Don't file a GitHub issue for security vulnerabilities.

## What the kit does in your workspace

This kit is provided as-is to help run a proof of concept. It isn't a supported Databricks product. Review it before you run it.

- The notebooks run as the person who runs them, with that person's permissions.
- They call only your workspace's own APIs: the SQL Statement Execution API and the SQL warehouses API. They call nothing outside your workspace.
- They use the notebook's own session token for those calls and never store it.
- They read the tables named in your query file, `system.query.history`, `system.billing.usage` and `system.billing.list_prices`.
- The only thing they write is the kit's own log table, in the schema you choose.
- The `latency` mode stops and restarts the POC warehouse it tests (`poc-dbsql` or `poc-rt`). The notebooks never change any other warehouse.
- The SQL files in `sql/` are for you to run yourself. Only `00_grants.sql` and `07_tune_tables.sql` change anything: grants, and how tables are stored.
