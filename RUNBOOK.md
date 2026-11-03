# Lakehouse//RT POC: Test Runbook

For the Lakehouse//RT POC test kit, version 2026-11-03

## What this runbook is for

This runbook is the step-by-step guide for a Lakehouse//RT proof of concept (POC). It runs the same dashboard queries on two warehouses and measures them the same way:

- **poc-dbsql:** a Databricks SQL Serverless warehouse with up to 3 clusters: the size you run in production, or one size smaller to see whether tuning lets you shrink it. This is the baseline.
- **poc-rt:** a Lakehouse//RT warehouse, reading the same tables.

There are three tests:

| Test | What it measures | When |
|---|---|---|
| Latency | How long each query takes when it runs alone: the first time (first touch) and once the warehouse is warm | Test week 1 |
| Load | What happens at your busiest minute's query rate (box 6) and at twice that rate: answered, timed out at your client timeout (box 7), p95, time queued | Test week 2 |
| Cost | DBUs each warehouse used, from the billing system table | The day after the load tests |

Every number comes from each warehouse's own query history and the billing system table, not from the notebook's stopwatch. Anyone who follows the same steps on the same queries gets the same numbers.

**Who does what**

| Role | Steps |
|---|---|
| Workspace admin | A1 to A3 (preview, warehouses) |
| Metastore admin | A4 and A5 (grants, schema) |
| POC engineer | A6 onward |
| Your Databricks account team | Joins each part by screen share, reviews the query file, reads the results with you |

**Time needed:** about 1 to 2 hours for setup, about 1 hour for the latency test, about 2 hours for the load tests, and 15 minutes for cost. Most of that time is waiting while a test runs.

## The kit

The kit has three files. You don't change the two notebooks; you only fill in the query file.

| File | What it is |
|---|---|
| `poc_run_test.py` | A notebook that runs the tests. It has four modes: check, smoke, latency and load. |
| `poc_results.py` | A notebook that reads the results from the query history and billing tables. |
| `poc_queries.sql` | The query file: the dashboard queries the POC runs. You write it with Databricks in step A7. |

**What the notebooks do, in plain terms:** they read the query file, send each query to the chosen warehouse through the Databricks SQL Statement Execution API, and write one row per query run to a log table in your POC schema. They run as you, with your permissions. They only read your dashboard tables; the only thing they write is the log table. The latency test also stops and restarts the chosen POC warehouse. They call nothing outside your Databricks workspace.

## Rules for the whole POC

1. **Use the two POC warehouses only for the POC.** Nothing else (dashboards, people, jobs) should use poc-dbsql or poc-rt.
2. **Run one test at a time.** Don't start a test while another is running on either POC warehouse.
3. **Don't change the warehouse settings once testing starts.** If you have to, tell Databricks; the latency tests then need to run again.
4. **Don't edit the notebooks.** Only change the settings boxes at the top and the query file.
5. **Always connect the notebooks to Serverless compute,** never to a SQL warehouse.
6. **Write down every Run ID** in the test log at the end of this runbook. The notebook prints it at the end of each run.

---

## Part A: Setup (readiness week, once)

### A1. Turn on Lakehouse//RT (workspace admin)

1. Accept the Lakehouse//RT preview terms. Databricks emails them to your main contact. No signature is needed.
2. In the POC workspace, click your user name at the top right, then **Previews**.
3. Search for **Lakehouse RT** and turn it on.
4. Check: click **Compute** in the left sidebar, open the **SQL warehouses** tab and click **Create SQL warehouse**. **Real-Time** should be offered as a type. Close the dialog without creating anything.

### A2. Create the baseline warehouse, poc-dbsql (workspace admin)

1. Click **Compute** in the left sidebar, open the **SQL warehouses** tab, and click **Create SQL warehouse**.
2. Fill in exactly the settings in the table below. Leave everything else as it is, and click **Create**.
3. On the warehouse's page, click **Permissions**. Add the POC engineer with **Can manage**, and save. The latency test needs Can manage to stop and restart the warehouse.

Settings for poc-dbsql:

| Setting | Value | Why |
|---|---|---|
| Name | `poc-dbsql` (lowercase, exactly) | The notebooks look for this name |
| Cluster size | The size you run in production, or one size smaller | One size smaller shows whether tuning lets you shrink the warehouse. If queries spill to disk or miss their targets, use the production size |
| Auto stop | On, after **10** minutes | Stops billing when idle |
| Scaling | Min. **1**, Max. **3** | Up to 3 clusters, so the baseline can absorb peak load without queueing |
| Type | **Serverless** | The same type as production |
| Advanced options, Tags | Key `project`, value `lakehouse-rt-poc` | Makes POC billing easy to find |

### A3. Create the Lakehouse//RT warehouse, poc-rt (workspace admin)

1. Click **Compute**, open the **SQL warehouses** tab, and click **Create SQL warehouse**.
2. Fill in exactly the settings in the table below, and click **Create**.
3. On the warehouse's page, click **Permissions**. Add the POC engineer with **Can manage**, and save.

Settings for poc-rt:

| Setting | Value | Why |
|---|---|---|
| Type | **Real-Time** | The Lakehouse//RT engine |
| Name | `poc-rt` (lowercase, exactly) | The notebooks look for this name |
| Query size | **Small** | The starting size for the POC |
| Autoscaling, maximum DBUs per hour | [Databricks to confirm before the POC starts] | The ceiling for adding compute under load |
| Auto stop | On, after **10** minutes | Stops billing when idle |
| Tags | Key `project`, value `lakehouse-rt-poc` | Makes POC billing easy to find |

### A4. Give the POC engineer read access to the system tables (metastore admin)

The results notebook reads three system tables. Open the **SQL editor**, pick any SQL warehouse except poc-rt (Lakehouse//RT can't read system tables), replace the email address, and run:

```sql
GRANT USE CATALOG ON CATALOG system TO `engineer@example.com`;
GRANT USE SCHEMA ON SCHEMA system.query TO `engineer@example.com`;
GRANT SELECT ON TABLE system.query.history TO `engineer@example.com`;
GRANT USE SCHEMA ON SCHEMA system.billing TO `engineer@example.com`;
GRANT SELECT ON TABLE system.billing.usage TO `engineer@example.com`;
GRANT SELECT ON TABLE system.billing.list_prices TO `engineer@example.com`;
```

If `system.query.history` doesn't exist, an account admin needs to enable the `query` system schema. Databricks can help with that. The kit's `sql/00_grants.sql` has these grants and the next step's in one file, plus two more that the table checks in step A10 need.

### A5. Create the POC schema for the log table (metastore admin)

Pick a catalog in the POC workspace, replace `your_catalog` and the email address, and run:

```sql
CREATE SCHEMA IF NOT EXISTS your_catalog.lakehouse_rt_poc COMMENT 'Lakehouse//RT POC test log';
GRANT USE CATALOG ON CATALOG your_catalog TO `engineer@example.com`;
GRANT USE SCHEMA, CREATE TABLE, SELECT, MODIFY ON SCHEMA your_catalog.lakehouse_rt_poc TO `engineer@example.com`;
```

The log table's full name is then `your_catalog.lakehouse_rt_poc.poc_runs`. Write it down; you type it into both notebooks. The check in step A8 creates the table.

### A6. Put the kit in your workspace (POC engineer)

1. Click **Workspace** in the left sidebar, then **Home** (your own folder).
2. Click **Create**, then **Folder**. Name it `lakehouse-rt-poc-kit`.
3. Open the folder. Click the **⋮** menu at the top right of the folder, then **Import**. Choose **File**, drop in `poc_run_test.py`, and click **Import**. Do the same for `poc_results.py`. Both show up as notebooks.
4. Still in the folder, click **Create**, then **File**. Name it exactly `poc_queries.sql`. Open the kit's `poc_queries.sql` on your computer, copy all of it, paste it into the new file, and close the tab. The file saves automatically.
5. Check: the folder holds exactly three items: the notebooks `poc_run_test` and `poc_results`, and the file `poc_queries.sql`.

### A7. Write the query file (POC engineer, with Databricks)

1. Open `poc_queries.sql`. The instructions at the top of the file explain the format.
2. Choose about 10 query shapes: the dashboard queries that are slowest, most frequent, or time out today. The kit's `sql/02_find_query_shapes.sql` finds them in your query history.
3. For each shape, write **4 blocks**: a typical and a large time window, each for 2 tenants or filter values. The example below shows one shape.
4. Each query should take less than 15 seconds to execute on an idle warehouse. The smoke test in step A9 shows you which ones don't; Databricks helps you optimize or narrow those.

A complete example for one shape:

```sql
-- @Q01 typical t1
SELECT service_name, count(*) AS requests, avg(latency_ms) AS avg_latency_ms
FROM my_catalog.analytics.http_events
WHERE tenant_id = 'tenant-a'
  AND event_time >= TIMESTAMP '2026-10-25 00:00:00' AND event_time < TIMESTAMP '2026-10-26 00:00:00'
GROUP BY service_name ORDER BY requests DESC LIMIT 20;

-- @Q01 typical t2
(the same query with tenant_id = 'tenant-b')

-- @Q01 large t1
(the same query for tenant-a with the largest window users can pick, for example 30 days)

-- @Q01 large t2
(the same query for tenant-b with the largest window)
```

The rules that matter most:

- Only `SELECT` or `WITH` queries, one per label.
- Real values instead of parameter markers.
- Fixed dates and times that have already ended, never `now()`. Then every run reads exactly the same rows, even while new telemetry arrives.
- Full table names (`catalog.schema.table`), and a unique name for every output column.

### A8. Run the check (POC engineer)

1. Open the `poc_run_test` notebook.
2. At the top right, open the compute menu and choose **Serverless**.
3. In the settings boxes at the top of the notebook, set **1. Mode** to `check`, set **2. Warehouse** to either one (the check looks at both), and type your log table name from step A5 into **5. Log table**, for example `your_catalog.lakehouse_rt_poc.poc_runs`. Set **6. Busiest minute** to the most queries your dashboards sent in one minute so far, and **7. Client timeout** to how long your application waits for a query, in seconds. The README has SQL that finds the busiest minute.
4. Click **Run all** at the top. Wait until the last cell prints **DONE** (1 to 3 minutes).
5. Read the report. Every line starts with **OK**, **WARN** or **FIX**. Fix every **FIX** line (each one says what to do), then run the check again. **WARN** lines don't block testing, but read them: for example, a query without a "large" window means one latency target can't be checked.
6. Repeat until the report ends with **READY**.

### A9. Run the smoke test on both warehouses (POC engineer)

1. In `poc_run_test`, set **1. Mode** to `smoke` and **2. Warehouse** to `poc-dbsql`. Click **Run all** and wait for **DONE**.
2. Set **2. Warehouse** to `poc-rt`. Click **Run all** and wait for **DONE**.
3. Every query must show **OK** on both warehouses. If any doesn't, fix the query file (see below), then run the check (A8) and the smoke test on both warehouses again.
4. Write both Run IDs in the test log.

If a query isn't OK:

- **TIMED OUT on poc-dbsql:** the query takes longer than the client timeout even on an idle warehouse. Narrow its window or optimize it with Databricks.
- **An error only on poc-rt:** the query uses something Lakehouse//RT doesn't support yet. Send the error to Databricks; together you'll rewrite or remove it.
- **An error on both:** the query itself has a problem. Fix it in the query file.

### A10. Check and tune the dashboard tables (POC engineer, with Databricks)

Both warehouses read the same tables, so tuning them helps both engines and keeps the comparison fair. Run the kit's SQL files in order, in the SQL editor, on poc-dbsql or another Databricks SQL warehouse. Don't use poc-rt: Lakehouse//RT runs only SELECT queries and can't read system tables.

1. `sql/03_list_dashboard_tables.sql`: the tables your dashboards read.
2. `sql/04_check_tables_for_lakehouse_rt.sql`: each table is a Unity Catalog managed table, with no row filters, column masks or ABAC policies.
3. `sql/05_check_table_layout.sql`: partitioning, file sizes, clustering keys, how each table is written, and what predictive optimization already does.
4. `sql/06_measure_queries.sql`: a "before" measurement of your 5 slowest query shapes.
5. `sql/07_tune_tables.sql`: refresh statistics, add liquid clustering, and reorganize the data. These change how tables are stored, never what's in them. Get the table owner's sign-off, and run them off-peak.
6. `sql/06_measure_queries.sql` again: the "after" measurement. These numbers are your tuned baseline.

Then don't change the tables' layout until the tests are done. Before each test day, run OPTIMIZE on the tables so both warehouses read the same, compacted data.

Setup is complete when the check says READY, every query is OK on both warehouses, and the tables are checked and tuned (A10).

---

## Part B: Latency test (test week 1)

The notebook stops and restarts the warehouse, then runs every query 4 times, one at a time. Round 1 is the **first touch**, with nothing cached on the warehouse yet. Rounds 2 to 4 are **warm**. Like your application, any query still running at the client timeout (box 7) is canceled and counted as timed out. The result cache can't answer, because every run is made unique.

1. Make sure no other test is running on either POC warehouse.
2. In `poc_run_test`, set **1. Mode** to `latency` and **2. Warehouse** to `poc-dbsql`. Click **Run all**.
3. You'll see "Stopping poc-dbsql…", "Starting poc-dbsql…", then one line per round. It takes 10 to 40 minutes, depending on your queries. Leave the notebook open, and don't run anything else on poc-dbsql. Wait for **DONE**.
4. Set **2. Warehouse** to `poc-rt`, click **Run all**, and wait for **DONE**. This one is usually much faster.
5. Write both Run IDs in the test log.
6. **Wait 15 to 30 minutes.** The query history takes that long to catch up.
7. Open the `poc_results` notebook. Choose **Serverless** in the compute menu. Set **1. Log table** to the same name as before, and leave boxes **2** and **3** as `latest`. Click **Run all**. If it stops and says the query history isn't complete yet, wait 10 minutes and click **Run all** again.
8. **Table 1. Latency** has one row per query and window. It gives first touch, median and slowest warm time for each warehouse (in seconds), timeouts, and **Rows match**.
9. Download Table 1: click the download icon under the table and choose CSV. Send the CSV to Databricks; the headline numbers go in the Result column of the POC plan's success criteria.
10. Check **Rows match** on every row, as explained below.

What Rows match means:

- **Yes:** both warehouses returned the same number of rows.
- **No:** run that query on both warehouses in the SQL editor, compare the results, and tell Databricks.
- **Not compared:** a run timed out or failed, so there's nothing to compare.

To repeat a latency test, for example after fixing a query, just run it again. `poc_results` always uses the latest latency run on each warehouse.

---

## Part C: Load tests (test week 2)

The notebook sends queries to the warehouse at a fixed rate, cycling through the query file. At 1x the rate is your busiest minute so far, the number in box 6. At 2x it's twice that, and at 5x five times. Many queries run at once, and each one is canceled at the client timeout (box 7), like your application. First, every query runs once as a warm-up, which isn't counted.

Run these one at a time, in this order. Each takes about 17 to 20 minutes.

| Step | 1. Mode | 2. Warehouse | 3. Load rate | 4. Minutes |
|---|---|---|---|---|
| C1 | load | poc-dbsql | 1x | 15 |
| C2 | load | poc-rt | 1x | 15 |
| C3 | load | poc-dbsql | 2x | 15 |
| C4 | load | poc-rt | 2x | 15 |
| C5 (optional, for headroom) | load | poc-rt | 5x | 15 |

For each step:

1. Make sure no other test is running on either POC warehouse.
2. In `poc_run_test`, set the four boxes as in the table, and click **Run all**.
3. The notebook prints progress every minute: sent, answered, timed out, errors. Leave it open and wait for **DONE**.
4. Near the end of the test, open **Compute**, then **SQL warehouses**, then the warehouse, then the **Monitoring** tab. Take a screenshot showing running queries, queued queries and clusters.
5. If the notebook prints a WARNING that queries were sent late, tell Databricks: the notebook couldn't keep up, so that run understates the load.
6. Write the Run ID in the test log.

When all the load tests are done:

1. Wait 15 to 30 minutes, then open `poc_results` and click **Run all** (same settings as before).
2. Find **Table 2. Load**: one row per load test. The columns that matter most are explained below.
3. Download Table 2 as CSV and send it to Databricks; the headline numbers go in the Result column of the POC plan's success criteria.

The columns that matter most in Table 2:

- **Timed out:** canceled at the client timeout. The target is 0.
- **Answered (%):** the share answered within 30 seconds.
- **p95:** the slowest 1 in 20 queries, in seconds, compared with the target in the success criteria.
- **p95 time queued:** time spent waiting for the warehouse; near 0 means no queueing.
- **Peak in flight:** the most queries running at once.

---

## Part D: Cost (the day after the load tests)

Billing data takes a few hours to arrive, so do this the next day.

1. Open `poc_results` and click **Run all**.
2. **Table 3. Cost** shows, for each POC warehouse and day: DBUs, list cost in US dollars, and hours with usage. The POC warehouses only ran for tests, so these are the tests' DBUs.
3. Download Table 3 as CSV, and send it to Databricks. Together you'll turn it into the monthly projection in the success criteria: DBUs per hour while running, times the hours a day your dashboards are busy.

---

## Part E: After the readout

- Keep the two POC warehouses if you'll keep testing. They cost nothing while stopped.
- Otherwise delete them: **Compute**, **SQL warehouses**, the warehouse, then the **⋮** menu and **Delete**.
- The log table can stay for reference, or a metastore admin can drop the `lakehouse_rt_poc` schema.

---

## Troubleshooting

| What you see | Why | What to do |
|---|---|---|
| An error about `dbutils`, `spark` or Python as soon as you click Run all | The notebook is connected to a SQL warehouse | Choose Serverless in the compute menu at the top right |
| STOPPED: Set '5. Log table' | The box is empty or not in catalog.schema.table form | Type the full name from step A5 |
| FIX: poc-dbsql (or poc-rt) not found | The warehouse is missing or named differently | Create it, or rename it to exactly that name (A2, A3) |
| FIX: is not a Real-Time warehouse | poc-rt was created with the wrong type | Delete it and create it again with Type = Real-Time (A3) |
| FIX: system.query.history (or billing): no access | The grants in A4 are missing | Ask the metastore admin to run A4 |
| FIX: log table: couldn't create it | Permissions on the POC schema are missing | Ask the metastore admin to run A5 |
| FIX: query file | A problem in `poc_queries.sql`; the line says what | Fix that line, then run the check again |
| STOPPED: Couldn't stop poc-… | You don't have Can manage on the warehouse | Ask the workspace admin (A2 step 4, A3 step 4) |
| Smoke test: TIMED OUT on poc-dbsql | The query takes longer than the client timeout on an idle warehouse | Narrow the window or optimize it with Databricks |
| Smoke test: an error only on poc-rt | A feature Lakehouse//RT doesn't support yet | Send the error to Databricks |
| Results: the query history isn't complete yet | Normal; it usually takes 15 to 30 minutes, sometimes longer | Wait 10 minutes, then click Run all again. If it's still not complete 2 hours after the test, tell Databricks |
| Results: Rows match says No | The two warehouses returned different row counts | Compare that query on both in the SQL editor; tell Databricks |
| Results: Table 3 says no billing rows yet | Billing takes a few hours | Run it again the next day |
| Load test: WARNING about queries sent late | The notebook couldn't keep up with the rate | Tell Databricks |
| FIX or errors mentioning HTTP 403, an IP address or access denied, when the notebook calls a warehouse | The workspace's network settings (for example an IP access list) block calls from serverless compute to the workspace | Ask the workspace admin and Databricks; the notebooks need to reach the workspace's own API |
| Anything else | | Send Databricks the Run ID and a screenshot of the full message |

---

## Test log

| Step | Date (UTC) | Warehouse | Mode and rate | Run ID | Notes |
|---|---|---|---|---|---|
| A9 | | poc-dbsql | smoke | | |
| A9 | | poc-rt | smoke | | |
| B | | poc-dbsql | latency | | |
| B | | poc-rt | latency | | |
| C1 | | poc-dbsql | load 1x | | |
| C2 | | poc-rt | load 1x | | |
| C3 | | poc-dbsql | load 2x | | |
| C4 | | poc-rt | load 2x | | |
| C5 | | poc-rt | load 5x (optional) | | |

---

## Appendix: settings built into the kit

| Setting | Value | Why |
|---|---|---|
| Query timeout | Box 7 (default 30 seconds), then canceled | Your application's client timeout. The Statement Execution API allows 5 to 50 seconds |
| Latency rounds | 4 per query; round 1 is first touch | One cold run and three warm runs per query |
| Load rate, 1x | Box 6 | Your busiest minute so far, from the query history (the README has the SQL) |
| Result cache | Can't answer | Each run adds a unique literal column, so every run is timed on the engine |
| Timing | Server-side, from `system.query.history` | Includes time waiting in the warehouse's queue; network time isn't counted |
| Row counts | From each run's result | Used for the Rows match check |
