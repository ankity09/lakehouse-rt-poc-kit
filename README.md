# Lakehouse//RT POC test kit

This kit runs your dashboard queries on two Databricks SQL warehouses and measures them the same way:

- **`poc-dbsql`**: the baseline, a Databricks SQL Serverless warehouse with up to 3 clusters, at the size you run in production or one size smaller
- **`poc-rt`**: a [Lakehouse//RT](https://docs.databricks.com/aws/en/compute/sql-warehouse/real-time) warehouse reading the same tables

It measures three things: **latency** (each query on its own), **load** (many queries at once, at and above your busiest minute so far) and **cost** (DBUs each warehouse used).

Everything runs inside your Databricks workspace, as you, with your permissions. The kit only reads your dashboard tables. The only thing it writes is its own log table, and it calls nothing outside your workspace.

## What's in this repo

| File | What it is | Do you edit it? |
|---|---|---|
| `poc_run_test.py` | A Databricks notebook that runs the tests. It has four modes: `check`, `smoke`, `latency` and `load`. | No |
| `poc_results.py` | A Databricks notebook that turns the test runs into three tables: latency, load and cost. | No |
| `poc_queries.sql` | The query file: the dashboard queries the tests run, on both warehouses. | Yes, you fill it in |
| `RUNBOOK.md` | The full step-by-step guide: setup, each test, troubleshooting and a test log. | No |
| `sql/` | Numbered SQL files you run yourself: grants, workload numbers, checks on your tables, and tuning | Only the placeholders |
| `LICENSE.md`, `SECURITY.md` | The license, and how to report a security issue | No |

### What the four modes of `poc_run_test` do

| Mode | What it does | How long |
|---|---|---|
| `check` | Checks your settings, the two POC warehouses, your permissions, the log table and the query file, and creates the log table. Runs no queries. | 1 to 3 minutes |
| `smoke` | Runs every query in the query file once on the chosen warehouse, and lists any that fail. | A few minutes |
| `latency` | Stops and restarts the chosen warehouse, then runs every query 4 times, one at a time. Round 1 is the first touch; rounds 2 to 4 are warm. | 10 to 40 minutes |
| `load` | Sends queries to the chosen warehouse at a fixed rate, many at once, for 15 minutes. At 1x the rate is your busiest minute so far. | About 17 to 20 minutes |

### What `poc_results` shows

| Table | What it shows |
|---|---|
| 1. Latency | Per query and time window: first touch, median and slowest warm run on each warehouse, timeouts, and whether row counts match |
| 2. Load | Per load test: queries sent, answered, timed out, p50, p95, p99, time queued, and the most queries in flight |
| 3. Cost | DBUs per POC warehouse per day, with list cost |

## How the measurements work

- Each query goes to the warehouse through the [Databricks SQL Statement Execution API](https://docs.databricks.com/aws/en/dev-tools/sql-execution-tutorial), the way an application sends it. Any query still running at your client timeout is canceled and counted as timed out, the way your application would see it.
- Every run is made unique, so the result cache can't answer. The tests time the engine, not a stored answer.
- Timing comes from each warehouse's own [query history](https://docs.databricks.com/aws/en/admin/system-tables/query-history), not from the notebook's stopwatch. DBUs come from the billing system table. Anyone who runs the same queries the same way gets the same numbers.
- For every query, the row counts from the two warehouses are compared.

## What happens when you click Run all

In every mode, `poc_run_test`:

1. Reads the boxes at the top of the notebook, and the query file in its own folder.
2. Finds `poc-dbsql` and `poc-rt` by name. It never starts, stops or queries any other warehouse.
3. Wraps each query so it returns one extra column with a unique tag (`poc_run_tag`). The answer is otherwise unchanged, but the result cache can't serve it, so every run is timed on the engine.
4. Sends each query to the chosen warehouse through the Statement Execution API, and waits up to the client timeout (box 7). A query still running then is canceled and counted as timed out.
5. Writes one row per query run to the log table: the run ID, the query label, the warehouse, the statement ID, the state, the rows returned, and the notebook's own start and end times.

Then, by mode:

- **check** looks at the settings, both warehouses, your permissions, the log table and the query file. The only query it sends is `SELECT 1`, to confirm you can use each warehouse.
- **smoke** runs each query once.
- **latency** stops and restarts the warehouse first, so round 1 is a true first touch, then runs every query 4 times, one at a time.
- **load** first runs each query once as a warm-up, which isn't counted. Then it sends queries on a fixed schedule, cycling through the query file: the next query goes out on time whether or not earlier ones have finished, the way real users send them. The rate is your busiest minute (box 6) times the load rate (box 3). It prints progress every minute, and warns if it couldn't keep up with the schedule.

`poc_results` then joins the log table to each warehouse's own query history on the statement ID, so every time it reports is the server's, including time spent waiting in the queue. Cost comes from the billing system table.

## What you need first

The full steps are in [RUNBOOK.md](RUNBOOK.md), Part A. In short:

1. **Lakehouse//RT turned on** in the POC workspace (step A1).
2. **Two warehouses, named exactly as below.** The notebooks look for these names. Tag both with `project` = `lakehouse-rt-poc`, and give the POC engineer **Can manage** on both.
   - `poc-dbsql`: Serverless, the size you run in production or one size smaller, minimum 1 and maximum 3 clusters, auto stop after 10 minutes (step A2)
   - `poc-rt`: Real-Time, query size Small, auto stop after 10 minutes (step A3). Ask your Databricks account team which autoscaling setting to use for the load tests.
3. **Read access to the system tables** the tests and checks use. A metastore admin runs `sql/00_grants.sql` once (step A4). The system tables cover every workspace in the region, so the POC workspace can read the history of the warehouse your dashboards use.
4. **Read access to your dashboard tables** for the POC engineer, from the POC workspace: SELECT on the tables, plus USE CATALOG and USE SCHEMA. `sql/00_grants.sql` has optional lines for this, for the table owner to run.
5. **A schema for the log table**, for example `your_catalog.lakehouse_rt_poc`. `sql/00_grants.sql` creates it too (step A5).
6. **Your dashboard tables checked and tuned**, so both warehouses read the same optimized tables. See [step 3 below](#3-check-and-tune-your-tables).
7. **Two numbers about your workload**, which you type into the notebook:
   - **Busiest minute:** the most queries your dashboards sent in one minute so far. It sets the load-test rate. `sql/01_find_busiest_minute.sql` finds it.
   - **Client timeout:** how long your application waits for a query before giving up, in seconds, from 5 to 50. The default is 30.

To find a warehouse ID, open **Compute**, then **SQL warehouses**, then the warehouse, then the **Connection details** tab. The ID is the last part of the **HTTP path** (`/sql/1.0/warehouses/<ID>`).

## Run it in Databricks

### 1. Put the kit in your workspace

Either way, the two notebooks and `poc_queries.sql` must sit in the same folder. The notebook reads the query file from its own folder.

**Option A: a Git folder**

1. In Databricks, click **Workspace**, then **Create**, then **Git folder**.
2. Paste this repository's URL, and click **Create Git folder**.
3. `poc_run_test` and `poc_results` show up as notebooks, and `poc_queries.sql` as a file.

If you pull an update from this repo later, save a copy of your query file first. Running the notebooks on serverless also adds a small environment header to them, which the Git folder shows as a change: discard those notebook changes before you pull.

See [Create and manage Git folders](https://docs.databricks.com/aws/en/repos/git-operations-with-repos) and [Connect your Git provider to Databricks](https://docs.databricks.com/aws/en/repos/get-access-tokens-from-git-provider).

**Option B: import the files**

1. On GitHub, click **Code**, then **Download ZIP**, and unzip it.
2. In Databricks, create a folder in your workspace, for example `lakehouse-rt-poc-kit`.
3. Import `poc_run_test.py` and `poc_results.py` into it (the folder's **⋮** menu, then **Import**). Both become notebooks.
4. In the same folder, click **Create**, then **File**, and name it exactly `poc_queries.sql`. Paste in the contents of the kit's `poc_queries.sql`.

See [Import and export Databricks notebooks](https://docs.databricks.com/aws/en/notebooks/notebook-export-import) and [What are workspace files?](https://docs.databricks.com/aws/en/files/workspace).

### 2. Write the query file

1. Find your top dashboard query shapes with `sql/02_find_query_shapes.sql`. You can run it in the SQL editor of any workspace in the same region as the warehouse your dashboards use, because the system tables cover every workspace in the region. Use any SQL warehouse except the one that serves the dashboards.
2. Pick about 10 shapes: the slowest, the most frequent and the ones that time out.
3. Get the full SQL of each shape you picked. `sample_sql` shows only the first 400 characters, so paste the shape's `example_statement_id` into the second query in `sql/06_measure_queries.sql`, which returns the whole statement.
4. In `poc_queries.sql`, write 4 blocks for each shape: a typical and a large time window, each for 2 tenants or filter values. The instructions at the top of the file explain the format and the rules: real values, fixed dates, and SELECT queries only.
5. For a new dashboard or endpoint that has no query history yet, write its queries by hand, in the same 4-block format.
6. Your filled-in query file stays in your workspace. Don't push it back to this repo.

### 3. Check and tune your tables

Both warehouses read the same tables, so checking and tuning them first helps both engines and keeps the comparison fair. Run the files in `sql/` in order, in the SQL editor, on any SQL warehouse except `poc-rt` unless the file says otherwise. Replace the placeholders in angle brackets first.

| File | What it does | Changes anything? |
|---|---|---|
| `03_list_dashboard_tables.sql` | Lists the tables your dashboards read, from Unity Catalog lineage | No |
| `04_check_tables_for_lakehouse_rt.sql` | Checks that each table is managed, with no row filters, column masks or ABAC policies | No |
| `05_check_table_layout.sql` | Shows partitioning, file sizes, clustering keys, predictive optimization, and how each table is written | No |
| `06_measure_queries.sql` | Measures your slowest queries. Run it before and after tuning | No |
| `07_tune_tables.sql` | Refreshes statistics, adds liquid clustering and reorganizes the data. Pick the steps that fit each table | Yes: how tables are stored, never what's in them. Get the table owner's sign-off |
| `08_smoke_test_on_lakehouse_rt.sql` | Reads each table on `poc-rt`: the final proof that Lakehouse//RT can read it | No |

Then keep the tables' layout as it is until the tests are done, and run OPTIMIZE on them before each test day.

### 4. Run the tests

For every run:

- Connect the notebook to **Serverless** compute (the compute menu at the top right), never to a SQL warehouse. See [Serverless compute for notebooks](https://docs.databricks.com/aws/en/compute/serverless/notebooks).
- Set the boxes at the top of the notebook, click **Run all**, and wait until the last cell prints **DONE**.
- Write down the **Run ID** the notebook prints at the end. The test log in the runbook has a place for each one.
- Run one test at a time, and use the two POC warehouses for nothing else.

In `poc_run_test`, set these boxes once. The notebook remembers them.

| Box | What to enter |
|---|---|
| **5. Log table** | Your log table, for example `your_catalog.lakehouse_rt_poc.poc_runs` |
| **6. Busiest minute** | Your busiest minute so far, in queries. Needed for the load tests |
| **7. Client timeout** | Your application's client timeout, in seconds (5 to 50) |

Then run these in order:

| Step | 1. Mode | 2. Warehouse | 3. Load rate | 4. Minutes | When |
|---|---|---|---|---|---|
| 1 | `check` | either one; it checks both | | | Setup. Fix every FIX line, then rerun until the report ends with READY |
| 2 | `smoke` | `poc-dbsql`, then `poc-rt` | | | Setup. Every query must show OK on both |
| 3 | `latency` | `poc-dbsql`, then `poc-rt` | | | Test week 1 |
| 4 | `load` | `poc-dbsql` | `1x` | `15` | Test week 2 |
| 5 | `load` | `poc-rt` | `1x` | `15` | Test week 2 |
| 6 | `load` | `poc-dbsql` | `2x` | `15` | Test week 2 |
| 7 | `load` | `poc-rt` | `2x` | `15` | Test week 2 |
| 8 (optional) | `load` | `poc-rt` | `5x` | `15` | Test week 2, for headroom |

### 5. Read the results

1. Wait 15 to 30 minutes after a test ends, because the query history takes that long to catch up.
2. Open `poc_results`, connect it to **Serverless**, set **1. Log table** to the same table, and click **Run all**. If it says the query history isn't complete yet, wait 10 minutes and run it again.
3. Boxes **2** and **3** choose which latency runs to compare. `latest` uses the most recent run on each warehouse. To compare a specific run, for example a rerun after a change, paste its Run ID.
4. For cost, run `poc_results` again the day after the load tests, because billing data arrives a few hours late.
5. Download each table as CSV with the download icon under it.

## Rules for the whole POC

1. Use `poc-dbsql` and `poc-rt` only for the POC.
2. Run one test at a time.
3. Don't change the warehouse settings once testing starts. If you have to, run the latency tests again.
4. Don't edit the notebooks. Change only the boxes at the top and the query file.
5. Always connect the notebooks to Serverless compute.

## If something goes wrong

- The `check` mode prints OK, WARN or FIX for each item. Every FIX line says what to do.
- A query that times out on `poc-dbsql` even when nothing else is running needs a narrower window or tuning.
- An error only on `poc-rt` means the query uses something Lakehouse//RT doesn't support yet. See the limitations in the [Lakehouse Real-Time documentation](https://docs.databricks.com/aws/en/compute/sql-warehouse/real-time).
- The Troubleshooting section of [RUNBOOK.md](RUNBOOK.md) covers the rest.

## About this kit

This kit is provided as-is to help you run a proof of concept. It isn't an official Databricks product. For help with a POC, contact your Databricks account team.

It's licensed under the Databricks License: see [LICENSE.md](LICENSE.md). To report a security issue, see [SECURITY.md](SECURITY.md).
