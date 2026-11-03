# Databricks notebook source
# MAGIC %md
# MAGIC # Lakehouse//RT POC: results
# MAGIC
# MAGIC 1. Connect this notebook to **Serverless** compute (the compute menu at the top right).
# MAGIC 2. Set **1. Log table** to the same table as in `poc_run_test`. Leave boxes 2 and 3 as `latest` unless the runbook says otherwise.
# MAGIC 3. Click **Run all**, and wait until the last cell prints **DONE**.
# MAGIC
# MAGIC The numbers come from each warehouse's own query history (server-side timing) and the billing system table.
# MAGIC The query history takes a few minutes to update, so run this about 15 minutes after a test ends. If it isn't
# MAGIC ready yet, the notebook says so.
# MAGIC
# MAGIC | Table | What it shows | Copy it into |
# MAGIC |---|---|---|
# MAGIC | 1. Latency | Per query and window: first touch, median and slowest warm run on each warehouse, timeouts, whether row counts match | the comparison sheet |
# MAGIC | 2. Load | Per load test: queries sent, answered, timed out, p50, p95, p99, time queued, queries in flight | the load results table |
# MAGIC | 3. Cost | DBUs per POC warehouse per day, with list price | the cost table |

# COMMAND ----------

dbutils.widgets.text("1_log_table", "", "1. Log table (catalog.schema.table)")
dbutils.widgets.text("2_dbsql_latency_run", "latest", "2. poc-dbsql latency run")
dbutils.widgets.text("3_rt_latency_run", "latest", "3. poc-rt latency run")

# COMMAND ----------

import datetime as dt
import json
import re

import pandas as pd

spark.conf.set("spark.sql.session.timeZone", "UTC")  # all times in UTC
LOG_TABLE = dbutils.widgets.get("1_log_table").strip().strip("`")
CTX = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
def in_job():
    """True when the notebook runs as a job (used only to return a summary to the job run)."""
    try:
        attrs = json.loads(CTX.safeToJson()).get("attributes", {})
        return bool(attrs.get("jobId") or attrs.get("currentRunId") or attrs.get("multitaskParentRunId"))
    except Exception:
        return False


IN_JOB = in_job()
OUT = {}


def stop(message):
    print("\n" + "=" * 100 + "\nSTOPPED. " + message + "\n" + "=" * 100)
    dbutils.notebook.exit("STOPPED: " + message)


if not re.fullmatch(r"[A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]+", LOG_TABLE or ""):
    stop("Set '1. Log table' to the same table as in poc_run_test (catalog.schema.table), then click Run all again.")
LOG = ".".join("`" + p + "`" for p in LOG_TABLE.split("."))
try:
    spark.sql(f"DESCRIBE TABLE {LOG}").collect()
except Exception:
    stop(f"The log table {LOG_TABLE} was not found. Check the name; it must match the one in poc_run_test.")

runs = spark.sql(f"""
    SELECT run_id, test_mode, warehouse_name, load_rate, min(client_start) AS started_utc, max(client_end) AS ended_utc,
           count_if(phase IN ('latency', 'load', 'smoke')) AS queries
    FROM {LOG} GROUP BY ALL ORDER BY started_utc DESC""").toPandas()
print(f"Runs in the log table {LOG_TABLE}:")
display(runs)


def pick_latency_run(warehouse, widget):
    v = dbutils.widgets.get(widget).strip()
    r = runs[(runs.test_mode == "latency") & (runs.warehouse_name == warehouse)]
    if v and v != "latest":
        if v not in set(r.run_id):
            stop(f"'{v}' is not a latency run on {warehouse}. Copy a run ID from the list above, or type latest.")
        return v
    return r.run_id.iloc[0] if len(r) else None


DBSQL_RUN = pick_latency_run("poc-dbsql", "2_dbsql_latency_run")
RT_RUN = pick_latency_run("poc-rt", "3_rt_latency_run")
LOAD_RUNS = list(runs[runs.test_mode == "load"].run_id)
SELECTED = [r for r in [DBSQL_RUN, RT_RUN] if r] + LOAD_RUNS
print(f"Latency runs used: poc-dbsql = {DBSQL_RUN or '(none yet)'}, poc-rt = {RT_RUN or '(none yet)'}")
print(f"Load runs found: {len(LOAD_RUNS)}")

WH_IDS = {r.warehouse_name: r.warehouse_id for r in spark.sql(
    f"SELECT warehouse_name, max_by(warehouse_id, client_start) AS warehouse_id FROM {LOG} GROUP BY ALL").collect()}

# COMMAND ----------

# Is the query history complete for these runs yet?
if SELECTED:
    sel = runs[runs.run_id.isin(SELECTED)]
    T0 = (sel.started_utc.min() - pd.Timedelta(minutes=10)).to_pydatetime()
    T1 = (sel.ended_utc.max() + pd.Timedelta(minutes=60)).to_pydatetime()
    ARGS = {"t0": T0, "t1": T1}
    WH_LIST = ", ".join("'" + w + "'" for w in WH_IDS.values())
    RUN_LIST = ", ".join("'" + r + "'" for r in SELECTED)
    QH = f"""SELECT statement_id, total_duration_ms, waiting_at_capacity_duration_ms, execution_duration_ms,
                    execution_status, from_result_cache
             FROM system.query.history
             WHERE start_time >= :t0 AND start_time <= :t1 AND compute.warehouse_id IN ({WH_LIST})"""
    chk = spark.sql(f"""
        WITH l AS (SELECT run_id, statement_id, client_end FROM {LOG}
                   WHERE statement_id IS NOT NULL AND run_id IN ({RUN_LIST})),
             q AS ({QH})
        SELECT l.run_id, count(*) AS sent, count(q.statement_id) AS in_history, max(l.client_end) AS ended_utc
        FROM l LEFT JOIN q USING (statement_id) GROUP BY l.run_id ORDER BY ended_utc""", args=ARGS).toPandas()
    now = pd.Timestamp.now(tz="UTC").tz_localize(None)
    waiting = []
    for r in chk.itertuples():
        if r.in_history < r.sent:
            recent = (now - pd.Timestamp(r.ended_utc).tz_localize(None)) < pd.Timedelta(hours=6)
            line = f"  {r.run_id}: {r.in_history:,} of {r.sent:,} queries are in the query history so far"
            if recent:
                waiting.append(line)
            else:
                print("NOTE" + line[1:] + " six hours after the run, so the missing ones are left out. Tell "
                      "Databricks.")
    if waiting:
        stop("The query history isn't complete yet for these runs:\n" + "\n".join(waiting) +
             "\nWait 10 minutes and click Run all again.")
    print("The query history is complete for the selected runs.")
else:
    print("No latency or load runs in the log table yet. Run tests with poc_run_test first.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table 1. Latency: one row per query and window
# MAGIC Times are in seconds, from the query history, including any time waiting in the warehouse's queue.
# MAGIC **First touch** is round 1, right after the warehouse restarted; **median** and **slowest** cover rounds 2 to 4.
# MAGIC **Rows match** compares the number of rows each warehouse returned for the same query.

# COMMAND ----------

if DBSQL_RUN or RT_RUN:
    lat = spark.sql(f"""
        WITH l AS (SELECT warehouse_name, query_id, time_window, variant, run_round, statement_id, state,
                          rows_returned
                   FROM {LOG} WHERE run_id IN ({", ".join("'" + r + "'" for r in [DBSQL_RUN, RT_RUN] if r)})
                     AND phase = 'latency'),
             q AS ({QH}),
             j AS (SELECT l.*, q.total_duration_ms / 1000 AS secs, q.execution_status, q.from_result_cache
                   FROM l LEFT JOIN q USING (statement_id)),
             per AS (SELECT query_id, time_window, warehouse_name,
                            max(CASE WHEN run_round = 1 THEN secs END) AS first_touch,
                            percentile(CASE WHEN run_round > 1 THEN secs END, 0.5) AS median_warm,
                            max(CASE WHEN run_round > 1 THEN secs END) AS slowest_warm,
                            count_if(execution_status = 'CANCELED') AS timeouts,
                            count_if(state NOT IN ('SUCCEEDED', 'CANCELED')) AS errors,
                            count_if(from_result_cache) AS cache_hits
                     FROM j GROUP BY ALL),
             rv AS (SELECT query_id, time_window, variant,
                           min(CASE WHEN warehouse_name = 'poc-dbsql' AND state = 'SUCCEEDED' THEN rows_returned END) AS d_min,
                           max(CASE WHEN warehouse_name = 'poc-dbsql' AND state = 'SUCCEEDED' THEN rows_returned END) AS d_max,
                           min(CASE WHEN warehouse_name = 'poc-rt' AND state = 'SUCCEEDED' THEN rows_returned END) AS r_min,
                           max(CASE WHEN warehouse_name = 'poc-rt' AND state = 'SUCCEEDED' THEN rows_returned END) AS r_max
                    FROM j GROUP BY ALL),
             m AS (SELECT query_id, time_window,
                          CASE WHEN count_if(d_max IS NULL OR r_max IS NULL) > 0 THEN 'Not compared: a run timed out or failed'
                               WHEN count_if(d_min != d_max OR r_min != r_max) > 0 THEN 'No: rows change between runs'
                               WHEN count_if(d_max != r_max) > 0 THEN 'No: check this query by hand'
                               ELSE 'Yes' END AS rows_match
                   FROM rv GROUP BY ALL)
        SELECT k.query_id AS `Query`, k.time_window AS `Window`,
               round(d.first_touch, 2) AS `poc-dbsql first touch`, round(d.median_warm, 2) AS `poc-dbsql median`,
               round(d.slowest_warm, 2) AS `poc-dbsql slowest`,
               round(r.first_touch, 2) AS `poc-rt first touch`, round(r.median_warm, 2) AS `poc-rt median`,
               round(r.slowest_warm, 2) AS `poc-rt slowest`,
               round(d.median_warm / nullif(r.median_warm, 0), 1) AS `poc-rt faster (x, median)`,
               d.timeouts AS `poc-dbsql timeouts`, r.timeouts AS `poc-rt timeouts`,
               coalesce(d.errors, 0) + coalesce(r.errors, 0) AS `Errors`,
               m.rows_match AS `Rows match`,
               coalesce(d.cache_hits, 0) + coalesce(r.cache_hits, 0) AS cache_hits
        FROM (SELECT DISTINCT query_id, time_window FROM j) k
        LEFT JOIN per d ON d.query_id = k.query_id AND d.time_window = k.time_window AND d.warehouse_name = 'poc-dbsql'
        LEFT JOIN per r ON r.query_id = k.query_id AND r.time_window = k.time_window AND r.warehouse_name = 'poc-rt'
        LEFT JOIN m ON m.query_id = k.query_id AND m.time_window = k.time_window
        ORDER BY `Query`, CASE `Window` WHEN 'typical' THEN 1 ELSE 2 END""", args=ARGS).toPandas()
    if lat.cache_hits.sum() > 0:
        print(f"WARNING: {int(lat.cache_hits.sum())} runs were answered from the result cache. Tell Databricks.")
    lat = lat.drop(columns=["cache_hits"])
    if not (DBSQL_RUN and RT_RUN):
        print("Only one warehouse has a latency run so far; its columns are filled in and the other's are empty.")
    display(lat)
    OUT["latency"] = lat.to_dict(orient="records")
else:
    print("No latency runs yet.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table 2. Load: one row per load test
# MAGIC Times are in seconds, from the query history. A query canceled at the client timeout counts as **timed out**, the
# MAGIC way your application experiences it, and its full time is included in the percentiles. **In flight** is the number
# MAGIC of queries running at the same moment, from the notebook's own timing.

# COMMAND ----------

if LOAD_RUNS:
    load = spark.sql(f"""
        WITH l AS (SELECT run_id, warehouse_name, load_rate, target_qps, minutes, statement_id, state
                   FROM {LOG} WHERE test_mode = 'load' AND phase = 'load'),
             q AS ({QH}),
             j AS (SELECT l.*, q.total_duration_ms, q.waiting_at_capacity_duration_ms, q.execution_status
                   FROM l LEFT JOIN q USING (statement_id))
        SELECT run_id AS `Run`, warehouse_name AS `Warehouse`, load_rate AS `Rate`,
               round(max(target_qps), 1) AS `Target queries/s`, max(minutes) AS `Minutes`,
               count(*) AS `Queries sent`,
               count_if(execution_status = 'FINISHED') AS `Answered`,
               count_if(execution_status = 'CANCELED') AS `Timed out`,
               count_if(state NOT IN ('SUCCEEDED', 'CANCELED')) AS `Errors`,
               round(100 * count_if(execution_status = 'FINISHED') / count(*), 2) AS `Answered (%)`,
               round(percentile(total_duration_ms, 0.5) / 1000, 2) AS `p50`,
               round(percentile(total_duration_ms, 0.95) / 1000, 2) AS `p95`,
               round(percentile(total_duration_ms, 0.99) / 1000, 2) AS `p99`,
               round(max(total_duration_ms) / 1000, 2) AS `Slowest`,
               round(percentile(coalesce(waiting_at_capacity_duration_ms, 0), 0.95) / 1000, 2) AS `p95 time queued`
        FROM j GROUP BY ALL ORDER BY `Run`""", args=ARGS).toPandas()
    t = spark.sql(f"""SELECT run_id, client_start, client_end FROM {LOG}
                      WHERE test_mode = 'load' AND phase = 'load'""").toPandas()
    flight = []
    for run_id, g in t.groupby("run_id"):
        ev = sorted([(s.value, 1) for s in g.client_start] + [(e.value, -1) for e in g.client_end])
        cur = peak = 0
        for _, d in ev:
            cur += d
            peak = max(peak, cur)
        span = (g.client_end.max() - g.client_start.min()).total_seconds()
        busy = (g.client_end - g.client_start).dt.total_seconds().sum()
        flight.append({"Run": run_id, "Peak in flight": peak, "Average in flight": round(busy / span, 1) if span else None})
    load = load.merge(pd.DataFrame(flight), on="Run", how="left")
    display(load)
    OUT["load"] = load.to_dict(orient="records")
else:
    print("No load runs yet.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table 3. Cost: DBUs per POC warehouse per day
# MAGIC From `system.billing.usage`, which can take a few hours to update, so run this the day after a test.
# MAGIC **List cost** uses the public list price. **Hours with usage** counts the hours in which the warehouse ran.

# COMMAND ----------

if WH_IDS:
    ids = ", ".join("'" + w + "'" for w in WH_IDS.values())
    names = " ".join(f"WHEN '{i}' THEN '{n}'" for n, i in WH_IDS.items())
    cost = spark.sql(f"""
        WITH p AS (SELECT DISTINCT sku_name, cloud, usage_unit, price_start_time, price_end_time,
                          pricing.default AS price
                   FROM system.billing.list_prices WHERE currency_code = 'USD')
        SELECT u.usage_date AS `Day`, CASE u.usage_metadata.warehouse_id {names} END AS `Warehouse`,
               u.sku_name AS `SKU`, round(sum(u.usage_quantity), 2) AS `DBUs`,
               round(sum(u.usage_quantity * p.price), 2) AS `List cost (USD)`,
               count(DISTINCT date_trunc('HOUR', u.usage_start_time)) AS `Hours with usage`
        FROM system.billing.usage u
        LEFT JOIN p ON u.sku_name = p.sku_name AND u.cloud = p.cloud AND u.usage_unit = p.usage_unit
                   AND u.usage_end_time > p.price_start_time
                   AND (p.price_end_time IS NULL OR u.usage_end_time <= p.price_end_time)
        WHERE u.usage_metadata.warehouse_id IN ({ids}) AND u.usage_date >= current_date() - INTERVAL 30 DAYS
        GROUP BY ALL ORDER BY `Day` DESC, `Warehouse`""").toPandas()
    if len(cost):
        display(cost)
    else:
        print("No billing rows yet for the POC warehouses. Billing can take a few hours; run this again tomorrow.")
    OUT["cost"] = cost.to_dict(orient="records")

print("\nDONE.")

# COMMAND ----------

if IN_JOB:  # only when run as a job: hand the tables back to the job run
    dbutils.notebook.exit(json.dumps(OUT, default=str))
