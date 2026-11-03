# Databricks notebook source
# MAGIC %md
# MAGIC # Lakehouse//RT POC: run a test
# MAGIC
# MAGIC Follow the POC test runbook. It says which settings to use at each step.
# MAGIC
# MAGIC 1. Connect this notebook to **Serverless** compute (the compute menu at the top right). Do not connect it to a SQL warehouse.
# MAGIC 2. Choose the settings in the boxes at the top of the notebook. Set **5. Log table**, **6. Busiest minute** and **7. Client timeout** once; they are remembered.
# MAGIC 3. Click **Run all**, and wait until the last cell prints **DONE**.
# MAGIC
# MAGIC | Mode | What it does | Time |
# MAGIC |---|---|---|
# MAGIC | check | Checks the two POC warehouses, your permissions, the log table and the query file. Creates the log table. Runs nothing else. | 1 to 3 minutes |
# MAGIC | smoke | Runs every query in the query file once on the chosen warehouse and lists any that fail. | a few minutes |
# MAGIC | latency | Stops and restarts the chosen warehouse, then runs every query 4 times, one at a time. | 10 to 40 minutes |
# MAGIC | load | Sends queries to the chosen warehouse at the chosen rate, many at once, for the chosen minutes. | the minutes chosen, plus 2 to 5 |
# MAGIC
# MAGIC Do not change the code. If something is wrong, the notebook stops and prints what to do.

# COMMAND ----------

dbutils.widgets.dropdown("1_mode", "check", ["check", "smoke", "latency", "load"], "1. Mode")
dbutils.widgets.dropdown("2_warehouse", "poc-dbsql", ["poc-dbsql", "poc-rt"], "2. Warehouse")
dbutils.widgets.dropdown("3_load_rate", "1x", ["1x", "2x", "5x"], "3. Load rate (load only)")
dbutils.widgets.dropdown("4_minutes", "15", ["2", "15"], "4. Minutes (load only)")
dbutils.widgets.text("5_log_table", "", "5. Log table (catalog.schema.table)")
dbutils.widgets.text("6_busiest_minute", "", "6. Busiest minute: queries (load only)")
dbutils.widgets.text("7_timeout_seconds", "30", "7. Client timeout: seconds")

# COMMAND ----------

import datetime as dt
import json
import math
import os
import re
import statistics
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import requests
from requests.adapters import HTTPAdapter
from pyspark.sql.types import (DoubleType, IntegerType, LongType, StringType, StructField, StructType,
                               TimestampType)

spark.conf.set("spark.sql.session.timeZone", "UTC")  # all times in UTC
KIT_VERSION = "2026-11-03"
QUERY_FILE_NAME = "poc_queries.sql"   # must sit in the same folder as this notebook
_timeout = dbutils.widgets.get("7_timeout_seconds").strip()
TIMEOUT_SECONDS = int(_timeout) if _timeout.isdigit() else 0  # your application's client timeout; queries still running then are canceled (checked below)
LATENCY_ROUNDS = 4                    # round 1 = first touch (cold), rounds 2 to 4 = warm
_busiest = dbutils.widgets.get("6_busiest_minute").replace(",", "").strip()
BUSIEST_MINUTE_QUERIES = int(_busiest) if _busiest.isdigit() else 0  # queries in your busiest minute so far; sets the 1x load rate (checked below)
RATE_MULTIPLIERS = {"1x": 1, "2x": 2, "5x": 5}
POC_WAREHOUSES = ("poc-dbsql", "poc-rt")

MODE = dbutils.widgets.get("1_mode")
WAREHOUSE_NAME = dbutils.widgets.get("2_warehouse")
LOAD_RATE = dbutils.widgets.get("3_load_rate")
MINUTES = int(dbutils.widgets.get("4_minutes") or 15)
LOG_TABLE = dbutils.widgets.get("5_log_table").strip().strip("`")

CTX = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
HOST = CTX.apiUrl().get().rstrip("/")
TOKEN = CTX.apiToken().get()
NOTEBOOK_PATH = CTX.notebookPath().get()
USER = spark.sql("SELECT current_user()").first()[0]
def in_job():
    """True when the notebook runs as a job (used only to return a summary to the job run)."""
    try:
        attrs = json.loads(CTX.safeToJson()).get("attributes", {})
        return bool(attrs.get("jobId") or attrs.get("currentRunId") or attrs.get("multitaskParentRunId"))
    except Exception:
        return False


IN_JOB = in_job()
RUN_ID = "-".join(x for x in (MODE, LOAD_RATE if MODE == "load" else "", WAREHOUSE_NAME,
                              dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d-%H%M%S")) if x)


def stop(message):
    """Stop the notebook with a clear message (no stack trace)."""
    print("\n" + "=" * 100 + "\nSTOPPED. " + message + "\n" + "=" * 100)
    dbutils.notebook.exit("STOPPED: " + message)


def new_session(pool=16):
    s = requests.Session()
    s.headers.update({"Authorization": "Bearer " + TOKEN, "Content-Type": "application/json"})
    s.mount("https://", HTTPAdapter(pool_connections=4, pool_maxsize=pool))
    return s


API = new_session()


def api(method, path, body=None):
    r = API.request(method, HOST + path, json=body, timeout=60)
    try:
        j = r.json()
    except ValueError:
        j = {"message": r.text[:300]}
    return r.status_code, j


def quote_table(name):
    return ".".join("`" + p + "`" for p in name.split("."))


# ------------------------------------------------------------------ query file
LABEL_RE = re.compile(r"^--\s*@(\S+)\s+(\S+)\s+(\S+)\s*$")

def strip_sql(sql):
    """The query with comments removed and string contents blanked, for the checks only (never executed)."""
    out, i, n = [], 0, len(sql)
    while i < n:
        c = sql[i]
        if sql.startswith("--", i):
            j = sql.find("\n", i)
            i = n if j < 0 else j
        elif sql.startswith("/*", i):
            j = sql.find("*/", i + 2)
            i = n if j < 0 else j + 2
            out.append(" ")
        elif c in ("'", '"', "`"):
            j = i + 1
            while j < n:
                if sql[j] == "\\":
                    j += 2
                    continue
                if sql[j] == c:
                    if j + 1 < n and sql[j + 1] == c:
                        j += 2
                        continue
                    break
                j += 1
            out.append(c + c)
            i = j + 1
        else:
            out.append(c)
            i += 1
    return "".join(out)



def parse_queries(text):
    """Blocks start at a label line '-- @<query id> <typical|large> <variant>'. Returns (blocks, problems, warnings)."""
    blocks, problems, warnings, cur = [], [], [], None
    for n, raw in enumerate(text.splitlines(), 1):
        s = raw.strip()
        if s.startswith("--") and s[2:].lstrip().startswith("@"):
            m = LABEL_RE.match(s)
            if not m:
                problems.append(f"Line {n}: a label line must look like '-- @Q01 typical t1'. Found: {s}")
                cur = None
                continue
            qid, win, var = m.groups()
            if win.lower() not in ("typical", "large"):
                problems.append(f"Line {n}: the second word must be 'typical' or 'large'. Found: {win}")
            cur = {"query_id": qid, "window": win.lower(), "variant": var, "line": n, "lines": []}
            blocks.append(cur)
        elif cur is not None:
            cur["lines"].append(raw)
    seen = set()
    for b in blocks:
        lines = [l.rstrip() for l in b["lines"]]
        while lines and (not lines[-1].strip() or lines[-1].strip().startswith("--")):
            lines.pop()  # trailing blank and comment lines
        if lines:  # a final ';', even when an inline comment follows it
            lines[-1] = re.sub(r";+\s*(--.*)?$", lambda m: (" " + m.group(1)) if m.group(1) else "", lines[-1])
        sql = "\n".join(lines).strip()
        b["sql"] = sql
        b["label"] = f"{b['query_id']} {b['window']} {b['variant']}"
        code = strip_sql(sql).strip()
        first = (code.split(None, 1) or [""])[0].upper()
        where = f"Line {b['line']} ({b['label']})"
        if not code:
            problems.append(f"{where}: there is no query under this label.")
        elif first not in ("SELECT", "WITH"):
            problems.append(f"{where}: the query must start with SELECT or WITH. Found: {first}")
        if ";" in code:
            problems.append(f"{where}: put only one query under each label (there is a ';' inside it).")
        if re.search(r"<\s*(catalog|schema|table)\s*>", code, re.I):
            problems.append(f"{where}: replace the example name <catalog>.<schema>.<table> with your real table.")
        if re.search(r"\b(now|getdate|current_timestamp|current_date|localtimestamp)\s*\(|\bcurrent_(timestamp|date)\b",
                     code, re.I):
            warnings.append(f"{where}: uses the current time. Use fixed dates and times instead, so every run reads "
                            f"the same rows.")
        key = (b["query_id"], b["window"], b["variant"])
        if key in seen:
            problems.append(f"{where}: this label is used more than once. Each label must be unique.")
        seen.add(key)
    if not blocks:
        problems.append("The query file has no labeled queries. Put a label line such as '-- @Q01 typical t1' "
                        "directly above each query.")
    by_q = {}
    for b in blocks:
        by_q.setdefault(b["query_id"], {}).setdefault(b["window"], set()).add(b["variant"])
    for q, wins in sorted(by_q.items()):
        for w in ("typical", "large"):
            k = len(wins.get(w, ()))
            if k == 0:
                warnings.append(f"{q}: has no '{w}' query. Add one so both latency targets can be checked.")
            elif k < 2:
                warnings.append(f"{q} {w}: has only 1 variant. Add a second one with another tenant or filter value.")
    return blocks, problems, warnings


def load_queries():
    path = "/Workspace" + os.path.dirname(NOTEBOOK_PATH) + "/" + QUERY_FILE_NAME
    if not os.path.exists(path):
        return path, None, [f"The query file was not found at {path}. Create a file named {QUERY_FILE_NAME} in the "
                            f"same folder as this notebook (runbook step A6)."], []
    with open(path) as f:
        text = f.read()
    blocks, problems, warnings = parse_queries(text)
    return path, blocks, problems, warnings


def wrap(sql, tag):
    """A unique literal column, so the result cache can never answer: every run is timed on the engine."""
    return f"SELECT *, '{tag}' AS poc_run_tag FROM (\n{sql}\n) AS poc_query"


# ------------------------------------------------------------------ warehouses
def list_warehouses():
    code, j = api("GET", "/api/2.0/sql/warehouses")
    if code != 200:
        stop(f"Couldn't list the SQL warehouses (HTTP {code}: {j.get('message')}).")
    return {w["name"]: w for w in j.get("warehouses", [])}


def get_state(wid):
    code, j = api("GET", f"/api/2.0/sql/warehouses/{wid}")
    return j.get("state") if code == 200 else None


def wait_for(wid, targets, timeout_s):
    t0 = time.time()
    while True:
        st = get_state(wid)
        if st in targets:
            return st
        if time.time() - t0 > timeout_s:
            stop(f"The warehouse did not become {' or '.join(sorted(targets))} within {timeout_s // 60} minutes "
                 f"(it is {st}). Check it on the SQL warehouses page, then run this notebook again.")
        time.sleep(5)


def start_and_wait(w):
    st = wait_for(w["id"], {"RUNNING", "STOPPED"}, 900)
    if st == "STOPPED":
        print(f"Starting {w['name']}...")
        code, j = api("POST", f"/api/2.0/sql/warehouses/{w['id']}/start")
        if code != 200:
            stop(f"Couldn't start {w['name']} (HTTP {code}: {j.get('message')}). You need 'Can use' or higher on it.")
        wait_for(w["id"], {"RUNNING"}, 900)
    print(f"{w['name']} is running.")


def restart_cold(w):
    """Stop, then start, so the first round reads data from cloud storage (first touch)."""
    st = wait_for(w["id"], {"RUNNING", "STOPPED"}, 900)
    if st == "RUNNING":
        print(f"Stopping {w['name']} so the first round is a true first touch...")
        code, j = api("POST", f"/api/2.0/sql/warehouses/{w['id']}/stop")
        if code != 200:
            stop(f"Couldn't stop {w['name']} (HTTP {code}: {j.get('message')}). The latency test needs "
                 f"'Can manage' on the warehouse (runbook step A2 or A3). Ask the workspace admin to grant it.")
        wait_for(w["id"], {"STOPPED"}, 900)
    start_and_wait(w)


# ------------------------------------------------------------------ running queries
def run_sql(session, wid, sql):
    """One query through the Statement Execution API. Canceled at TIMEOUT_SECONDS, like your application's client."""
    body = {"warehouse_id": wid, "statement": sql, "wait_timeout": f"{TIMEOUT_SECONDS}s",
            "on_wait_timeout": "CANCEL", "disposition": "INLINE", "format": "JSON_ARRAY"}
    t0 = time.time()
    try:
        r = session.post(HOST + "/api/2.0/sql/statements", json=body, timeout=TIMEOUT_SECONDS + 60)
    except Exception as e:  # network problem on the notebook side
        return {"statement_id": None, "state": "CLIENT_ERROR", "error": repr(e)[:500], "rows_returned": None,
                "t0": t0, "t1": time.time()}
    t1 = time.time()
    try:
        j = r.json()
    except ValueError:
        j = {}
    if r.status_code != 200:
        return {"statement_id": j.get("statement_id"), "state": f"HTTP_{r.status_code}",
                "error": str(j.get("message") or r.text)[:500], "rows_returned": None, "t0": t0, "t1": t1}
    status = j.get("status") or {}
    msg = (status.get("error") or {}).get("message")
    return {"statement_id": j.get("statement_id"), "state": status.get("state"), "error": msg[:500] if msg else None,
            "rows_returned": (j.get("manifest") or {}).get("total_row_count"), "t0": t0, "t1": t1}


# ------------------------------------------------------------------ the log table
LOG_SCHEMA = StructType([StructField(n, t) for n, t in [
    ("run_id", StringType()), ("test_mode", StringType()), ("phase", StringType()), ("kit_version", StringType()),
    ("warehouse_name", StringType()), ("warehouse_id", StringType()), ("load_rate", StringType()),
    ("target_qps", DoubleType()), ("minutes", IntegerType()), ("timeout_seconds", IntegerType()),
    ("query_id", StringType()), ("time_window", StringType()), ("variant", StringType()), ("label", StringType()),
    ("run_round", IntegerType()), ("seq", IntegerType()), ("statement_id", StringType()), ("state", StringType()),
    ("error", StringType()), ("rows_returned", LongType()), ("due_at", TimestampType()),
    ("client_start", TimestampType()), ("client_end", TimestampType()), ("client_seconds", DoubleType()),
    ("run_by", StringType())]])


def ts(t):
    return dt.datetime.fromtimestamp(t, tz=dt.timezone.utc) if t is not None else None


def log_row(w, b, res, phase, round_=None, seq=None, due=None, target_qps=None):
    return (RUN_ID, MODE, phase, KIT_VERSION, w["name"], w["id"], LOAD_RATE if MODE == "load" else None,
            target_qps, MINUTES if MODE == "load" else None, TIMEOUT_SECONDS, b["query_id"], b["window"],
            b["variant"], b["label"], round_, seq, res["statement_id"], res["state"], res["error"],
            int(res["rows_returned"]) if res["rows_returned"] is not None else None, ts(due), ts(res["t0"]),
            ts(res["t1"]), round(res["t1"] - res["t0"], 3), USER)


def write_log(rows):
    if rows:
        spark.createDataFrame(rows, LOG_SCHEMA).write.mode("append").saveAsTable(quote_table(LOG_TABLE))
        print(f"Saved {len(rows)} rows to the log table {LOG_TABLE} (run ID {RUN_ID}).")


def log_table_exists():
    try:
        spark.sql(f"DESCRIBE TABLE {quote_table(LOG_TABLE)}").collect()
        return True
    except Exception:
        return False


def summarize(rows, idx_state=17, idx_secs=23):
    states = [r[idx_state] for r in rows]
    secs = [r[idx_secs] for r in rows if r[idx_state] == "SUCCEEDED"]
    return {"queries": len(rows), "answered": states.count("SUCCEEDED"), "timed_out": states.count("CANCELED"),
            "errors": len(rows) - states.count("SUCCEEDED") - states.count("CANCELED"),
            "median_s": round(statistics.median(secs), 2) if secs else None}


def peak_in_flight(rows):
    ev = sorted([(r[21].timestamp(), 1) for r in rows] + [(r[22].timestamp(), -1) for r in rows],
                key=lambda e: (e[0], e[1]))
    cur = peak = 0
    for _, d in ev:
        cur += d
        peak = max(peak, cur)
    return peak


print(f"Lakehouse//RT POC test kit, version {KIT_VERSION}")
print(f"Mode: {MODE} | Warehouse: {WAREHOUSE_NAME} | Load rate: {LOAD_RATE} | Minutes: {MINUTES} | Log table: "
      f"{LOG_TABLE or '(not set)'}")
print(f"Busiest minute: {BUSIEST_MINUTE_QUERIES or '(not set)'} queries | Client timeout: {TIMEOUT_SECONDS or '(not set)'} s")
print(f"Run ID: {RUN_ID} | Run by: {USER}")

# COMMAND ----------

# Checks shared by every mode
if MODE not in ("check", "smoke", "latency", "load") or WAREHOUSE_NAME not in POC_WAREHOUSES \
        or LOAD_RATE not in RATE_MULTIPLIERS:
    stop("A setting at the top of the notebook has an unexpected value. Pick Mode, Warehouse and Load rate from the "
         "lists, then click Run all again.")
if not re.fullmatch(r"[A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]+", LOG_TABLE or ""):
    stop("Set '5. Log table' at the top of the notebook to a full table name: catalog.schema.table, for example "
         "main.lakehouse_rt_poc.poc_runs (runbook step A5). Then click Run all again.")
if not 5 <= TIMEOUT_SECONDS <= 50:
    stop("Set '7. Client timeout' to a whole number of seconds from 5 to 50: how long your application waits for a "
         "query before giving up. The Statement Execution API waits at most 50 seconds. Then click Run all again.")
if MODE == "load" and BUSIEST_MINUTE_QUERIES < 1:
    stop("Set '6. Busiest minute' before a load test: the most queries your application sent in one minute so far. "
         "The README has SQL that finds it in your query history. Then click Run all again.")

QUERY_PATH, BLOCKS, PROBLEMS, WARNINGS = load_queries()
WAREHOUSES = list_warehouses()

# COMMAND ----------

def mode_check():
    report = []

    def add(status, item, detail=""):
        report.append(status)
        print(f"[{status:<4}] {item}" + (f": {detail}" if detail else ""))

    print("\n0. Your settings")
    add("OK", "client timeout", f"{TIMEOUT_SECONDS} s")
    if BUSIEST_MINUTE_QUERIES:
        add("OK", "busiest minute", f"{BUSIEST_MINUTE_QUERIES:,} queries a minute, so 1x is "
                                    f"{BUSIEST_MINUTE_QUERIES / 60:.1f} queries a second")
    else:
        add("WARN", "busiest minute", "box 6 isn't set yet. You need it before the load tests; the README has "
                                      "SQL that finds it.")
    print("\n1. The two POC warehouses")
    for name in POC_WAREHOUSES:
        w = WAREHOUSES.get(name)
        if not w:
            add("FIX", name, "not found. Create it with exactly this name, as in runbook step "
                             f"{'A2' if name == 'poc-dbsql' else 'A3'}.")
            continue
        wt, size = w.get("warehouse_type"), w.get("cluster_size")
        if name == "poc-dbsql":
            if wt == "REALTIME":
                add("FIX", name, "is a Real-Time warehouse. poc-dbsql must be a Serverless SQL warehouse (step A2).")
                continue
            if not w.get("enable_serverless_compute"):
                add("FIX", name, "is not Serverless. Edit it and set Type to Serverless (step A2).")
                continue
            add("OK", name, f"Serverless, size {size}, clusters {w.get('min_num_clusters')} to "
                            f"{w.get('max_num_clusters')}, auto stop {w.get('auto_stop_mins')} minutes")
            if (w.get("max_num_clusters") or 1) < 3:
                add("WARN", name, "maximum clusters is below 3. The runbook uses 3, so the baseline can absorb peak load.")
        else:
            if wt != "REALTIME":
                add("FIX", name, f"is not a Real-Time warehouse (its type is {wt}). Delete it and create it again "
                                 f"with Type = Real-Time (step A3).")
                continue
            add("OK", name, f"Real-Time, query size {size}, auto stop {w.get('auto_stop_mins')} minutes")
        res = run_sql(API, w["id"], "SELECT 1 AS ok")
        if res["state"] == "SUCCEEDED":
            add("OK", name, f"you can run queries on it ({res['t1'] - res['t0']:.1f} s, including start-up)")
        elif res["state"] == "CANCELED":
            add("WARN", name, f"the test query was still waiting after {TIMEOUT_SECONDS} s, probably while the warehouse started. "
                              "Run Mode = check again in a minute.")
        else:
            add("FIX", name, f"a test query failed ({res['state']}: {res['error']}). You need 'Can manage' on it "
                             f"(step A2 or A3).")

    print("\n2. Read access to the system tables (for the results)")
    for t in ("system.query.history", "system.billing.usage", "system.billing.list_prices"):
        try:
            spark.sql(f"SELECT 1 FROM {t} LIMIT 1").collect()
            add("OK", t)
        except Exception as e:
            add("FIX", t, "no access. Ask a metastore admin to run the GRANT statements in runbook step A4. "
                          f"({str(e).splitlines()[0][:160]})")

    print("\n3. The log table")
    catalog, schema, _ = LOG_TABLE.split(".")
    try:
        spark.sql(f"DESCRIBE SCHEMA `{catalog}`.`{schema}`").collect()
        add("OK", f"schema {catalog}.{schema} exists")
        try:
            cols = ", ".join(f"`{f.name}` {f.dataType.simpleString().upper()}" for f in LOG_SCHEMA.fields)
            spark.sql(f"CREATE TABLE IF NOT EXISTS {quote_table(LOG_TABLE)} ({cols}) "
                      f"COMMENT 'Lakehouse//RT POC test kit: one row per query run'")
            have = {f.name for f in spark.table(quote_table(LOG_TABLE)).schema.fields}
            missing = [f.name for f in LOG_SCHEMA.fields if f.name not in have]
            if missing:
                add("FIX", f"log table {LOG_TABLE}", f"exists but is missing columns {missing}. Use a new table name.")
            else:
                add("OK", f"log table {LOG_TABLE}", "ready")
        except Exception as e:
            add("FIX", f"log table {LOG_TABLE}", "couldn't create it. You need CREATE TABLE and MODIFY on the schema "
                                                 f"(step A5). ({str(e).splitlines()[0][:160]})")
    except Exception as e:
        add("FIX", f"schema {catalog}.{schema}", "not found or no access. Ask the admin to create it and grant you "
                                                 f"access (step A5). ({str(e).splitlines()[0][:160]})")

    print(f"\n4. The query file ({QUERY_PATH})")
    if BLOCKS is None:
        add("FIX", "query file", PROBLEMS[0])
    else:
        for p in PROBLEMS:
            add("FIX", "query file", p)
        for wmsg in WARNINGS:
            add("WARN", "query file", wmsg)
        ids = sorted({b["query_id"] for b in BLOCKS})
        if not PROBLEMS:
            add("OK", "query file", f"{len(BLOCKS)} queries: {len(ids)} query IDs ({', '.join(ids)})")
        for b in BLOCKS:
            first = next((l.strip() for l in b["sql"].splitlines() if l.strip() and not l.strip().startswith("--")), "")
            print(f"         {b['label']:<30} line {b['line']:<5} {first[:70]}")

    fixes, warns = report.count("FIX"), report.count("WARN")
    print("\n" + "=" * 100)
    if fixes:
        print(f"NOT READY: fix the {fixes} item(s) marked FIX above, then run Mode = check again.")
    else:
        print("READY." + (f" Read the {warns} WARN item(s) above; they don't block the tests." if warns else ""))
    print("=" * 100)
    return {"status": "NOT_READY" if fixes else "READY", "fix": fixes, "warn": warns}


def require_ready_for_tests():
    if BLOCKS is None or PROBLEMS:
        stop("The query file has problems. Run Mode = check to see them, fix the file, and run again.")
    if not log_table_exists():
        stop(f"The log table {LOG_TABLE} doesn't exist yet. Run Mode = check first; it creates the table.")
    w = WAREHOUSES.get(WAREHOUSE_NAME)
    if not w:
        stop(f"The warehouse {WAREHOUSE_NAME} was not found. Run Mode = check to see what's missing.")
    is_rt = w.get("warehouse_type") == "REALTIME"
    if (WAREHOUSE_NAME == "poc-rt") != is_rt:
        stop(f"{WAREHOUSE_NAME} has the wrong type ({w.get('warehouse_type')}). Run Mode = check to see what to fix.")
    return w


def mode_smoke(w):
    start_and_wait(w)
    rows = []
    print(f"\nRunning each of the {len(BLOCKS)} queries once on {w['name']}:\n")
    for i, b in enumerate(BLOCKS, 1):
        res = run_sql(API, w["id"], wrap(b["sql"], f"{RUN_ID}-s{i}"))
        rows.append(log_row(w, b, res, "smoke", round_=1, seq=i))
        status = {"SUCCEEDED": "OK", "CANCELED": "TIMED OUT"}.get(res["state"], res["state"])
        print(f"{i:>3}/{len(BLOCKS)}  {b['label']:<30} {status:<10} {res['t1'] - res['t0']:6.1f} s   "
              f"rows: {res['rows_returned'] if res['rows_returned'] is not None else '-'}"
              + (f"\n          error: {res['error']}" if res["error"] else ""))
    write_log(rows)
    s = summarize(rows)
    bad = [r[13] for r in rows if r[17] != "SUCCEEDED"]
    print("\n" + "=" * 100)
    if bad:
        print(f"{len(bad)} of {len(rows)} queries did not succeed on {w['name']}: {', '.join(bad)}.\n"
              f"Fix or remove them in the query file (runbook step A8), then run the smoke test again on BOTH "
              f"warehouses.")
    else:
        print(f"All {len(rows)} queries succeeded on {w['name']}.")
    print("=" * 100)
    return s


def mode_latency(w):
    restart_cold(w)
    rows = []
    n = len(BLOCKS)
    print(f"\nRunning the {n} queries {LATENCY_ROUNDS} times, one at a time. Round 1 is the first touch.\n")
    try:
        for rnd in range(1, LATENCY_ROUNDS + 1):
            t_round = time.time()
            for i, b in enumerate(BLOCKS, 1):
                res = run_sql(API, w["id"], wrap(b["sql"], f"{RUN_ID}-r{rnd}-{i}"))
                rows.append(log_row(w, b, res, "latency", round_=rnd, seq=(rnd - 1) * n + i))
            s = summarize([r for r in rows if r[14] == rnd])
            print(f"Round {rnd} of {LATENCY_ROUNDS} done in {time.time() - t_round:,.0f} s: {s['answered']} answered, "
                  f"{s['timed_out']} timed out, {s['errors']} errors; median {s['median_s']} s (notebook-side).")
    finally:
        write_log(rows)
    return summarize(rows)


def mode_load(w):
    mult = RATE_MULTIPLIERS[LOAD_RATE]
    qps = BUSIEST_MINUTE_QUERIES * mult / 60.0
    start_and_wait(w)
    print(f"\nWarm-up: running each of the {len(BLOCKS)} queries once (not counted in the results)...")
    warm = []
    for i, b in enumerate(BLOCKS, 1):
        res = run_sql(API, w["id"], wrap(b["sql"], f"{RUN_ID}-w{i}"))
        warm.append(log_row(w, b, res, "warmup", seq=i))
    workers = min(1500, int(math.ceil(qps * (TIMEOUT_SECONDS + 10))) + 16)
    session = new_session(workers)
    rows, lock = [], threading.Lock()

    def task(seq, b, due):
        res = run_sql(session, w["id"], wrap(b["sql"], f"{RUN_ID}-q{seq}"))
        row = log_row(w, b, res, "load", seq=seq, due=due, target_qps=qps)
        with lock:
            rows.append(row)

    print(f"\nLoad test: {LOAD_RATE} the busiest minute = {qps:.1f} queries per second "
          f"({BUSIEST_MINUTE_QUERIES * mult} a minute) for {MINUTES} minutes, cycling through the query file. "
          f"Queries still running after {TIMEOUT_SECONDS} s are canceled, like your application.\n")
    start = time.time() + 2
    deadline = start + MINUTES * 60
    sent, next_report = 0, start + 60
    try:
        with ThreadPoolExecutor(max_workers=workers) as ex:
            while True:
                due = start + sent / qps
                if due >= deadline:
                    break
                delay = due - time.time()
                if delay > 0:
                    time.sleep(delay)
                ex.submit(task, sent + 1, BLOCKS[sent % len(BLOCKS)], due)
                sent += 1
                if time.time() >= next_report:
                    with lock:
                        s = summarize(list(rows))
                    print(f"{(time.time() - start) / 60:4.1f} min: {sent:,} sent, {s['answered']:,} answered, "
                          f"{s['timed_out']:,} timed out, {s['errors']:,} errors")
                    next_report += 60
            print(f"All {sent:,} queries sent. Waiting up to {TIMEOUT_SECONDS} s for the last ones...")
    finally:
        write_log(warm + rows)
    s = summarize(rows)
    late = sum(1 for r in rows if r[21] and r[20] and (r[21] - r[20]).total_seconds() > 1)
    s.update({"target_qps": round(qps, 2), "sent": sent, "peak_in_flight": peak_in_flight(rows), "sent_late": late})
    print(f"\nNotebook-side summary: {s['answered']:,} answered, {s['timed_out']:,} timed out, {s['errors']:,} errors; "
          f"median {s['median_s']} s; peak {s['peak_in_flight']} queries in flight.")
    if late > 0.01 * max(sent, 1):
        print(f"WARNING: {late:,} queries were sent more than 1 s late, so the notebook couldn't keep up with the "
              f"rate. Tell Databricks; the results understate the load.")
    return s


# COMMAND ----------

if MODE == "check":
    SUMMARY = mode_check()
else:
    W = require_ready_for_tests()
    for wmsg in WARNINGS:
        print("WARN (query file): " + wmsg)
    SUMMARY = {"smoke": mode_smoke, "latency": mode_latency, "load": mode_load}[MODE](W)
    print("\nOfficial numbers come from the poc_results notebook, about 15 minutes after a test ends "
          "(the query history takes a few minutes to update).")

SUMMARY.update({"run_id": RUN_ID, "mode": MODE, "warehouse": WAREHOUSE_NAME})
print(f"\nDONE. Run ID: {RUN_ID}")

# COMMAND ----------

if IN_JOB:  # only when run as a job: hand the summary back to the job run
    dbutils.notebook.exit(json.dumps(SUMMARY, default=str))
