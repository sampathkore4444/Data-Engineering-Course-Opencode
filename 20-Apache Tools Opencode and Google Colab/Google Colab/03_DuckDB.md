# Lesson 3 — DuckDB
## The SQLite of Analytics

> **Banking case:** FinBank's AML (Anti-Money-Laundering) analyst gets an ad-hoc request: *"Find all accounts with >50 cash deposits under ₹50k in the last quarter, joined against the PEP watchlist."* The data: 900M rows across 12 Parquet files on a shared drive. No Spark cluster. No DBA. She runs **DuckDB from her laptop** — query finishes in 90 seconds, zero infrastructure.

---

## 1. What Is DuckDB?

DuckDB is an **embedded analytical (OLAP) SQL database**:

- **Embedded**: no server process; it runs *inside* your Python/R/Java/app process like SQLite.
- **Columnar + vectorized**: columnar storage engine, vectorized execution (processes data in batches of ~2048 values per operator) → near-SIMD speed.
- **SQL-complete**: window functions, CTEs, `GROUP BY ALL`, `PIVOT`, full-text search, list/struct/map types, and more.
- **Zero-config, single file** (`pip install duckdb`), MIT-licensed.

### Where it sits vs the others

| | SQLite | PostgreSQL | Spark | **DuckDB** |
|---|---|---|---|---|
| Mode | embedded OLTP | server OLTP | cluster OLAP | **embedded OLAP** |
| Data size | GBs | TBs (tuned) | PBs | **GBs–low TBs per node** |
| Sweet spot | app storage | transactions | huge ETL | **analytics anywhere** |
| Setup | none | heavy | heavy | `pip install duckdb` |

> Mental model: **SQLite : MySQL :: DuckDB : Snowflake/Spark** (roughly).

### Why it's fast (the 3 pillars)

1. **Vectorized execution** — operators process columns of ~2048 values per step instead of tuple-at-a-time volcano style; amortizes interpreter overhead, unlocks CPU cache/SIMD.
2. **Columnar everything** — scans read only needed columns/chunks; min/max zonemaps prune data without touching it.
3. **Out-of-core & parallel** — hash joins/aggregations spill to disk gracefully; multi-core by default.

---

## 2. First Contact — 60 Seconds to Productive

```python
import duckdb
duckdb.sql("SELECT 42 AS answer, 'hello' AS msg").show()
```

### The magic trick: query files directly — no import step

```python
import duckdb

# Query a CSV straight off disk:
duckdb.sql("SELECT count(*) FROM 'txns.csv'").show()

# Query Parquet with globs and filters pushed into file reads:
con = duckdb.connect()   # persistent handle
res = con.sql("""
    SELECT mcc, sum(amount) AS volume, count(*) AS n_txns
    FROM read_parquet('lake/transactions/**/*.parquet', hive_partitioning=true)
    WHERE year = 2026 AND month = 7
      AND amount BETWEEN 1000 AND 50000
    GROUP BY mcc ORDER BY volume DESC LIMIT 10
""")
print(res)
```

DuckDB's "readers" (`read_csv`, `read_parquet`, `read_json`, `read_json_auto`) do schema inference, snappy/zstd decompression, projection & predicate pushdown automatically.

---

## 3. The Interop Hub (Arrow ↔ Pandas ↔ Polars ↔ Postgres)

DuckDB is the glue of Lesson 2's Arrow world:

```python
import duckdb, pandas as pd, pyarrow as pa, polars as pl

df_pd    = pd.read_csv("branches.csv")          # pandas in
tbl_arw  = pa.table({"acct": ["A1","A2"], "bal": [100.0, 250.0]})
pl_df    = pl.DataFrame({"mcc": ["5411"], "n": [7]})

con = duckdb.connect()

out_pd = con.sql("SELECT * FROM df_pd").df()        # result → pandas (.df())
out_arw = con.sql("SELECT * FROM tbl_arw").arrow()  # result → Arrow  (.arrow())
out_pl = con.sql("SELECT * FROM pl_df").pl()        # result → polars (.pl())

# Register explicitly when names collide or for fine control:
con.register("accounts_arrow", tbl_arw)
con.sql("SELECT sum(bal) FROM accounts_arrow").show()
```

Also speaks: PostgreSQL scanner extension (`ATTACH` a Postgres DB!), MySQL, SQLite, Iceberg (Lesson 4!), httpfs for S3/GCS/Azure over HTTPS, Excel...

---

## 4. Core SQL Features You'll Actually Use in Banking

```sql
-- GROUP BY ALL: group by every non-aggregated column. No more typos.
SELECT branch, channel, status, avg(amount) AS avg_amt
FROM txns GROUP BY ALL;

-- QUALIFY: filter window-function results without a subquery wrap.
SELECT *, row_number() OVER (PARTITION BY account ORDER BY ts DESC) AS rn
FROM txns
QUALIFY rn = 1;                       -- latest txn per account

-- PIVOT / UNPIVOT built-in
PIVOT txns ON channel USING sum(amount) GROUP BY branch;

-- ASOF joins: point-in-time lookups (balances as they were at txn time!)
SELECT t.txn_id, t.amount, b.available_balance
FROM txns t
ASOF JOIN account_snapshots b
  ON t.account = b.account AND t.ts >= b.snapshot_ts;

-- Lateral column aliases
SELECT amount, amount * 0.18 AS gst, amount + gst AS gross FROM txns;
```

Other notables: `LIST`/`STRUCT`/`MAP` types, lambda functions (`list_transform`), `range(...)`, `strftime`, sampling (`USING SAMPLE 1%`), full text search via extension, user-defined Python functions (`create_function`) callable inside SQL!

Exploration helpers analysts love:

```sql
SUMMARIZE 'warehouse/txns_q2.parquet';   -- per-column stats: min, max, ~distinct, nulls
DESCRIBE SELECT * FROM txns;             -- inferred schema without executing fully
SELECT * FROM txns USING SAMPLE 0.1%;    -- instant feel of the data
```

---

## 5. Persistence, Views & Macros

```python
con = duckdb.connect("finbank.duckdb")     # single-file database on disk

con.sql("""CREATE SEQUENCE IF NOT EXISTS alert_seq START 1""")

con.sql("""CREATE TABLE IF NOT EXISTS alerts (
              alert_id BIGINT DEFAULT nextval('alert_seq'),
              account VARCHAR, rule VARCHAR, score DOUBLE,
              created_at TIMESTAMP DEFAULT now())""")

con.sql("""CREATE VIEW daily_volume AS
           SELECT date_trunc('day', ts) AS day, sum(amount) vol
           FROM read_parquet('lake/transactions/**/*.parquet',
                             hive_partitioning=true)
           GROUP BY 1""")

con.sql("""CREATE MACRO is_high_value(a) AS a >= 100000""")
print(con.sql("""
    SELECT count(*) FROM read_parquet('warehouse/txns_q2.parquet')
    WHERE is_high_value(amount)
""").fetchall())

con.close()   # everything persisted in finbank.duckdb
```

---

## 6. 🏦 Banking Scenario — End-to-End AML Investigation Workbench

**Problem:** AML team needs repeatable investigations over raw card + wire data sitting as Parquet exports, plus a watchlist in CSV, plus live scoring — all on an analyst laptop, air-gapped (bank security!).

**Solution:** one DuckDB database file + parameterized queries + Python UDFs for custom risk math.

### Full runnable pipeline

```bash
pip install duckdb pandas numpy
```

```python
"""
aml_workbench.py — end-to-end DuckDB banking analytics.
"""
import numpy as np, pandas as pd, pyarrow.parquet as pq
import duckdb, datetime as dt

rng = np.random.default_rng(11)

# ============================================================
# STEP 1 — Simulate source extracts (as the ETL would drop them)
# ============================================================
N_TXN, N_ACCT = 2_000_000, 40_000
accounts = pd.DataFrame({
    "account":   [f"A{i:06d}" for i in range(N_ACCT)],
    "cust_type": rng.choice(["RETAIL", "SME", "CORP"], N_ACCT, p=[.8,.15,.05]),
    "branch":    rng.choice(["MUM", "DEL", "BLR", "HYD"], N_ACCT),
    "opened":    pd.to_datetime("2015-01-01") +
                 pd.to_timedelta(rng.integers(0, 4000, N_ACCT), unit="D"),
})
pep_watchlist = pd.DataFrame({                     # Politically Exposed Persons
    "account": [f"A{i:06d}" for i in rng.choice(N_ACCT, 150, replace=False)],
})

txns = pd.DataFrame({
    "txn_id":  np.arange(1, N_TXN + 1),
    "account": rng.choice(accounts["account"], N_TXN),
    "channel": rng.choice(["CASH_DEPOSIT", "WIRE", "POS", "ECOM"],
                          N_TXN, p=[.15,.20,.40,.25]),
    "amount":  np.round(rng.lognormal(6.0, 1.5, N_TXN), 2),
    "ts":      pd.to_datetime("2026-04-01") +
               pd.to_timedelta(rng.integers(0, 91*86400, N_TXN), unit="s"),
})
# Inject a classic structuring pattern: many just-under-threshold cash deposits
smurfs = rng.choice(N_ACCT, 40, replace=False)
mask = rng.random(N_TXN) < 0.002
txns.loc[mask, "channel"] = "CASH_DEPOSIT"
txns.loc[mask, "amount"]  = np.round(rng.uniform(9_000, 9_999, mask.sum()), 2)
txns.loc[mask, "account"] = rng.choice([f"A{i:06d}" for i in smurfs], mask.sum())

pq.write_table(pa.Table.from_pandas(txns), "warehouse/txns_q2.parquet",
               compression="zstd")
accounts.to_parquet("warehouse/accounts.parquet")
pep_watchlist.to_parquet("warehouse/pep.parquet")

# ============================================================
# STEP 2 — Build the analyst workbench in DuckDB
# ============================================================
con = duckdb.connect("aml.duckdb")

con.sql(f"""
CREATE OR REPLACE VIEW v_txns AS
    SELECT * FROM 'warehouse/txns_q2.parquet';
CREATE OR REPLACE VIEW v_accounts AS
    SELECT * FROM 'warehouse/accounts.parquet';
CREATE OR REPLACE VIEW v_pep AS
    SELECT * FROM 'warehouse/pep.parquet';

-- Rule: structuring = >=30 cash deposits in [9k,10k) within the quarter
CREATE OR REPLACE TABLE aml_alerts AS
SELECT t.account,
       count(*)                              AS n_cash_deposits,
       round(sum(t.amount), 2)               AS total_cash,
       min(t.ts)                             AS first_seen,
       max(t.ts)                             AS last_seen
FROM v_txns t
WHERE t.channel = 'CASH_DEPOSIT'
  AND t.amount >= 9000 AND t.amount < 10000
GROUP BY t.account
HAVING count(*) >= 30;

-- Enrich with customer + PEP status
CREATE OR REPLACE TABLE alert_enriched AS
SELECT a.*, ac.cust_type, ac.branch,
       (p.account IS NOT NULL) AS is_pep,
       CASE WHEN p.account IS NOT NULL THEN 'CRITICAL'
            WHEN a.n_cash_deposits >= 100 THEN 'HIGH'
            ELSE 'MEDIUM' END          AS severity
FROM aml_alerts a
JOIN v_accounts ac USING (account)
LEFT JOIN v_pep p USING (account)
ORDER BY severity, total_cash DESC;
""")

print(con.sql("SELECT * FROM alert_enriched LIMIT 10"))
print(con.sql("""
    SELECT severity, count(*) alerts, round(sum(total_cash)/1e6,1) mn_cash
    FROM alert_enriched GROUP BY ALL ORDER BY mn_cash DESC
"""))

# ============================================================
# STEP 3 — Parameterized drill-down (the analyst's daily driver)
# ============================================================
def account_timeline(account_no: str):
    return con.execute("""
        SELECT ts, channel, amount
        FROM v_txns
        WHERE account = $acct
        ORDER BY ts
    """, {"acct": account_no}).df()

top_account = con.sql(
    "SELECT account FROM alert_enriched ORDER BY total_cash DESC LIMIT 1"
).fetchone()[0]
timeline = account_timeline(top_account)
print(timeline.head(15))

# ============================================================
# STEP 4 — Python UDF inside SQL (custom velocity score)
# ============================================================
def velocity_score(gaps_seconds):
    # gaps may contain None (first txn of an account has no predecessor)
    gaps = np.asarray(gaps_seconds, dtype="float64")
    gaps = gaps[~np.isnan(gaps)]
    if len(gaps) == 0:
        return 0.0
    # rapid-fire bursts -> higher score
    return float(np.clip(1.0 - np.mean(np.minimum(gaps, 3600)) / 3600, 0, 1))

con.create_function("velocity_score", velocity_score, ["BIGINT[]"], "DOUBLE")

bursty = con.sql(f"""
WITH ordered AS (
  SELECT account,
         datediff('second', lag(ts) OVER w, ts) AS gap_s
  FROM v_txns
  WHERE account IN (SELECT account FROM aml_alerts)
  WINDOW w AS (PARTITION BY account ORDER BY ts)
),
gapped AS (
  SELECT account, list(gap_s) AS gaps
  FROM ordered
  GROUP BY account
)
SELECT account, round(velocity_score(gaps), 3) AS v_score
FROM gapped ORDER BY v_score DESC LIMIT 5
""").df()
print(bursty)

con.close()
```

**What just happened**

- 2M synthetic txns written once as columnar Parquet.
- DuckDB views over files = "external tables" with **no ingestion copy**.
- One SQL statement implements the classic **structuring/smurfing rule** (many sub-threshold cash deposits).
- `ASOF`-style enrichment, `QUALIFY`, parameterized drill-downs.
- Custom Python risk function callable **inside SQL**, vectorized via lists.

On a typical laptop this whole notebook runs in well under two minutes — including generating 2M rows.

---

## 7. Performance Toolkit

```sql
EXPLAIN ANALYZE SELECT ... ;        -- real execution plan + timings
SET threads TO 8;                   -- default: all cores
SET memory_limit='4GB';             -- then spills go to temp_directory
SET temp_directory='/fast_ssd/spill';  -- control where spill files land
PRAGMA enable_progress_bar;
```

Reading `EXPLAIN ANALYZE` output:

- Look for **`PARQUET_SCAN`/`READ_PARQUET`** nodes showing `Scanning: 3/12 files` or `Filters: ...` — proof that pushdown pruned inputs. If it says all files, your filter column lacks useful stats.
- **`HASH_JOIN` vs `BLOCKWISE_NL_JOIN`** — the latter means no equality key was found; almost always a bug in the join condition.
- Operators report *actual* tuple counts and time; the biggest % is where to optimize.

Tuning instincts:

- Prefer Parquet over CSV (pushdown works on Parquet only).
- Filter early, project only needed columns (DuckDB does it, but help it).
- For repeated queries over same big table: `CREATE TABLE ... AS SELECT` materialize once.
- Persistent `.duckdb` file gives you indexes (ART), statistics, faster restarts than ad-hoc in-memory.
- Sort large tables by the common filter dimension before heavy analytical reuse — improves zone-map pruning, exactly like Lesson 1's row-group stats.

### Gotchas worth knowing before production

| Gotcha | What happens | Guidance |
|---|---|---|
| **Single-writer process** | Two Python processes opening the same `.duckdb` file read-write → second one fails with a lock error | One writer process; parallel readers open with `duckdb.connect(path, read_only=True)` |
| In-memory default | `duckdb.sql(...)` without `connect()` uses a throwaway in-memory DB; created tables vanish | Persist to `*.duckdb` when you create tables/views you want to keep |
| `memory_limit` exceeded | Query fails (or spills if `temp_directory` set) for big joins on laptops | Raise limit only to what RAM allows; let out-of-core handle the rest |
| Extension install needs network | `INSTALL httpfs` fails air-gapped | Pre-download extensions into `.duckdb/extension/` on the golden image |

## 8. Cheat Sheet

```python
import duckdb
con = duckdb.connect("db.duckdb")            # ":memory:" default
con = duckdb.connect("db.duckdb", read_only=True)   # safe parallel readers
con.sql("SELECT ...").show()
res = con.sql("SELECT ...")                  # result adapters:
res.df(); res.arrow(); res.pl(); res.fetchnumpy()
con.execute(sql, {"param": val})             # prepared params ($name)
con.register("view_name", df_or_arrow)       # query python objects
con.create_function("fn", py_fn, [types], ret_type)
duckdb.read_parquet / read_csv / read_json
con.table("t").insert([...]); con.begin(); con.commit()

# Extensions (run via con.sql or any SQL interface):
#   INSTALL httpfs; LOAD httpfs;              -- S3 via s3:// URLs
#   CREATE SECRET (TYPE s3, KEY_ID '...', SECRET '...', REGION 'ap-south-1');
#   SELECT count(*) FROM 's3://finbank-lake/txns/**/*.parquet';
#   INSTALL postgres; ATTACH 'dbname=x host=y' AS pg (TYPE postgres);
#   INSTALL iceberg; LOAD iceberg;            -- iceberg_scan() — see Lesson 4
```

## 9. Exercises

1. Re-run the AML pipeline with the threshold moved to `[45k, 50k)` and ≥20 deposits. Which accounts survive both rules?
2. Use `ASOF JOIN` to attach each transaction's most-recent prior balance snapshot; flag overdrafts.
3. Replace the Python UDF with a pure-SQL window formulation of the velocity score; compare results & runtime.
4. Put the Parquet on local HTTP (`httpfs`) or S3 and run the main query remotely; measure pushdown benefit via `EXPLAIN ANALYZE`.
5. Build a monthly pivot: branches × months of cash-deposit volume using `PIVOT`.

## 10. Quiz

1. What does "vectorized execution" mean concretely in DuckDB?
2. Why does predicate pushdown work on Parquet but not plain CSV?
3. Difference between `VIEW` over `read_parquet(...)` and a `TABLE`?
4. When would you still choose Spark over DuckDB?
5. How do you call Python code from within a DuckDB query?

*(Answers: 1. Operators process ~2048-value column vectors per invocation instead of one row at a time; 2. Parquet has footer stats/min-max per row group + columnar layout enabling skipping; CSV must be fully parsed; 3. View re-reads files at query time (always fresh, no copy); Table materializes into the .duckdb file (faster repeats, stale); 4. Multi-TB/petabyte cluster-scale jobs, streaming pipelines, non-SQL complex DAG orchestration, when data already lives in a cluster; 5. con.create_function(...) then use it in SQL.)*

---

➡️ **Next:** `04_Apache_Iceberg.md` — DuckDB is brilliant for single-node analysis. But who guarantees correctness when 12 Spark jobs write to the lake simultaneously? Enter the table format.
