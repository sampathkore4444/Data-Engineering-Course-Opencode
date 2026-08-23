# Lesson 05 — DuckDB: The Embedded Analytical Engine

> **Meridian Trust Bank case study, Part 5**: A risk analyst has a 90 GB Parquet lake of card
> transactions on S3 and a question: "Which cards show 5+ transactions in 10 minutes across 2
> countries?" Spinning up a Spark cluster needs a ticket and 40 minutes of IT approval.
> DuckDB answers in seconds - inside her laptop, inside a Jupyter notebook, no server.

---

## 1. What DuckDB is

**DuckDB** is an open-source, **in-process OLAP SQL database**. Think:

> "SQLite, but built from scratch for analytics" - columnar, vectorized, parallel,
> standard SQL, runs embedded in your Python/R/Java/Node process.

No server to install, no daemon, no ports. It is a **library** that becomes a full analytical
database inside your process:

```python
import duckdb
duckdb.sql("SELECT 42 AS answer").show()
```

| Property | DuckDB | Spark | Postgres |
|---|---|---|---|
| Process model | in-process library | JVM cluster driver+executors | server daemon |
| Storage | columnar, mmap-able `.duckdb` files | reads external files | row store |
| Sweet spot | 10^0..10^10 rows / 1 machine | multi-TB clusters | OLTP |
| Zero-ops? | yes | no | no |

Positioning in our stack: **Parquet is the disk format, Arrow the memory format,
DuckDB the engine** that computes over them with full SQL.

```
        ┌──────────────────── your process ────────────────────┐
        │  pandas ▲                                            │
        │         │ zero-copy (C Data Interface)               │
        │         ▼                                            │
        │      pyarrow ◀─▶ DUCKDB ENGINE                       │
        │                     │  vectorized, morsel-parallel  │
        │       ┌─────────────┼──────────────┐                │
        │       ▼             ▼              ▼                │
        │  parquet files   CSV/JSON     .duckdb db file        │
        └──────────────────────────────────────────────────────┘
                    also: S3/http via httpfs extension
                          Iceberg via iceberg extension
```

## 2. Architecture: why it is fast

```
SQL text
   │  parser
   ▼
logical plan ── binder/resolver (schemas, types)
   │  optimizer (predicate/projection pushdown, join order,
   │             statistics-based cardinality, filter push into scans)
   ▼
physical plan
   │  MORSEL-DRIVEN PARALLELISM: pipeline breaks data into ~128K-row
   ▼  morsels; operators are vectorized (SIMD) and stateless per morsel;
      all cores chew morsels independently - no shuffle stage locally
Columnar scan ─▶ hash agg/join/sort/window kernels ─▶ result vectors (Arrow-compatible)
```

Three headline ideas:

1. **Vectorized execution**: operators work on batches ("vectors") of ~2048 values, not rows -
   the Lesson 01 SIMD argument, industrialized.
2. **Morsel-driven parallelism**: each core grabs a morsel and runs the *entire* pipeline piece;
   scheduling is cache-aware. No coordinator bottleneck.
3. **Zero-copy interchange**: DuckDB's vectors convert to/from **Arrow** without copying
   (C Data Interface), and it reads Parquet natively - decoding straight into its vectors.

It also has serious database features people underestimate: ACID transactions (single-writer,
MVCC snapshots for readers), persistent single-file databases, indexes (ART), and a huge
standard-SQL surface.

## 3. The SQL superpowers you will actually use

### 3.1 Query files directly - no import step

```sql
SELECT mcc, count(*), sum(amount)
FROM read_parquet('lake/card_txns/**/*.parquet', hive_partitioning = true)
GROUP BY mcc;
```

Globs, hive partitioning, and predicate pushdown just work. Same for
`read_csv_auto('trades_*.csv')`, `read_json(...)`, and even remote URLs/S3 with `httpfs`.

### 3.2 Window functions everywhere

```sql
-- velocity feature: txns per card in trailing 10 minutes
SELECT txn_id, card_id, ts,
       count(*) OVER w AS cnt_10m,
       sum(amount) OVER w AS amt_10m
FROM txns
WINDOW w AS (
    PARTITION BY card_id
    ORDER BY ts
    RANGE BETWEEN INTERVAL 10 MINUTE PRECEDING AND CURRENT ROW
);
```

`RANGE` with time intervals = true sliding windows in plain SQL - the bread and butter of
fraud/risk features.

### 3.3 ASOF JOIN - point-in-time lookups

```sql
-- attach the balance as-of each transaction (no fuzzy joins by hand)
SELECT t.txn_id, t.card_id, b.balance_eur
FROM txns t
ASOF JOIN balances b
  ON t.card_id = b.card_id AND b.as_of <= t.ts;
```

ASOF JOIN finds the most recent right-side row at or before the left-side time. Banks use it
for FX rates, balance reconstruction, market-data enrichment. Doing this manually is error-
prone; here it is one keyword.

### 3.4 QUALIFY, GROUP BY ALL, PIVOT

```sql
-- newest row per account without subquery gymnastics
SELECT * FROM ledger_entries
QUALIFY row_number() OVER (PARTITION BY acct ORDER BY posted_at DESC) = 1;

-- let the engine figure the non-aggregated columns
SELECT branch_code, currency, count(*), sum(amount) FROM txns GROUP BY ALL;
```

## 4. DuckDB <-> Python ecosystem

```python
import duckdb, pandas as pd, pyarrow as pa

con = duckdb.connect()                      # or duckdb.connect("meridian.duckdb")

# IN: pandas/arrow objects are visible as tables automatically (replacement scans)
out_rel = con.sql("SELECT ... FROM my_df WHERE ...")

# OUT: choose your representation - all cheap
rel.df()          # -> pandas DataFrame
rel.arrow()       # -> pyarrow Table (zero-copy)
rel.fetchall()    # -> python tuples
rel.write_parquet("out.parquet")

# register explicitly when names collide
con.register("txn_view", arrow_table_or_df)

# parameterized (never f-string user input!)
con.execute("SELECT * FROM txns WHERE amount > ?", [1000])
```

Result object is a lazy **relation**: compose `.filter()/.aggregate()/.order()` chains or
just hand it SQL. Relations execute when consumed.

## 5. Persistence, catalogs and extensions

- `duckdb.connect("file.duckdb")` - durable single-file DB with ACID commits; great as a
  local mart/cache layer.
- `ATTACH 'audit.duckdb' AS audit;` - query several databases in one statement.
- `CREATE VIEW recent AS SELECT ... FROM read_parquet(...)` - views over external files make
  a "poor man's lakehouse" schema.
- Extensions (auto-download): `httpfs` (S3/HTTPS reads), `iceberg` (read Iceberg tables),
  `spatial`, `postgres_scanner` (federated OLTP reads!), `parquet` is built-in.

```sql
INSTALL httpfs; LOAD httpfs;    -- once per environment
SELECT count(*) FROM 's3://meridian-lake/txns/2026/07/*.parquet';
```

## 6. Banking scenario walkthrough

Three Meridian production patterns:

1. **Analyst sandbox**: notebooks connect to Parquet on S3 through httpfs. Questions answered
   without moving data into any warehouse. Governance: read-only IAM role + query logging.
2. **Overnight ETL steps**: Airflow tasks call DuckDB for heavy local transforms between
   extract (S3 parquet) and load (Iceberg). No cluster cost, deterministic, unit-testable SQL.
3. **Feature backfills**: window-function SQL (velocity, frequency, deviation-from-average)
   materialized into Parquet partitions that feed model training - exactly the Section 7 lab.

## 7. End-to-end example: fraud features with one embedded engine

Generate a synthetic month of card transactions + a balances table, write them as Parquet
with PyArrow, then let DuckDB do everything SQL-shaped: pushdown scans, sliding windows,
ASOF balance reconstruction, dedup via QUALIFY - and export results to Arrow/pandas/Parquet.

```python
"""
lesson05_duckdb_lab.py
DuckDB over PyArrow-written Parquet: fraud features & point-in-time joins.
Deps: duckdb, pyarrow, numpy, pandas
"""
import shutil
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import duckdb

rng = np.random.default_rng(5)
LAKE = "/tmp/opencode/ducklake"
shutil.rmtree(LAKE, ignore_errors=True)

# ---- 1. synthetic data: 1M txns (July 2026) + card balances snapshots ----------
N = 1_000_000
tx = pa.table({
    "txn_id":   pa.array(np.arange(N), type=pa.int64()),
    "card_id":  pa.array(rng.choice(np.arange(400_000, 410_000), N), type=pa.int64()),
    "ts":       pa.array(pd.to_datetime(rng.choice(
                    pd.date_range("2026-07-01", "2026-08-01", freq="min"), N))),
    "amount":   pa.array(np.round(rng.gamma(2.0, 40.0, N) + .5, 2)),
    "currency": pa.array(rng.choice(["EUR", "USD"], N, p=[.8, .2])),
    "country":  pa.array(rng.choice(["DE", "FR", "US", "BR"], N)),
    "mcc":      pa.array(rng.choice([5411, 5541, 3005, 5812], N), type=pa.int32()),
})
pq.write_to_dataset(tx, root_path=f"{LAKE}/txns",
                    partition_cols=["mcc"], compression="zstd")

NB = 200_000
bal = pa.table({
    "card_id":     pa.array(rng.choice(np.arange(400_000, 410_000), NB), type=pa.int64()),
    "as_of":       pa.array(pd.to_datetime(rng.choice(
                       pd.date_range("2026-07-01", "2026-07-31", freq="h"), NB))),
    "balance_eur": pa.array(np.round(rng.uniform(0, 5000, NB), 2)),
})
pq.write_table(bal, f"{LAKE}/balances.parquet", compression="zstd")

con = duckdb.connect()          # in-memory engine; attach files instead of importing!

# ---- 2. query the LAKE directly: partition pruning + projection pushdown --------
q1 = con.sql(f"""
    SELECT count(*) AS n, sum(amount) AS volume_eur
    FROM read_parquet('{LAKE}/txns/**/*.parquet', hive_partitioning = true)
    WHERE mcc = 5812 AND amount > 100
""")
print(q1)

# prove the plan pushed filters INTO the scan:
print(con.sql(f"""
    EXPLAIN SELECT sum(amount) FROM read_parquet('{LAKE}/txns/**/*.parquet')
    WHERE mcc = 5812
""").fetchall()[0][1][:600])

# ---- 3. sliding-window velocity features (fraud bread & butter) ------------------
con.sql(f"""
    CREATE OR REPLACE TEMP VIEW txns AS
    SELECT * FROM read_parquet('{LAKE}/txns/**/*.parquet',
                               hive_partitioning = true)
""")

q2 = con.sql("""
    SELECT txn_id, card_id, ts, amount,
           count(*)    OVER vel AS cnt_10m,
           sum(amount) OVER vel AS amt_10m,
           count(DISTINCT country) OVER vel AS countries_10m
    FROM txns
    WINDOW vel AS (
        PARTITION BY card_id ORDER BY ts
        RANGE BETWEEN INTERVAL 10 MINUTE PRECEDING AND CURRENT ROW)
    QUALIFY cnt_10m >= 5 AND countries_10m >= 2   -- filter window results!
""")
alerts = q2.arrow().read_all()   # .arrow() STREAMS RecordBatches; collect them
print("velocity alerts:", alerts.num_rows)

# ---- 4. ASOF JOIN: balance at the moment of each transaction ----------------------
q3 = con.sql(f"""
    SELECT t.txn_id, t.card_id, t.ts, t.amount, b.balance_eur
    FROM read_parquet('{LAKE}/txns/**/*.parquet') t
    ASOF JOIN read_parquet('{LAKE}/balances.parquet') b
      ON t.card_id = b.card_id AND b.as_of <= t.ts
    LIMIT 5
""")
print(q3)    # auditors love this: exact point-in-time state

# ---- 5. GROUP BY ALL + write results back to parquet & pandas ----------------------
daily = con.sql(f"""
    SELECT date_trunc('day', ts) AS day, currency,
           count(*) n, sum(amount) vol
    FROM read_parquet('{LAKE}/txns/**/*.parquet')
    GROUP BY ALL ORDER BY day, currency
""")
daily.write_parquet(f"{LAKE}/daily_volumes.parquet")
print(daily.df().head())

# ---- 6. persistence: durable db file + ATTACH ---------------------------------------
disk = duckdb.connect(f"{LAKE}/meridian.duckdb")
disk.execute(f"""
    CREATE OR REPLACE TABLE alerts_cache AS
    SELECT * FROM read_parquet('{LAKE}/txns/**/*.parquet') WHERE mcc = 5812
""")
disk.close()

con.execute(f"ATTACH '{LAKE}/meridian.duckdb' AS meridian;")
print("cached rows in attached db:",
      con.sql("SELECT count(*) FROM meridian.alerts_cache").fetchone())
```

Typical output (abridged):

```
┌────────┬────────────────────┐
│   n    │    volume_eur      │
│ int64  │      double        │
├────────┼────────────────────┤
│ 72,307 │ 10,948,155.44      │
└────────┴────────────────────┘

EXPLAIN (abridged):
┌───────────────────────────┐
│    UNGROUPED_AGGREGATE    │
...
┌───────────────────────────┐
│        READ_PARQUET       │   <- filters pushed into the scan,
│    Projections: amount    │      only 'amount' column decoded
...

velocity alerts: 1,8xx          <- cards w/ 5+ txns & 2+ countries / 10 min

ASOF JOIN sample:
│ txn_id │ card_id │         ts          │ amount │ balance_eur │
│  33890 │  400007 │ 2026-07-02 05:39:00 │   47.4 │     1203.63 │
...

daily volumes DataFrame head printed here
cached rows in attached db: (249920,)
```

What to internalize:

1. **No import step**: DuckDB scanned the Parquet lake directly; `EXPLAIN` shows the scan
   node carrying your pushed-down filters.
2. **Window + INTERVAL RANGE + QUALIFY** turned pages of feature code into reviewable SQL.
3. **ASOF JOIN** is a compliance-grade primitive: exact point-in-time state.
4. Every boundary crossing (`.df()`, `.arrow()`, `write_parquet`) is cheap - stay lazy
   until you must materialize.

## 8. DuckDB concurrency model (production reality)

DuckDB's concurrency model is simple but often misunderstood:

```
┌─────────────────────────────────────────────────────────┐
│  DuckDB .duckdb file                                   │
│                                                         │
│  Writers:   ONE at a time (single-writer lock)          │
│  Readers:   MANY concurrent (MVCC snapshot isolation)   │
│  Readers see: consistent snapshot from their start time │
│  Writers see: their own uncommitted changes             │
└─────────────────────────────────────────────────────────┘
```

**Key rules:**

| Rule | What it means |
|---|---|
| **Single writer** | Only one process/thread may write to a `.duckdb` file at a time. A second writer blocks or fails. |
| **Multiple readers** | Any number of read-only queries run concurrently, each seeing a consistent MVCC snapshot. |
| **Readers don't block writers** | A long analytical query doesn't prevent ETL from appending. |
| **Writers don't block readers** | New commits are invisible until the reader's snapshot era advances. |
| **No read replicas** | Unlike PostgreSQL, there's no streaming replication. Each DuckDB instance is independent. |

**Practical implications for Meridian:**

```python
# SCENARIO 1: analyst notebook + ETL — SAFE (one writer, read-only analyst)
analyst_con = duckdb.connect("meridian.duckdb", config={"access_mode": "read_only"})
etl_con     = duckdb.connect("meridian.duckdb")  # sole writer
# analyst queries see consistent pre-ETL snapshot; no conflicts

# SCENARIO 2: two ETL jobs — UNSAFE (both try to write)
job_a = duckdb.connect("meridian.duckdb")
job_b = duckdb.connect("meridian.duckdb")  # BLOCKED or ERROR on write
# FIX: serialize writes (Airflow sensor/lock) or use separate files + ATTACH

# SCENARIO 3: memory budget (shared environment)
con = duckdb.connect()
con.execute("SET memory_limit='4GB'")     # cap per-connection memory
con.execute("SET threads=4")             # limit CPU usage
```

**When you need real concurrency:**
- Many concurrent writers → PostgreSQL / Oracle (OLTP)
- Many concurrent readers at high QPS → ClickHouse / Snowflake / warehouse
- Multiple teams writing → client-server warehouse with proper locking

DuckDB shines **embedded**: one process, heavy reads, SQL over files. That describes most
analyst notebooks and most ETL steps.

## 9. DuckDB vs Spark: when to cross the boundary

Both engines read the same Parquet/Iceberg files. The question is when to graduate:

```
                 DuckDB SWEET SPARK SWEET
                    │                    │
   ┌────────────────┼────────────────────┼──────────────────┐
   │ < 1 machine    │   GRAY ZONE       │ > 1 machine      │
   │ < 10 min query │  (try DuckDB      │ Multi-TB shuffles │
   │ Interactive SQL│   first!)         │ Hour-long batch   │
   │ Ad-hoc analysis│                   │ Feature pipelines │
   └────────────────┼────────────────────┼──────────────────┘
                    │                    │
              DuckDB wins           Spark wins
```

| Decision factor | DuckDB | Spark |
|---|---|---|
| **Data size** | Fits on one machine (RAM + disk) | Multi-TB, doesn't fit in one node |
| **Query latency** | Sub-second to minutes | Minutes to hours |
| **Startup cost** | Instant (in-process library) | Seconds to minutes (JVM + cluster) |
| **Concurrency** | Single writer, many readers | Distributed, many executors |
| **Cost** | Free, runs on laptop | Cluster cost (EMR/Dataproc/GKE) |
| **SQL features** | Rich standard SQL + extensions | Spark SQL + DataFrame API |
| **Streaming** | Micro-batch via Python | Structured Streaming, Flink |
| **Existing infra** | No cluster needed | Piggybacks on existing Spark cluster |

**The hybrid pattern at Meridian:**

```
Analyst notebook (DuckDB)          Overnight ETL (Spark)
  interactive SQL over Parquet       batch features over 2B rows
  "which cards are suspicious?"     "build all velocity features"
        │                                   │
        └──── same Parquet lake ────────────┘
```

> **Rule of thumb:** Always try DuckDB first. If the query times out or you run out of RAM,
> graduate to Spark. The SQL ports almost 1:1 — the only additions are window frame syntax
> and partitioning hints.

## 10. Exercises

1. Rewrite Lesson 04's feature pipeline loop as one DuckDB query; benchmark both on 5M rows.
2. Build a "card velocity" materialized view partitioned by day, then query only yesterday.
3. Use `ASOF JOIN` to attach hourly FX rates to EUR/USD transactions and restate volumes
   in a single currency.
4. `EXPLAIN ANALYZE` two versions of the velocity query (with/without the country filter)
   and compare operator timings.
5. Install `httpfs` (`INSTALL httpfs; LOAD httpfs;`) and count rows of any public Parquet
   over HTTPS without downloading it manually.
6. Open two connections to the same `.duckdb` file: one read-only, one read-write. Run a
   long query on the read-only connection while the writer appends. Prove MVCC: the reader
   doesn't see new rows until its snapshot advances.
7. Set `memory_limit='256MB'` and `threads=1`, then run the velocity query on 5M rows.
   Observe memory usage via `duckdb.peak_memory_usage_bytes()` and explain the wall-clock
   difference vs unlimited resources.

## 11. Cheat sheet

| Task | DuckDB |
|---|---|
| Open engine | `duckdb.connect()` / `duckdb.connect("f.duckdb")` |
| Read-only mode | `duckdb.connect(path, config={"access_mode": "read_only"})` |
| Memory budget | `SET memory_limit='4GB'` / `SET threads=4` |
| Query files | `read_parquet(glob, hive_partitioning=true)`, `read_csv_auto` |
| pandas in/out | replacement scans by name; `.df()` |
| Arrow in/out | `con.register(name, table)`; `.arrow()` (zero-copy) |
| Point-in-time join | `ASOF JOIN ... ON k AND r.t <= l.t` |
| Filter window output | `QUALIFY` |
| Lazy compose | `con.sql(...).filter(...).aggregate(...).order(...)` |
| Persist result | `rel.write_parquet(p)` or CREATE TABLE AS |
| Explain plan | `EXPLAIN [ANALYZE] SELECT ...` |
| Concurrency | single writer, many readers (MVCC); use read_only for analysts |
| vs Spark | data fits one machine → DuckDB; multi-TB shuffles → Spark |

**Next:** Lesson 06 - many Parquet files are still not a TABLE. Enter Apache Iceberg:
transactions, time travel, evolution.
