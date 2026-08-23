# Lesson 13 — Apache Spark: When One Laptop Isn't Enough

> **Meridian Trust Bank case study, Part 13**: The fraud model just went to production and the
> data science team wants per-card rolling features over **two billion** card events. DuckDB
> - hero of Lessons 05-11 - hits a hard wall: one machine's RAM. The lake on S3 (Lesson 12)
> is fine; the *compute* must scale out. Enter Apache Spark, the heavyweight engine that reads
> the exact same Parquet keys - because the files are the contract.

---

## 1. Concept: cluster computing without the cluster mystique

A Spark job is a **lazy plan** that a *driver* builds and *executors* execute in parallel
partitions. Nothing runs until an *action* demands a result:

```
   you write:   txns.filter(...).groupBy(...).agg(...)      TRANSFORMATIONS (lazy)
                    │  driver builds a DAG of stages
                    ▼
        .count() / .show() / .write()                     ACTION -> jobs run
                    │
   ┌──────────────┴───────────────┐
   ▼                              ▼
 executor 1                   executor N          each takes whole partitions:
 scan parquet ┐               scan parquet ┐      filter/aggregate live where the
 local shuffle├─> stage 2 <───local shuffle┘       bytes are; shuffles move only
 aggregate   ┘     │            aggregate   ┘      what cross-partition logic needs
                   ▼
              result to driver
```

| | DuckDB (L05) | Spark |
|---|---|---|
| Process model | single embedded process | driver + many executors |
| Data fit | must fit (mostly) in one machine's RAM/disk | spreads across a fleet |
| Optimization | vectorized interpreter | Catalyst: plan rewriting + codegen |
| Sweet spot | interactive SQL on one node | batch/ETL/feature pipelines at TB-PB scale |
| Reads | Parquet/Arrow/Iceberg | the same Parquet/Iceberg + JDBC, Kafka, ... |

The magic for us: `master("local[2]")` runs the whole architecture in one JVM with two worker
threads. Every concept - partitions, lazy plans, shuffles, pushdown - is real Spark code that
transfers unchanged to a 500-node cluster by swapping one config string.

## 2. Banking scenario: velocity features at scale

The AML team's card-testing detector needs, per transaction: how many authorizations did this
card see in the trailing 10 minutes, and for how much money? Over billions of rows:

- One machine cannot hold one card's history? It doesn't have to: Spark **hash-partitions by
  `card_id`**, so all rows of a card land on the same executor, and window frames are computed
  locally there.
- The fraud team also refuses to maintain a second copy of the lake. Spark reads Meridian's
  existing hive-partitioned Parquet directly - written here by PyArrow, exactly like Lesson 12
  wrote it to S3.
- Outputs go back as partitioned Parquet that DuckDB (dashboards) and Iceberg (governed
  tables) can adopt later.

## 3. End-to-end example

```python
"""
lesson13_spark_lab.py
PySpark local[2] over the SAME Parquet lake pattern used since Lesson 02:
hive-partitioned reads, pushdown (proof via EXPLAIN), sliding velocity windows,
sorted/partitioned write-back. No cluster needed - 'local[N]' fakes one in-process.
Deps: pip install pyspark pyarrow pandas numpy
"""
import contextlib
import io
import os
import shutil
import time
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

LAKE = "/tmp/opencode/sparklake"
shutil.rmtree(LAKE, ignore_errors=True)
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

# ---- 1. write the lake WITH PYARROW (fast bulk I/O), consume with SPARK ----------
# teaching point: files are the contract - any writer, any reader.
# timestamps are generated as a POISSON PROCESS per card (exponential gaps),
# plus injected burst clusters = classic card-testing attacks -> velocity alerts.
rng = np.random.default_rng(9)
CARDS, PER = 10_000, 30                       # 30 txns per card
N = CARDS * PER                               # 300_000 rows
gap = rng.exponential(600, N)                 # ~1 txn / 10 min baseline
hot = rng.random(N) < 0.30                    # 30% of gaps are BURSTS
gap[hot] = rng.uniform(2, 20, int(hot.sum())) # seconds apart (card testing!)

c = np.cumsum(gap)                            # per-card clocks via block resets
blocks = np.arange(0, N, PER)
block_start = np.concatenate([[0.0], c[blocks[1:] - 1]])   # clock resets here
wake_up = rng.uniform(0, 5 * 86_400, CARDS)   # each card is active on its own
# schedule inside the week Mon-Fri
tsec = c - np.repeat(block_start, PER) + np.repeat(wake_up, PER)
order = np.argsort(tsec)                      # one timeline for the whole bank
assert tsec.max() < 7 * 86_400                # sanity: one week of traffic

EPOCH_US = np.datetime64("2026-07-01", "s").astype("int64") * 1_000_000
tx = pa.table({
    "txn_id":   pa.array(np.arange(N), type=pa.int64()),
    "card_id":  pa.array(np.repeat(np.arange(CARDS) + 300_000, PER)[order],
                         type=pa.int64()),
    # MICROSECONDS since epoch: Spark reads timestamp(us), not ns
    "ts":       pa.array(EPOCH_US + (tsec[order] * 1e6).astype("int64"),
                         type=pa.timestamp("us")),
    "amount":   pa.array(np.round(rng.gamma(2.0, 40.0, N)[order] + .5, 2)),
    "currency": pa.array(rng.choice(["EUR", "USD"], N, p=[.8, .2])[order]),
    "country":  pa.array(rng.choice(["DE", "FR", "US", "BR"], N)[order]),
    "mcc":      pa.array(rng.choice([5411, 5541, 3005], N)[order],
                         type=pa.int32()),
})
span_h = (tx.column("ts").to_pandas().max()
          - tx.column("ts").to_pandas().min()).total_seconds() / 3600
print(f"lake: {N:,} txns | {CARDS:,} cards | {span_h:.1f}h of traffic")
pq.write_to_dataset(tx, f"{LAKE}/txns", partition_cols=["mcc"],
                    compression="zstd")

# ---- 2. Spark enters --------------------------------------------------------------
from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F

spark = (SparkSession.builder.appName("meridian-fraud")
         .master("local[2]")
         .config("spark.driver.memory", "900m")
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.ui.enabled", "false")
         .getOrCreate())
print(f"\nSpark {spark.version} | default parallelism = "
      f"{spark.sparkContext.defaultParallelism}")

txns = spark.read.parquet(f"{LAKE}/txns")          # hive partitions auto-detected
txns.printSchema()

# ---- 3. pushdown proof: filters reach the SCAN, not the executor -------------------
cold = (txns.filter(F.col("mcc") == 3005)
            .filter(F.col("amount") > 400)
            .select("txn_id", "amount"))
buf = io.StringIO()
with contextlib.redirect_stdout(buf):
    cold.explain(mode="formatted")
for line in buf.getvalue().splitlines():
    if "PushedFilters" in line or "ReadSchema" in line:
        print(line.strip())

# ---- 4. sliding 10-minute velocity windows (fraud bread & butter, L05 redux) ------
# RANGE frames need a NUMERIC order column -> unix timestamp
w = (Window.partitionBy("card_id")
           .orderBy(F.unix_timestamp("ts"))
           .rangeBetween(-600, 0))                  # trailing 10 minutes

velocity = (txns.withColumn("cnt_10m", F.count("*").over(w))
                .withColumn("amt_10m", F.round(F.sum("amount").over(w), 2))
                .filter(F.col("cnt_10m") >= 5))

t0 = time.time()
alerts = velocity.select("txn_id", "card_id", "ts", "cnt_10m", "amt_10m")
n_alerts = alerts.count()
print(f"\nvelocity alerts: {n_alerts:,} ({time.time()-t0:.1f}s incl. JIT warmup)")
alerts.orderBy(F.desc("amt_10m")).show(5, truncate=False)

# ---- 5. cluster-style aggregation + hive-partitioned write-back -------------------
daily = (txns.groupBy(F.to_date("ts").alias("day"), "currency")
             .agg(F.count("*").alias("n"),
                  F.round(F.sum("amount"), 2).alias("vol")))
(daily.sortWithinPartitions("day")               # sort inside files: better
      .write.mode("overwrite")                   # delta/RLE downstream (L01!)
      .partitionBy("day")                        # hive dirs: day=2026-07-01/ ...
      .parquet(f"{LAKE}/daily_volumes"))

out_files = [f for r, _, fs_ in os.walk(f"{LAKE}/daily_volumes") for f in fs_
             if f.endswith(".parquet")]
print(f"daily_volumes written: {len(out_files)} parquet file(s), "
      f"hive-partitioned by day")
spark.read.parquet(f"{LAKE}/daily_volumes").orderBy("day", "currency") \
     .show(6, truncate=False)
spark.stop()
```

Sample output (abridged):

```
lake: 300,000 txns | 10,000 cards | 126.5h of traffic

Spark 4.2.0 | default parallelism = 2
root
 |-- txn_id: long (nullable = true)
 |-- card_id: long (nullable = true)
 |-- ts: timestamp_ntz (nullable = true)
 |-- amount: double (nullable = true)
 |-- currency: string (nullable = true)
 |-- country: string (nullable = true)
 |-- mcc: integer (nullable = true)

PushedFilters: [IsNotNull(amount), GreaterThan(amount,400.0)]
ReadSchema: struct<txn_id:bigint,amount:double>

velocity alerts: 41,192 (2.6s incl. JIT warmup)
+------+-------+--------------------------+-------+-------+
|txn_id|card_id|ts                        |cnt_10m|amt_10m|
+------+-------+--------------------------+-------+-------+
|163539|302840 |2026-07-03 19:45:18.830812|14     |1760.53|
|116301|309401 |2026-07-03 00:41:02.883948|13     |1732.34|
...

daily_volumes written: 6 parquet file(s), hive-partitioned by day
+--------+-----+----------+----------+
|currency|n    |vol       |day       |
+--------+-----+----------+----------+
|EUR     |42956|3453195.79|2026-07-01|
|USD     |10745|855225.24 |2026-07-01|
...
```

## 4. Reading the physical plan like a senior engineer

The EXPLAIN excerpt is the whole performance story:

```
(1) Scan parquet
    PartitionFilters: [isnotnull(mcc#6), (mcc#6 = 3005)]     <- whole dirs skipped
    PushedFilters:    [IsNotNull(amount), GreaterThan(amount,400.0)]  <- into Parquet
    ReadSchema: struct<txn_id:bigint,amount:double>           <- 2 of 7 columns read
```

Three optimizations fire before a single row crosses the wire:

1. **Partition pruning** - `mcc = 3005` matches the hive directory layout, so entire subtrees
   (`mcc=5411/`, `mcc=5541/`) are never listed or opened.
2. **Predicate pushdown** - `amount > 400` travels into the Parquet reader and consults
   per-row-group min/max statistics (Lesson 02); whole row groups vanish from the job.
3. **Column pruning (ReadSchema)** - only `txn_id` and `amount` byte ranges are fetched;
   the other five columns cost zero I/O.

The `ColumnarToRow` operator above the scan exists because Spark's internal execution is still
row-based (unless using Gluten/Photon-class engines); the scan stays columnar for exactly these
three wins.

## 5. Production notes you will get asked in interviews

- **Shuffle partitions**: default 200 is wrong for everyone. Start near
  `shuffle blocks / target-128MB`, then let Adaptive Query Execution (AQE,
  `spark.sql.adaptive.enabled=true`, default-on in Spark 3.x+) coalesce them at runtime -
  our tiny lab pinned 4 explicitly for predictable output files.
- **Skew is the silent killer**: one merchant doing 40% of volume means one partition does 40%
  of the work. Detect via stage-level task-time imbalance; fix with salting or
  AQE skew-join handling. Window functions partitioned by `card_id` inherit this risk.
- **RANGE window frames require numeric order keys** - hence `unix_timestamp(ts)` above.
  ROWS frames would misbehave with duplicate/out-of-order timestamps; RANGE over seconds is
  the semantically correct choice for "trailing N minutes".
- **Write for the next reader**: `sortWithinPartitions` clusters values inside each file so
  downstream encodings (dictionary/RLE, Lesson 01) compress harder; `partitionBy` produces
  hive layouts every engine already understands (Lessons 02/12).
- **When NOT to use Spark**: if data fits on one machine, DuckDB starts faster, optimizes
  comparably, and needs no JVM. Graduate to Spark when RAM walls, multi-hour batch windows,
  or existing clusters demand it - the query logic ports almost 1:1.

## 6. DuckDB vs Spark: the complete decision matrix

Both engines read the same Parquet/Iceberg files (Lessons 02, 06). Here is the definitive
guide for when to use which, with banking-specific reasoning:

| Factor | DuckDB wins | Spark wins |
|---|---|---|
| **Data fits in RAM** | ✅ yes, embedded | ❌ overhead unjustified |
| **Data > 1 machine** | ❌ hard wall | ✅ distributes across fleet |
| **Query < 5 minutes** | ✅ sub-second to minutes | ❌ JVM startup alone takes seconds |
| **Query > 10 minutes** | ⚠️ may hit RAM limits | ✅ parallel shuffles |
| **Interactive/ad-hoc** | ✅ instant startup, REPL-friendly | ❌ cluster provisioning |
| **Overnight batch ETL** | ✅ if single-node sufficient | ✅ if multi-TB or existing cluster |
| **Feature engineering** | ✅ for < 10M rows per card | ✅ for 2B+ rows, distributed windows |
| **Existing Spark cluster** | ❌ why not use it? | ✅ piggyback on infra |
| **Cost** | ✅ free, runs on laptop | ❌ cluster cost (EMR/Dataproc) |
| **JVM dependency** | ✅ none | ❌ requires Java + Spark jars |
| **Concurrency** | single writer, MVCC reads | distributed, many executors |
| **SQL features** | rich standard SQL + extensions | Spark SQL + DataFrame API |

**The hybrid pattern at Meridian (production reality):**

```
                    ┌─────────────────────────────────────┐
                    │         SAME PARQUET LAKE            │
                    │    s3://meridian/card_txns/          │
                    └──────────┬──────────┬───────────────┘
                               │          │
              ┌────────────────┤          ├────────────────┐
              ▼                ▼          ▼                ▼
         DuckDB            DuckDB      Spark            Spark
      (analyst)          (ETL step)  (overnight)     (feature eng)
      interactive SQL    transform   2B row velocity  distributed
      over Parquet       between     window funcs     model training
                         zones
```

**Portability: the SQL almost 1:1**

```sql
-- DuckDB (Lesson 05)
SELECT card_id, count(*) AS cnt_10m
FROM txns
WHERE ts >= now() - INTERVAL '10' MINUTE
GROUP BY card_id
HAVING count(*) >= 5;

-- Spark (Lesson 13) - same logic, different window syntax
SELECT card_id, count(*) AS cnt_10m
FROM txns
WHERE ts >= current_timestamp() - INTERVAL 10 MINUTES
GROUP BY card_id
HAVING count(*) >= 5;

-- The core SQL (GROUP BY, HAVING, WHERE) is identical.
-- Only window frame syntax differs (RANGE vs ROWS, INTERVAL syntax).
```

> **Interview answer**: "We use DuckDB for interactive analytics and lightweight ETL
> because it starts instantly, needs no cluster, and reads Parquet natively. When data
> exceeds one machine's RAM or we need distributed window functions over billions of rows,
> we graduate to Spark. Both engines read the same Parquet/Iceberg files — the lake is
> the contract, the engine is interchangeable."

## 7. Exercises

1. Re-run with `.master("local[1]")` vs `local[4]` and time the velocity action; explain why
   scaling stops helping once the scan is I/O-bound.
2. Add a `merchant_id` column where one merchant carries 30% of rows; observe the shuffle-stage
   skew in task durations (set `spark.ui.enabled=true` and open the Spark UI), then mitigate
   with AQE or salting.
3. Replace the RANGE frame with `rowsBetween(-100, 0)` and find a card where both give
   different answers; justify which is correct for "trailing 10 minutes".
4. Write `velocity` results back with `.partitionBy("day")` after adding a `day` column, then
   query them from DuckDB (Lesson 12 settings) against the same directory - one lake, three
   engines across the course.
5. Point Spark at Lesson 12's moto S3 bucket instead of local disk by configuring
   `s3a://` Hadoop-AWS options; confirm identical alert counts. That is storage/compute
   decoupling in one exercise.
6. Run the velocity query in DuckDB (Lesson 05) on 300K rows, then in Spark on 300K rows.
   Measure wall-clock time for each. Now scale to 3M rows — which engine handles the 10×
   increase better and why?
7. Write the same SQL query in DuckDB and Spark SQL syntax. Identify the syntactic
   differences (INTERVAL, window functions, GROUP BY ALL). Which differences matter?

## 8. Cheat sheet

| Task | PySpark |
|---|---|
| Session | `SparkSession.builder.master("local[N]").getOrCreate()` |
| Read hive dataset | `spark.read.parquet(path)` - partition columns inferred |
| Prove pushdown | `df.explain(mode="formatted")` - look for PartitionFilters/PushedFilters/ReadSchema |
| Trailing window | `Window.partitionBy(k).orderBy(unix_timestamp(ts)).rangeBetween(-600, 0)` |
| Aggregate | `df.groupBy(...).agg(F.count("*"), F.sum(...))` |
| Sorted output files | `df.sortWithinPartitions(...).write.partitionBy(...).parquet(path)` |
| Tune shuffle | `spark.sql.shuffle.partitions` (+ AQE coalescing) |
| Stop cleanly | `spark.stop()` - releases executor threads and temp dirs |
| vs DuckDB | data fits one machine → DuckDB; multi-TB shuffles → Spark; SQL ports 1:1 |

---

*This closes the extended course: format (Parquet), memory (Arrow), embedded engine (DuckDB),
table format (Iceberg), protocol (Flight SQL), architecture (lakehouse), storage (S3) and
scale-out compute (Spark). Same banking story throughout: prove the ledger, catch the fraud,
serve any client - on open infrastructure you fully understand.*
