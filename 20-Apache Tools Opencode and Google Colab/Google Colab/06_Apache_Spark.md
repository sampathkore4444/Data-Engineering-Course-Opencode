# Lesson 6 — Apache Spark
## The Distributed Engine for When One Machine Isn't Enough

> **Banking case:** FinBank's fraud features were built nightly on one beefy server. Then two acquisitions later, card transactions jumped from 2B to 9B rows/year and model retraining moved from weekly to hourly. A single node now takes 6+ hours. On a **10-node Spark cluster** reading the same Parquet/Iceberg lake: **35 minutes** — same code patterns, distributed. This lesson bridges "analytics on my laptop" (Lessons 1–5) to "analytics at bank scale".

---

## 1. What Is Apache Spark?

Spark is a **distributed compute engine**: it splits your data-parallel program into stages of tasks and runs them across a cluster — handling scheduling, shuffle, caching, and fault tolerance for you.

Key facts for this course:

- **APIs in Python/SQL/Scala/Java/R.** We use PySpark + Spark SQL.
- **Lazy & optimized:** you *declare* transformations (`filter`, `groupBy`); Spark builds a DAG, optimizes it (Catalyst), executes on an *action* (`count`, `show`, `write`).
- **Columnar-first:** native Parquet support (Lesson 1!) and **Apache Iceberg** integration (Lesson 4!).
- **Where it sits:** DuckDB = same mental model on *one* machine (GBs–low TBs); Spark = across *many* machines (TBs–PBs).

### Architecture in 60 seconds

```
   you (PySpark)                    CLUSTER
 ┌─────────────┐            ┌──────────────────────────┐
 │ driver      │  plans →   │ executor 1   executor 2  │
 │ DAG/Catalyst│ ─────────► │ [task][task] [task][task]│
 │ schedules   │ ◄───────── │     partial results      │
 └─────────────┘   pull     └──────────────────────────┘
        Data lives in S3/HDFS as Parquet/Iceberg tables
```

- **Driver**: your `spark` session; builds the plan, coordinates.
- **Executors**: worker JVMs running tasks on partitions of your data.
- **Shuffle**: the expensive step where data moves between executors (joins, group-bys). Minimize it — this is 80% of Spark tuning.
- **AQE (Adaptive Query Execution)**: Spark 3.x runtime optimizer — coalesces small partitions, converts sort-merge joins to broadcast joins automatically.

Deployment modes in one line each: **local[*]** (laptop, all cores), **YARN/Kubernetes** (enterprise schedulers — where bank prod clusters live), **Standalone** (Spark's own simple cluster manager), and **Spark Connect** (thin remote client over gRPC, no local driver memory concerns).

---

## 2. First Contact — PySpark in 60 Seconds

```bash
pip install pyspark   # bundles everything; runs locally, no cluster needed
```

```python
from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder
         .appName("finbank-l6")
         .master("local[*]")          # all laptop cores; yarn/k8s URLs in prod
         .getOrCreate())

df = spark.createDataFrame(
    [("A100", 42.50, "5411"), ("A200", 120.00, "5945"), ("A100", 9.99, "5812")],
    "account string, amount double, mcc string")

(df.filter(F.col("amount") > 10)
   .groupBy("account").agg(F.sum("amount").alias("spend"))
   .show())
# Lazy until .show() — nothing executed before this line.
```

Nothing here needs a cluster. The payoff comes when `df` points at **billions of rows on object storage** — identical code then runs on hundreds of executors.

### Habits that transfer from Lessons 1–4

| Lesson | In Spark |
|---|---|
| L1 Parquet | `spark.read.parquet(...)`; partition folders prune automatically |
| L2 Arrow | `spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")` makes `toPandas()`/`createDataFrame` Arrow-fast |
| L4 Iceberg | `spark.read.format("iceberg")`, time travel, `MERGE INTO`, procedure calls |

---

## 3. Core Patterns You'll Use in Banking

```python
from pyspark.sql import Window, functions as F

txns = spark.read.parquet("lake/transactions/")      # hive-partitioned (L1)

# Projection + predicate pushdown (Parquet stats skip row groups)
big = txns.where(F.col("amount") > 50_000).select("txn_id", "amount", "account")

# Window function: latest txn per account (cf. DuckDB QUALIFY, L3)
w = Window.partitionBy("account").orderBy(F.col("ts").desc())
latest = txns.withColumn("rn", F.row_number().over(w)).where(F.col("rn") == 1).drop("rn")

# Per-account 30d velocity features
feats = (txns
    .where(F.col("ts") >= F.date_sub(F.current_date(), 30))
    .groupBy("account")
    .agg(F.count("*").alias("n_txns_30d"),
         F.sum("amount").alias("vol_30d"),
         F.avg(F.when(F.col("channel") == "ECOM", 1).otherwise(0)).alias("ecom_ratio")))

# Broadcast join: tiny watchlist copied to every executor, no shuffle of big side
pep = spark.read.parquet("warehouse/pep.parquet")
flagged = feats.join(F.broadcast(pep), on="account", how="left_semi")

# SQL when you prefer it — and ALWAYS inspect the plan:
feats.createOrReplaceTempView("feats")
spark.sql("""
    SELECT account, vol_30d,
           percent_rank() OVER (ORDER BY vol_30d) AS pct_rank
    FROM feats
""").explain(True)
```

Reading `.explain(True)` like a pro:

- `FileScan parquet ... PushedFilters: [...]` → predicate pushdown works.
- `Exchange hashpartitioning(...)` → a shuffle; count them.
- `BroadcastHashJoin` → good (no big-side shuffle).
- `SortMergeJoin` → fine, but shuffles both sides; ask if broadcast applies.

---

## 4. 🏦 Banking Scenario — Nightly Fraud Feature Build at Scale

**Requirements**

- Read yesterday's card transactions from the Iceberg table (written by CDC, Lesson 4).
- Build per-account velocity features; write alerts back **transactionally** so dashboards never see half-finished results.
- Reruns must not duplicate rows (idempotent).

```python
"""
nightly_features.py — spark-submit nightly_features.py
Local mode works out of the box; for Iceberg support add:
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2
"""
from pyspark.sql import SparkSession, functions as F, Window

spark = (SparkSession.builder.appName("finbank-nightly")
         # --- Iceberg catalog: the SAME table PyIceberg/DuckDB see (L3/L4) ---
         .config("spark.sql.extensions",
                 "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
         .config("spark.sql.catalog.finbank", "org.apache.iceberg.spark.SparkCatalog")
         .config("spark.sql.catalog.finbank.type", "sql")     # demo; REST/Glue in prod
         .config("spark.sql.catalog.finbank.uri", "sqlite:///finbank_iceberg.db")
         .config("spark.sql.catalog.finbank.warehouse", "file:///tmp/iceberg_warehouse")
         .config("spark.sql.execution.arrow.pyspark.enabled", "true")   # L2!
         .getOrCreate())

TXNS   = "finbank.banking.transactions"    # created in Lesson 4
ALERTS = "finbank.banking.fraud_alerts"

# ---- 1) EXTRACT — Iceberg prunes partitions/files from the predicate -----
yday = (spark.read.format("iceberg").load(TXNS)
        .where(F.to_date("txn_ts") == F.date_sub(F.current_date(), 1)))

# ---- 2) TRANSFORM — velocity features ------------------------------------
w = Window.partitionBy("account").orderBy(F.col("txn_ts"))
feats = (yday
    .withColumn("gap_s", F.unix_timestamp("txn_ts")
                         - F.lag(F.unix_timestamp("txn_ts")).over(w))
    .groupBy("account")
    .agg(F.count("*").alias("n_txns"),
         F.avg(F.coalesce("gap_s", F.lit(3600))).alias("avg_gap_s")))
scored = feats.withColumn(
    "risk_score",
    F.least(F.lit(1.0),
            F.greatest(F.lit(0.0), 1 - F.col("avg_gap_s") / 3600)))

# ---- 3) LOAD — transactional MERGE into Iceberg (idempotent rerun!) ------
scored.createOrReplaceTempView("scored_updates")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {ALERTS} (
        account STRING, n_txns BIGINT, avg_gap_s DOUBLE, risk_score DOUBLE)
""")
spark.sql(f"""
    MERGE INTO {ALERTS} t
    USING scored_updates s
    ON t.account = s.account
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

print(spark.read.format("iceberg").load(ALERTS)
      .orderBy(F.desc("risk_score")).limit(5).toPandas())
spark.stop()
```

**Why this is production-shaped**

- The only data leaving the cluster is the final 5 rows (`toPandas` over Arrow).
- `MERGE INTO` + Iceberg = atomic upsert; rerunning yesterday's job changes nothing.
- Swap `local[*]` for a real master and the same script scales horizontally.

---

## 5. Performance Toolkit

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")          # AQE (default on 3.2+)
spark.conf.set("spark.sql.shuffle.partitions", "200")         # tune to data size!
df.persist()          # cache for reuse; df.unpersist() when done
df.explain(True)      # read the plan before blaming the hardware
```

**Shuffle-partition sizing rule:** aim for ~100–200 MB *per post-shuffle partition*. `partitions ≈ shuffle_output_bytes / 150MB`. With AQE on, start high-ish and let `spark.sql.adaptive.coalescePartitions.enabled` merge them down automatically.

**Caching levels — pick deliberately:**

| Storage level | Where data lives | When |
|---|---|---|
| `MEMORY_ONLY` | RAM, deserialized | fits comfortably; fastest reuse |
| `MEMORY_AND_DISK` (default for DataFrames) | RAM, spill to disk | general choice |
| `MEMORY_ONLY_SER` / `_SER` | RAM, serialized bytes | RAM tight, CPU cheap |
| `DISK_ONLY` | disk | huge intermediate, rare reuse |
| `persist(StorageLevel.OFF_HEAP)` | off-heap/Tachyon-style | specialized clusters |

Also worth knowing in 2026: **Spark Connect** (`Databricks Session`-style remote clients) lets a thin Python process talk to a remote cluster over gRPC — the driver no longer needs to live beside your notebook, which removes most "driver OOM" footguns for interactive work.

Tuning instincts:

| Symptom | Likely cause | Fix |
|---|---|---|
| One straggler task | data skew on a join key | salting, AQE skew join, broadcast |
| OOM on executors | too-big partitions / collect() | raise shuffle partitions; never `collect()` big data |
| Slow small files | many tiny Parquet files | compaction (`CALL ... rewrite_data_files`, L4) |
| Shuffle storm | unnecessary wide joins | broadcast small tables, filter before joining |

## 6. Cheat Sheet

```python
from pyspark.sql import SparkSession, functions as F, Window

spark = SparkSession.builder.appName("x").getOrCreate()
df  = spark.read.parquet(path)                 # also .format("iceberg").load(t)
df.where/filter/select/groupBy/agg/join/orderBy
w   = Window.partitionBy(...).orderBy(...)     # row_number/rank/lag over w
df.createOrReplaceTempView("v"); spark.sql(...)
df.write.mode("overwrite").parquet(out)        # or .format("iceberg").saveAsTable(t)
df.toPandas()                                  # Arrow-enabled; keep results SMALL
```

```sql
-- Spark SQL highlights
MERGE INTO t USING s ON ... WHEN MATCHED THEN UPDATE SET *;
CALL finbank.system.rewrite_data_files(table => 'banking.txns');
SELECT * FROM banking.transactions VERSION AS OF 1234567;
```

## 7. Pitfalls

| Pitfall | Consequence | Fix |
|---|---|---|
| `df.toPandas()` on billions of rows | driver OOM | aggregate first; export via Parquet/Iceberg |
| Default shuffle partitions (200) | tiny tasks or huge tasks | size to data: ~100–200MB per partition |
| Python UDFs everywhere | serialization overhead | prefer built-in `F.*`; pandas_udf if needed |
| Ignoring `.explain()` | blind tuning | read the plan, count shuffles |
| Skipping Iceberg compaction after MERGE | delete-file buildup, slow reads | schedule `rewrite_data_files` |

## 8. Exercises

1. Run Lesson 1's dataset through the L6 feature job in local mode; compare wall-clock vs DuckDB (L3). Where does each win?
2. Force a `SortMergeJoin`, then convert it to `BroadcastHashJoin` with `F.broadcast()`; measure both.
3. Deliberately create skew (one hot account = 50% of rows); observe the straggler; fix with salting.
4. Time-travel: write two versions of an Iceberg table from Spark; query `VERSION AS OF`.
5. Replace the rule-based risk score with a pandas_udf vectorized model call.

## 9. Quiz

1. Lazy vs eager execution in Spark — why does laziness enable optimization?
2. What is a shuffle and why is minimizing it the #1 performance lever?
3. When do you choose Spark over DuckDB for FinBank workloads?
4. How does the Iceberg connector make the nightly job idempotent?
5. Why should `toPandas()` output always be small?

*(Answers: 1. Transformations build a DAG that Catalyst can optimize/prune globally before any I/O happens; 2. Wide ops redistribute data across the network between stages — disk+network+serialization cost grows with shuffles; 3. Multi-TB+, multi-node parallelism, cluster-shared ETL schedules, integrations with streaming; DuckDB stays ideal for single-node interactive analytics; 4. Atomic snapshot commit + MERGE semantics — reruns match on keys and update instead of duplicating; 5. Everything lands on the driver's single JVM heap — it defeats distribution and OOMs at scale.)*

---

➡️ **Final step:** `07_Capstone_FinBank_Platform.md` — wire Lessons 1–6 into one working fraud-analytics platform.
