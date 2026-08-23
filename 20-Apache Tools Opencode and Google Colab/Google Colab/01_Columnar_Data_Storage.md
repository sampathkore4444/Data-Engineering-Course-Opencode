# Lesson 1 — Columnar Data Storage
## The Foundation of Modern Analytics

> **Banking case:** FinBank must scan **2 billion card transactions** every night to compute "average spend per merchant category" for its fraud model. On row storage this takes 45 minutes. After switching to columnar Parquet: **38 seconds**. This lesson explains exactly *why*.

---

## 1. What Is Columnar Storage?

A table can be physically laid out on disk in two fundamental ways.

### Row-oriented (OLTP style — e.g., PostgreSQL, MySQL, SQLite)

Data is stored one **row at a time**, contiguously:

```
File bytes:  [Txn#1 complete][Txn#2 complete][Txn#3 complete] ...

┌──────────────────────────────────────────────────────────────┐
│ txn_id │ acct_id │ amount │ mcc │ ts                  │ status│   ← row block
│      1 │ A-1001  │ 42.50  │ 5411│ 2026-08-01T09:00:01 │ OK    │
│      2 │ A-1002  │ 120.00 │ 5945│ 2026-08-01T09:00:03 │ OK    │
│      3 │ A-1003  │ 9.99   │ 5812│ 2026-08-01T09:00:07 │ FLAG  │
│    ...                                                        │
└──────────────────────────────────────────────────────────────┘
```

**Optimized for:** point reads/writes of a full row → `UPDATE accounts SET status='LOCKED' WHERE acct_id='A-1002'`. This is what banking OLTP systems need.

### Column-oriented (OLAP style — e.g., Parquet, ORC, ClickHouse)

Each **column is stored together**, contiguously:

```
txn_id : [1][2][3][4]...[N]
acct_id: [A-1001][A-1002][A-1003]...
amount : [42.50][120.00][9.99]...
mcc    : [5411][5945][5812]...
ts     : [...][...][...]...
status : [OK][OK][FLAG]...
```

**Optimized for:** aggregations/scans over a few columns across billions of rows:

```sql
SELECT mcc, AVG(amount) FROM transactions GROUP BY mcc;
```

### Row vs Column at a glance

| Dimension | Row store | Column store |
|---|---|---|
| Write pattern | single INSERT touches one location | one value per column segment |
| Point lookup / UPDATE of a full row | ✅ fast | ❌ scattered reads |
| Scan of 3/40 columns | reads 100% of bytes | reads ~7.5% of bytes |
| Compression | poor (heterogeneous bytes) | excellent (homogeneous runs) |
| Aggregation throughput | limited by memory hops | vectorized, cache-friendly |
| Typical workload | banking core, orders, app backend | fraud analytics, reporting, ML features |

> Neither wins everywhere — that's why banks run **both**: PostgreSQL for transactions, Parquet for the lake. CDC (change data capture) bridges them.

---

## 2. Why Columns Crush Rows for Analytics

### Reason 1 — You only read the columns you touch

Query `SELECT mcc, AVG(amount) ...` touches **2 columns out of 6+**.
With row storage you still read the entire wide rows (all columns' bytes) into memory and throw most away.
With column storage you read only `mcc` + `amount` byte ranges → **I/O drops proportionally to unused columns eliminated**.

> Rule of thumb: analytical queries typically touch 5–10% of columns. Row storage forces 100% I/O.

### Reason 2 — Compression is dramatically better

Values in a single column are homogeneous and repetitive → compress extremely well.

Example, the `mcc` (merchant category code) column over 10M grocery-store txns:

```
Raw int32:        5411 5411 5411 5411 5411 5411 ...
Run-Length Enc.:  (5411, ×10_000_000)              → ~8 bytes total!
Dictionary:       dict={5411:0}, codes=[0,0,0,...]
```

Real-world numbers on financial data:

| Encoding | Typical size reduction |
|---|---|
| None | 1× |
| Snappy/GZIP generic | 3–5× |
| Dictionary + RLE + bit-packing | 5–20× |

Smaller data = fewer disk pages = faster scans = lower S3 bills.

### Reason 3 — Vectorized execution & CPU cache friendliness

Column storage lets the engine process values as tight arrays (`for i in range(n): out[i] = a[i]+b[i]`) that sit sequentially in CPU cache lines — modern CPUs crunch millions of values/sec per core. Row storage jumps around memory chasing fields scattered across pages (cache misses everywhere).

### Reason 4 — Late materialization

The engine keeps only compact column indices/row-ids through filters/joins and fetches the expensive wide rows **only at the very end**, for just the surviving rows. E.g., filter 2B rows down to 300 flagged frauds, then materialize all 30 columns for those 300 rows only.

---

## 3. Inside Apache Parquet (the de-facto columnar file format)

```
┌─────────────────────────────────────────┐
│              PARQUET FILE               │
├─────────────────────────────────────────┤
│ Magic "PAR1"                            │
│ ┌────────────── Row Group 0 ─────────┐ │
│ │  Column chunk: txn_id  (pages…)    │ │
│ │  Column chunk: amount  (pages…)    │ │
│ │  Column chunk: mcc     (pages…)    │ │
│ │  Column chunk: ts      (pages…)    │ │
| │  ...one chunk per column           │ │
│ └────────────────────────────────────┘ │
│ ┌────────────── Row Group 1 ─────────┐ │
│ │ ...                                │ │
│ └────────────────────────────────────┘ │
│ Footer: schema + per-column-chunk      │
│   statistics (min/max/null_count/distinct) │
│ Magic "PAR1"                           │
└─────────────────────────────────────────┘
```

Key concepts:

- **Row group**: horizontal slice of the table (typically 128MB–1GB compressed). Unit for parallelism & pruning.
- **Column chunk**: all values of one column within a row group; stored together → sequential I/O.
- **Page**: smallest unit inside a chunk (~1MB), individually compressed/encoded. Three kinds matter: **data pages** (the values), **dictionary pages** (the lookup table for dictionary-encoded columns, written first), and **index pages** (rare; offset/bloom indexes).
- **Statistics** (min/max per chunk): enable **predicate pushdown / partition pruning** — skip whole chunks without reading them.
- **Footer**: read last! Contains the schema, row-group/column-chunk map, and per-chunk statistics — that's how engines plan a scan by reading a few KB before touching any data.

```sql
-- Engine reads footer stats: "chunk has amount min=0.01 max=19.99"
-- Query needs amount > 5000  →  ENTIRE CHUNK SKIPPED. Zero I/O.
SELECT * FROM txns WHERE amount > 5000;
```

> 💡 **Sizing math:** with 20M rows × 100 bytes/row ≈ 2 GB uncompressed → ZSTD ≈ 400 MB. Four 128MB–512MB row groups = good parallelism on a 4–8 core machine. Too many small row groups → stats overhead + tiny I/O requests; too few → no parallelism.

### Common encodings you should know by name

| Encoding | Best for | Example |
|---|---|---|
| Plain | unique-ish values | txn ids |
| Dictionary (RLE_DICTIONARY) | low-cardinality (`status`, `branch_code`, `currency`) | `["OK","FLAG"]` + `[0,0,1,...]` |
| Run-Length Encoding | long runs of same value | sorted dates |
| Delta encoding | sorted ints/timestamps | `[1001,+0,+1,+0,...]` stores deltas |
| Bit-packing | small ints fit in few bits | boolean flags |
| Byte Stream Split | floats (interleaves mantissa/exponent bytes) | amounts improve compression |
| **Bloom filters** | high-cardinality equality lookups (`txn_id = X`) | per-chunk "maybe present" filter lets engines skip chunks even when min/max can't |

Bloom filter tip: min/max stats are useless for columns where every chunk contains nearly all values (e.g., `account_no` spread evenly). A bloom filter (opt-in via `bloom_filter_enabled` / `write_page_index`) still answers "is this txn_id in this chunk?" cheaply.

### Compression codecs

- **SNAPPY** — fast, moderate ratio (default). Good for hot data.
- **ZSTD** — best ratio/speed balance today. Recommended default for lakes. Tunable: level 1 (faster) to ~19 (smaller) via `compression_level`; level 3 is the sweet spot for most lakes.
- **GZIP** — old, slow; avoid except for compliance export.
- **BROTLI** — max ratio when size matters more than speed.

Rule of thumb: compression saves disk but costs CPU *on every read*. Hot tables queried hundreds of times/day may justify SNAPPY; cold archival data always deserves ZSTD/BROTLI.

### Nested data: repetition & definition levels

Parquet stores nested structures (structs/lists/maps) using **Dremel-style definition levels** (how deep is this field defined?) and **repetition levels** (at which list index does it repeat?). You rarely touch these manually, but they're why Parquet handles JSON-like schemas losslessly.

---

## 4. 🏦 Banking Scenario — Nightly Fraud Feature Build

**FinBank's problem**

- Core banking DB (PostgreSQL, row-based): 2B card transactions, ~40 columns.
- Fraud team runs nightly: `SELECT mcc, customer_segment, AVG(amount), COUNT(*) FROM transactions WHERE ts BETWEEN ... GROUP BY 1,2`.
- On PostgreSQL with row pages: reads **entire 40-column rows** → 45 min, saturates production DB (unacceptable).
- Also required by RBI/Fed regulators: point-in-time snapshots ("show me the data exactly as it was on June 30") — impossible with in-place updates.

**Solution architecture (this lesson's part)**

1. CDC (Debezium) streams changes from PostgreSQL.
2. Batch job converts to **columnar Parquet, ZSTD, dictionary-encoded, partitioned by date**, row groups ≈ 512MB, min/max stats enabled.
3. Fraud job reads only `mcc, customer_segment, amount, ts` chunks (~15% of bytes) → 38 seconds.
4. Partition pruning means a "yesterday-only" backfill reads 1 day folder instead of 7 years.
5. Regulator snapshot = immutable dated folders (later upgraded to Iceberg time travel in Lesson 4).

---

## 5. 💻 End-to-End Python

```bash
pip install pyarrow pandas numpy faker
```

### Step 0 — Generate synthetic bank transaction data

```python
"""
generate_txns.py — create a realistic card-transaction dataset.
"""
import numpy as np
import pandas as pd

rng = np.random.default_rng(42)
N = 20_000_000                      # 20M transactions

MCCS = ["5411", "5541", "5812", "5945", "6011", "4829", "5999", "7011"]  # grocers, fuel, restaurants...
BRANCHES = ["MUM", "DEL", "BLR", "CHN", "HYD"]
CHANNELS = ["POS", "ECOM", "ATM", "UPI"]
STATUS   = ["OK", "FLAGGED", "REVERSED", "FAILED"]

df = pd.DataFrame({
    "txn_id":     np.arange(1, N + 1, dtype="int64"),
    "account_no": rng.choice([f"A{i:07d}" for i in range(250_000)], N),
    "amount":     np.round(rng.lognormal(mean=5.0, sigma=1.2, size=N), 2),
    "mcc":        rng.choice(MCCS, N, p=[.28,.12,.22,.08,.06,.05,.14,.05]),
    "branch":     rng.choice(BRANCHES, N),
    "channel":    rng.choice(CHANNELS, N, p=[.45,.25,.10,.20]),
    "is_intl":    rng.integers(0, 2, N, dtype="int8"),
    "status":     rng.choice(STATUS, N, p=[.93,.04,.02,.01]),
    "txn_ts":     pd.to_datetime("2026-01-01") +
                  pd.to_timedelta(rng.integers(0, 365*24*3600, N), unit="s"),
})
print(df.head())
df.to_parquet("txns_raw.parquet")   # quick save; optimized write below
```

### Step 1 — Write *optimized* columnar Parquet (the way pros do it)

```python
"""
write_parquet_optimized.py — run AFTER generate_txns.py (reuses `df`).
"""
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd

table = pa.Table.from_pandas(df, preserve_index=False)

pq.write_table(
    table,
    "txns_bank.parquet",
    compression="zstd",            # best ratio/speed codec
    use_dictionary=["mcc","branch","channel","status","account_no"],
    write_statistics=True,         # min/max per chunk → predicate pushdown
    row_group_size=5_000_000,      # ~row groups tuned for parallel scans
    data_page_size=1 << 20,        # 1MB pages
)
```

### Step 2 — Prove the wins: size, projection, pushdown

```python
"""
benchmark_columnar.py — measure the three columnar superpowers.
"""
import os, time
import pyarrow.parquet as pq
import pyarrow.compute as pc

t = pq.read_table("txns_bank.parquet")
raw_size = os.path.getsize("txns_raw.parquet")
par_size = os.path.getsize("txns_bank.parquet")
print(f"rows={t.num_rows:,}  raw-parquet={raw_size/1e6:.1f} MB  "
      f"optimized={par_size/1e6:.1f} MB  ({par_size/raw_size:.2f}x)")

# ---- Superpower 1: PROJECTION — read only 2 columns -------------------
t0 = time.perf_counter()
subset = pq.read_table("txns_bank.parquet", columns=["mcc", "amount"])
print(f"projection read (2 cols): {time.perf_counter()-t0:.2f}s")

# ---- Superpower 2: PREDICATE PUSHDOWN — stats skip chunks -------------
t0 = time.perf_counter()
big = pq.read_table(
    "txns_bank.parquet",
    filters=[("amount", ">", 50_000)],       # footer min/max prune row-groups
)
print(f"pushdown read (>50k, {big.num_rows:,} rows): {time.perf_counter()-t0:.2f}s")

# ---- Superpower 3: VECTORIZED compute on Arrow arrays ------------------
t0 = time.perf_counter()
avg_by_mcc = t.group_by("mcc").aggregate([("amount", "mean")]).sort_by("amount_mean")
print(f"group-by avg(amount) by mcc: {time.perf_counter()-t0:.2f}s")
print(avg_by_mcc.to_pandas())

# ---- Metadata inspection: see encodings & stats like a pro -------------
pf = pq.ParquetFile("txns_bank.parquet")
print(pf.metadata)
md = pf.metadata.row_group(0).column(3)          # 'mcc'
print(md.statistics)                             # min/max/null_count
print("encodings:", md.encodings)                # RLE_DICTIONARY etc.
```

**Typical output (varies by machine):**

```
projection read (2 cols): 0.31s            # vs reading all columns: ~1.9s
pushdown read: 0.18s                       # skipped row groups entirely
group-by avg(amount) by mcc: 0.41s         # vectorized over 20M rows
<pyarrow._parquet.FileMetaData>
  created_by: parquet-cpp-arrow version ...
  num_columns: 9
  num_rows: 20000000
  num_row_groups: 4
```

### Step 3 — Partitioning (organize files so queries prune by path)

```python
"""
partitioned_write.py — date-partitioned lake layout.
"""
import pyarrow as pa, pyarrow.parquet as pq

table = pa.Table.from_pandas(df.assign(
    year  = df.txn_ts.dt.year,
    month = df.txn_ts.dt.month,
))
pq.write_to_dataset(
    table, "lake/transactions/",
    partition_cols=["year", "month"],     # hive-style folders:
    compression="zstd",
)                                          # lake/transactions/year=2026/month=1/part-0.parquet

# Reading one month only touches that folder:
jan = pq.read_table("lake/transactions/",
                    filters=[("month", "=", 1)])
print(jan.num_rows)
```

Resulting layout (what S3/GCS/Azure Blob will look like):

```
lake/transactions/
├── year=2025/month=8/part-0.parquet
├── ...
├── year=2026/month=1/part-0.parquet   ← "January report" reads ONLY these folders
└── year=2026/month=2/part-0.parquet
```

---

## 6. Cheat Sheet

```python
# WRITE
pq.write_table(table, path, compression="zstd",
               use_dictionary=True, write_statistics=True,
               row_group_size=5_000_000)
pq.write_to_dataset(table, root, partition_cols=["year","month"])

# READ
tbl  = pq.read_table(path, columns=[...], filters=[("amount",">",5000)])
pf   = pq.ParquetFile(path)          # .metadata, .read(row_group=i)
df   = pd.read_parquet(path, filters=...)

# INSPECT
pf.metadata.num_rows, pf.metadata.num_row_groups
pf.metadata.row_group(i).column(j).statistics
pf.schema_arrow                      # logical schema
```

**Decision rules**

- Analytics/reports/lake → columnar (Parquet). OLTP/app backend → row store.
- Compression default: **ZSTD**. Low-cardinality strings: always dictionary.
- Partition by coarse time (day/month) — never by high-cardinality IDs.
- Target row groups 128MB–1GB; align with your parallelism.
- **Sort data within files** by your most common filter column (e.g., `ts`): tight per-row-group min/max ranges make predicate pushdown skip far more chunks. Unsorted rows → every chunk spans the full value range → stats useless.

## 7. Pitfalls

| Pitfall | Consequence | Fix |
|---|---|---|
| Tiny files (one per minute) | S3 request storms, metadata overload | Compact/batch writes |
| One giant file, no row-group tuning | No parallelism | Set row_group_size |
| Partitioning by account_no | Millions of folders | Partition by date |
| Re-writing whole table for appends | Costly | Use Iceberg (Lesson 4) |
| Ignoring statistics | Full scans | write_statistics=True |
| Writing unsorted data | Min/max stats can't prune anything | Sort by filter column before writing |
| Storing money as float32/float64 | Rounding drift in sums | decimal128(18,2) or integer minor units |

## 8. Exercises

1. Generate 50M rows; benchmark full-read vs `columns=["amount","mcc"]`. What % time saved?
2. Write the same data with `compression=None`, `"snappy"`, `"gzip"`, `"zstd"`; tabulate sizes & read times.
3. Show (via `pf.metadata.row_group(i).column(j).statistics`) that a filtered read skips row groups.
4. Break it deliberately: write 10,000 tiny files vs 10 large ones; compare `read_table` times.

## 9. Quiz

1. Your query uses 3 of 40 columns. Roughly how much less I/O does columnar give (ignoring compression)?
2. What file structure enables predicate pushdown in Parquet?
3. Name an encoding suited to `status ∈ {OK, FLAGGED}` and one suited to sorted timestamps.
4. Why are row groups sized in hundreds of MB?
5. When would you *not* choose columnar storage?

*(Answers: 1. You read only 3/40 ≈ 7.5% of the bytes — roughly a 13× I/O reduction; 2. Footer per-column-chunk min/max statistics; 3. Dictionary/RLE, Delta encoding; 4. Sequential I/O + parallel scan units + amortized footer/stats overhead; 5. Frequent single-row updates/deletes with full-row reads — classic OLTP.)*

---

➡️ **Next:** `02_Apache_Arrow.md` — columnar doesn't stop at disk. Arrow makes it the universal *in-memory* language too.
