# Lesson 01 — Columnar Storage Fundamentals

> **Meridian Trust Bank case study, Part 1**: The fraud team wants "average transaction amount
> per merchant category for the last 3 years" — 2 billion rows, 60 columns. The core banking
> database takes 40 minutes to answer because it reads every column of every row.
> After this lesson you will know why — and how to fix it.

---

## Table of Contents

| Section | Topic |
|---|---|
| [1](#1-how-data-physically-lands-on-disk) | How data physically lands on disk |
| [2](#2-row-oriented-storage-the-oltp-classic) | Row-oriented storage (the OLTP classic) |
| [3](#3-column-oriented-storage-the-olap-workhorse) | Column-oriented storage (the OLAP workhorse) |
| [4](#4-column-encodings--the-real-magic) | Column encodings — the real magic |
| [5](#5-architecture-where-each-storage-style-lives-in-a-bank) | Architecture: where each storage style lives in a bank |
| [6](#6-banking-scenario-walkthrough) | Banking scenario walkthrough |
| [7](#7-end-to-end-example-feel-the-difference-in-python) | End-to-end example: feel the difference in Python |
| [8](#8-exercises-do-these) | Exercises |
| [9](#9-cheat-sheet) | Cheat sheet |

---

## 1. How data physically lands on disk

Disks (and SSDs) do not understand rows or columns. They understand **fixed-size blocks/pages**
(4 KB–8 KB typically) and can only transfer whole blocks. Every storage format is a strategy for
answering one question:

> **"Which bytes do I have to read off the disk to answer this query?"**

The fewer irrelevant bytes you read, the faster the query. That single idea explains almost
everything in this course.

```
Disk reality:
┌────────┬────────┬────────┬────────┬────────┐
│ block 0│ block 1│ block 2│ block 3│ ...    │   each 4–8 KB
└────────┴────────┴────────┴────────┴────────┘
     ▲ disk transfers whole blocks, never single fields
```

---

## 2. Row-oriented storage (the OLTP classic)

A row store writes complete records one after another.

**Table: `card_transactions`**

| txn_id | account_id | ts | merchant | mcc | amount | currency | channel | fraud_flag |
|---:|---:|---|---|---:|---|---|---|---|
| 1 | A-1001 | 2026-08-01 09:14 | ACME Grocers | 5411 | 42.10 | USD | POS | false |
| 2 | A-1002 | 2026-08-01 09:15 | SkyAir | 3005 | 512.00 | EUR | ECOM | true |
| 3 | A-1001 | 2026-08-01 09:17 | MetroFuel | 5541 | 61.35 | USD | POS | false |

### Physical layout on disk

```
Block 1: [ txn_id=1|acct=A-1001|ts=09:14|mcc=5411|amt=42.10|... ]   ← full row 1
         [ txn_id=2|acct=A-1002|ts=09:15|mcc=3005|amt=512.00|...]   ← full row 2
         [ txn_id=3|acct=A-1001|ts=09:17|mcc=5541|amt=61.35|... ]   ← full row 3
Block 2: [ row 4 ][ row 5 ][ row 6 ] ...
```

### Strengths of row storage

1. **Fast point reads/writes**: `SELECT * FROM accounts WHERE id = 'A-1001'` touches one location.
2. **Fast OLTP mutations**: UPDATE/INSERT affect one contiguous record; locks are row-granular.
3. **Write pattern matches business events**: a payment happens → write its whole row once.
4. **Row-level recovery**: WAL/undo logs map naturally to whole records.

This is why **core banking ledgers run on row stores** (Oracle, PostgreSQL, DB2). A transfer is
a point mutation against two rows — row layout is optimal.

### Weakness of row storage for analytics

```sql
-- Fraud team query: average amount by MCC over 2B rows
SELECT mcc, AVG(amount) FROM card_transactions GROUP BY mcc;
```

With row layout the engine must read **every block**, parse out only `mcc` and `amount` from each
row, and discard the other ~58 columns. You may need 2 of 60 columns but you pay I/O for 60/60.
Typically <5% of bytes read are actually useful. This is exactly Meridian's 40-minute problem.

```
Row store reading 2 columns out of 60:

Block: [c1 c2 c3 c4 c5 c6 c7 c8 ... c60] [c1 c2 c3 ... c60] ...
        ██                                  ██          ██ = wanted columns
→ must stream ALL bytes through CPU just to extract 2 fields per row
```

---

## 3. Column-oriented storage (the OLAP workhorse)

Store each column's values together, contiguously.

### Physical layout on disk

```
txn_id      : [1][2][3][4][5]...
account_id  : [A-1001][A-1002][A-1001][A-1003]...
ts          : [09:14][09:15][09:17][09:22]...
merchant    : [ACME Grocers][SkyAir][MetroFuel][ACME Grocers]...
mcc         : [5411][3005][5541][5411]...
amount      : [42.10][512.00][61.35][18.20]...
fraud_flag  : [F][T][F][F]...
              ▲ values of ONE column live together across ALL rows
```

### Why analytics loves columns

1. **Projection pushdown (read less)** — the query above reads only `mcc` + `amount` files/segments.
   60-column table, 2-column query → ~96% less data touched *before any CPU work*.

2. **Compression loves repetition** — similar values sit adjacent (`5411, 5411, 5411...`),
   so encodings like Run-Length and Dictionary encoding become extremely effective
   (Section 4). Banks routinely see **5–20× compression** vs raw text.

3. **Vectorized execution (CPU efficiency)** — process a *batch* of N values per function call
   instead of an IF/else per row. Tight loops over contiguous arrays keep the CPU cache hot and
   let the compiler auto-vectorize (SIMD: one instruction processes 8 int64s).

4. **Late materialization** — filter/compress/aggregation happens on narrow column data;
   full rows are assembled *only* at the very end, for the few survivors.

```
Column store answering the same query:

mcc    : ████████ (read)
amount : ████████ (read)
ts     : ──────── (skipped!)
...48 more columns skipped...

I/O ≈ 2/60 of the row-store plan. Then SIMD scans compress further.
```

### Weaknesses of columns

- Point lookup of one wide record = many seeks (one per column).
- Updates are expensive → most columnar systems prefer **append-only + rewrite** patterns,
  which is fine for immutable event streams like card transactions.
- Deletes usually become "delete masks"/tombstones rather than immediate rewrites.

> **Rule of thumb**: OLTP (many small reads/writes of whole entities) → rows.
> OLAP (aggregations/scans over few columns of billions of rows) → columns.
> Modern banks run BOTH and copy data between them (that pipeline is your lakehouse).

---

## 4. Column encodings — the real magic

Encoding transforms raw values into compact representations *before* generic compression.
This is where columnar wins most of its size advantage.

### 4.1 Run-Length Encoding (RLE)

Replace repeats of the same value with `(value, count)`.

```
Raw:    USD, USD, USD, USD, USD, USD, USD, USD, EUR, EUR
RLE:    (USD, ×8), (EUR, ×2)                    → ~90% smaller
```

Perfect for low-cardinality columns: `currency`, `branch_code`, `country`, `channel`.

### 4.2 Dictionary encoding

Assign integer IDs to distinct values; store IDs + a dictionary.

```
merchant: [ACME Grocers][SkyAir][MetroFuel][ACME Grocers][SkyAir]
dict:     0→ACME Grocers, 1→SkyAir, 2→MetroFuel
stored:   [0][1][2][0][1]           ← tiny ints instead of long strings
```

Great when distinct values ≪ total rows (merchants, cities, currencies).
Bad when every value is unique (txn GUIDs) — fall back to plain/delta.

### 4.3 Delta encoding

Store differences between consecutive values instead of absolute values.

```
account_id ints: 100001, 100002, 100003, 101000, 101001
delta:           100001, +1, +1, +997, +1     → small numbers fit in 1–2 bytes
```

Ideal for sorted/near-sorted numeric keys and timestamps (event streams arrive time-ordered!).

### 4.4 Bit-packing / FOR (frame-of-reference)

Values that fit in fewer bits get stored with fewer bits.
E.g., `mcc` codes fit in 16 bits → store as uint16 even if schema says int32.

```
amounts in cents, all < 256: [42, 199, 61] → pack into 1 byte each (vs 8-byte int64)
```

### 4.5 Encoding + compression = compound effect

Encodings produce highly regular byte streams which general-purpose compressors
(Snappy/LZ4 for speed, ZSTD/Gzip for ratio) then crush further.

```
raw CSV line  : "2026-08-01T09:14:00,A-1001,POS,USD,5411,42.10,false"   ~65 bytes
columnar+enc  : dictionary id 3, delta-ts, RLE'd flags                  ~6 bytes typical
```

### Choosing encodings (what engines decide per column)

| Column trait | Best encoding |
|---|---|
| Few distinct values (currency, status) | RLE / Dictionary |
| Many repeats, medium cardinality (merchant) | Dictionary |
| Sorted or near-sorted numerics/timestamps | Delta + bit-packing |
| High-cardinality random (GUIDs) | Plain + light compression |
| Free text | Plain + ZSTD |

---

## 5. Architecture: where each storage style lives in a bank

```
                        MERIDIAN TRUST — STORAGE TOPOLOGY

  OLTP SIDE (rows)                          ANALYTICS SIDE (columns)
┌───────────────────────┐                 ┌──────────────────────────────────┐
│ Core Banking (Oracle) │  nightly CDC/   │ Card txns Parquet lake (Iceberg) │
│  - accounts           │  batch export   │  - fraud scans                   │
│  - ledger entries     │ ──────────────▶ │  - AML history                   │
│  - payments           │                 │ Risk marts (Parquet)             │
└───────────────────────┘                 │ Regulatory snapshots             │
        ▲                                 └──────────────────────────────────┘
        │ point updates                              ▲
  tellers, ATMs, apps                         analysts, ML, BI
  (need fast single-row ops)                  (scan few cols, billions of rows)
```

Key insight: **you don't replace the row store**. You add a columnar analytical copy that is
optimized for questions the row store was never designed to answer.

---

## 6. Banking scenario walkthrough

**Fraud velocity feature**: "count of transactions per card in the last 10 minutes".

- Input: 2B rows/year, 60 columns.
- Row store: scan all blocks, parse 60 fields/row → tens of minutes.
- Column store: read `card_id`, `ts`, `amount` segments only (~3/60 of bytes), dictionaries
  already built → seconds.

**AML report**: "total EUR outflow per customer per month, 5 years".
Only `account_id`, `ts`, `currency`, `amount`, `direction` needed → columnar shines again.

**Why not just index the row store?** 

Secondary indexes help equality lookups, but a GROUP BY
over 2B rows still moves every row through the engine. Compression alone cuts I/O 10×, and
vectorized aggregation multiplies CPU throughput another order of magnitude. Indexes complement;
they don't substitute.

### pls explain Secondary indexes help equality lookups, but a GROUP BY over 2B rows still moves every row through the engine. Compression alone cuts I/O 10×, and vectorized aggregation multiplies CPU throughput another order of magnitude. Indexes complement; they don't substitute.

```
This is one of the most misunderstood concepts in database engineering. Let me break down **why indexes fail at analytics** with concrete numbers and internal mechanics.

## The Index Fallacy: What They Actually Do

### Secondary Index = Point Lookup Optimizer
```
CREATE INDEX idx_card ON transactions(card_id);

-- This is fast (index works):
SELECT * FROM transactions WHERE card_id = '1234-5678';
-- Index: B-tree traversal (3-4 I/Os) → fetch 1 row
```

### Secondary Index = Useless for Aggregation
```
-- This is STILL slow (index doesn't help):
SELECT card_id, COUNT(*) 
FROM transactions 
WHERE ts > '2024-01-01' 
GROUP BY card_id;
-- Index? Only helps if you have (card_id, ts) and even then...
```

## Why GROUP BY Scans Every Row (Even With Indexes)

### Scenario: 2B rows, 60 columns, 5 years of data

**Row Store with Secondary Index:**
```
Index B-tree: card_id → row pointers (100GB)
┌─────────────────────────────────────────┐
│ card_id: 1234 → [Block 42, Offset 8192] │
│ card_id: 1234 → [Block 87, Offset 4096] │
│ card_id: 5678 → [Block 12, Offset 2048] │
└─────────────────────────────────────────┘

To GROUP BY card_id:
1. Index doesn't have COUNT(*) precomputed
2. Must follow EVERY pointer in the index (2B entries)
3. For each pointer: random I/O to fetch the row
   └─ Block 42 → read 8KB block
   └─ Block 87 → read 8KB block (different location!)
   └─ ... 2B random seeks
```

**The Math:**
- 2B rows × (at least) 1 random seek each
- HDD: 10ms/seek → 2B × 0.01s = **20,000,000 seconds** (231 days)
- SSD: 0.1ms/seek → 2B × 0.0001s = **200,000 seconds** (55 hours)
- Even with NVMe (0.02ms) → **11 hours of pure seek time**

### Column Store: No Indexes, But Sequential Scan

**Columnar Layout (Parquet/ORC):**
```
File 1: card_id column (compressed) → 2B × 8 bytes = 16GB
File 2: ts column (compressed)      → 2B × 8 bytes = 16GB  
File 3: amount column (compressed)  → 2B × 8 bytes = 16GB
(Other 57 columns: skipped entirely)

To GROUP BY card_id:
1. Read card_id column sequentially → ONE sequential read (16GB)
2. Read ts column sequentially → ONE sequential read (16GB)
3. No random seeks, just streaming from disk
```

**The Math:**
- Sequential read: 500MB/s (NVMe) → 32GB / 500MB/s = **64 seconds**
- Random I/O eliminated. That's **600x faster** than index approach.

## The GROUP BY Execution: What Actually Happens

### Row Store Execution Plan (Index-assisted)
```
Aggregate (GROUP BY card_id)
  └─ Index Scan (idx_card)         ← Reads 2B index entries
       └─ Table Access by Row ID   ← 2B random I/Os
            └─ Project columns     ← Parses 60 columns each time
```
**I/O: 2B random reads + 120B column parsing operations**

### Column Store Execution Plan (No Index)
```
Aggregate (GROUP BY card_id)
  └─ Vectorized Scan (card_id)     ← Sequential read, decompress
       └─ Vectorized Scan (ts)      ← Sequential read, decompress
            └─ Filter (ts>...)
                 └─ Hash Aggregate  ← SIMD hash table (no disk I/O)
```
**I/O: 2 sequential reads + 32GB total**

## The "Compression Alone Cuts I/O 10×" Explained

### Dictionary Encoding (Columnar Magic)
```
Raw card_id values: 2B rows × 16 bytes (UUID) = 32GB

Dictionary:
┌────────────┬──────┐
│ card_id    │ code │
├────────────┼──────┤
│ 1234-5678  │  1   │
│ 5678-1234  │  2   │
│ ...        │ ...  │ (10M unique cards)
└────────────┴──────┘

Encoded column: [1, 2, 1, 3, 1, 2, ...] 
  → 2B rows × 4 bytes = 8GB (4x compression)

Run-length encoding (RLE) for sorted data:
If transactions sorted by time, card_id becomes:
[1,1,1,1,2,2,2,2,2,3,3,3,3,3,3,...]
  → RLE: [(1, 1M), (2, 2M), (3, 500K), ...]
  → 10M entries × 12 bytes = 120MB (267x compression!)
```

**Result:** 32GB card_id column → 120MB after compression. That 32GB sequential read becomes **0.12GB**—a 267x I/O reduction.

### Vectorized Aggregation: CPU Throughput Boost

**Row Store Aggregation (per row, scalar):**
```c
for (i=0; i<2B; i++) {
    hash = murmur3(row[i].card_id);  // 1 operation
    slot = hash & mask;              // 1 operation
    while (collision) { ... }        // branching
    count[slot]++;                   // 1 operation
}
```
**CPU:** 2B iterations × ~50 instructions = 100B instructions
**Time:** 100B / 3GHz = 33 seconds (if perfect pipeline, but it's not)

**Column Store Aggregation (vectorized, SIMD):**
```c
// Load 8 card_id codes at once (AVX2)
__m256i codes = _mm256_load_si256(&card_codes[i]);
// 8-way parallel hash computation
__m256i hashes = _mm256_hash_epi32(codes);  // SIMD hash
// 8-way parallel bucket updates (masked)
__m256i buckets = _mm256_load_si256(&counts[hashes]);
buckets = _mm256_add_epi32(buckets, _mm256_set1_epi32(1));
_mm256_store_si256(&counts[hashes], buckets);
```
**CPU:** 2B/8 = 250M SIMD iterations × 10 instructions = 2.5B instructions
**Time:** 2.5B / 3GHz = 0.83 seconds (10-15x faster)

## Real-World Impact: 5-Year AML Report

### Query: 
```sql
SELECT 
    account_id, 
    SUM(amount) as total_outflow 
FROM transactions 
WHERE currency = 'EUR' 
  AND direction = 'OUT' 
  AND ts BETWEEN '2019-01-01' AND '2024-01-01'
GROUP BY account_id;
```

### Row Store with Indexes:
```
Table: 2B rows × 60 cols × 200 bytes = 400GB
Indexes:
  - idx_currency (on 1 column) → 16GB
  - idx_direction (on 1 column) → 16GB  
  - idx_ts (on 1 column) → 16GB

Execution:
1. Index Scan on idx_ts: get 2B row pointers (can't use others)
2. For each pointer: fetch 200-byte row (random I/O)
   → 2B × 200 bytes = 400GB random reads (worst-case)
3. Parse 60 columns per row to find currency, direction, amount
   → 2B × 60 field extractions
4. Hash aggregate in memory (if fits) or spill to disk

Time: 400GB / (200MB/s random NVMe) = 2000s + CPU time
→ ~33 minutes
```

### Column Store (No Indexes):
```
Column files (compressed):
  - account_id: 8GB (dictionary + RLE)
  - ts: 12GB (delta encoding)
  - currency: 1GB (dictionary: "EUR","USD"...)
  - direction: 1GB (dictionary: "IN","OUT")
  - amount: 16GB (plain, maybe compression)

Execution:
1. Read currency column: 1GB sequential (filter to EUR)
   → SIMD compare: 2B rows in 0.5s
   → Creates bitmap: 2B bits = 250MB
2. Read direction column: 1GB sequential
   → SIMD compare with bitmap mask
   → Updated bitmap: 250MB
3. Read ts column: 12GB sequential
   → SIMD range check with bitmap
   → Updated bitmap
4. ONLY read account_id and amount for matching rows
   → Suppose 10% match = 200M rows
   → account_id: 800MB sequential
   → amount: 1.6GB sequential
5. Vectorized aggregation: 200M rows in 0.5s

Time: (1+1+12+0.8+1.6)GB / 3GB/s = 5.4s + CPU 2s
→ ~7.4 seconds (268x faster than row store)
```

## The "Index Complement" Part: When They Work Together

**Hybrid approach (what modern databases do):**

```
Column Store with Secondary Indexes:
1. Use columnar min/max statistics (zone maps) to skip entire segments
   → If a segment's max(ts) < 2019, skip 1M rows instantly
   → This is like an "automatic index" on every column

2. Use bloom filters on frequently filtered columns
   → currency = 'EUR' → bloom filter says "this segment has no EUR"
   → Skip entire 100MB segment without reading

3. Use secondary index only for point lookups
   → SELECT * FROM transactions WHERE transaction_id = 12345;
   → Index points to exact row in columnar storage (fast)
```

## The Mind-Blowing Truth

**For analytics, reading 400GB sequentially is FASTER than reading 2B random rows with indexes.** 

Why?
- Sequential NVMe: 3-7 GB/s → 400GB = 1-2 minutes
- Random NVMe: 0.1-0.2 GB/s → 400GB = 30-60 minutes
- Plus CPU parsing overhead: 2B row-by-row operations vs. vectorized

**Indexes are for OLTP (point queries). Column stores are for OLAP (aggregations).** You can't make one do the other's job efficiently—it's like using a sports car for cargo shipping.

## The Compression + Vectorization Synergy

The real killer is the **combination**:

1. **Compression reduces I/O by 10-100x** → less data to move
2. **Vectorization processes data 10x faster** → fewer CPU cycles per byte
3. **Column projection reads only needed columns** → 3/60 = 5% of data

**Multiplicative effect:** 100x (compression) × 10x (vectorization) × 20x (column projection) = **20,000x faster** than row store with indexes.

That's how 2B rows become queryable in seconds instead of days. END END END END END

---

## 7. End-to-end example: feel the difference in Python

We simulate both layouts on synthetic bank data and measure what it costs to answer one
analytical question under each design.

```python
"""
lesson01_layout_benchmark.py
Simulates ROW-layout vs COLUMN-layout access cost on banking data.
No external deps beyond numpy/pandas/pyarrow.
"""
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import time

rng = np.random.default_rng(42)

N_ROWS = 1_000_000                     # 1M synthetic card transactions
# (kept small so the *row-store simulation* fits in laptop RAM:
#  it materializes every value as a Python object, exactly the cost
#  a real row engine pays per row)

# ---- Build a realistic 12-column banking table -------------------------------
txns = pd.DataFrame({
    "txn_id":       np.arange(N_ROWS),
    "account_id":   rng.integers(100_000, 999_999, N_ROWS),
    "ts":           pd.date_range("2024-01-01", periods=N_ROWS, freq="ms"),
    "merchant":     rng.choice(["ACME Grocers", "SkyAir", "MetroFuel",
                                "CafeNord", "GigaMall"], N_ROWS),
    "mcc":          rng.choice([5411, 3005, 5541, 5812, 5912], N_ROWS),
    "amount_cents": rng.integers(100, 500_000, N_ROWS),
    "currency":     rng.choice(["USD", "EUR", "GBP"], N_ROWS, p=[.6,.3,.1]),
    "channel":      rng.choice(["POS", "ECOM", "ATM"], N_ROWS),
    "country":      rng.choice(["US", "DE", "FR", "GB"], N_ROWS),
    "status":       rng.choice(["SETTLED", "PENDING"], N_ROWS, p=[.95,.05]),
    "fraud_flag":   rng.choice([False, True], N_ROWS, p=[.998, .002]),
    "memo":         rng.choice(["", "coffee", "fuel", "groceries"], N_ROWS),
})

arrow_tbl = pa.Table.from_pandas(txns, preserve_index=False)

def measure(label, fn):
    t0 = time.perf_counter()
    result = fn()
    dt = time.perf_counter() - t0
    print(f"{label:<38} {dt*1000:9.1f} ms")
    return result, dt

# ---- Simulate ROW STORE: must touch every column of every row ---------------
# We emulate the physical cost by converting EVERY column to python-object
# parsing cost proportional to total width (this is what a row engine pays).
def row_store_query():
    # 'materialize' all 12 columns, then aggregate the 2 we care about
    all_cols = {name: arrow_tbl.column(name).to_pylist()      # ← full-row decode
                for name in arrow_tbl.column_names}
    from collections import defaultdict
    s = defaultdict(int); c = defaultdict(int)
    for i in range(N_ROWS):
        s[all_cols["mcc"][i]] += all_cols["amount_cents"][i]
        c[all_cols["mcc"][i]] += 1
    return {k: s[k]/c[k]/100 for k in s}

row_res, row_dt = measure("ROW layout (touch all 12 columns)", row_store_query)

# ---- Simulate COLUMN STORE: touch ONLY mcc + amount --------------------------
def col_store_query():
    mcc    = arrow_tbl.column("mcc")               # zero-copy slice of memory
    amount = arrow_tbl.column("amount_cents")
    mean_per_mcc = {}
    for value in mcc.unique().to_pylist():
        mask = pc.equal(mcc, value)                # vectorized, no row decoding
        mean_per_mcc[value] = pc.mean(
            amount.filter(mask)).as_py() / 100
    return mean_per_mcc

col_res, col_dt = measure("COLUMN layout (touch 2 of 12)", col_store_query)

print(f"\nSpeedup from column-only access: ~{row_dt/col_dt:.0f}x")
assert abs(row_res[5411] - col_res[5411]) < 0.01, "results must match!"

# ---- Bonus: show compression potential of encodings --------------------------
import pyarrow.parquet as pq
pq.write_table(arrow_tbl, "/tmp/opencode/txns_default.parquet",
               compression="NONE", use_dictionary=False)          # raw columnar
pq.write_table(arrow_tbl, "/tmp/opencode/txns_encoded.parquet",
               compression="ZSTD", use_dictionary=True)           # encoded+compressed
import os
raw = os.path.getsize("/tmp/opencode/txns_default.parquet")
enc = os.path.getsize("/tmp/opencode/txns_encoded.parquet")
csv_size = len(txns.to_csv(index=False).encode())                  # naive row/text

print(f"\nCSV (text rows):        {csv_size/1e6:8.1f} MB")
print(f"Parquet raw columns:    {raw/1e6:8.1f} MB")
print(f"Parquet encoded+ZSTD:   {enc/1e6:8.1f} MB   "
      f"({csv_size/enc:.1f}x smaller than CSV)")
```

Sample output on a laptop (1M rows):

```
ROW layout (touch all 12 columns)        2016.9 ms
COLUMN layout (touch 2 of 12)              29.7 ms

Speedup from column-only access: ~68x

CSV (text rows):            90.3 MB
Parquet raw columns:        92.8 MB
Parquet encoded+ZSTD:       12.3 MB   (7.4x smaller than CSV)
```

Two independent effects stack up:

1. **Reading fewer columns** → ~68× CPU/I-O win for the analytical query.
2. **Encoding + compression** → ~7× storage win.

Note the honest detail in the output: *uncompressed* columnar (`txns_default.parquet`)
is roughly CSV-sized here because most columns are **random** integers/strings with no
structure to exploit. The dramatic shrink appears only once encodings
(dictionary/RLE/delta) plus ZSTD are enabled — repetition and locality are what compress.
Real banking columns (currencies, merchants, sorted timestamps) have far more structure
than this synthetic data, so real ratios of 10–30× are common.

---

## 8. Exercises (do these!)

1. Change the benchmark so the query needs **8 of 12 columns**. Does the column advantage shrink?
   At what ratio does row layout win?
2. Make `mcc` 90% `5411`. Re-run with RLE-style logic — how much faster is counting now?
3. Sort the table by `account_id` before writing parquet with `compression="ZSTD"`.
   Compare file size vs unsorted. Explain using delta encoding intuition.
4. Add a `txn_uuid` random-string column. Which encoding becomes useless for it?

---

## 9. Interview questions: columnar storage in banking

### Concept 1: Why columnar storage?

**Q1: Why does a fraud query over 2 billion rows finish in seconds with columnar storage but takes minutes with row storage?**
```
A: Columnar storage reads only the columns needed (projection pushdown). A fraud query like "average amount per merchant category" reads only `amount` and `mcc` — 2 columns out of 60. Row storage reads all 60 columns for every row. The I/O reduction is 30×, which directly translates to query speed.
```
**Q2: A banking analyst says "our OLTP database is fast, why can't we use it for analytics?" How do you explain the difference?**
```
A: OLTP databases are optimized for point lookups (row storage, indexes, locks). Analytics require scanning millions of rows across a few columns — columnar storage reads only what's needed, uses vectorized SIMD instructions, and avoids loading irrelevant data. The access patterns are fundamentally different.
```
**Q3: How does dictionary encoding help with the `currency` column that has only 3 values (EUR, USD, GBP) across 2 billion rows?**
```
A: Dictionary encoding stores a small dictionary (3 entries) and replaces each occurrence with a 1-byte index. Instead of storing "EUR" (3 bytes) 700 million times, you store index 0 (1 byte) 700 million times. The data footprint drops by 66%, and filtering becomes a simple integer comparison.
```
**Q4: What happens to columnar performance when a query needs ALL columns (e.g., `SELECT *`)?**
```
A: The columnar advantage shrinks because you must read all columns anyway. However, columnar still helps because: (1) compression is better per-column, (2) vectorized processing still works, and (3) you can skip columns added later. The crossover point is around 40-60% of columns — beyond that, row storage may be competitive.
```
**Q5: In a banking data lake with 500 columns, an analyst queries 5 columns. What's the theoretical I/O reduction?**
```
A: 500/5 = 100× reduction in I/O. In practice, you get 30-80× because of metadata overhead, compression differences, and page alignment. But the key insight is that columnar storage scales with query selectivity — the fewer columns you read, the faster it gets.
```
### Concept 2: Encodings and compression

**Q1: A transaction table has a `timestamp` column sorted in ascending order. Which encoding exploits this, and why?**
```
A: Delta encoding. Instead of storing full timestamps (8 bytes each), you store the difference between consecutive values (typically small integers). If transactions are 1 second apart, you store 1 (4 bytes) instead of 1690000000 (8 bytes). Combined with run-length encoding on the deltas, this can compress timestamps by 10×.
```
**Q2: Why is run-length encoding ineffective on a `transaction_uuid` column but highly effective on a `status` column?**
```
A: UUIDs are unique — no consecutive repeats, so RLE provides no benefit. The `status` column has low cardinality (e.g., 'completed', 'pending', 'failed') with long runs of the same value. RLE stores (value, count) pairs, which is extremely compact for repeated values.
```
**Q3: How does compression interact with predicate pushdown in a columnar format?**
```
A: Columnar statistics (min/max per page) allow skipping entire pages without decompressing. If a page's max amount is 500 and you filter `amount > 1000`, the page is skipped entirely. This means compression doesn't hurt filter performance — you skip compressed pages, not decompress-and-skip.
```
**Q4: A bank stores 100 TB of transaction data. After columnar encoding and compression, what size reduction is realistic?**
```
A: Typical reduction is 5-10× for numeric data (delta + RLE + compression) and 3-5× for strings (dictionary + compression). Realistic estimate: 100 TB → 10-20 TB. The exact ratio depends on data cardinality, sort order, and compression algorithm (ZSTD is typically best).
```
**Q5: Why does sorting data by `card_id` before writing improve compression?**
```
A: Sorting groups similar values together, which improves delta encoding (small differences between consecutive values) and run-length encoding (longer runs of identical values). For a `card_id` column, sorting means all transactions for card 300001 are adjacent, making the card_id deltas small and compressible.
```
### Concept 3: OLTP vs OLAP

**Q1: A bank's core banking system uses PostgreSQL (row store). Why can't they just add columnar indexes to make analytics fast?**
```
A: Columnar indexes help but don't solve the fundamental problem: row stores still read entire rows from disk, and index maintenance overhead hurts write performance. Columnar storage is purpose-built for analytics: no per-row overhead, vectorized processing, and page-level skipping. The architectural difference matters more than indexes.
```
**Q2: How does a bank typically split workloads between OLTP and OLAP systems?**
```
A: OLTP (PostgreSQL/Oracle) handles real-time transactions: account balances, payment processing, card authorizations. OLAP (columnar lake/warehouse) handles analytics: fraud detection, regulatory reporting, customer analytics. Data flows from OLTP → ETL → OLAP via CDC or batch extracts. The two systems serve different access patterns.
```
**Q3: What's the "embarrassingly parallel" property of columnar scans, and why does it matter for banking analytics?**
```
A: Columnar scans are embarrassingly parallel because each column can be processed independently — no dependencies between values. For a 2-billion-row scan, you can split across 100 cores, each processing 20 million values with SIMD instructions. This is why analytical queries scale linearly with cores, unlike row-oriented operations.
```
**Q4: A real-time fraud system needs both point lookups (is this card blocked?) AND analytics (what's the average transaction amount?). How do you architect this?**
```
A: Use a lambda or kappa architecture: OLTP store (Redis/PostgreSQL) for real-time point lookups, columnar store (Iceberg/DuckDB) for analytics. CDC streams from OLTP to the columnar store. The fraud system queries both: Redis for instant card checks, columnar for pattern analysis. This gives you the best of both worlds.
```
**Q5: Why do modern lakehouses (Databricks, Snowflake) blur the OLTP/OLAP boundary?**
```
A: Lakehouses add ACID transactions to columnar storage via table formats (Iceberg/Delta). This allows UPDATE/DELETE operations that were previously OLTP-only. The result: a single system handles both analytics and light transactional workloads. The trade-off: write performance is lower than true OLTP, but the operational simplicity of one system often wins.
```
---

## 10. Cheat sheet

| Term | Meaning |
|---|---|
| Page/block | Minimum unit of disk I/O (KBs); formats pack data into pages |
| Projection pushdown | Read only referenced columns |
| Predicate pushdown | Apply filters early, skip whole data ranges (Lesson 02) |
| Vectorized execution | Process batches of values per CPU call, enabling SIMD |
| Late materialization | Reassemble full rows only at the end of the plan |
| RLE / Dict / Delta | Encodings exploiting repeats / low cardinality / sortedness |
| OLTP vs OLAP | Transactional point ops (rows) vs analytical scans (columns) |

**Next:** Lesson 02 turns these ideas into the concrete industry-standard container:
**Apache Parquet** — row groups, column chunks, pages, and a metadata footer that makes
predicate pushdown automatic.
