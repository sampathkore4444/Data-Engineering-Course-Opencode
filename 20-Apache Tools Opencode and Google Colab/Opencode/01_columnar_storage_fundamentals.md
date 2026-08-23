# Lesson 01 — Columnar Storage Fundamentals

> **Meridian Trust Bank case study, Part 1**: The fraud team wants "average transaction amount
> per merchant category for the last 3 years" — 2 billion rows, 60 columns. The core banking
> database takes 40 minutes to answer because it reads every column of every row.
> After this lesson you will know why — and how to fix it.

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

**Why not just index the row store?** Secondary indexes help equality lookups, but a GROUP BY
over 2B rows still moves every row through the engine. Compression alone cuts I/O 10×, and
vectorized aggregation multiplies CPU throughput another order of magnitude. Indexes complement;
they don't substitute.

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

## 9. Cheat sheet

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
