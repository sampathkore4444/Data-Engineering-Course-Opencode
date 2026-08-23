# Lesson 2 — Apache Arrow
## The Universal In-Memory Columnar Format

> **Banking case:** FinBank's risk pipeline moves a 10 GB transaction table through 5 stages: extract → validate → feature-engineer → score → load. With Pandas↔Spark↔DB handoffs, each hop **serializes to JSON/CSV and re-parses**, costing ~40 minutes total and 6 copies in RAM. With Arrow: **zero-copy** between DuckDB, Pandas, Polars and Parquet — the whole pipeline runs in 4 minutes with 2 copies.

---

## 1. What Is Apache Arrow?

Apache Arrow is a **specification for an in-memory columnar data representation**, plus implementations (C++, C/Glib, Go, Java, JS, Rust, Python bindings) that are byte-compatible across languages.

Think of it as:

- **Parquet's twin sibling**: Parquet = *serialized on-disk* columnar format; Arrow = *live in-RAM* columnar format. (Same founding community; `pyarrow` reads/writes both.)
- A **standard memory layout** so any two systems can share tabular data without conversion.
- A toolbox: compute kernels, IPC format, Flight RPC (Lesson 5), datasets API, C Data Interface.

### The core idea: one canonical layout

```
Arrow array of int64 [10, 20, 30]:

Validity bitmap : [1, 1, 0]        ← bit per slot: is it non-null?
Buffer (values) : |10|20|??|       ← fixed-width slots, CPU-aligned
```

Every implementation (Java service, Python notebook, Rust engine, C++ database) agrees on this exact buffer arrangement — so handing data over = handing over pointers.

### Why not just use Pandas?

| Aspect | Pandas | Arrow |
|---|---|---|
| Layout | object wrappers, block manager, mixed | strict typed columnar buffers |
| Nulls | sentinel hacks (`NaN`, `None`, `NaT`) | validity bitmap (first-class) |
| Strings | Python objects (~50+ bytes each!) | UTF-8 in contiguous buffers + offsets |
| Zero-copy | ✗ (copies everywhere) | ✓ |
| Cross-language | Python only | 10+ languages, identical bytes |

---

## 2. Anatomy of Arrow Arrays & Tables

```python
import pyarrow as pa

amounts = pa.array([120.50, None, 9.99], type=pa.float64())
mccs    = pa.array(["5411", "5945", "5812"], type=pa.string())

txn_batch = pa.RecordBatch.from_arrays(
    [amounts, mccs],
    names=["amount", "mcc"],
)

table = pa.Table.from_batches([txn_batch])
print(table.schema)
# amount: double
# mcc: string
```

Key container types:

- **`pa.Array`** — one column, chunked internally.
- **`pa.ChunkedArray`** — logical column split into chunks (streaming friendly).
- **`pa.RecordBatch`** — collection of equal-length arrays (a "columnar mini-table"); the unit of streaming.
- **`pa.Table`** — schema + chunked columns (the workhorse).

Why chunks exist: data usually *arrives* in pieces (file fragments, network batches). A `Table` just keeps a list of those pieces instead of paying for one big copy:

```python
t = pa.table({"amount": [1.0, 2.0]})          # 1 chunk
t2 = pa.concat_tables([t, t, t])               # 3 chunks, zero copies
print(t2.num_chunks)                           # → 3
big = t2.combine_chunks()                      # explicit rechunk when you
                                               # need contiguous memory
```

Most compute kernels handle chunks transparently; `combine_chunks()` mainly matters before `to_numpy(zero_copy_only=True)` or C-data-interface handoffs.

### Common types you'll use in banking schemas

```python
pa.int64(), pa.float64(), pa.string(),
pa.timestamp("us", tz="UTC"), pa.date32(),
pa.decimal128(18, 2),          # money! exact arithmetic, no float drift
pa.dictionary(pa.int32(), pa.string()),   # dictionary-encoded categories
pa.list_(pa.float64()),        # e.g., last-N transaction amounts
pa.struct([("city", pa.string()), ("country", pa.string())]),
```

String type notes worth knowing: classic `string()` uses 32-bit offsets (≤2 GB per array); `large_string()` uses 64-bit offsets for monster columns; modern Arrow ≥16 also offers **`string_view()`**, which stores short strings inline and makes substring/equality checks faster without materializing.

> 💰 **Banking rule:** store monetary values as `decimal128(precision, scale)` or integer minor units (paise/cents), never float64, if downstream does accounting math. Analytics aggregates tolerate floats; ledgers do not.

---

## 3. Zero-Copy: The Superpower

"Zero-copy" means passing Arrow data between systems **without deserializing or copying bytes** — they read the same buffers.

### 3.1 Arrow ↔ Pandas ↔ NumPy ↔ DuckDB ↔ Polars ↔ Parquet

```python
# pip install pyarrow pandas numpy duckdb polars
import pyarrow as pa, pyarrow.parquet as pq
import pandas as pd, numpy as np
import duckdb, polars as pl

df_pd = pd.DataFrame({
    "acct": ["A1", "A2", "A3"],
    "balance": [1000.0, 2500.5, 99.99],
})

tbl_arrow = pa.Table.from_pandas(df_pd)      # pandas → arrow
df_back   = tbl_arrow.to_pandas()            # arrow → pandas
np_vals   = tbl_arrow["balance"].to_numpy(zero_copy_only=True)  # arrow → numpy view!

duckdb.sql("SELECT sum(balance) FROM tbl_arrow").show()   # duckdb queries arrow directly!
pl_df     = pl.from_arrow(tbl_arrow)         # polars takes arrow natively
pq.write_table(tbl_arrow, "accounts.parquet")  # arrow → parquet on disk
```

No serialization to strings, no intermediate CSVs. One memory layout, five ecosystems.

### 3.1.1 Pandas 2.x: Arrow as a *storage engine* for DataFrames

Modern pandas can back its columns with Arrow arrays directly — fixing most of the pain in the comparison table above (nulls, strings, memory):

```python
df_arrow_backed = df_pd.convert_dtypes(dtype_backend="pyarrow")
print(df_arrow_backed.dtypes)
# acct      string[pyarrow]        ← real strings, ~10 bytes vs ~50+
# balance   double[pyarrow]

# Or per-column at construction:
pd.DataFrame({"mcc": ["5411", "5945"]}, dtype="string[pyarrow]")
```

Why this matters in banking pipelines:

- **Memory**: object-dtype string columns shrink 3–10×.
- **Nulls**: proper bitmask instead of `NaN`/`NaT` sentinel hacks.
- **Interop**: `pa.Table.from_pandas()` on Arrow-backed frames is near zero-copy; no object→Arrow conversion step.

Rule of thumb: keep data **Arrow end-to-end** (Parquet → Table → compute), and only touch pandas (ideally Arrow-backed) at the visualization/export edge.

### 3.2 IPC: streaming & memory-mapping Arrow files

Arrow defines its own wire/file formats:

- **IPC stream** (`new_stream` / record batches) — endless stream, perfect for sockets.
- **IPC file** (random-access `.arrow` file) — footer + buffers.
- **Memory mapping** — OS maps the file into your address space; "reading" a 40 GB file costs zero copy until you touch pages.

```python
# Write an .arrow file once...
with pa.OSFile("big_txns.arrow", "wb") as sink:
    with pa.ipc.new_file(sink, table.schema) as writer:
        writer.write_table(table)

# ...and mmap it: instant "load", no full-file copy
source = pa.memory_map("big_txns.arrow", "r")
table2 = pa.ipc.open_file(source).read_all()
```

---

## 4. Compute Kernels — SQL-ish ops on raw arrays

PyArrow ships vectorized kernels (SIMD-accelerated in C++): filters, aggregations, joins, temporal math, string ops.

```python
import pyarrow.compute as pc

t = pa.table({
    "txn_id": pa.array(range(1, 7)),
    "amount": pa.array([42.5, 5000.0, None, 9.99, 12500.75, 300.0]),
    "mcc":    pa.array(["5411","6011","5411","5812","6011","5411"]),
    "status": pa.array(["OK","OK","OK","FLAGGED","FLAGGED","OK"]),
})

# Filter (vectorized)
flagged = t.filter(pc.equal(t["status"], "FLAGGED"))

# Aggregation
stats = t.group_by("mcc").aggregate([
    ("amount", "mean"),
    ("amount", "count"),
]).sort_by("amount_mean")

# Elementwise + temporal
big_flag = pc.and_(pc.greater(t["amount"], 1000),
                   pc.equal(t["status"], "OK"))
hourly = pc.hour(t["ts"])            # if ts column exists
```

Use these when you want speed without pulling in a SQL engine; use DuckDB when logic gets complex.

---

## 5. Datasets API — bigger-than-memory tables across many files

```python
import pyarrow.dataset as ds

dataset = ds.dataset(
    "lake/transactions/",
    format="parquet",
    partitioning="hive",             # year=/month= folders from Lesson 1
)

# Lazy scan: pushes projection + predicate down to files
scan = dataset.to_table(
    columns=["mcc", "amount"],
    filter=ds.field("month") == 1,
)
print(scan.num_rows)
```

Features: multiple formats (parquet/arrow/csv/ipc), hive partitioning, schema unification across files, fragment-level parallelism, writing sharded datasets.

---

## 6. 🏦 Banking Scenario — Risk Feature Pipeline (before vs after)

**Legacy (CSV/JSON hops)**

```
Postgres → CSV export (8 min) → Pandas read_csv (11 min, RAM spike ×4)
→ transform → CSV (6 min) → Spark job re-parse (9 min) → JDBC insert (7 min)
Total ≈ 41 min, brittle types (dates→strings!), crashes on nulls.
```

**Arrow-native**

```python
"""
risk_pipeline.py — end-to-end, all stages speak Arrow.
pip install pyarrow pandas duckdb numpy
"""
import numpy as np
import pandas as pd
import pyarrow as pa, pyarrow.parquet as pq, pyarrow.compute as pc
import duckdb

# ---- Stage 0: pretend this came from CDC of the card platform ----------
rng = np.random.default_rng(7)
N = 5_000_000
ts = (pd.to_datetime("2026-07-01")
      + pd.to_timedelta(rng.integers(0, 31*86400, N), unit="s")).to_numpy()

txns = pa.table({
    "txn_id":  pa.array(np.arange(N, dtype="int64")),
    "account": pa.array(rng.choice([f"A{i:06d}" for i in range(80_000)], N)),
    "amount":  pa.array(np.round(rng.lognormal(4.5, 1.1, N), 2)),
    "mcc":     pa.array(rng.choice(["5411", "6011", "5812", "5999"], N)),
    "is_intl": pa.array(rng.integers(0, 2, N, dtype="int8")),
    "ts":      pa.array(ts),
})

# ---- Stage 1: VALIDATE (vectorized rules, no Python loops) -------------
bad = pc.or_kleene(
    pc.less(txns["amount"], 0.0),
    pc.greater(txns["amount"], 200_000.0),   # regulatory single-txn cap
)
clean = txns.filter(pc.invert(bad.fill_null(False)))
print(f"dropped {N - clean.num_rows} violations")

# ---- Stage 2: FEATURE ENGINEER via DuckDB ON THE ARROW TABLE -----------
features = duckdb.sql("""
    SELECT account,
           count(*)                        AS txn_count_30d,
           avg(amount)                     AS avg_amt_30d,
           max(amount)                     AS max_amt_30d,
           sum(CASE WHEN is_intl=1 THEN 1 ELSE 0 END) AS intl_ratio_num
    FROM txns                              -- Arrow table used directly!
    WHERE ts >= TIMESTAMP '2026-07-01'
    GROUP BY account
""").arrow()                               # result comes back as Arrow

# ---- Stage 3: SCORE (numpy view over Arrow, zero copy) -----------------
amt = features["avg_amt_30d"].to_numpy(zero_copy_only=False)
z   = (amt - amt.mean()) / amt.std()
features = features.append_column(
    "risk_score", pa.array(np.clip(z, -3, 3)))

# ---- Stage 4: LOAD — partitioned parquet for the lake ------------------
import os
os.makedirs("lake/risk_features", exist_ok=True)   # pq.write_table won't mkdir -p
pq.write_table(features, "lake/risk_features/features.parquet",
               compression="zstd")
print("done — all stages shared ONE in-memory layout")
```

**Result:** one process, one layout, zero serialization boundaries. On a laptop this pipeline runs in seconds-to-minutes where the CSV chain took ~40 minutes.

---

## 7. Cheat Sheet

```python
import pyarrow as pa, pyarrow.compute as pc, pyarrow.parquet as pq
import pyarrow.dataset as ds

# Construct
arr   = pa.array([...], type=pa.decimal128(18,2))
tbl   = pa.table({"a": arr})
batch = pa.RecordBatch.from_arrays([...], names=[...])

# Convert
pa.Table.from_pandas(df);  tbl.to_pandas()
pl.from_arrow(tbl);        duckdb.sql("... FROM tbl").arrow()

# Compute
tbl.filter(pc.equal(tbl["status"], "FLAG"))
agg = tbl.group_by("mcc").aggregate([("amount", "sum")])
# kernels: pc.greater, pc.less, pc.equal, pc.and_, pc.or_, pc.cast, pc.strftime ...

# IO
pq.read_table/write_table/write_to_dataset
ds.dataset(path, partitioning="hive").to_table(filter=..., columns=[...])

# Streams / mmap
with pa.ipc.new_stream(sink, schema) as w: w.write_batch(rb)
tbl = pa.ipc.open_file(pa.memory_map("f.arrow")).read_all()

# C Data Interface / PyCapsule protocol (zero-copy across libraries):
# any modern object exposing __arrow_c_array__ / __arrow_c_stream__
# can hand buffers to another library with NO pyarrow import involved:
stream = tbl.__arrow_c_stream__()        # e.g., consumed by DuckDB/Polars/pandas
```

**Memory hygiene quick reference**

| Need | Call |
|---|---|
| Count pieces of a column | `tbl["col"].num_chunks` |
| Merge chunks into contiguous memory | `tbl.combine_chunks()` |
| Release pandas/numpy views before freeing big tables | drop references; Arrow frees via refcount (no GC waits) |
| Check actual buffer bytes | `tbl.nbytes` (may differ from `getsizeof` intuitions) |

## 8. Exercises

1. Build a `pa.Table` with `decimal128(18,2)` amounts and prove `(0.1+0.2)==0.3` behaves exactly (vs float64).
2. Stream 3 record batches through `new_stream`/`open_stream`; concatenate into one table.
3. Memory-map a large `.arrow` file; compare "open time" vs `pd.read_csv` of equivalent CSV.
4. Rewrite the risk pipeline so validation happens on **chunks** (`RecordBatchReader`) — i.e., constant-memory streaming.
5. Benchmark `to_pandas()` vs `to_numpy(zero_copy_only=True)` on an int64 column of 100M rows.

## 9. Quiz

1. Difference between `RecordBatch` and `Table`?
2. How does Arrow represent nulls?
3. What is the Arrow C Data Interface for?
4. Why is dictionary encoding also useful *in memory*, not only on disk?
5. When must `to_numpy(zero_copy_only=True)` fail?

*(Answers: 1. Batch = arrays of equal length, unit of streaming; Table = schema + chunked columns; 2. Validity bitmaps per array; 3. Moving buffers between runtimes/libraries in-process without PyArrow objects (e.g., Rust engine ↔ Python); 4. Cheap equality/joins/group-bys on repeated categories, smaller RAM; 5. Non-contiguous/chunked arrays, non-numeric types, or when nulls present for numeric dtype conversions requiring masks/copies.)*

---

➡️ **Next:** `03_DuckDB.md` — now that everything speaks Arrow, let's put a blazing-fast SQL engine on top of it.
