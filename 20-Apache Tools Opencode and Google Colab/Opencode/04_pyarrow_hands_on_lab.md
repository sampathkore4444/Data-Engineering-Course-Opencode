# Lesson 04 — PyArrow Hands-On Lab

> **Meridian Trust Bank case study, Part 4**: You just joined the fraud-data platform team.
> Your first ticket: "Build the nightly feature pipeline that turns raw card transactions into
> model features, in pure PyArrow — pandas is melting at 2B rows/day." This lesson gives you
> every tool you need to pass code review.

---

## 1. Concept recap and mental model

From Lesson 03 you know Arrow's memory model. PyArrow is its Python face:

```
pandas DataFrame          pyarrow Table                Parquet files
(object/np dtypes)  <->   (Arrow buffers)       <-->   (encoded on disk)
        copy on convert         zero-copy slices            decode->Arrow
```

Key classes you will live with:

| Class | Think of it as |
|---|---|
| `pa.Array` | one column, immutable, buffer-backed |
| `pa.ChunkedArray` | column split into pieces (streaming result) |
| `pa.RecordBatch` | several equal-length arrays + schema = a "page" of data |
| `pa.Table` | full table: schema + ChunkedArrays; the workhorse |
| `pa.Schema` | names + types + nullability + metadata |
| `pyarrow.dataset` | lazy view over MANY files (parquet/csv/ipc) with pushdown |

---

## 2. Types & schemas: get them right first

Banking rule #1: money is never float. Use integers of minor units or decimal.

```python
import pyarrow as pa

schema = pa.schema([
    pa.field("txn_id",      pa.int64(),    nullable=False),
    pa.field("account_id",  pa.int64(),    nullable=False),
    pa.field("ts",          pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("amount_minor",pa.int64(),    nullable=False),  # cents!
    pa.field("currency",    pa.string()),
    pa.field("mcc",         pa.int32()),
    pa.field("channel",     pa.dictionary(pa.int8(), pa.string())),  # enum-like
    pa.field("fraud_score", pa.float32()),                   # ML output ok as f32
], metadata={
    "owner": "fraud-platform",
    "pii.columns": "account_id",
})

print(schema)
print(schema.get_field_index("amount_minor"))   # -> 3
```

Why it matters:

- **Explicit schemas** kill inference surprises ("2026-07-01" becoming date vs string).
- **Dictionary type** for low-cardinality strings = Lesson-01 dictionary encoding, in RAM.
- Schema-level **metadata** travels with the data everywhere (into Parquet, Flight...).
- `timestamp("us", tz="UTC")`: always fix units + timezone; banks fail audits over
  timestamp ambiguity.

## 3. Construction patterns

```python
import numpy as np, pyarrow as pa

rng = np.random.default_rng(0)

# from columns
t1 = pa.table({
    "id": pa.array(range(5), type=pa.int64()),
    "amt": pa.array([100, 250, None, 99, 40], type=pa.int64()),
})

# from rows (careful: row-wise python is slow for big N)
t2 = pa.Table.from_pylist([
    {"id": 1, "amt": 10}, {"id": 2, "amt": 20},
])

# from pandas (copy) / numpy (usually zero-copy for numeric)
import pandas as pd
df = pd.DataFrame({"a": np.arange(3)})
t3 = pa.Table.from_pandas(df, preserve_index=False)

arr = pa.array(np.arange(1_000_000, dtype=np.int64))   # wraps numpy buffer

# building big arrays efficiently: use builders or numpy, not python lists
ids = pa.array(rng.integers(0, 2**31, 1_000_000))
```

Performance rules:

1. Never build large arrays from Python lists in hot paths - go via NumPy or builders.
2. `from_pandas` copies once; keep data in Arrow after that.
3. Prefer fixed-width types; strings only when truly needed (or dictionary-encode).

## 4. Slicing, filtering, taking - all vectorized

```python
import pyarrow.compute as pc

amounts = pa.array(np.round(rng.gamma(2, 50, 1_000_000) + .01, 2))

# boolean filter (kernel)
big = pc.filter(amounts, pc.greater(amounts, 500.0))

# take = fancy indexing by positions (gather)
sample = amounts.take([0, 77, 999_999])

# slices are VIEWS (zero-copy)
head = amounts.slice(0, 1000)

# sort_indices + take = sort without pandas
idx = pc.sort_indices(
    pa.table({"a": amounts}), sort_keys=[("a", "descending")])
top10 = pc.take(amounts, idx.slice(0, 10))
```

## 5. Compute kernels: your new standard library

PyArrow ships ~300 kernels. The ones you will use weekly:

| Kernel | Purpose |
|---|---|
| `pc.filter`, `pc.take` | selection |
| `pc.sum/mean/min_max/count` | aggregations (null-aware) |
| `pc.equal/greater/is_in/matches_substring` | predicates |
| `pc.cast` | safe type conversion (with options) |
| `pc.utf8_upper/is_finite/round` | scalar math & strings |
| `pc.strftime/strptime` | timestamp formatting/parsing |
| `hash_aggregate` via `TableGroupBy` | GROUP BY |
| `pc.binary_join_element_wise` | string concat |

```python
tbl = pa.table({
    "mcc":     pa.array(rng.choice([5411, 5541, 5812], 1_000_000)),
    "amount":  amounts,
    "eur":     pa.array(rng.random(1_000_000) < .7),
})

# GROUP BY with multiple aggs
g = tbl.group_by("mcc").aggregate([
    ("amount", "sum"),
    ("amount", "mean"),
    ("amount", "count"),
])
print(g.sort_by([("amount_sum", "descending")]))

# conditional logic without if/else loops (scalars are broadcast)
flagged = pc.if_else(pc.greater(tbl.column("amount"), 400.0),
                     "HIGH", "NORMAL")

# is_in for allow-lists
groceries = pc.is_in(tbl.column("mcc"), value_set=pa.array([5411, 5912]))
```

## 6. TableGroupBy, joins and set ops

```python
rates = pa.table({"currency": ["EUR", "USD"], "to_usd": [1.09, 1.0]})
tx    = pa.table({"currency": ["EUR", "USD", "EUR"], "minor": [100, 200, 50]})

usd = tx.join(rates, keys="currency", right_keys="currency",
              join_type="left outer")
usd = usd.append_column(
    "usd_cents",
    pc.multiply(usd.column("minor").cast(pa.float64()),
                usd.column("to_usd")))
print(usd)
```

Joins are hash-based, multithreaded, null-safe-ish (`join_type` supports inner/left/right/
outer/full/left semi/left anti). For set semantics there are `pc.unique`,
`Table.drop_duplicates()` equivalents via group_by tricks.

## 7. The Dataset API: lazy scans over many files

This is how production pipelines touch Parquet lakes (Lesson 02) lazily and in parallel:

```python
import pyarrow.dataset as ds

dset = ds.dataset("/tmp/opencode/lake/card_txns",   # from Lesson 02 lab
                  format="parquet", partitioning="hive")

print(dset.schema)                    # inferred; can override
print(dset.count_rows())              # metadata-driven

scan = dset.scanner(
    filter=(ds.field("mcc") == 3005) & (ds.field("amount") > 100),
    columns=["txn_id", "ts", "amount"],
    batch_size=65_536,                # RecordBatch size for streaming consumers
)
batches = scan.to_batches()           # generator: constant memory!
tbl = scan.to_table()                 # or materialize if it fits
```

Why `to_batches()` matters: a 2B-row table never fits RAM. Streaming batches into
model scoring / writing / aggregation keeps memory flat while using all cores.

Writing datasets with partitioning + file options:

```python
ds.write_dataset(
    tbl, base_dir="/tmp/opencode/out",
    format="parquet",
    partitioning=ds.partitioning(pa.schema([("mcc", pa.int32())]), flavor="hive"),
    existing_data_behavior="overwrite_or_ignore",
    max_rows_per_file=10_000_000,
    min_rows_per_group=1_000_000, max_rows_per_group=2_000_000,  # sane row groups
)
```

## 8. pandas interop - the two dtypes worlds

```python
import pandas as pd

# classic conversion (copies, numpy dtypes)
df1 = tbl.to_pandas()

# zero-copy when possible (numeric, no nulls -> numpy views)
a = pa.array(np.arange(6), type=pa.int64())
np_view = a.to_numpy(zero_copy_only=True)

# pandas 2.x Arrow-backed: pandas columns ARE Arrow memory
df_arrow_backed = tbl.to_pandas(types_mapper=pd.ArrowDtype)
back_again = pa.Table.from_pandas(df_arrow_backed)  # cheap, no re-inference
```

Rule: cross the boundary once at the edges; stay in Arrow inside pipelines.

## 9. Memory management & gotchas

- `pa.total_allocated_bytes()` on tables/arrays shows real buffer usage.
- ChunkedArrays accumulate; `combine_chunks()` before IPC write of one batch.
- Slices keep parent buffers alive -> after heavy filtering, `.combine_chunks()` or
  `pc.filter` results are compact copies; big slices you keep forever should be copied.
- `pa.set_memory_pool(...)` / `pa.default_memory_pool().bytes_allocated()` for tracking.

## 10. Banking scenario walkthrough: the nightly feature pipeline

Requirements from the fraud team:

1. Read yesterday's transactions from the Parquet lake (partitioned by day).
2. Build per-card features: txn count, avg amount, max amount, foreign-country flag ratio.
3. Join merchant risk list; score-ready output as Parquet + Arrow IPC for the model server.

Full runnable pipeline below (self-contained synthetic data):

```python
"""
lesson04_feature_pipeline.py
Nightly fraud-feature pipeline in pure PyArrow.
Deps: pyarrow, numpy
"""
import shutil, os
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds

rng = np.random.default_rng(11)
BASE = "/tmp/opencode/features"

# ---- 0. simulate 'yesterday's raw transactions' as a parquet dataset ----------
country = rng.choice(["US", "US", "US", "DE", "BR"], 500_000)   # draw ONCE...
raw = pa.table({
    "txn_id":     pa.array(np.arange(500_000), type=pa.int64()),
    "card_id":    pa.array(rng.integers(400_000, 409_999, 500_000), type=pa.int64()),
    "amount_usd": pa.array(np.round(rng.gamma(2, 45, 500_000) + .5, 2)),
    "country":    pa.array(country),
    "is_foreign": pa.array(country != "US"),   # ...derive from the SAME draw,
})                                             # or country='DE' + is_foreign=False rows appear
shutil.rmtree(BASE, ignore_errors=True)
ds.write_dataset(raw, BASE, format="parquet",
                 partitioning=ds.partitioning(
                     pa.schema([("is_foreign", pa.bool_())]), flavor="hive"))

# ---- 1. lazy scan with pushdown -------------------------------------------------
dset = ds.dataset(BASE, format="parquet", partitioning="hive")
scan = dset.scanner(columns=["txn_id", "card_id", "amount_usd", "is_foreign"])

# ---- 2. streaming aggregation per card (constant memory) -------------------------
from collections import defaultdict
sums, cnts, mxs, frn = defaultdict(float), defaultdict(int), defaultdict(float), defaultdict(int)

for batch in scan.to_batches():                       # stream!
    cards = batch.column("card_id").to_numpy()
    amts = batch.column("amount_usd").to_numpy()
    fr = batch.column("is_foreign").to_numpy(zero_copy_only=False)
    for card, amt, f in zip(cards, amts, fr):         # demo loop; see note below
        sums[card] += amt; cnts[card] += 1
        if amt > mxs[card]: mxs[card] = amt
        if f: frn[card] += 1

# ---- 3. back to Arrow for the join & output --------------------------------------
cards_arr = pa.array(list(sums.keys()), type=pa.int64())
features = pa.table({
    "card_id":       cards_arr,
    "txn_count":     pa.array(list(cnts.values()), type=pa.int64()),
    "amount_sum":    pa.array(list(sums.values())),
    "amount_max":    pa.array(list(mxs.values())),
    "foreign_ratio": pc.divide(
        pa.array(list(frn.values()), type=pa.float64()),
        pa.array(list(cnts.values()), type=pa.float64())),
})

# ---- 4. join merchant-risk list (here: card-level risk seed) ----------------------
risk = pa.table({"card_id": pa.array(rng.choice(cards_arr, 1000), type=pa.int64()),
                 "risk_seed": pa.array(rng.random(1000))})
features = features.join(risk, keys="card_id", right_keys="card_id",
                         join_type="left outer")
features = features.set_column(
    features.schema.get_field_index("risk_seed"), "risk_seed",
    features.column("risk_seed").fill_null(0.0))

# ---- 5. write parquet + arrow IPC for the model server -----------------------------
os.makedirs(f"{BASE}/out", exist_ok=True)
import pyarrow.parquet as pq
pq.write_table(features, f"{BASE}/out/card_features.parquet", compression="zstd")
with pa.ipc.new_file(f"{BASE}/out/card_features.arrow", features.schema) as w:
    w.write_table(features.combine_chunks())

print(features.num_rows, "cards featured")
print(features.slice(0, 3).to_pandas())
```

> Note: step 2 uses a Python dict loop for clarity. At scale you would express this as a
> `group_by("card_id").aggregate([...])` kernel call (multithreaded C++), or use DuckDB
> (Lesson 05) - try rewriting it that way as exercise 1!

## 11. Exercises

1. Replace step 2's Python loop with `raw.group_by("card_id").aggregate([...])`.
   Measure the speedup on 5M rows.
2. Add a `pc.strftime`-derived `hour_of_day` column and build per-hour fraud ratios.
3. Rewrite the pipeline to write output partitioned by `risk_bucket` (use
   `pc.bin` or nested if_else to bucket `risk_seed`).
4. Prove zero-copy: after `t = pa.table({...big...})`, slice 10 rows, delete `t`,
   and check `sys.getsizeof`/buffer addresses - what keeps memory alive?
5. Benchmark `to_pandas()` vs `to_pandas(dtype_backend="pyarrow")` on a table with
   3 string columns; explain the gap using Lesson 03 (offsets vs python objects).

## 12. Cheat sheet

| Task | PyArrow |
|---|---|
| Read many parquet lazily | `ds.dataset(dir, format="parquet", partitioning="hive")` |
| Stream results | `scanner(...).to_batches()` |
| Pushdown filter | `ds.field("col") == x` in `scanner(filter=)` |
| GROUP BY | `table.group_by(k).aggregate([(col, agg)])` |
| Join | `table.join(other, keys=..., join_type=...)` |
| Conditional col | `pc.if_else(mask, a, b)` / `pc.case_when` (newer) |
| Null handling | `.fill_null()`, `.drop_null()`, kernels are null-aware |
| Money type | int64 minor units (or decimal128) - never float64 in ledgers |

**Next:** Lesson 05 - DuckDB, the engine that lets you do ALL of the above with SQL,
directly over these Parquet files and Arrow tables.
