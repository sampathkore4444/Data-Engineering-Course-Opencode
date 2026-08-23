# Lesson 03 — Apache Arrow: A Standard For In-Memory Columnar Data

> **Meridian Trust Bank case study, Part 3**: The fraud scoring service receives batches from a
> Spark job, a Python feature pipeline, and a Java risk engine. Each hop converts formats:
> Spark -> CSV -> pandas -> NumPy -> JSON -> Java. Profiling shows **70% of CPU time is spent
> serializing**, not computing fraud scores. Arrow eliminates the conversions.

---

## 1. The problem Arrow solves: the N-by-M serialization swamp

Before Arrow, every system had its own memory representation:

```
            before Arrow: every pair needs its own converter
   Spark ──▶ CSV ──▶ pandas ──▶ NumPy ──▶ custom proto ──▶ Java ──▶ ...
     │                                                      ▲
     └────────────── another bespoke path ──────────────────┘
        M systems x M targets = M² converters, all copying data
```

Symptoms at Meridian:

- Converting a 10 GB batch takes minutes and doubles/triples RAM (copies).
- Each library's null handling, string encoding, and timestamp semantics differ subtly.
- Cross-language teams write endless glue code.

**Apache Arrow's bet**: define ONE specification for columnar data *in memory* plus a wire
format (IPC) and an ABI for cross-language sharing (C Data Interface), implemented natively
in ~20 languages (C++, Java, Rust, Go, Python, R, JavaScript...). Then any pair of systems
exchanges data with **zero or near-zero copying**.

```
             after Arrow: everyone speaks one format
   Spark ══╗                    ╔══ DuckDB
   pandas ═╣═══ Arrow memory ══╣═══ Java risk engine
   Polars ═╝  (zero-copy)      ╚══ Flight SQL server
```

Arrow is NOT a database, NOT an execution engine (though it ships compute kernels), NOT a
storage format. It is a **data layout standard** — like UTF-8, but for tabular memory.

---

## 2. Architecture: the Arrow stack

```
┌───────────────────────────────────────────────────────────────────┐
│                     APPLICATIONS / ENGINES                        │
│   pandas · Spark · DuckDB · Polars · DataFusion · your code       │
├───────────────────────────────────────────────────────────────────┤
│  LANGUAGE BINDINGS (Python pyarrow, Java arrow-vector, rust ...)  │
├───────────────┬──────────────────────────┬────────────────────────┤
│ COMPUTE       │ IPC FORMAT               │ C DATA INTERFACE       │
│ kernels       │ (Feather V2 files,       │ (stable ABI: pointers  │
│ filter/sort/  │  streams, Flight gRPC    │  passed across         │
│ hash_agg ...  │  payloads)               │  runtimes = zero-copy) │
├───────────────┴──────────────────────────┴────────────────────────┤
│           MEMORY POOLS (allocation, tracking, OOM control)        │
├───────────────────────────────────────────────────────────────────┤
│     SPEC: flatbuffers metadata + contiguous value buffers         │
└───────────────────────────────────────────────────────────────────┘
```

Three integration surfaces to remember:

| Surface | Use when | Copies |
|---|---|---|
| **IPC stream/file** | persist or send batches (files, sockets) | 0–1 (deserialize = point at buffer) |
| **C Data Interface** | two runtimes in one process (pyarrow<->duckdb, <->polars) | **0** |
| **Flight/gRPC** | network RPC carrying record batches | ~0 on receive (bytes ARE the array) |

---

## 3. The memory model — what an Arrow array actually is

Every logical array = small flatbuffers **metadata** pointing into one or more immutable
**buffers**. Two canonical shapes:

### 3.1 Fixed-width primitive (`amount` as float64, n=7, one NULL)

```
Validity bitmap (1 bit per value, 1 = valid):
byte0 bits: [v0 v1 v2 v3 v4 v5 v6 v7] = [1 0 1 1 1 1 1 1]
             ▲  ▲
          valid NULL

Data buffer (contiguous little-endian float64):
offset  0        8        16       24 ...
      [ 12.5 ][  8.0 ][ -1.25 ][ ... ]      <- slot 1 unused (null)
```

- Nulls are cheap: a bit flip, not a pointer. `IS NULL` counting = popcount.
- Values stay contiguous, so SIMD scans work even with nulls interleaved.

### 3.2 Variable-length strings (`merchant`)

Add an **offsets buffer** (int32/int64, n+1 entries) plus one UTF-8 **data buffer**:

```
values: ["ACME", "SkyAir", "MetroFuel"]

offsets buffer (int32): [0, 4, 10, 19]         <- n+1 entries
data buffer (utf8):     [ACMESkyAirMetroFuel]  <- concatenated bytes
                         ^   ^      ^
                       off0 off1   off2

string_i = data[ offset[i] : offset[i+1] ]
```

Slicing `merchant[1:3]` just re-points offsets — **no byte copying**.

### 3.3 Nested types (structs / lists / maps)

Child arrays! `LIST<STRUCT<pan_masked:string, amount:double>>` (card authorizations) is a
parent offsets buffer + child arrays recursively. Parquet nested data maps onto this cleanly.

### 3.4 Key properties

1. **Aligned contiguous buffers** (64-byte) -> SIMD-friendly, mmap-able, DMA-able.
2. **Immutable** once built -> safe concurrent reads, cheap snapshots.
3. **Standardized layout/endianness** -> same bytes everywhere.
4. **No per-value object headers**: one billion float64s = exactly 8 GB, not ~32 GB of
   Python-object soup.

### 3.5 View types: modern strings without copy pressure

Classic `string` arrays own ONE contiguous UTF-8 buffer + offsets. Two costs follow:
`take`/`filter` on strings must COPY the surviving bytes into fresh buffers, and keeping
one small slice pins its whole parent buffer alive.

The newer **view layout** (`pa.string_view()` / `pa.binary_view()`) breaks that coupling.
Each element is a fixed **16-byte view**: length + inline prefix (strings up to 12 chars
live *inside* the view itself), or a pointer into shared data buffers:

```
classic : offsets [0,4,10,...] ──▶ one shared UTF-8 blob   (take => copy bytes)
view    : [len|prefix][len|ptr,buf_idx,off]...            (take => copy 16B views)
          "ACME" fits inline; "MetroFuel..." points into a shared buffer
```

```python
sv = pa.array(["ACME", "SkyAir", None], type=pa.string_view())
classic = sv.cast(pa.string())     # materialize contiguity only where a kernel demands it
```

Rule of thumb: classic `string` stays the safe default at storage/IPC boundaries (Parquet,
Flight); reach for views in string-heavy transform pipelines - masking PANs, parsing memos,
joining addresses - where take/filter churn would otherwise copy megabytes of UTF-8.

---

## 4. Core containers

```
Buffer(s) ───────────▶ Array (typed, length n, null_count k)
Equal-length arrays ──▶ RecordBatch   (unit of IPC / streaming!)
RecordBatches stacked ▶ Table (via ChunkedArray per column)
Schema = fields(types, nullable) + metadata → attached to everything
```

Why **ChunkedArray**? Data streams in incrementally; appending to one immutable buffer would
force realloc + copy. Columns are lists of chunks — kernels iterate chunks; call
`combine_chunks()` when you want a single piece.

Why is **RecordBatch** the streaming unit? Schema + several equal-length arrays = the perfect
packet for "the next 65,536 rows" over Flight, files, or queues.

---

## 5. Zero-copy: slicing, IPC, and the C Data Interface

### 5.1 Slicing

`arr.slice(1000, 500)` returns metadata pointing INTO the original buffers. No copy.
Results reference parent memory until something materializes them.

### 5.2 IPC format (Feather V2 files / streams)

IPC serializes flatbuffers metadata + buffer bodies. Reading does not parse values - it maps
buffers into memory and reattaches metadata. Loading a 10 GB Feather file costs an mmap,
not a decode. (Feather V2 *is* the Arrow IPC file format.)

### 5.3 C Data Interface

A tiny stable ABI: two structs holding pointers (schema + buffers). Libraries exchange arrays
**in-process without serialization**: DuckDB consumes pyarrow tables directly; Polars,
DataFusion, and pandas (Arrow-backed dtypes) interoperate through it too.

---

## 6. Where Arrow sits in this course

| Lesson | Relationship |
|---|---|
| Parquet (02) | Parquet = disk encoding; Arrow arrays = its decoded in-memory target |
| DuckDB (05) | zero-copy table exchange via C Data Interface |
| Iceberg (06/07) | scan tasks yield Arrow RecordBatches (PyIceberg is Arrow-native) |
| Flight SQL (08/09) | protocol payload IS Arrow record batches |

```
     PARQUET (disk, compressed)              ARROW (RAM, raw speed)
  ┌───────────────────────────┐         ┌──────────────────────────────┐
  │ pages: dict/RLE + ZSTD    │ decode ▶│ contiguous int64/float64 bufs │
  │ footer: stats, schema     │ ◀encode └──────────────────────────────┘
  └───────────────────────────┘              ▲ zero-copy handoff
                                             │ C Data Interface
                                       DuckDB / Polars / engines
```

---

## 7. Banking scenario walkthrough

**Real-time fraud scoring service** at Meridian Trust:

1. Card switch sends authorization events -> Kafka -> ingest job writes micro-batch Parquet.
2. Feature job reads Parquet -> Arrow RecordBatch streams, computes velocity features with
   Arrow compute kernels (no per-row object overhead).
3. Features cross a process boundary to the model server via shared-memory IPC, or to the
   Java rules engine via the **C Data Interface** — zero serialization.
4. Decisions stream back as Arrow batches -> appended to an Iceberg decisions table.

Without Arrow: JSON encode/decode at every hop (~70% of CPU). With Arrow: hops cost
memcpy-level time and RAM stays flat because nothing is duplicated.

**End-of-day risk batch**: the VaR engine needs positions from a Spark result set plus market
data from a C++ pricing library. Both export Arrow; the join happens on Arrow buffers without
either side knowing the other's language.

---

## 8. End-to-end example

Inspect real buffer layouts, prove zero-copy slicing, benchmark IPC vs CSV round-trips,
and hand a pyarrow table straight to DuckDB (C Data Interface preview of Lesson 05).

```python
"""
lesson03_arrow_lab.py
Arrow internals: buffers, bitmaps, offsets, zero-copy, IPC vs CSV speed.
Deps: pyarrow, pandas, duckdb, numpy
"""
import os, time
import numpy as np
import pandas as pd
import pyarrow as pa

rng = np.random.default_rng(3)

# ---- 1. Look INSIDE an int64 array ------------------------------------------
vals = pa.array([10, None, 30, 40], type=pa.int64())
print("type:", vals.type, "| nulls:", vals.null_count)
validity, data = vals.buffers()[0], vals.buffers()[1]
print("validity bitmap bytes:", validity.to_pybytes().hex())  # bits per value
print("data buffer hex     :", data.to_pybytes().hex(" "))    # 8-byte slots
# note: the slot for the NULL still exists - it is just masked out.

# ---- 2. Strings: offsets + concatenated utf-8 --------------------------------
merch = pa.array(["ACME", "SkyAir", "MetroFuel"])
v, o, d = merch.buffers()
print("\nstring offsets:", np.frombuffer(o, dtype=np.int32)[:4])  # [0 4 10 19]
print("string data    :", d.to_pybytes()[:19])

# ---- 3. Zero-copy slice --------------------------------------------------------
big = pa.array(np.arange(100_000_000, dtype=np.int64))   # ~800 MB window
t0 = time.perf_counter()
sl = big.slice(50_000_000, 10)                            # just 10 values
dt_slice = time.perf_counter() - t0
t0 = time.perf_counter()
_ = sl.to_numpy(zero_copy_only=False)                     # materialize tiny piece
dt_copy = time.perf_counter() - t0
print(f"\nslice of 800MB array : {dt_slice*1e6:8.1f} us (zero-copy)")
print(f"+ materialize 10 vals : {dt_copy*1e6:8.1f} us")
print("slice shares parent memory:",
      sl.buffers()[1].address == big.buffers()[1].address)

# ---- 4. Banking table -> RecordBatch streaming ---------------------------------
N = 2_000_000
batch_df = pd.DataFrame({
    "card_id":  rng.integers(400_000, 499_999, N),
    "mcc":      rng.choice([5411, 3005, 5541], N),
    "amount":   np.round(rng.gamma(2, 40, N) + .5, 2),
    "is_fraud": rng.choice([0, 1], N, p=[.999, .001]),
})
schema = pa.schema([
    ("card_id", pa.int64()), ("mcc", pa.int64()),
    ("amount", pa.float64()), ("is_fraud", pa.int8()),
])
table = pa.Table.from_pandas(batch_df, schema=schema, preserve_index=False)

def batches(tbl, size=65536):
    for i in range(0, tbl.num_rows, size):   # slice -> zero-copy view,
        yield from tbl.slice(i, size).to_batches()  # then materialize batches

stream = list(batches(table))
print(f"\ntable {table.num_rows:,} rows -> {len(stream)} RecordBatches "
      f"of <=65,536 (Flight-style paging)")

# ---- 5. IPC roundtrip vs CSV roundtrip ------------------------------------------
os.makedirs("/tmp/opencode/ipc", exist_ok=True)

t0 = time.perf_counter()
with pa.ipc.new_file("/tmp/opencode/ipc/txns.arrow", table.schema) as w:
    for b in stream:
        w.write_batch(b)
ipc_w = time.perf_counter() - t0

t0 = time.perf_counter()
with pa.ipc.open_file("/tmp/opencode/ipc/txns.arrow") as r:
    tbl2 = r.read_all()
ipc_r = time.perf_counter() - t0

t0 = time.perf_counter()
batch_df.to_csv("/tmp/opencode/ipc/txns.csv", index=False)
csv_w = time.perf_counter() - t0
t0 = time.perf_counter()
df_csv = pd.read_csv("/tmp/opencode/ipc/txns.csv")
csv_r = time.perf_counter() - t0

mb = lambda p: os.path.getsize(p) / 1e6
print(f"\n{'':<11}{'write':>8}{'read':>8}{'size MB':>10}")
print(f"{'Arrow IPC':<11}{ipc_w:7.2f}s{ipc_r:7.2f}s"
      f"{mb('/tmp/opencode/ipc/txns.arrow'):9.1f}")
print(f"{'CSV':<11}{csv_w:7.2f}s{csv_r:7.2f}s"
      f"{mb('/tmp/opencode/ipc/txns.csv'):9.1f}")

# ---- 6. Arrow -> DuckDB with ZERO serialization (C Data Interface) ---------------
import duckdb
con = duckdb.connect()
con.register("txn_view", table)          # wraps the Arrow buffers directly
res = con.execute("""
    SELECT mcc, count(*) AS n, sum(amount) AS vol, sum(is_fraud) AS frauds
    FROM txn_view GROUP BY mcc ORDER BY vol DESC
""").to_arrow_table()                    # result returns AS Arrow table
print("\nDuckDB over the same Arrow memory:")
print(res.to_pandas())

# ---- 7. Compute kernels: columnar ops without pandas ------------------------------
import pyarrow.compute as pc
fraud_amt = pc.filter(table.column("amount"),
                      pc.equal(table.column("is_fraud"), 1))
print(f"\nfraud amount mean via kernel: {pc.mean(fraud_amt).as_py():.2f} "
      f"(vectorized, no row objects)")
```

Typical output (timings vary by machine):

```
type: int64 | nulls: 1
validity bitmap bytes: 0d          <- bits [1,0,1,1]: value 1 is the NULL
data buffer hex     : 0a .. 00 .. 1e .. 28   (10, ?, 30, 40 as int64 slots)

string offsets: [ 0  4 10 19]
string data    : b'ACMESkyAirMetroFuel'

slice of 800MB array :     16.2 us (zero-copy)
+ materialize 10 vals :    402.2 us
slice shares parent memory: True

table 2,000,000 rows -> 31 RecordBatches of <=65,536 (Flight-style paging)

               write    read   size MB
Arrow IPC     0.06s   0.05s     50.0
CSV           2.36s   0.42s     40.3

DuckDB over the same Arrow memory:
    mcc       n           vol frauds
0  5411  666520  5.368563e+07    625
1  3005  667363  5.366510e+07    687
2  5541  666117  5.365769e+07    670

fraud amount mean via kernel: 81.33 (vectorized, no row objects)
```

Read that block honestly: Arrow IPC is **~40x faster** to write and read than CSV because
reading it is just mapping buffers - but for purely numeric data its file can be *larger*
than CSV text, since IPC stores raw fixed-width values with zero compression. You trade
bytes for zero parse cost; when you also need small files, compress (Parquet) on top of
the same Arrow memory. And DuckDB queried the original Python-owned buffers without
copying them once.

---

## 9. Exercises

1. Build `pa.array(["AA", "BBB", None, "C"])`; print all buffers; verify offsets by hand.
2. Slice a string array mid-buffer and inspect the `.offset` attribute; explain why no copy occurs.
3. Stream 100 RecordBatches through `pa.ipc.new_stream()` into a file and read back with a
   generator; measure peak RSS vs building one giant Table first.
4. Convert the same table to pandas twice: default vs `dtype_backend="pyarrow"`. Compare
   memory with `df.info(memory_usage="deep")`.
5. Time `pc.sum()` on a 100M-element array vs `np.sum`. Why are they comparable, while
   summing a pandas object-dtype Series is 100x slower?

## 10. Cheat sheet

| Concept | Fact |
|---|---|
| Array | metadata + buffers: validity bitmap (+ data [+ offsets [+ children]]) |
| Validity bitmap | 1 bit/value, set = valid; absent bitmap = all valid |
| Offsets buffer | var-length & nested types; n+1 entries |
| String/binary view | 16-byte views (+ inline ≤12-char strings) over shared buffers; zero-copy take/filter of strings |
| RecordBatch | equal-length arrays + schema = unit of streaming/IPC |
| Table | columns as ChunkedArrays; slices are lazy views |
| IPC / Feather V2 | flatbuffers + raw buffer bodies; load = map, not parse |
| C Data Interface | pointer ABI for zero-copy cross-runtime exchange |
| Compute kernels | vectorized filter/take/hash_agg on Arrow memory |

**Next:** Lesson 04 makes all of this muscle memory with a full PyArrow lab on banking data.
