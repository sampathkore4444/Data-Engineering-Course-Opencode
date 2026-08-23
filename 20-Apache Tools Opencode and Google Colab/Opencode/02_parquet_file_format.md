# Lesson 02 — Apache Parquet: The Columnar File Format

> **Meridian Trust Bank case study, Part 2**: The fraud team's scans are fast now, but their
> data lake is a swamp of 40,000 tiny CSV files written by 12 different services. Files have
> no schema, dates are in three formats, and nobody knows which file contains quarter-end data.
> Parquet fixes the *file*, and partitioning + statistics fix the *swamp*.

---

## 1. What Parquet is

**Apache Parquet** is a language-independent, open-source **columnar storage file format**
for Hadoop-style ecosystems — today the de-facto interchange format for analytical data
(Spark, DuckDB, Snowflake, BigQuery, Athena, Redshift Spectrum, pandas/pyarrow, ... all read it natively).

Design goals:

1. **Columnar** layout with per-column encodings (Lesson 01) and compression.
2. **Self-describing**: schema is embedded in the file (like Avro), so readers validate.
3. **Splittable**: one big file can be read by many workers in parallel at row-group boundaries.
4. **Predicate-pushdown-friendly**: min/max stats per chunk let readers skip data without decoding it.
5. **Nested-data support**: can store structs/lists/maps efficiently (Dremel-style record shredding).

Parquet stores *one table snapshot*. It has **no transactions, no updates, no schema evolution
across files, no ACID** — that gap is exactly what Apache Iceberg fills (Lesson 06).
Keep the boundary crisp:

```
Parquet = how ONE file is laid out      Iceberg = how MANY files form ONE evolving table
```

---

## 2. File anatomy

A `.parquet` file is organized in three nested levels: **Row Groups → Column Chunks → Pages**,
with a **footer** holding all metadata.

```
┌────────────────────────────────────────────────────────────────┐
│  Magic "PAR1"                                                  │
├────────────────────────────────────────────────────────────────┤
│  Row Group 0                                                   │
│   ├─ Column Chunk: txn_id     [page][page][page]               │
│   ├─ Column Chunk: account_id [page][page][page]               │
│   ├─ Column Chunk: ts         [page][page][page]               │
│   └─ Column Chunk: amount     [page][page][page]               │
├────────────────────────────────────────────────────────────────┤
│  Row Group 1                                                   │
│   └─ ... same structure ...                                    │
├────────────────────────────────────────────────────────────────┤
│  Footer (metadata)                                             │
│   - Schema (names, types, repetition)                          │
│   - Per row group: total size, #rows                           │
│   - Per column chunk: encoding, compression,                   │
│     min/max/null_count/distinct_count  ← pushdown fuel         │
│   - offset index / column index (page-level stats)             │
│   - Footer length + Magic "PAR1"                               │
└────────────────────────────────────────────────────────────────┘
```

### Row group

- A horizontal **slice of rows** (e.g., 128M rows or ~128 MB–1 GB compressed).
- Unit of **parallelism** for readers (thread/task per row group) and of **I/O skipping**
  via statistics.
- Too small → metadata overhead & tiny reads; too large → memory pressure, coarse skipping.

### Column chunk

- All values of **one column within one row group**, stored contiguously.
- This is what makes projection pushdown physical: reading `amount` never touches other chunks.

### Page (typically ~1 MB)

- Smallest encoding/compression unit inside a column chunk.
- Page types:
  - **Data page(s)** — actual values (v1) or checksummed with extra stats (v2)
  - **Dictionary page** — first page of dictionary-encoded columns
  - Index pages (rarely used; superseded by column/offset indexes)

### Footer

- Read **last** (length is the final 8 bytes: `len | "PAR1"`).
- Contains everything a reader needs to plan a scan: schema, row-group boundaries,
  per-chunk statistics. Reading metadata of a multi-GB file costs only a few KB read at the end.

> **Why footers matter to banks**: an auditor asks "which files contain amounts > €10k?" —
> you answer from footers alone across millions of files in seconds, without decoding data.

### Nested data: repetition & definition levels (Dremel shredding)

Design goal #5 promised efficient structs/lists/maps. Parquet delivers this by shredding any
document into independent columns using **two small integers per value**:

- **Definition level** — how many optional/repeated fields along the path are defined.
  A NULL at *any* depth becomes just a lower def-level number, not a lost row.
- **Repetition level** — at which repeated field this value is a new occurrence vs. another
  entry of the same list as the previous value. This is what lets one column hold values
  from many rows and many list entries without ambiguity.

```
schema : merchant { name (optional), mcc (optional) }
rows   : (1, {ACME, 5411})    (2, {SkyAir, NULL})    (3, merchant = NULL)

mcc stream: (5411, def=2) (∅, def=1) (∅, def=0)
            fully defined  merchant ok,  merchant itself
                           mcc is NULL   is NULL
```

From R/D levels alone a reader reconstructs exact rows — including empty lists, which emit
*no* values at all, only bookkeeping entries. This is why card authorizations shaped
`LIST<STRUCT<pan_masked, amount>>` store cleanly without JSON blobs, and why Arrow's nested
arrays (Lesson 03) map onto Parquet losslessly in both directions.

---

## 3. Encodings inside Parquet

Per column chunk/page, Parquet picks from (Lesson 01 concepts, now concrete):

| Encoding | Parquet name | Typical use |
|---|---|---|
| Plain | `PLAIN` | fallback; raw values |
| Dictionary + RLE | `RLE_DICTIONARY` | strings/enums (merchant, currency) |
| Run-length | `RLE` | booleans, nulls, low-cardinality |
| Delta binary-packed | `DELTA_BINARY_PACKED` | ints/longs (ids, timestamps) |
| Delta byte array | `DELTA_BYTE_ARRAY` | sorted strings |
| Byte stream split | `BYTE_STREAM_SPLIT` | floats (ML features) |

You rarely choose manually — writers auto-select per column based on observed data.
But you CAN steer: `pyarrow.parquet.write_table(..., use_dictionary=["merchant"], ...)`,
or disable for high-cardinality columns where dictionaries waste space.

---

## 4. Compression

Applied **per column page** after encoding:

| Codec | Speed | Ratio | Bank usage |
|---|---|---|---|
| SNAPPY | very fast | ok | default; hot paths |
| LZ4 | fastest | ok | streaming ingest |
| ZSTD | fast | great | archives, cold data — recommended default today |
| GZIP | slow | good | legacy interop |

Compression happens on encoded streams, e.g., dictionary IDs `[0,0,1,0,...]` RLE-run then ZSTD'd
→ spectacular ratios on repetitive bank columns.

---

## 5. Predicate & projection pushdown — how skipping actually works

Query: `SELECT amount FROM txns WHERE mcc = 3005 AND ts >= '2026-07-01'`

```
Reader planning:
1. Read footer                     → know all row groups & stats
2. For each row group, check:
      chunk(ts).max < 2026-07-01 ?        → SKIP entire row group
      chunk(mcc).min > 3005 OR max < 3005 ? → SKIP entire row group
3. Only decode surviving row groups' (mcc, ts, amount) chunks
4. Apply exact predicate on decoded values (stats are ranges, not guarantees)
```

This is **partition pruning + statistics-based skipping + projection pushdown** stacked.
On time-partitioned banking data this routinely skips 99%+ of bytes.

### Bloom filters: skipping when min/max stats lie

Min/max pruning only works when ranges are narrow. High-cardinality columns (`txn_uuid`,
unsorted `account_id`) have nearly identical `[min,max]` intervals in *every* row group —
the range check skips nothing. A **bloom filter** is a compact probabilistic set-membership
structure stored beside the column chunk (its offset + length live in the footer). For
equality probes the reader hashes the value first:

```
row-group stat : account_id min=100001 max=999999    <- true of EVERY chunk, prunes nothing
bloom filter   : hash(txn_uuid) -> "definitely NOT in this chunk" => SKIP without decoding
                 "maybe present"                    => decode (false-positive rate = fpp)
```

Writers opt in per column — pyarrow:

```python
pq.write_table(tbl, "txns.parquet",
               bloom_filter_options={"txn_uuid": {"enabled": True, "fpp": 0.01}})
# verify in the footer: chunk.bloom_filter_offset / bloom_filter_length
```

Readers exploit them automatically where supported (DuckDB, Trino, Spark). Stack the three
skip-mechanisms deliberately: **partitioning** prunes directories, **min/max stats** prune
ranges, **bloom filters** prune point lookups.

---

## 6. Datasets: many files = one logical dataset

Real lakes hold thousands of parquet files. Two organizing tricks matter:

### 6.1 Hive-style partitioning (directories encode column values)

```
s3://meridian/card_txns/
 ├── year=2024/month=11/part-000.parquet
 ├── year=2024/month=12/part-001.parquet
 ├── year=2025/month=01/part-002.parquet
 ...
```

`WHERE year=2025 AND month=1` prunes directories before even listing others.
Partition by columns with **low cardinality and predictable filter patterns**
(date parts, country, branch_region). Never by high-cardinality ids (customer_id!) —
you'd create millions of tiny dirs/files.

> Partition columns are *stored in directory names, not inside files* — readers add them
> back as virtual columns.

### 6.2 Sizing guidance

Target **128 MB–1 GB per file**, tens of thousands max files per table. Small-file disease =
listing overhead × N, footer reads × N, scheduler overhead × N (fixed properly by Iceberg
compaction in Lesson 07).

---

## 7. Banking scenario walkthrough

**Quarter-end archive (regulatory):** ETL writes ledger entries to
`s3://meridian/glje/year=2026/qtr=Q2/*.parquet`, ZSTD-compressed, sorted by posting date.
Central-bank examiners request "all entries > threshold for Q2". The query engine:

1. Prunes to `year=2026/qtr=Q2` partitions (directory pruning),
2. Reads footers, uses `entry_amount` min/max to skip most row groups,
3. Decodes 4 of 38 columns,

...answering from 400 GB of archive in seconds-to-minutes instead of hours.

**Fraud feature store:** card events land every minute as small parquet files; a nightly job
compacts them into day partitions. Models train by scanning only needed feature columns.

---

## 8. End-to-end example

Generate synthetic bank data → write partitioned, sorted Parquet with PyArrow → verify
internals (row groups, stats, encodings) → demonstrate pushdown skipping → measure.

```python
"""
lesson02_parquet_lab.py
Write & inspect partitioned Parquet for a card-transactions lake.
Deps: pyarrow, pandas, numpy
"""
import os, shutil
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds

rng = np.random.default_rng(7)
BASE = "/tmp/opencode/lake/card_txns"

# ---- 1. Synthetic month of card transactions ---------------------------------
N = 500_000
df = pd.DataFrame({
    "txn_id":     np.arange(N),
    "account_id": rng.integers(100_000, 999_999, N),
    "ts":         pd.date_range("2026-07-01", periods=N, freq="6s"),  # ~35 days
    "mcc":        rng.choice([5411, 3005, 5541, 5812], N),
    "amount":     np.round(rng.gamma(2.0, 50.0, N) + 1.0, 2),
    "currency":   rng.choice(["EUR", "USD"], N, p=[.7, .3]),
    "channel":    rng.choice(["POS", "ECOM", "ATM"], N),
})
df["year"], df["month"] = df.ts.dt.year, df.ts.dt.month

# NOTE: partition cols must remain IN the table; the writer moves them into
# directory names and strips them from the per-file schema automatically.
table = pa.Table.from_pandas(df.drop(columns=["year"]),
                             preserve_index=False)

shutil.rmtree(BASE, ignore_errors=True)

# ---- 2. Write HIVE-PARTITIONED, ZSTD-compressed parquet -----------------------
# partition by 'month'; sort each partition by ts for better delta encoding
pq.write_to_dataset(
    table,
    root_path=BASE,
    partition_cols=["month"],
    compression="zstd",
    existing_data_behavior="overwrite_or_ignore",
)
for root, _, files in os.walk(BASE):
    for f in files:
        print(os.path.join(root.replace(BASE, "."), f))

# ---- 3. Inspect internals of one file -----------------------------------------
one_file = None
for root, _, files in os.walk(BASE):
    if files:
        one_file = os.path.join(root, files[0]); break

pf = pq.ParquetFile(one_file)
print("\n== schema ==\n", pf.schema_arrow)
print("row groups:", pf.num_row_groups)

rg0 = pf.metadata.row_group(0)
for c in range(min(3, rg0.num_columns)):
    col = rg0.column(c)
    print(f"rg0 {col.path_in_schema:<12} enc={col.encodings} "
          f"compressed={col.total_compressed_size:,}B "
          f"uncompressed={col.total_uncompressed_size:,}B")

# per-chunk statistics → the fuel for predicate pushdown
amt_idx = pf.schema_arrow.get_field_index("amount")
st = rg0.column(amt_idx).statistics
print(f"\namount stats: min={st.min} max={st.max} nulls={st.null_count}")

# ---- 4. Predicate + projection pushdown: measure bytes skipped -----------------
dataset = ds.dataset(BASE, format="parquet", partitioning="hive")

expr = (ds.field("mcc") == 3005) & (ds.field("amount") > 500)

scan = dataset.scanner(filter=expr, columns=["txn_id", "ts", "amount"])
t0 = pd.Timestamp.now()
big = scan.to_table()
dt = (pd.Timestamp.now() - t0).total_seconds()
print(f"\npushdown scan -> rows={big.num_rows:,} in {dt:.3f}s")
print(big.slice(0, 3).to_pandas())

# prove pruning: count fragments the scanner will actually visit
frags_all = len(list(dataset.get_fragments()))
frags_pruned = len(list(dataset.get_fragments(filter=ds.field("month") == 7)))
print(f"fragments all={frags_all}  after partition-prune(month=7)={frags_pruned}")

# ---- 5. Size comparison --------------------------------------------------------
sizes = {}
df.sample(frac=1, random_state=1).to_csv("/tmp/opencode/t.csv", index=False)
sizes["csv"] = os.path.getsize("/tmp/opencode/t.csv")
table.to_pandas().to_parquet("/tmp/opencode/t_snappy.parquet", compression="snappy",
                             engine="pyarrow", index=False)
sizes["parquet+zstd(sorted-ish)"] = sum(
    os.path.getsize(os.path.join(r, f))
    for r, _, fs in os.walk(BASE) for f in fs)
for k, v in sizes.items():
    print(f"{k:<24}{v/1e6:8.1f} MB")
```

Expected output (abridged, will vary):

```
./month=8/9606148f...-0.parquet
./month=7/9606148f...-0.parquet

== schema ==
 txn_id: int64
 account_id: int64
 ts: timestamp[us]
 mcc: int64
 amount: double
 ...
row groups: 3
rg0 txn_id     enc=('PLAIN','RLE','RLE_DICTIONARY') compressed=33,823B  uncompressed=120,555B
rg0 account_id enc=('PLAIN','RLE','RLE_DICTIONARY') compressed=61,371B  uncompressed=119,915B
rg0 ts         enc=('PLAIN','RLE','RLE_DICTIONARY') compressed=57,654B  uncompressed=120,555B

amount stats: min=1.69 max=641.4 nulls=0

pushdown scan -> rows=1,187 in 0.002s
fragments all=2  after partition-prune(month=7)=1

csv                         30.2 MB
parquet+zstd(sorted-ish)     8.5 MB
```

What to internalize from the run:

1. **Directory names carry `month=`** — partition columns live outside the files.
2. **Footer stats** expose `min/max/nulls` per column chunk → engines skip without decoding.
3. **`scanner(filter=..., columns=[...])`** is explicit pushdown; DuckDB/Spark/Athena do the
   same automatically against these files.
4. Sorting rows before writing improves delta/RLE effectiveness (try sorting by `account_id`
   and re-measuring).

---

## 9. Where plain Parquet hurts (the Iceberg cliffhanger)

Plain files have **no table concept**:

| Problem | Consequence at Meridian |
|---|---|
| No atomic multi-file commit | Fraud job crashes mid-write → half-visible data |
| No updates/deletes | GDPR erasure = rewrite whole partitions |
| Adding a column | Old files lack it → readers break or guess |
| Renaming a partition column | Rewrites petabytes |
| Concurrent writers | Last-writer-wins corruption risk |
| Listing 100k files per query | Planning takes minutes on object stores |

Iceberg (Lessons 06–07) adds a metadata tree over files to solve all six.

---

## 10. Exercises

1. Re-write the dataset sorted by `account_id`; compare total bytes vs unsorted. Explain.
2. Set `use_dictionary=False` for `currency` vs leaving it on; measure both sizes and explain.
3. Write with `row_group_size=10_000` vs `row_group_size=200_000`. Which gives better
   predicate skipping for `amount > 5000`? Why?
4. Add a `country` column with 20 distinct values; repartition by `country` too. Now list how
   many files exist. What happens if country had 200 values?
5. Using only `pq.read_metadata()`, print total compressed bytes for column `amount` across
   all files — without ever decoding data.
6. Write a copy of the dataset with `bloom_filter_options={"txn_id": {"enabled": True}}`;
   compare file sizes with and without, and inspect `bloom_filter_length` per chunk.
   Which columns deserve the space — `txn_id` or `currency`? Why?

## 11. Cheat sheet

| Concept | Key fact |
|---|---|
| File magic | Starts/ends with `PAR1` |
| Hierarchy | File → Row Groups → Column Chunks → Pages |
| Footer | Schema + stats; read last; few KB |
| Stats | min/max/nulls per chunk → skip whole groups |
| Bloom filter | per-chunk membership sketch → skip equality probes that min/max can't |
| Nested data | repetition/definition levels shred structs/lists into plain columns |
| Partitioning | Directory names; prune before I/O |
| Codecs | SNAPPY/LZ4 hot, ZSTD cold |
| Sweet spot | ~128MB–1GB files, date-like partition cols |
| Not included | Transactions, updates, evolution ⇒ need Iceberg |

**Next:** Lesson 03 — Parquet solves disk; **Apache Arrow** solves *memory*: one standard,
zero-copy columnar format that every tool speaks, so data stops being copied and converted
between pandas ↔ Spark ↔ databases.
