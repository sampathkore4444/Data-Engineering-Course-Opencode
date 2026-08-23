# Lesson 02 — Apache Parquet: The Columnar File Format

> **Meridian Trust Bank case study, Part 2**: The fraud team's scans are fast now, but their
> data lake is a swamp of 40,000 tiny CSV files written by 12 different services. Files have
> no schema, dates are in three formats, and nobody knows which file contains quarter-end data.
> Parquet fixes the *file*, and partitioning + statistics fix the *swamp*.

---

## Table of Contents

| Section | Topic |
|---|---|
| [1](#1-what-parquet-is) | What Parquet is |
| [2](#2-file-anatomy) | File anatomy |
| [3](#3-encodings-inside-parquet) | Encodings inside Parquet |
| [4](#4-compression) | Compression |
| [5](#5-predicate--projection-pushdown--how-skipping-actually-works) | Predicate & projection pushdown |
| [6](#6-datasets-many-files--one-logical-dataset) | Datasets: many files = one logical dataset |
| [7](#7-banking-scenario-walkthrough) | Banking scenario walkthrough |
| [8](#8-end-to-end-example) | End-to-end example |
| [9](#9-where-plain-parquet-hurts-the-iceberg-cliffhanger) | Where plain Parquet hurts |
| [10](#10-exercises) | Exercises |
| [11](#11-cheat-sheet) | Cheat sheet |

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

### Bloom filter trade-offs: when to use them (and when not to)

Bloom filters are not free. Understanding their cost/benefit prevents misuse:

| Factor | Impact |
|---|---|
| **Space cost** | ~10 bits per value. A 1-billion-row column with bloom costs ~1.2 GB of metadata per file. At 100K files that adds up. |
| **False positive rate (fpp)** | fpp=0.01 means 1% of chunks decode unnecessarily. Lower fpp = more space. Typical: 0.01 (1%) or 0.001 (0.1%). |
| **Write cost** | Bloom filter construction adds ~5–10% write latency. Negligible for batch ETL; noticeable for streaming micro-batches. |
| **Read benefit** | Eliminates point-lookup I/O for columns where min/max spans the entire value range. Can save 10–100× on high-cardinality equality probes. |

**When to bloom:**
- High-cardinality columns used in equality lookups: `card_id`, `txn_uuid`, `account_id`
- Point-in-time lookups: "find this specific transaction"
- Join keys between large tables

**When NOT to bloom:**
- Low-cardinality columns (`currency`, `status`, `channel`): min/max stats already prune effectively
- Range queries (`amount > 1000`): bloom filters only help equality, not ranges
- Columns you never filter on: pure waste of space

**Banking example:** Meridian enables bloom on `card_id` and `txn_uuid` (point lookups for
fraud investigation), but NOT on `currency` (only 3 values — min/max handles it) and NOT on
`amount` (range queries, not equality).

```python
# selective bloom: only on columns used in equality probes
pq.write_table(tbl, "txns.parquet",
    bloom_filter_options={
        "card_id":  {"enabled": True, "fpp": 0.01},   # high cardinality, point lookups
        "txn_uuid": {"enabled": True, "fpp": 0.001},  # very high cardinality, fpp matters
        # "currency": NOT bloomed — 3 values, min/max is enough
        # "amount":   NOT bloomed — range queries, bloom doesn't help
    })
```

### Time zone handling in Parquet timestamps

Banks operate across time zones. Parquet and Arrow distinguish three timestamp representations:

| Type | Parquet physical type | Meaning |
|---|---|---|
| `INT64` (logical: `TIMESTAMP_MILLIS`) | milliseconds since epoch | no timezone — **ambiguous** |
| `INT64` (logical: `TIMESTAMP_MICROS`) | microseconds since epoch | no timezone — **ambiguous** |
| `INT32/INT64` (logical: `DATE`) | days since epoch | date only, no tz issue |

Parquet stores timestamps as **integers since epoch** (UTC). The timezone annotation is
metadata only — readers must handle it. The trap:

```
A card transaction at 2026-07-01 23:30:00 UTC+8 (Tokyo) = 2026-07-01 15:30:00 UTC

If stored WITHOUT timezone info:
  - A UTC reader sees: 2026-07-01 15:30:00 UTC  ✓ correct
  - A local-time reader sees: 2026-07-01 23:30:00 (thinks it's UTC) ✗ WRONG date partition!

Result: the same transaction lands in DIFFERENT day partitions depending on the reader.
```

**Banking rule: always store timestamps in UTC with timezone metadata.**

```python
import pyarrow as pa

# CORRECT: timezone-aware timestamp
schema = pa.schema([
    pa.field("ts", pa.timestamp("us", tz="UTC")),  # unambiguous
    pa.field("amount", pa.float64()),
])

# WRONG: ambiguous — readers may interpret as local time
schema_bad = pa.schema([
    pa.field("ts", pa.timestamp("us")),  # no tz! dangerous
])
```

**Partitioning with time zones:** partition by `DATE(ts)` after converting to UTC,
not by local date. This ensures a Tokyo transaction and a London transaction
on the same UTC day land in the same partition:

```python
import pyarrow.compute as pc

# normalize to UTC before partitioning
ts_utc = pc.cast(table.column("ts"), pa.timestamp("us", tz="UTC"))
table = table.set_column(
    table.schema.get_field_index("ts"), "ts", ts_utc)

# partition by UTC date, not local date
table = table.append_column("txn_date", pc.cast(ts_utc, pa.date32()))
pq.write_to_dataset(table, path, partition_cols=["txn_date"])
```

**Banking scenario:** Meridian processes card transactions from 40 countries. A fraud alert
for "5 transactions in 10 minutes" must use UTC timestamps — otherwise a card used in
New York (UTC-5) and then Tokyo (UTC+9) appears to have a 14-hour gap instead of the
real 2-hour gap. Regulatory quarter-end cutoffs ("all transactions before March 31 23:59:59 UTC")
are always in UTC.

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
7. Write transactions with timestamps in `tz="UTC"` and WITHOUT tz. Read both back and
   partition by date. Show how the same Tokyo transaction lands in different partitions
   depending on reader timezone interpretation.
8. Enable bloom on `card_id` with fpp=0.01 and fpp=0.001. Compare file sizes and measure
   how many false-positive chunk decodes each produces for 1000 random card lookups.

---

## 11. Interview questions: Parquet in banking

### Concept 1: Parquet file structure

**Q1: Explain the Parquet file hierarchy. Why does this structure matter for analytical queries?**

A: File → Row Groups → Column Chunks → Pages. Row groups are the unit of parallelism — each can be read independently. Column chunks store all values for one column in a row group. Pages are the unit of compression and I/O (typically 1MB). For analytics, this means: (1) you can skip entire row groups via min/max stats, (2) you read only needed column chunks, and (3) pages compress independently.

**Q2: Why does Parquet store the file footer at the END, not the beginning?**

A: The footer contains schema, statistics, and bloom filters — metadata needed to plan the query. By storing it at the end, writers can append row groups without rewriting the footer. Readers seek to the end, read the footer (few KB), plan which row groups to skip, then read only the needed data. This is efficient for both writing (append-only) and reading (metadata-first).

**Q3: A Parquet file has 10 row groups. A query filters on `amount > 1000`. How does Parquet skip data?**

A: Each row group stores min/max statistics for `amount`. If a row group's max is 500, the entire row group is skipped without reading any data. With 10 row groups, if 7 have max < 1000, you skip 70% of the file. This is predicate pushdown at the row group level.

**Q4: What's the difference between row group size and page size, and how do they affect performance?**

A: Row group size (typically 128MB-1GB) determines parallelism — each row group can be read by a different thread. Page size (typically 1MB) determines compression and I/O granularity — smaller pages compress better but have more overhead. For banking analytics: larger row groups = fewer files to manage, smaller pages = better compression and skipping.

**Q5: How does Parquet handle nested data (e.g., a transaction with multiple line items)?**

A: Parquet uses Dremel-style record shredding with repetition and definition levels. A nested STRUCT becomes multiple columns with the same name, distinguished by repetition/definition levels. This flattens nested data into columnar format while preserving the structure. Readers reconstruct the nesting using these levels.

### Concept 2: Encodings and compression

**Q1: A bank stores transaction amounts as `DECIMAL(18,2)`. Which Parquet encoding is most effective, and why?**

A: Delta encoding + dictionary encoding. Decimal values are typically sorted or clustered (amounts around similar ranges). Delta encoding stores differences between consecutive values (small integers). Dictionary encoding groups similar values. Combined with ZSTD compression, this can reduce storage by 5-10× compared to raw decimals.

**Q2: Why does SNAPPY compression perform better than GZIP for analytical workloads?**

A: SNAPPY is optimized for speed (compression/decompression throughput), while GZIP is optimized for ratio. Analytical workloads are I/O-bound, so decompression speed matters more than file size. SNAPPY decompresses 2-3× faster than GZIP, which directly translates to faster queries. The file size difference is typically only 10-20%.

**Q3: How does dictionary encoding interact with predicate pushdown for a `currency` column?**

A: Dictionary encoding stores a small dictionary (EUR, USD, GBP) and 1-byte indices. When filtering `currency = 'EUR'`, Parquet checks if 'EUR' is in the dictionary (O(1) lookup). If not present, the entire column chunk is skipped. If present, it filters the 1-byte indices. This is much faster than scanning string values.

**Q4: A Parquet file uses ZSTD compression. What's the typical compression ratio for numeric banking data?**

A: For sorted numeric data (timestamps, IDs), ZSTD achieves 8-15× compression. For unsorted amounts, 4-8×. For low-cardinality strings (currency, status), 10-20× with dictionary encoding. Realistic overall: 5-10× for mixed banking data. The exact ratio depends on data distribution, sort order, and ZSTD level.

**Q5: Why is LZ4 preferred for hot data and ZSTD for cold data?**

A: LZ4 is the fastest compression/decompression algorithm — ideal for frequently queried (hot) data where decompression speed matters. ZSTD achieves better compression ratios — ideal for rarely queried (cold) data where storage cost matters. The trade-off: LZ4 decompresses 2× faster but compresses 30% less than ZSTD.

### Concept 3: Partitioning and pushdown

**Q1: A banking table is partitioned by `date`. A query filters `date = '2026-07-15' AND amount > 1000`. How does Parquet optimize this?**

A: Two-level pruning: (1) Partition pruning — only the `date=2026-07-15` directory is scanned (skips 364 other days). (2) Row group pruning — within that day's files, row groups with max(amount) ≤ 1000 are skipped. (3) Page pruning — within surviving row groups, pages with max ≤ 1000 are skipped. The result: only relevant pages are decompressed.

**Q2: What's the problem with partitioning by `card_id` (high cardinality)?**

A: Too many partitions (one per card = millions of directories). This causes: (1) listing overhead — S3 API calls to list millions of objects, (2) small files — each partition has few rows, (3) metadata bloat — millions of partition entries. Better: partition by date (low cardinality) and use bucketing for card_id.

**Q3: How does predicate pushdown differ from partition pruning?**

A: Partition pruning eliminates entire directories before reading any data (e.g., skip all days except July 15). Predicate pushdown eliminates row groups within a partition using min/max statistics (e.g., skip row groups where max(amount) < 1000). Partition pruning is coarse-grained (directory level), predicate pushdown is fine-grained (row group level).

**Q4: A bank has 100 TB of transaction data partitioned by month. A query needs 3 months of data. How much data is actually read?**

A: With partition pruning, only 3/12 = 25% of directories are listed. Within those directories, predicate pushdown may skip additional row groups. Realistic: 20-25% of total data is scanned. Without partitioning, 100% would be scanned. The savings come from avoiding I/O on 9 months of irrelevant data.

**Q5: Why does Parquet store min/max statistics per row group, not per file?**

A: Row groups are the unit of parallelism and skipping. Per-file statistics would be too coarse — a 1GB file might have row groups ranging from 100 to 5000 in amount. Per-row-group statistics allow finer-grained skipping. The overhead is minimal (a few bytes per row group) but the skipping benefit is enormous.

### Concept 4: Bloom filters

**Q1: When should you use a Bloom filter on a Parquet column? When should you NOT?**

A: USE for: high-cardinality equality lookups (card_id, txn_uuid) where min/max can't help. DON'T USE for: low-cardinality columns (currency, status) — dictionary encoding is more efficient. DON'T USE for: range queries — Bloom filters only help with equality. The trade-off: ~10 bits/value storage overhead vs skipping unnecessary decodes.

**Q2: A Bloom filter has fpp=0.01 (1% false positive rate). What does this mean in practice?**

A: For 1000 random lookups, 10 will falsely match (the Bloom filter says "maybe present" when it's not). This causes 10 unnecessary page decodes. The benefit: 990 lookups are answered without decoding any pages. The net effect: 99% of equality lookups are answered instantly.

**Q3: How does a Bloom filter interact with dictionary encoding?**

A: Dictionary encoding stores a small dictionary (e.g., 1000 card_ids). A Bloom filter on the same column adds a probabilistic membership test. For equality lookups: check Bloom filter first (O(1)), then dictionary (O(1)). The Bloom filter skips column chunks where the value definitely isn't present; the dictionary confirms presence. Together, they're extremely efficient.

**Q4: A bank queries `card_id = 300001` across 1000 Parquet files. Without Bloom filters, how many pages are decoded?**

A: Without Bloom filters: every page containing `card_id` must be decoded to check for the value. With 1000 files × 10 row groups × 5 pages = 50,000 page decodes. With Bloom filters: only pages where the Bloom filter says "maybe present" are decoded — typically 5-50 page decodes. The savings: 1000× fewer decodes.

**Q5: Why is Bloom filter size measured in bits per value, not bytes?**

A: Bloom filter space is proportional to the number of values, not their size. A card_id (8 bytes) and a timestamp (8 bytes) each need ~10 bits for a 1% fpp. The filter doesn't store the values — it stores hash fingerprints. The bits-per-value metric directly tells you the overhead: 10 bits/value × 1M values = 1.25 MB overhead.

### Concept 5: Time zones and timestamps

**Q1: A Tokyo transaction at 11 PM JST on July 15 is stored as `2026-07-15 23:00:00 JST`. If the reader interprets this as UTC, which partition does it land in?**

A: UTC would be `2026-07-15 14:00:00` — still July 15. But if the timestamp were 10 AM JST on July 16, UTC would be `2026-07-16 01:00:00` — July 16 in UTC but July 15 in JST. This is why banks must normalize to UTC at ingestion: the same transaction lands in different partitions depending on timezone interpretation.

**Q2: Why does Parquet store timestamps as microseconds since epoch, not as strings?**

A: Epoch timestamps are fixed-width integers (8 bytes) that compress well (delta encoding) and sort correctly. Strings like "2026-07-15 23:00:00" are variable-length, don't compress as well, and sorting is lexicographic ("9" > "10"). Epoch timestamps enable efficient range queries and partition pruning.

**Q3: A bank processes transactions in 50 countries. How do you handle timezone normalization?**

A: Normalize ALL timestamps to UTC at ingestion (the card switch sends local time, the ingest job converts to UTC). Store with `tz="UTC"` metadata. For display, convert to local timezone at query time. This ensures: (1) consistent partitioning, (2) correct comparisons across timezones, (3) audit trail shows UTC (regulatory standard).

**Q4: What's the difference between `timestamp_us`, `timestamp_ns`, and `timestamp_tz` in Parquet?**

A: `timestamp_us` = microseconds precision, no timezone (local time). `timestamp_ns` = nanoseconds precision, no timezone. `timestamp_tz` = microseconds with timezone offset stored in the file. For banking: use `timestamp_us` with `tz="UTC"` metadata — microseconds are sufficient for transactions, UTC ensures consistency.

**Q5: A regulatory report requires "transactions as of March 31st, 11:59:59 PM EST". How do you handle this in Parquet?**

A: Convert the cutoff to UTC: March 31st 11:59:59 PM EST = April 1st 03:59:59 AM UTC. Filter `WHERE ts < '2026-04-01T03:59:59Z'`. Store all timestamps in UTC; convert to EST only for display. This ensures the regulatory cutoff is applied consistently regardless of the reader's timezone.

---

## 12. Cheat sheet

| Concept | Key fact |
|---|---|
| File magic | Starts/ends with `PAR1` |
| Hierarchy | File → Row Groups → Column Chunks → Pages |
| Footer | Schema + stats; read last; few KB |
| Stats | min/max/nulls per chunk → skip whole groups |
| Bloom filter | per-chunk membership sketch → skip equality probes that min/max can't; ~10 bits/value |
| Bloom fpp | false-positive rate: 0.01 = 1% unnecessary decodes; trade space for accuracy |
| Bloom when | high-cardinality equality lookups (card_id, txn_uuid); NOT ranges or low-cardinality |
| Nested data | repetition/definition levels shred structs/lists into plain columns |
| Partitioning | Directory names; prune before I/O |
| Codecs | SNAPPY/LZ4 hot, ZSTD cold |
| Timestamps | always UTC with tz metadata; partition by UTC date, not local date |
| Sweet spot | ~128MB–1GB files, date-like partition cols |
| Not included | Transactions, updates, evolution ⇒ need Iceberg |

**Next:** Lesson 03 — Parquet solves disk; **Apache Arrow** solves *memory*: one standard,
zero-copy columnar format that every tool speaks, so data stops being copied and converted
between pandas ↔ Spark ↔ databases.
