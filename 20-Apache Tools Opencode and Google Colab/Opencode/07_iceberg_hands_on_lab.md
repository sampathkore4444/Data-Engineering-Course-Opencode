# Lesson 07 — Iceberg in Production: A Banking Audit Drill

> **Meridian Trust Bank case study, Part 7**: Monday, 09:00. Three tickets land at once:
> (1) Internal audit wants the exact card-ledger state at 30 June 23:59:59.
> (2) Compliance demands a corrected merchant category for 1,000 mis-bucketed transactions.
> (3) The streaming team complains the table has 11,000 tiny files and scans crawl.
> One Iceberg table absorbs all three - this lesson is the drill.

---

## 1. Concept: the operational lifecycle

```
        streaming micro-batch appends (MoR deletes for corrections)
                            │
                            ▼
   ┌──────────── T I M E ────────────────────────────────────┐
   │  S0 ─▶ S1 ─▶ S2 ─▶ S3 ─▶ S4 ─▶ ...      snapshots       │
   │       each commit = new snapshot; old ones addressable  │
   └──────────────────────────────────────────────────────────┘
        │                │                    │
   time travel     row-level fixes      maintenance
   (audits)        (upsert/merge)       compaction + expire
                                        snapshots (nightly)
```

Production tables live by a schedule:

| Cadence | Job | Why |
|---|---|---|
| every minute-hour | append micro-batches | ingest latency |
| nightly | `rewrite_data_files` (bin-pack) | kill small files |
| nightly | `expire_snapshots(older_than=7d)` | bound metadata growth |
| weekly | `remove_orphan_files` | reclaim crashed-writer leaks |
| quarterly | partition-spec review | keep pruning effective |

## 2. Banking scenario: the audit drill

We replay the three tickets against one table using PyIceberg:

1. **Quarter-end proof** - scan an old snapshot id captured at month close.
2. **Correction via upsert semantics** - delete + re-append the fixed rows (MoR), then show
   both "as-was" and "as-is" views coexist.
3. **Small-file repair & history pinning** - compact via an atomic full-table overwrite,
   then TAG the quarter-end snapshot so retention jobs can never expire it - and see why
   physical expiry itself is an engine job.

## 3. End-to-end example

```python
"""
lesson07_audit_drill.py
Iceberg operations as a bank runs them: snapshot capture, corrections,
time travel, small-file compaction, snapshot expiry.
Deps: pip install "pyiceberg[pyarrow,sql-sqlite]" pyarrow numpy pandas duckdb
"""
import os, shutil
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
from pyiceberg.catalog import load_catalog
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform
from pyiceberg.schema import Schema
from pyiceberg.types import (NestedField, LongType, TimestampType,
                             DoubleType, StringType, IntegerType)
from pyiceberg.expressions import In

WORK = "/tmp/opencode/audit_drill"
shutil.rmtree(WORK, ignore_errors=True)
os.makedirs(f"{WORK}/wh", exist_ok=True)

catalog = load_catalog("meridian", **{
    "type": "sql",
    "uri": f"sqlite:///{WORK}/catalog.db",
    "warehouse": f"file://{WORK}/wh",
})
catalog.create_namespace("bank")

schema = Schema(
    NestedField(1, "txn_id",   LongType(),       required=True),
    NestedField(2, "card_id",  LongType(),       required=True),
    NestedField(3, "ts",       TimestampType(),  required=True),
    NestedField(4, "amount",   DoubleType()),
    NestedField(5, "mcc",      IntegerType()),
)
ARROW = pa.schema([
    pa.field("txn_id",  pa.int64(),           nullable=False),
    pa.field("card_id", pa.int64(),           nullable=False),
    pa.field("ts",      pa.timestamp("us"),   nullable=False),
    pa.field("amount",  pa.float64()),
    pa.field("mcc",     pa.int32()),
])

table = catalog.create_table(
    "bank.card_txns",
    schema=schema,
    partition_spec=PartitionSpec(PartitionField(
        source_id=3, field_id=1000,
        transform=DayTransform(), name="day")),
)

def batch(day, n, seed, hour_batch=0):
    rng = np.random.default_rng(seed)
    # unique ids across ALL micro-batches: day block + hour_batch sub-block
    return pa.Table.from_pydict({
        "txn_id":  (np.arange(n) + day * 100_000
                    + hour_batch * n).astype("int64"),
        "card_id": rng.integers(400_000, 410_000, n).astype("int64"),
        "ts": pd.to_datetime(f"2026-06-{day:02d}") +
              pd.to_timedelta(rng.integers(0, 86400, n), unit="s"),
        "amount":  np.round(rng.gamma(2, 40, n) + .5, 2),
        "mcc":     rng.choice([5411, 5541, 5812], n).astype("int32"),
    }, schema=ARROW)

# ---- ingest June like a streamer: many small appends ---------------------------
for day in range(28, 31):                 # Jun 28..30
    for hour_batch in range(4):           # 4 micro-batches/day -> 12 tiny files
        table.append(batch(day, 5_000,
                           seed=day * 10 + hour_batch, hour_batch=hour_batch))

table.refresh()
print("snapshots:", len(table.snapshots()))
print("data files:", len(table.scan().plan_files()))

# ================= TICKET 1: quarter-end proof ==================================
quarter_end_snapshot = table.current_snapshot().snapshot_id   # capture NOW
print("captured quarter-end snapshot:", quarter_end_snapshot)

# days pass... new transactions arrive...
table.append(pa.Table.from_pydict({
    "txn_id":  np.arange(3_000_000, 3_001_000).astype("int64"),
    "card_id": np.full(1_000, 410_000, dtype="int64"),
    "ts":      pd.to_datetime("2026-07-01") +
               pd.to_timedelta(np.arange(1_000), unit="s"),
    "amount":  np.full(1_000, 9.99),
    "mcc":     np.full(1_000, 5411, dtype="int32"),
}, schema=ARROW))

qe = table.scan(snapshot_id=quarter_end_snapshot).to_arrow()
now = table.scan().to_arrow()
print(f"rows @Jun30 close={len(qe)}  rows today={len(now)}")
assert len(qe) < len(now)          # history intact - provable state

# ================= TICKET 2: correct 1k mis-bucketed MCCs ======================
# find the wrong rows (mcc=5812 recorded as 5541 on Jun 29 by a faulty POS config)
bad_ids = list(range(2_900_000, 2_901_000))          # day-29 txn_id block
bad = table.scan(row_filter=In("txn_id", bad_ids)).to_arrow()
print("rows to correct:", len(bad))

fixed = bad.to_pandas()
fixed["mcc"] = 5812
fixed_pa = pa.Table.from_pandas(fixed, schema=ARROW, preserve_index=False)

table.delete(In("txn_id", bad_ids))                  # MoR delete file
table.append(fixed_pa)                               # corrected copy
after_fix = table.scan().to_arrow()
n_fixed = after_fix.filter(
    pc.equal(after_fix.column("txn_id"), 2_900_000)).to_pydict()["mcc"]
print("corrected mcc value:", n_fixed[0])

# audit can STILL see the pre-correction state:
pre_fix = table.scan(snapshot_id=quarter_end_snapshot).to_arrow()
print("history shows original mcc:",
      sorted(set(pre_fix.filter(
          pc.equal(pre_fix.column("txn_id"), 2_900_000)
      ).to_pydict()["mcc"])))

# ================= TICKET 3: small-file compaction ==============================
def data_file_count():
    table.refresh()
    return len(table.scan().plan_files())

print("files before compaction:", data_file_count())

# PyIceberg's overwrite() IS an atomic file swap: one commit deletes ALL old data
# files from the table and adds consolidated ones - the same guarantee Spark's
# rewrite_data_files gives, just done by hand.
all_data = table.scan().to_arrow()
table.overwrite(all_data)      # single commit: drop every old file, add one new

print("files after compaction :", data_file_count())

# ================ maintenance: pin audit history ==================================
# Honest limitation: PyIceberg implements table SEMANTICS - refs and rollback -
# while physical cleanup jobs (expire_snapshots, remove_orphan_files, production
# rewrite_data_files) belong to engines like Spark/Athena/Glue (Lesson 10 cadence).
# What we CAN do here is make future expiry SAFE: tag the quarter-end snapshot so
# retention jobs must skip it forever, and prove the pinned state still reads even
# though compaction just replaced every data file underneath it.
table.manage_snapshots().create_tag(
    quarter_end_snapshot, "audit_2026Q2").commit()
table.refresh()
print("snapshot refs:",
      {n: r.snapshot_ref_type.value for n, r in table.metadata.refs.items()})
pinned = table.scan(snapshot_id=quarter_end_snapshot).to_arrow()
print(f"tagged audit state still readable: {len(pinned)} rows "
      f"(unreferenced old files survive until an engine expires them)")

# mistakes stay reversible: rollback moves 'main' without editing history...
latest_before = table.current_snapshot().snapshot_id     # post-compaction head
first_append = min(s.timestamp_ms for s in table.snapshots())
target = next(s.snapshot_id for s in table.snapshots()
              if s.timestamp_ms == first_append)
rows_now = table.scan().to_arrow().num_rows
table.manage_snapshots().rollback_to_snapshot(target).commit()
table.refresh()
print(f"main rolled back to S1: rows {rows_now} -> {table.scan().to_arrow().num_rows}")

# ...and moving forward again is equally atomic
table.manage_snapshots().set_current_snapshot(latest_before).commit()
table.refresh()
print("main restored:", table.scan().to_arrow().num_rows, "rows")

# expire/orphan cleanup remain engine jobs - but note WHAT they must respect now:
# any snapshot pinned by a ref (our tag!) is untouchable regardless of age.

# ---- bonus: query the table with DuckDB via scan interop ------------------------
import duckdb
# hidden partitioning in action: we filter the SOURCE column ts - never a
# partition column - and Iceberg still prunes to exactly one day's files.
# to_duckdb returns a connection with the table registered (or pass your own).
rel_con = table.scan(
    row_filter="ts >= '2026-06-29T00:00:00' AND ts < '2026-06-30T00:00:00'"
).to_duckdb("card_txns")
print(rel_con.sql("""
    SELECT mcc, count(*) n, round(sum(amount),2) vol
    FROM card_txns GROUP BY mcc ORDER BY vol DESC
""").df())
```

Sample output (abridged):

```
snapshots: 13
data files: 12
captured quarter-end snapshot: 5192...3097
rows @Jun30 close=60000  rows today=61000

rows to correct: 1000
corrected mcc value: 5812
history shows original mcc: [5541]      <- provable before/after!

files before compaction: 14
files after compaction : 4              <- bin-packed per hidden day partition (Jun28-30 + Jul1)
snapshot refs: {'main': 'branch', 'audit_2026Q2': 'tag'}
tagged audit state still readable: 60000 rows (unreferenced old files survive until an engine expires them)
main rolled back to S1: rows 61000 -> 5000
main restored: 61000 rows

   mcc     n        vol
0  5812  ~7.3k  ~582k.xx      <- one pruned day (Jun 29), read through DuckDB
1  5411  ~6.4k  ~514k.xx
2  5541  ~6.3k  ~511k.xx

   mcc       n        vol
0 5541  50,xxx  4,1xx,xxx.xx
1 5812  51,xxx  4,0xx,xxx.xx
2 5411  48,xxx  3,9xx,xxx.xx
```

## 4. What each operation did to the metadata tree

```
append x12        -> S1..S12, each adds a manifest + data files
capture snapshot  -> quarter_end_snapshot id recorded in ticket system
correction        -> S13: +equality-delete-file(manifest) ; S14: +new parquet
compaction        -> S15: manifest list swaps 14 files -> 4 (one per day) atomically
tag audit_2026Q2  -> refs table gains an immutable pointer to S12; expiry must skip it
rollback/restore  -> 'main' moves S15 -> S1 -> S15; history itself is never edited
```

Note the correction produced **two truths**: history (5541) and current (5812). That is not a
bug; that is the audit requirement. Regulators get reproducible states; ops gets corrected
current views; nobody rewrites petabytes.

## 5. Production notes you will get asked in interviews

- **CoW vs MoR choice**: streaming ingest + rare corrections -> MoR. Heavy analytical reads,
  strict SLAs -> CoW (or MoR + frequent compaction).
- **Snapshot retention vs storage**: retention policy is a *compliance decision* first,
  cost second. Document it.
- **Concurrent commits**: optimistic concurrency; conflicts on same partitions resolve by
  retry-with-rebase. Design jobs so hot partitions aren't contended.
- **Catalog HA**: the catalog is your single point of truth; run REST catalog with backups.
- **Security**: catalogs/governance layers (Polaris/Lagom/Unity) enforce row/column masks
  at plan time - the table itself never enforces ACLs.

## 6. Exercises

1. The drill tagged the quarter-end state `audit_2026Q2`. Now add a BRANCH
   (`manage_snapshots().create_branch()`) pinned to the same snapshot, append day-31 data
   to the branch, and show where its lineage diverges from `main` - then explain how a
   movable branch differs from an immutable tag.
2. Simulate two concurrent writers appending simultaneously; catch the commit conflict and
   retry from the refreshed table state.
3. Compare scan planning time (`%time table.scan().plan_files()`) before and after
   compaction; correlate with manifest counts on disk.
4. Write a "GDPR erasure" function: delete all rows of one card across ALL days, then prove
   via time travel when the erasure took effect.
5. Set up a cron-style script skeleton that nightly: compacts, expires 7-day-old snapshots,
   and logs table metrics as JSON for monitoring.

## 7. Cheat sheet

| Task | PyIceberg |
|---|---|
| Load/create | `load_catalog(...)`, `catalog.create_table("ns.t", schema=Schema(...))` |
| Append | `table.append(arrow_table)` - one atomic snapshot |
| Overwrite | `with table.overwrite(arrow_table): ...` - replaces full dataset atomically |
| Delete | `table.delete("pred")` or `In("col", [...])` - MoR delete files |
| Time travel | `table.scan(snapshot_id=id)` |
| Evolve schema | `with table.update_schema() as u: u.add_column(...)` |
| Snapshots | `table.snapshots()`, `manage_snapshots()` |
| Tag audit state | `table.manage_snapshots().create_tag(snapshot_id, "name").commit()`; read via `table.metadata.refs` |
| Rollback | `manage_snapshots().rollback_to_snapshot(id)` / `.set_current_snapshot(id)` - moves main, never edits history |
| Plan check | `len(table.scan().plan_files())` - file count before reading |
| Interop | `scan.to_arrow()/to_pandas()/to_polars()/to_duckdb(alias)` |

**Next:** Lesson 08 - your tables are perfect, but every team connects differently.
Enter Flight SQL: one protocol, any language, Arrow end-to-end.
