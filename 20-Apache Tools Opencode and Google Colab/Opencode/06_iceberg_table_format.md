# Lesson 06 — Apache Iceberg: Tables on a Data Lake

> **Meridian Trust Bank case study, Part 6**: A GDPR erasure request arrives for customer
> C-88412. With plain Parquet (Lesson 02), complying means finding every file containing that
> ID and rewriting them - while 14 pipelines read those files. The regulator also asks:
> "Prove the ledger state as of March 31st." There is no answer in a file swamp.
> Apache Iceberg turns the file lake into real tables with ACID guarantees and time travel.

---

## 1. Concept: what a "table format" is

Files are bytes. **Tables** need semantics:

- Which files currently belong to the table? (files change over time!)
- What schema, what partitioning?
- Can two writers commit simultaneously without corruption?
- Can readers see a consistent snapshot mid-write?

A **table format** = a metadata layer that defines the answer to these questions on top of
object storage. Iceberg is an open standard table format (spec + implementations in
Spark/Flink/Trino/Presto/DuckDB/PyIceberg...).

The three generations of solving this:

| Generation | Example | Problem |
|---|---|---|
| Hive-format tables | directory listing = partition dirs | listing cost; no atomicity across engines; implicit stats |
| First-gen ACID formats | Delta Lake (transaction log), Apache Hudi (timeline + record indexes) | solved ACID, but each grew up inside one ecosystem (Spark); broader engine support came later |
| Open metadata-tree spec | **Apache Iceberg** | (the modern choice) |

All three modern formats deliver ACID-on-open-files - treat them as siblings with
different upbringings, not as winner vs losers:

| Format | Center of gravity | Signature strengths |
|---|---|---|
| **Delta Lake** | Spark / Databricks ecosystem | most mature tooling; UniForm exposes Delta tables to Iceberg/Hudi clients |
| **Apache Hudi** | streaming & CDC upserts | record-level indexes, incremental pulls, built for near-real-time mutation |
| **Apache Iceberg** | engine neutrality | hidden partitioning, partition evolution, broadest engine matrix (Spark/Trino/Flink/DuckDB/PyIceberg...) |

In practice the choice follows your compute ecosystem more than feature checklists.
This course uses Iceberg because engine independence is the property Meridian needs
most - but every concept from here on (snapshots, deletes, compaction) has a direct
equivalent in the other two.

```
                 THE ICEBERG IDEA (one sentence)
   A table is a POINTER to a TREE of metadata files; each commit atomically
   swaps the pointer, creating a new immutable SNAPSHOT. Readers never see
   partial state; old snapshots remain addressable => TIME TRAVEL.
```

## 2. Architecture: the Iceberg metadata tree

```
Catalog (maps "txns" -> current metadata.json location)
   e.g. Hive Metastore / Nessie / JDBC / REST (Polaris, Unity) / AWS Glue
        │
        ▼
metadata.json            (v53)  ← versioned; contains schemas, partition-specs,
        │                        snapshot list, current-snapshot-id
        ▼
Manifest List  (for current snapshot)
   one row per manifest: path, added_files, deleted_files,
   partitions bounds (min/max), ...
        │
        ▼
Manifest Files
   one row per DATA FILE: path, format(parquet/avro/orc),
   column-level min/max/null counts/nan counts, value counts,
   partition tuple, record count, split offsets...
        │
        ▼
DATA FILES  (.parquet - usually Lesson 02 files!)
```

All metadata layers above data files are themselves columnar-ish binary (Avro) with rich
statistics - so planning a query touches kilobytes-to-megabytes, not the petabytes below.

### How a query runs

```
1. Ask catalog for table -> returns current metadata.json pointer
2. Read metadata.json    -> get current snapshot id -> manifest list
3. Prune manifests by partition bounds
4. Prune data files by per-file column stats (like Parquet pushdown, but table-wide)
5. Read only surviving Parquet files' needed columns
```

### How a commit works

```
Writer: plan new/deleted data files
     -> write new data files
     -> write NEW manifest(s) referencing them
     -> write manifest list
     -> write metadata.json v54
     -> CAS-swap catalog pointer  v53 -> v54      ATOMIC!
Concurrent writer fails swap -> retries from fresh snapshot (optimistic concurrency)
Readers: always resolve pointer once -> consistent snapshot during their whole query
```

## 3. The superpowers, concretely

| Feature | Mechanism | Banking payoff |
|---|---|---|
| ACID transactions | atomic pointer swap + optimistic concurrency | fraud job & ETL write same table safely |
| Time travel | keep old snapshots addressable (`snapshot-id` / `AS OF`) | quarter-end reproducibility, audits |
| Schema evolution | schemas live in metadata, NOT files | add/rename/drop columns without rewrite |
| Partition evolution | partition-spec is per-snapshot metadata | change day->month partitioning; old data stays valid |
| Hidden partitioning | engine derives partition values FROM column predicates | users never write partition columns wrong |
| Row-level ops | copy-on-write / merge-on-read delete files | GDPR erasure, corrections, upserts |
| Engine independence | open spec, any engine reads same table | Spark writes, Trino+DuckDB+PyIceberg read |

### 3.1 Hidden partitioning (a genuinely big deal)

In Hive-world, analysts had to write `WHERE day = '2026-08-01'` matching directory names -
get it subtly wrong and you silently scan everything or miss rows.

Iceberg stores a **partition transform** (e.g., `days(ts)`, `months(ts)`, `bucket(16, id)`,
`truncate(10, amount)`). You query `WHERE ts >= X AND ts < Y`; the engine *automatically*
derives which partitions can match. Partitioning becomes an invisible performance detail.

### 3.2 Snapshot model = audit-grade history

Every change (append, overwrite, delete, alter) creates a snapshot with: parent id,
operation, timestamp, summary statistics. Keeping N days of snapshots gives you:

```sql
SELECT * FROM txns VERSION AS OF 4821137906233423103;   -- historical snapshot id
SELECT * FROM txns TIMESTAMP AS OF '2026-03-31 23:59';  -- quarter-end proof
```

### 3.3 Row-level changes: CoW vs MoR

Deletes/updates are implemented via **delete files**:

- **Copy-on-Write**: affected parquet files rewritten without the rows. Reads stay fast;
  writes expensive. Choose when reads dominate (regulatory marts).
- **Merge-on-Read**: write small positional/equality delete files; readers merge at scan.
  Writes fast, reads pay. Choose for streaming ingest (card events!).

### 3.4 Branches & tags: git-like navigation over history

Snapshots form a lineage; **snapshot references** let you name points in it:

| Ref | Semantics | Banking use |
|---|---|---|
| `main` (branch) | mutable lineage; every commit moves it | the live table |
| branch `release/2026Q3` | its own movable head: stage a whole release, validate, then fast-forward | release trains, what-if replays |
| tag `quarter_end_2026Q2` | **immutable** pointer to one snapshot | audit pins - retention jobs must skip tagged snapshots forever |

Two properties make this more than bookkeeping:

1. **Refs survive data-file rewrites.** A compaction may replace every file underneath
   a snapshot, but the tagged state stays readable until an engine explicitly expires
   files - refs are part of the expiry decision, not victims of it.
2. **Rollback is just moving `main`.** `manage_snapshots().rollback_to_snapshot(id)`
   un-mistakes a bad write in one atomic pointer swap. History is never edited.

### 3.5 Row-level ops have SQL names: UPDATE / DELETE / MERGE INTO

CoW/MoR are implementation machinery; engines expose them through standard verbs:

```sql
UPDATE txns SET mcc = 5812 WHERE txn_id BETWEEN ...;      -- CoW/MoR under the hood
DELETE FROM customers WHERE erasure_requested = TRUE;
MERGE INTO card_txns t                                    -- the upsert workhorse
USING staged_fixes s ON t.txn_id = s.txn_id
WHEN MATCHED THEN UPDATE SET mcc = s.mcc;
```

Lesson 07's "delete + re-append" correction is exactly what `MERGE INTO` performs,
atomically and in one statement. Know both layers: the SQL verb you write, and the
delete files / rewritten files it produces.

### 3.6 Schema evolution deep dive: field IDs, type widening, nested changes

Iceberg assigns each column a **permanent field ID** at creation time. This is the key
invariant that makes evolution safe across engines and time:

```
Snapshot v1:  field_id=1 "txn_id"  (int64)
              field_id=2 "amount"  (double)
              field_id=3 "currency" (string)

Snapshot v2:  field_id=1 "txn_id"  (int64)        ← same ID, even if renamed
              field_id=2 "amount"  (double)
              field_id=3 "currency" (string)
              field_id=4 "mcc"     (int32)         ← new column

Snapshot v3:  field_id=1 "txn_id"  (int64)
              field_id=2 "amount"  (double)
              field_id=3 "currency" (string)
              field_id=5 "risk"    (float)          ← field_id=4 was dropped;
                                                        new field gets next ID
```

**Safe operations (no data rewrite needed):**

| Operation | What happens | Banking example |
|---|---|---|
| **Add column** | New field ID appended; old files return NULL for it | Adding `merchant_city` from upstream upgrade |
| **Rename column** | Field ID stays; metadata updated | `amount` → `amount_eur` for clarity |
| **Drop column** | Field ID removed from schema; old files still readable (column invisible) | Dropping `memo` field no one uses |
| **Reorder columns** | Cosmetic; field IDs unchanged | Move `fraud_flag` to first position |

**Type widening (safe, no rewrite):**

| From | To | Why safe |
|---|---|---|
| `int32` | `int64` | wider type can hold all original values |
| `float32` | `float64` | more precision, no loss |
| `decimal(10,2)` | `decimal(18,2)` | wider scale, no loss |
| `string` | `string` | no-op (already variable-length) |

```sql
-- Iceberg SQL: widen amount from float64 to decimal (precision upgrade)
ALTER TABLE card_txns ALTER COLUMN amount TYPE decimal(18, 2);
-- old files still readable; new writes use decimal; readers see cast-on-read
```

**Unsafe operations (require rewrite or careful handling):**

| Operation | Risk | Mitigation |
|---|---|---|
| Narrowing type (`int64` → `int32`) | data loss if values overflow | validate first; Iceberg may reject |
| Drop + re-add same name | new field gets NEW ID; old data won't populate it | use rename instead |
| Changing nullability (`nullable` → `required`) | existing NULLs violate constraint | rewrite data first |

**Nested field evolution:** adding/removing fields inside a `STRUCT` works the same way —
each nested field gets its own permanent ID:

```python
# original schema with nested struct
schema_v1 = Schema(
    NestedField(1, "txn_id", LongType(), required=True),
    NestedField(2, "card", StructType(
        NestedField(3, "card_id", LongType()),
        NestedField(4, "expiry", StringType()),
    )),
)

# evolve: add risk_flags inside the card struct
with table.update_schema() as u:
    u.add_column("card.risk_flags", StringType())
# field_id=5 assigned to card.risk_flags — old files return NULL for it
```

**Banking scenario:** Meridian adds `merchant_city` (from an enriched upstream feed) to the
card_transactions table. The column doesn't exist in historical Parquet files. Iceberg's
field-ID mapping ensures: (a) old readers skip the column safely, (b) new readers see
NULLs for historical rows, (c) no data rewrite is needed. The schema evolution is recorded
in `metadata.json` diffs that auditors can trace.

### 3.7 PCI-DSS: column-level security and data masking

Banks storing card data must comply with **PCI-DSS** (Payment Card Industry Data Security
Standard). The key requirements that affect the data lake:

| PCI-DSS Requirement | Lakehouse implementation |
|---|---|
| **Mask PAN** (Primary Account Number) | Store only last 4 digits; full PAN tokenized at ingestion |
| **Restrict access to cardholder data** | Column-level masking via Iceberg views + catalog authZ |
| **Audit all access** | Flight SQL gateway logs principal + query + bytes served |
| **Encryption at rest** | S3 SSE / client-side encryption (not covered here) |
| **Key rotation** | Rotate encryption keys per policy; Parquet footers unaffected |

**Pattern: column-level masking via Iceberg views**

```
┌────────────────────────────────────────────────────────────────┐
│  Iceberg Table: bank.card_txns (stores masked PAN only)        │
│  Columns: txn_id, card_masked, amount, merchant, mcc, ...     │
│  card_masked = '****-1234' (last 4 digits only)               │
└──────────────────────┬─────────────────────────────────────────┘
                       │
        ┌──────────────┼──────────────────┐
        ▼              ▼                  ▼
   VIEW: fraud_team    VIEW: compliance   VIEW: merchant_analyst
   (full card_masked)  (full card_masked) (NO card column at all)
   + risk_score col    + audit_metadata   + merchant stats only
```

**Why NOT store full PANs in the lake:**
- Even encrypted PANs expand the attack surface — every replica, backup, and cache
  must be encrypted and access-logged.
- Tokenization at ingestion: the OLTP system holds the PAN-to-token mapping;
  the lake never sees raw card numbers.
- If a breach occurs, the regulator asks "was the full PAN ever exposed?" — answer:
  "No, only masked versions existed in the analytical layer."

```python
# Tokenization at ingestion (simplified)
def mask_pan(pan: str) -> str:
    """PCI-DSS: show only last 4 digits."""
    return "*" * (len(pan) - 4) + pan[-4:]

# in the Bronze ingestion pipeline
raw_df["card_masked"] = raw_df["card_number"].apply(mask_pan)
raw_df.drop(columns=["card_number"], inplace=True)  # DROP full PAN immediately

# Silver layer: only masked PANs exist
silver_table = pa.Table.from_pandas(raw_df)  # card_number is GONE
```

**Iceberg view-based access control (production pattern):**

```sql
-- governance catalog creates role-specific views
CREATE VIEW fraud_team.card_txns AS
SELECT txn_id, card_masked, amount, merchant, mcc, ts, risk_score
FROM bank.card_txns;

CREATE VIEW compliance.card_txns AS
SELECT txn_id, card_masked, amount, mcc, ts, audit_metadata
FROM bank.card_txns;

CREATE VIEW merchant_analyst.daily_summary AS
SELECT merchant, mcc, count(*) n, sum(amount) vol
FROM bank.card_txns
GROUP BY merchant, mcc;  -- NO card column at all
```

The catalog (REST/Polaris) enforces which views each principal can access. Flight SQL
gateways resolve the view at plan time — analysts never see the underlying table path.

**Banking scenario:** A PCI-DSS auditor asks: "Show me exactly which users accessed
cardholder data in the last 90 days." The Flight SQL gateway audit log answers:

```
2026-08-01 14:22 | risk_analyst | SELECT ... FROM fraud_team.card_txns | 847 rows | 2.1 KB
2026-08-01 14:25 | compliance   | SELECT ... FROM compliance.card_txns | 1,204 rows | 3.8 KB
2026-08-01 15:00 | merchant_analyst | SELECT ... FROM merchant_analyst.daily_summary | 42 rows | 0.4 KB
```

No raw PAN was ever accessed. The audit trail is complete and reproducible.


## 4. Catalogs: the source of truth for pointers

The catalog answers ONE question - "where is the current metadata for table X?" - but its
properties decide your guarantees:

```
                    ┌────────────────────────┐
   writers  ──swap──▶  CATALOG (atomic swap)  ◀──resolve── readers
                    └────────────────────────┘
     Hive Metastore   | atomic rename in HMS
     AWS Glue         | managed, S3-native
     Nessie           | git-like branches/tags for tables!
     REST (Polaris/   | open API; multi-cloud governance
       Unity/Lagom)   |
     JDBC             | simple, portable
     Hadoop (legacy)  | filesystem CAS - avoid
```

Bank pattern: **REST catalog** fronted by a governance layer -> every read/write is an
authenticated, auditable, row/column-filtered event (more in Lesson 10).

## 5. Maintenance: what keeps tables healthy

| Operation | Problem it solves | Iceberg tool |
|---|---|---|
| Compaction / `rewrite_data_files` | small-file disease from streaming ingest | bin-pack files to ~512MB |
| `expire_snapshots` | metadata bloat from long snapshot retention | delete files only referenced by expired snapshots |
| `remove_orphan_files` | crashed jobs leak untracked files | delete files not referenced by any metadata (careful!) |
| `rewrite_manifests` | fragmented manifests slow planning | consolidate |

Rule: streaming writes + nightly compaction + weekly expiry = fast and cheap forever.

## 6. Banking scenario walkthrough: quarter-end, three ways

**Without Iceberg**: ETL copies the entire ledger to a frozen "quarter_end_2026Q2" copy.
Storage doubles; "which copy is canonical?" becomes an audit finding.

**With Iceberg**: nothing is copied. The regulator asks for March 31st state ->
`SELECT ... TIMESTAMP AS OF '2026-03-31'`. Snapshots ARE the proof. Storage cost = only the
files that differ between then and now.

**GDPR erasure**: `DELETE FROM customers WHERE customer_id = 'C-88412'` creates a new
snapshot where those rows are gone; MoR delete files mark them dead in current data files;
compaction physically rewrites affected files; after `expire_snapshots`, the old snapshots
(and their files) age out per policy. Compliance gets a reproducible story end-to-end.

## 7. Format spec details worth knowing

- **Data formats**: Parquet (dominant), ORC, Avro - mixable within one table.
- **Types**: full nested model (struct/list/map), decimal, timestamp with tz,
  `timestamp_ns`; IDs are assigned per field so renames never break lineage.
- **Partition transforms**: years/months/days/hours, bucket(N), truncate(W) -
  all hidden from queries.
- **Sort orders**: declared per table, used by writers (z-order/linear) improving
  data clustering & pruning.
- **Invariants**: schema field-ids make column evolution safe across engines.

## 8. End-to-end example: watch the metadata tree work

Using **PyIceberg** (pure-python Iceberg implementation) with a SQL catalog on local disk,
we: create a table, append, overwrite, delete rows, evolve schema, and time-travel - then
inspect every metadata artifact by hand.

```python
"""
lesson06_iceberg_lab.py
PyIceberg + DuckDB: ACID, time travel, hidden partitioning, evolution.
Deps: pip install "pyiceberg[pyarrow,sql-sqlite]" pyarrow duckdb
"""
import os, shutil
import numpy as np
import pandas as pd
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform
from pyiceberg.schema import Schema
from pyiceberg.types import (NestedField, LongType, TimestampType,
                             DoubleType, StringType, FloatType)
from pyiceberg.expressions import EqualTo

WORK = "/tmp/opencode/iceberg"
shutil.rmtree(WORK, ignore_errors=True)
os.makedirs(f"{WORK}/warehouse", exist_ok=True)

# ---- 1. local SQL catalog (SQLite) backed warehouse ---------------------------
catalog = load_catalog(
    "meridian",
    **{
        "type": "sql",
        "uri": f"sqlite:///{WORK}/catalog.db",
        "warehouse": f"file://{WORK}/warehouse",
    })

# ---- 2. namespace + table with HIDDEN day partitioning -------------------------
catalog.create_namespace("bank")

# Iceberg-native schema: FIELD IDS are permanent identities used by every
# engine; renames never break downstream readers.
schema = Schema(
    NestedField(1, "txn_id",   LongType(),      required=True),
    NestedField(2, "card_id",  LongType(),      required=True),
    NestedField(3, "ts",       TimestampType(), required=True),
    NestedField(4, "amount",   DoubleType(),    required=False),
    NestedField(5, "currency", StringType(),     required=False),
)

table = catalog.create_table(
    "bank.txns",
    schema=schema,
    partition_spec=PartitionSpec(
        PartitionField(source_id=3, field_id=1000,
                       transform=DayTransform(), name="ts_day")),
)

# Arrow side must mirror nullability exactly ("required" <=> nullable=False)
ARROW_SCHEMA = pa.schema([
    pa.field("txn_id",   pa.int64(),          nullable=False),
    pa.field("card_id",  pa.int64(),          nullable=False),
    pa.field("ts",       pa.timestamp("us"),  nullable=False),
    pa.field("amount",   pa.float64()),
    pa.field("currency", pa.string()),
])

# ---- 3. append day-1 data (a real commit -> snapshot #1) ------------------------
def make_txns(day, n, seed):
    rng = np.random.default_rng(seed)
    return pa.Table.from_pydict({
        "txn_id":   np.arange(n) + day * 1_000_000,
        "card_id":  rng.integers(400_000, 410_000, n).astype("int64"),
        "ts":       pd.to_datetime(f"2026-07-{day:02d}") +
                    pd.to_timedelta(rng.integers(0, 86400, n), unit="s"),
        "amount":   np.round(rng.gamma(2, 40, n) + .5, 2),
        "currency": rng.choice(["EUR", "USD"], n),
    }, schema=ARROW_SCHEMA)

table.append(make_txns(1, 50_000, seed=1))
print("snapshots after append:",
      [(s.snapshot_id, s.summary.operation) for s in table.snapshots()])

# ---- 4. second append (snapshot #2) ---------------------------------------------
table.append(make_txns(2, 30_000, seed=2))
table.refresh()                       # pick up our own latest pointer
current_id = table.current_snapshot().snapshot_id

# ---- 5. TIME TRAVEL: read the FIRST snapshot -------------------------------------
first_id = table.snapshots()[0].snapshot_id
scan1 = table.scan(snapshot_id=first_id).to_arrow()
scan_now = table.scan().to_arrow()
print(f"rows in snapshot#1={len(scan1)}  rows now={len(scan_now)}")

# ---- 6. row-level DELETE (GDPR-style erasure on card 400001) ----------------------
table.delete("card_id = 400001")      # MoR delete file + new snapshot
after_del = table.scan(
    row_filter=EqualTo("card_id", 400001)).to_arrow()
print("rows for erased card:", len(after_del))     # -> 0

# ---- 7. SCHEMA EVOLUTION: add column without rewriting data -----------------------
with table.update_schema() as update:
    update.add_column("risk_score", FloatType())  # Iceberg type, not Arrow
evolved = table.scan().to_arrow()
print("schema now has risk_score:", "risk_score" in evolved.schema.names)

# ---- 8. inspect the metadata tree by hand ------------------------------------------
for root, dirs, files in os.walk(WORK):
    rel = root.replace(WORK, ".")
    depth = rel.count("/")
    if depth <= 3:
        name = os.path.basename(root) or "."
        print("  "*depth + name + "/")
        for f in sorted(files)[:4]:
            print("  "*(depth+1) + f)
```

Run it and then peek at what got created:

```
warehouse/bank/txns/
├── metadata/
│   ├── 00000-...metadata.json        <- v0: create
│   ├── 00001-...metadata.json        <- v1: append day-1
│   ├── ...
│   ├── version-hint.text             <- current metadata version
│   └── *.avro manifests...
├── data/ts_day=2026-07-01/*.parquet  <- hidden partition dirs
└── data/ts_day=2026-07-02/*.parquet
```

## 9. Querying the same table from DuckDB (engine interop)

```python
import duckdb, glob
con = duckdb.connect()
try:
    con.execute("LOAD iceberg;")          # built into recent duckdb releases
except Exception:
    con.execute("INSTALL iceberg; LOAD iceberg;")

# pass the latest metadata JSON as the table location (works everywhere;
# a REST/HMS catalog would resolve this pointer for you in production)
meta = max(glob.glob(f"{WORK}/warehouse/bank/txns/metadata/*.metadata.json"))
rows = con.sql(f"""
    SELECT count(*), sum(amount)
    FROM iceberg_scan('{meta}')
""").fetchone()
print("duckdb sees:", rows)     # (79_993, ...) - post-erasure snapshot!
```

The same physical table that PyIceberg wrote is read by DuckDB with zero conversion - Spark,
Trino, Flink would do the same against a shared catalog. This decoupling of *storage truth*
from *compute engines* is the whole point of the lakehouse.

## 10. Exercises

1. Append day-3 data, then time-travel to snapshot #2 and compute the difference in rows.
2. Evolve the partition spec from `days(ts)` to `hours(ts)`; append new data; verify old
   partitions still query correctly (partition evolution!).
3. Perform an `overwrite` that replaces all EUR amounts > 500 with a "flagged" marker column
   value; inspect `snapshot.summary` to see files added/deleted.
4. Delete with `table.delete(...)` twice for two different cards; count delete files on disk;
   then simulate compaction by rewriting (PyIceberg exposes maintenance via add-files APIs -
   or use DuckDB/Spark if available) and observe file counts drop.
5. Point a second catalog instance at the same SQLite URI and prove readers can list/read
   while a writer commits.
6. Evolve the schema: add `merchant_city` (StringType) and `risk_flags` (StructType with
   nested fields `aml: BooleanType`, `kyc: BooleanType`). Append new data with these columns;
   time-travel to a snapshot before the evolution and verify old data returns NULLs.
7. Widen `amount` from `DoubleType()` to a higher-precision type. Append data in both types;
   query all rows and verify no precision loss.
8. Build a PCI-DSS demo: create the card_txns table with only `card_masked` (last 4 digits).
   Create two Iceberg views (`fraud_team` sees card_masked + risk; `merchant_analyst` sees
   only merchant + mcc). Query through DuckDB and prove each view returns different columns.

## 11. Cheat sheet

| Concept | Fact |
|---|---|
| Table | pointer -> metadata.json -> manifest list -> manifests -> parquet |
| Commit | atomic catalog swap of metadata pointer; optimistic concurrency |
| Snapshot | immutable table state; id + parent + operation + summary |
| Time travel | `scan(snapshot_id=...)` / `VERSION AS OF` / `TIMESTAMP AS OF` |
| Hidden partitioning | transforms (days/bucket/truncate); users filter columns only |
| Schema evolution | field IDs in metadata; add/rename/drop without rewrite |
| Type widening | int32→int64, float32→float64 safe; narrowing requires rewrite |
| Nested evolution | add/remove struct fields via field IDs; old files return NULL |
| PCI-DSS masking | tokenize PAN at ingestion; store only last-4 in lake; views enforce access |
| Deletes | equality/positional delete files (MoR) or rewritten files (CoW) |
| SQL row verbs | UPDATE / DELETE / MERGE INTO = delete files or rewrites underneath |
| Branches & tags | named snapshot refs: tags immutable (audit pins), branches movable heads; rollback = move main |
| Maintenance | rewrite_data_files, expire_snapshots, remove_orphan_files |
| Catalogs | HMS, Glue, Nessie, REST, JDBC - swap pointer atomically |

**Next:** Lesson 07 puts Iceberg through a full banking audit drill: upserts, GDPR erasure,
quarter-end time travel, and maintenance schedules.
