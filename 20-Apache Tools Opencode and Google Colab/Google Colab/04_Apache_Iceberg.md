# Lesson 4 — Apache Iceberg
## ACID Tables on the Data Lake

> **Banking case:** At FinBank's quarter close, three jobs run at once: (1) Spark backfills June data, (2) a CDC job streams intraday card txns into the same table, (3) an auditor re-reads "June 30 EOD" numbers to sign off the report. On raw Parquet folders this was a disaster: readers saw half-written files, two writers corrupted each other, and nobody could reproduce the audited snapshot. With **Apache Iceberg**: all three run concurrently, safely — and the auditor time-travels to the exact certified snapshot.

---

## 1. The Problem: A Folder of Parquet Files Is Not a Table

Lesson 1 gave us columnar *files*. But a "table" that's just `s3://lake/txns/*.parquet` has brutal flaws:

| Flaw | What goes wrong in a bank |
|---|---|
| **No atomicity** | Job crashes mid-write → table now contains partial data. Quarter-close reports are wrong. |
| **No isolation** | Reader lists files while writer adds/removes them → phantom or missing rows. |
| **No concurrent writes** | Two pipelines appending = both rename/overwrite the same folder → corruption or silent loss. |
| **No evolution** | Adding a column? Renaming? Rewriting every file. |
| **No deletes/updates** | GDPR erasure request ("delete customer X") → rewrite entire dataset. |
| **No history** | Auditor asks "what did we report as of June 30?" → shrug. |

These are exactly the problems databases solved decades ago with transaction metadata. Iceberg brings that metadata layer **to object storage**.

> **Table format** = a specification for organizing data files + metadata so many engines can treat a lake folder as a real database table with transactions.

Competitors: Delta Lake (Databricks) and Apache Hudi solve the same class of problem. Iceberg is vendor-neutral and became the industry convergence point (Snowflake, BigQuery, Spark, Trino, Flink, DuckDB all read/write it).

| | **Iceberg** | Delta Lake | Apache Hudi |
|---|---|---|---|
| Backed by | OSS community, neutral | Databricks-first (OSS Delta too) | Uber-origin, OSS |
| Hidden partitioning | ✅ native | limited (generated columns) | partial |
| Partition evolution | ✅ without rewrite | ❌ | ❌ |
| Engine support | broadest (Spark/Trino/Flink/DuckDB/Snowflake…) | Spark/Databricks strongest | Spark/Flink focused |
| Sweet spot | multi-engine lakehouse | Databricks-centric shops | heavy CDC/upsert streaming |

---

## 2. How Iceberg Works — The Metadata Tree

```
s3://lake/transactions/
├── data/
│   ├── txn_2026_07_01__0001.parquet     ← immutable data files
│   ├── txn_2026_07_01__0002.parquet
│   └── ...
└── metadata/
    ├── 00000-<uuid>.metadata.json       ← current pointer: latest version
    ├── v1.metadata.json
    ├── v2.metadata.json                 ← full history retained!
    └── <uuid>.snap.avro                 ← snapshot + manifest lists

SNAPSHOT (one per commit)
  └── MANIFEST LIST (which manifests make up this snapshot)
        └── MANIFESTS (per-partition file lists + column stats)
              └── DATA FILES (parquet paths, min/max per column, null counts)
```

**Commit protocol:** a write is *staged*, then published by one atomic swap of the `metadata.json` pointer (catalog-assisted compare-and-swap). Readers either see the old snapshot completely or the new one completely. **That's ACID.**

What you get from this tree:

1. **ACID transactions** — multi-file commits are atomic; concurrent writers retry via optimistic concurrency.
2. **Time travel** — every snapshot is addressable (`VERSION AS OF n` / `TIMESTAMP AS OF '...'`).
3. **Partition & file pruning** — manifests hold per-file min/max; engines skip files before even listing them.
4. **Hidden partitioning** — you declare `days(ts)`; queries with `ts BETWEEN ...` prune correctly *even if the user forgets to filter on the partition column*. No more Hive's "partition column drift" bugs.
5. **Schema & partition evolution** — add/rename/drop columns is metadata-only; changing partition scheme works without rewriting history.
6. **Row-level ops** — copy-on-write or merge-on-read UPDATE/MERGE/DELETE.
7. **Engine independence** — any engine speaking the spec reads the same bytes.

### Format versions (what `format-version` means)

| Version | Adds | Notes |
|---|---|---|
| **v1** | Base spec (append-only snapshots) | Legacy; don't start new tables here |
| **v2** | Row-level deletes (position + equality), sequence numbers | The current workhorse; required for MoR upserts/deletes |
| **v3** | Default values, new deletion vectors, multi-arg transforms, nanosecond timestamps | Rolling out across engines in 2025+ |

### Beyond partitioning: sort orders & clustering

Partition pruning only helps when queries filter on the partition key. Iceberg also supports **table-level sort orders** (`ORDER BY account, ts`) and engines can compact with Z-order/multi-dimensional clustering so *every* column's min/max ranges stay tight → file-level pruning works for arbitrary filters. Think of it as "make Lesson 1's statistics useful, automatically, on every compaction."

---

## 3. The Catalog

Who says which `metadata.json` is current? The **catalog** — it owns the pointer and provides the atomic compare-and-swap that makes commits safe:

| Catalog | Atomic on object storage? | When to use |
|---|---|---|
| HadoopCatalog / filesystem | ❌ no atomic rename on S3 | Local demos only |
| Hive Metastore | ✅ (via DB) | Legacy Hadoop estates |
| **REST (Polaris, Gravitino, Lakekeeper, Unity)** | ✅ | The modern standard — engine-agnostic, multi-cloud |
| Nessie | ✅ + Git-like branches/tags | Environment isolation, "PR for data" |
| AWS Glue / Snowflake / BigQuery managed | ✅ | Staying inside one cloud platform |

For learning below we use PyIceberg's SQL catalog backed by SQLite/DuckDB — zero infra, same APIs.

---

## 4. 🏦 Banking Scenario — Regulatory Reporting Table with Time Travel

**FinBank requirements**

- RBI/Fed examiners certify "EOD balances as of June 30". Must be reproducible byte-for-byte months later → **snapshot pinning**.
- Intraday CDC stream appends while batch jobs correct records → **concurrent ACID writes**, MERGE for corrections.
- GDPR: erase a customer on request → **DELETE rows** without rewriting the whole lake.
- Schema changes must not break 14 downstream jobs overnight → **safe schema evolution**.

### End-to-end Python with PyIceberg

```bash
pip install "pyiceberg[sql-sqlite,pyarrow]" pyarrow pandas numpy
```

```python
"""
iceberg_bank.py — end-to-end Iceberg table lifecycle.
"""
import numpy as np, pandas as pd, datetime as dt
import pyarrow as pa
from pyiceberg.catalog import load_catalog

rng = np.random.default_rng(21)

# ============================================================
# STEP 0 — Catalog (SQLite-backed; swap 'sql' -> REST/Glue in prod)
# ============================================================
catalog = load_catalog(
    "finbank_lake",
    **{
        "type": "sql",
        "uri": "sqlite:///finbank_iceberg.db",   # catalog metadata store
        "warehouse": "file:///tmp/iceberg_warehouse",
    },
)
catalog.create_namespace_if_not_exists("banking")

# ============================================================
# STEP 1 — Create the table (declarative schema + partitioning)
# ============================================================
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField, LongType, StringType, DoubleType,
    TimestamptzType, IntegerType,
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform

schema = Schema(
    NestedField(1, "txn_id",   LongType(),      required=True),
    NestedField(2, "account",  StringType(),    required=True),
    NestedField(3, "amount",   DoubleType(),    required=False),
    NestedField(4, "mcc",      StringType(),    required=False),
    NestedField(5, "branch_id",IntegerType(),   required=False),
    NestedField(6, "txn_ts",   TimestamptzType(),required=True),
)

spec = PartitionSpec(
    PartitionField(source_id=6, field_id=1000,
                   transform=DayTransform(), name="txn_day")   # hidden partitioning
)

try:
    tbl = catalog.load_table("banking.transactions")
except Exception:
    tbl = catalog.create_table(
        "banking.transactions", schema=schema,
        partition_spec=spec,
        properties={
            "write.parquet.compression-codec": "zstd",
            "format-version": "2",           # row-level deletes support
        },
    )

# ============================================================
# STEP 2 — Append day 1 data (atomic commit #1)
# ============================================================
def gen_day(day: pd.Timestamp, n=200_000):
    return pd.DataFrame({
        "txn_id":   np.arange(n) + day.value % 10**7,
        "account":  rng.choice([f"A{i:06d}" for i in range(20_000)], n),
        "amount":   np.round(rng.lognormal(5, 1.2, n), 2),
        "mcc":      rng.choice(["5411","6011","5812","5999"], n),
        "branch_id":rng.integers(1, 50, n),
        "txn_ts":   day + pd.to_timedelta(rng.integers(0, 86400, n), unit="s"),
    })

d1 = dt.datetime(2026, 7, 1, tzinfo=dt.timezone.utc)
day1 = pa.Table.from_pandas(gen_day(pd.Timestamp(d1)))
tbl.append(day1)                                    # atomic commit #1
print("current snapshot:", tbl.current_snapshot().snapshot_id)

# ============================================================
# STEP 3 — Concurrent-style append of day 2 (atomic commit #2)
# ============================================================
d2 = d1 + dt.timedelta(days=1)
tbl.append(pa.Table.from_pandas(gen_day(pd.Timestamp(d2))))

# ============================================================
# STEP 4 — Time travel: read the world as of commit #1
# ============================================================
snap1 = [s for s in tbl.snapshots()][0]
old = tbl.scan(snapshot_id=snap1.snapshot_id).to_arrow()
now = tbl.scan().to_arrow()
print(f"rows @snapshot1={old.num_rows:,}  rows@head={now.num_rows:,}")
# Auditor pins the certified snapshot id in the sign-off document ✔

# ============================================================
# STEP 5 — Row-level correction: UPSERT (merge late-arriving fixes)
# ============================================================
fixes = pa.table({
    "txn_id":  old.column("txn_id")[:500],            # 500 corrected amounts
    "account": old.column("account")[:500],
    "amount":  pa.array(np.round(rng.uniform(1, 99_999, 500), 2)),
    "mcc":     old.column("mcc")[:500],
    "branch_id": old.column("branch_id")[:500],
    "txn_ts":  old.column("txn_ts")[:500],
})
tbl.upsert(df=fixes.to_pandas(),                    # PyIceberg upsert (v0.8+)
           join_cols=["txn_id"])                    # → delete+insert under the hood

after_fix = tbl.scan().to_arrow()
print("rows after merge:", after_fix.num_rows)      # count unchanged → true upsert

# ============================================================
# STEP 6 — GDPR delete: remove one customer, atomically
# ============================================================
victim = old.column("account")[0].as_py()
tbl.delete(delete_filter=f"account = '{victim}'")
print("deleted customer:", victim)

# ============================================================
# STEP 7 — Schema evolution: add column, no rewrite
# ============================================================
from pyiceberg.types import StringType as ST
with tbl.update_schema() as update:
    update.add_column(path=("risk_band"), field_type=ST(),
                      required=False)
print(tbl.schema())

# Backfill it for existing rows (full overwrite with the column populated)
backfill = tbl.scan().to_arrow().drop_columns(["risk_band"])
band = np.where(np.asarray(backfill.column("amount")) > 10_000, "HIGH", "NORMAL")
backfill = backfill.append_column(
    pa.field("risk_band", pa.string()), pa.array(band))
tbl.overwrite(backfill)

# ============================================================
# STEP 8 — Maintenance: expire old snapshots (keep last 7 days in prod)
# ============================================================
# Default retention keeps snapshots newer than 5 days; pass older_than=<ts>
# (or call repeatedly with a cutoff) to prune harder in production.
tbl.expire_snapshots()
print("remaining snapshots:", len(list(tbl.snapshots())))
```

### Reading the same table from other engines (the whole point!)

```python
# --- DuckDB reads Iceberg directly ---
import duckdb
con = duckdb.connect()
con.execute("INSTALL iceberg; LOAD iceberg;")
rows = con.sql("""
    SELECT txn_day, count(*) n, sum(amount) vol
    FROM iceberg_scan('file:///tmp/iceberg_warehouse/banking/transactions')
    GROUP BY 1 ORDER BY 1
""").show()
```

```sql
-- Spark SQL sees identical data:
SELECT * FROM banking.transactions VERSION AS OF <certified_snapshot_id>;
-- Trino:
SELECT count(*) FROM banking.transactions FOR TIMESTAMP AS OF TIMESTAMP '2026-06-30 23:59:59';
```

One table, five engines, guaranteed consistency. This is why banks standardize on Iceberg.

---

## 5. Write Modes: Copy-on-Write vs Merge-on-Read

When you UPDATE/DELETE, Iceberg can:

| Strategy | Mechanism | Read speed | Write speed | Use when |
|---|---|---|---|---|
| **Copy-on-Write (CoW)** | Rewrite affected data files immediately | Fast | Slow (rewrite) | Read-heavy tables, strict freshness |
| **Merge-on-Read (MoR)** | Write position/equality-delete files; merge at read time | Slower (merge) | Fast | Write-heavy CDC ingestion |
| **Hybrid** | Deletes as MoR, periodically compacted | Tunable | Tunable | Most lakes |

Related maintenance: **compaction** (`rewrite_data_files`) merges tiny files; **expire_snapshots** prunes history; **orphan cleanup** removes untracked files. Schedule these like DBA vacuum jobs.

## 6. Cheat Sheet

```python
from pyiceberg.catalog import load_catalog
cat = load_catalog(name="prod", type="rest", uri="https://polaris.bank/api",
                   credential="...")

tbl = cat.load_table("banking.transactions")
tbl.append(arrow_tbl)                       # atomic insert
tbl.overwrite(arrow_tbl)                    # replace snapshot contents
tbl.upsert(df, join_cols=["txn_id"])        # merge/upsert
tbl.delete(delete_filter="country='XX'")
tbl.add_files([parquet_paths])              # adopt existing parquet!

scan = tbl.scan(row_filter="amount > 10000",
                selected_fields=("txn_id","amount"))
arrow = scan.to_arrow(); pdf = scan.to_pandas()

# Time travel
old = tbl.scan(snapshot_id=<id>).to_arrow()
# Maintenance: expire snapshots; compaction via Spark procedure
#   CALL cat.system.rewrite_data_files(table => 'banking.txns')  -- or scheduled jobs
```

```sql
-- Engine-side (Spark/Trino/DuckDB-iceberg)
CREATE TABLE banking.txns (...) PARTITIONED BY days(txn_ts);
MERGE INTO t USING s ON t.id=s.id WHEN MATCHED THEN UPDATE SET ...
SELECT * FROM t FOR TIMESTAMP AS OF '2026-06-30 23:59+00';
ALTER TABLE t ADD COLUMN risk_band STRING;          -- metadata-only
CALL catalog.system.rewrite_data_files(table=>'banking.txns', options=>map('min_input_files','5'));
```

## 7. Pitfalls

| Pitfall | Consequence | Fix |
|---|---|---|
| Never expiring snapshots | Metadata bloat, storage costs | Expire weekly/monthly |
| Millions of tiny files | Slow planning, S3 latency | Compaction jobs; target 128MB–1GB files |
| High-cardinality partitioning | Small-file explosion | Partition by days(branch)? usually just days(ts)/bucket(id) |
| Filesystem catalog on S3 | Non-atomic commits possible | REST/Hive/Glue/Nessie catalog |
| Ignoring MoR delete accumulation | Reads slow down over time | Scheduled compaction merges deletes |

## 8. Exercises

1. Create the table partitioned by `HourTransform(txn_ts)` vs `DayTransform`; measure scan times filtered to one hour.
2. Prove atomicity: kill a Python process mid-append (SIGKILL), then show the table still reads the pre-append row count.
3. Perform a GDPR delete, then time-travel to *before* the delete — discuss what compliance policy should govern snapshot retention.
4. Add a nested struct column (`customer: struct<segment:string, tier:int>`); read it back from DuckDB.
5. Simulate two processes appending simultaneously; verify both commits land (optimistic concurrency).

## 9. Quiz

1. What single operation makes an Iceberg commit atomic?
2. Explain hidden partitioning and how it prevents query bugs.
3. CoW vs MoR — pick one for a CDC-ingested, rarely-queried-during-day table.
4. Where do per-file min/max statistics live, and what do they enable?
5. How does an auditor retrieve "the table as certified on June 30"?

*(Answers: 1. Atomic swap/publish of the table metadata pointer via the catalog (compare-and-swap); 2. You declare transforms like days(ts); engines derive partition filters automatically from any predicate on ts — users can't forget the "partition column"; 3. MoR — fast ingest, compaction later; 4. In manifest files (Avro) within each snapshot; they enable file-level pruning without opening data files; 5. Look up the snapshot id recorded at certification and scan with snapshot_id / VERSION AS OF.)*

---

➡️ **Next:** `05_Apache_Flight_SQL.md` — your lakehouse is solid; now serve it to thousands of BI tools and notebooks at wire speed.
