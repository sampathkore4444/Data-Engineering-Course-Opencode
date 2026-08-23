# Lesson 10 — Lakehouse Architecture: Putting It All Together

> **Meridian Trust Bank case study, Part 10**: You now own every layer - Parquet on
> disk, Arrow in memory, DuckDB as the engine, Iceberg for tables, Flight SQL as the
> front door. This lesson is the wiring diagram: how real banks arrange these into
> medallion zones, keep them fast (compaction), clean (orphan cleanup) and safe
> (governance) - and where each lesson's technology earns its salary.

---

## 1. Concept: what "lakehouse" actually means

Three eras of analytics platforms:

| Era | Storage | Engine | Pain |
|---|---|---|---|
| Warehouse | proprietary blobs | the warehouse | expensive, closed, no data science files |
| Data lake | raw files (CSV/Parquet) | anything | no ACID, no schema, file swamp ("data swamp") |
| **Lakehouse** | open files + table format + catalog | many engines | solved: warehouse guarantees ON open storage |

```
LAKEHOUSE = Parquet files        (physical layer   - Lesson 02)
          + Arrow                (memory/wire      - Lessons 03/04/08)
          + engines (DuckDB...)  (compute          - Lesson 05)
          + Iceberg tables       (ACID/time travel - Lessons 06/07)
          + catalog & gateway    (governance       - this lesson / Flight SQL L08-09)
```

The bank wins because storage truth is OPEN (any engine can read the Parquet under an
Iceberg table) while GUARANTEES are warehouse-grade (ACID, audits via snapshots).

## 2. Medallion architecture: zones with contracts

Data moves through three named zones; each zone has a *contract* - what quality, what
format, who may write:

```
 sources ──▶ BRONZE  ──▶ SILVER  ──▶ GOLD  ──▶ consumers
             raw         clean      curated
 ────────────────────────────────────────────────────────────────
 contract   append-only  deduped,   business-level,
            as-received   typed,     aggregated marts
                          conformed
 format     JSON/Avro    Parquet    Parquet+Iceberg
 writers    ingest only  one team   data products owners
 readers    reprocessing analysts    BI, fraud, regulator
 retention  7d hot/cold  5 years    10 years (audited)
```

Meridian mapping:

| Zone | Content | Technology choice |
|---|---|---|
| Bronze | raw card-event stream drops, SWIFT messages | object store, partitioned by hour |
| Silver | validated, deduplicated transactions | Parquet, hidden-partitioned by day |
| Gold | `fraud_features`, `regulatory_marts`, `customer_360` | **Iceberg** (time-travelable!) |

Rule of thumb: **Bronze/Silver are immutable-ish pipelines; Gold is where tables need
transactions** (analysts overwrite marts, GDPR deletes, quarter-end proofs) - that's why
Iceberg lives at Gold.

### 2.1 How bronze actually gets fed: batch drops, micro-batches, CDC

Lesson 01 sketched a "nightly CDC export" arrow; here is the real taxonomy:

| Pattern | Mechanism | Meridian example |
|---|---|---|
| Batch drop | scheduled file export (SFTP/object store), full or delta | GL journal entries, EOD positions |
| Micro-batch append | consumer polls a queue every N seconds/minutes | card events from Kafka → hourly Parquet |
| **Log-based CDC** | connector (Debezium-style) tails the OLTP WAL - Oracle redo / Postgres logical decoding - and streams every INSERT/UPDATE/DELETE as an event | accounts & ledger changes without hammering core banking |

CDC specifics that bite teams who skip this lesson:

- Each event carries an **operation type** (`I`/`U`/`D`) and the row key; updates are
  after-images, so silver dedups by key with latest-wins - exactly the
  `drop_duplicates(subset=["txn_id"])` the lab below performs.
- **Deletes arrive as tombstone events** and must be honored downstream - GDPR erasure
  starts at the source log, not at a nightly diff.
- Ingest must be **idempotent**: track offsets/watermarks so replays never double-append;
  bronze's append-only Iceberg tables make retries cheap (worst case: duplicate rows that
  silver's contract removes).
- **Late-arriving data** is normal: partition by event time, let compaction absorb stragglers.

### Exactly-once delivery: the full picture

"Exactly-once" is the hardest guarantee in distributed systems. In practice, the lakehouse
achieves it through a combination of mechanisms, not one silver bullet:

```
┌──────────────────────────────────────────────────────────────────────────┐
│  EXACTLY-ONCE = AT-MOST-ONCE + AT-LEAST-ONCE + IDEMPOTENT CONSUMER     │
│                                                                          │
│  Kafka/Flink provides: at-least-once delivery (retries on failure)       │
│  Iceberg provides:     atomic commits (no partial writes visible)        │
│  Silver provides:      idempotent dedup (retries produce same result)    │
└──────────────────────────────────────────────────────────────────────────┘
```

**Layer by layer:**

| Layer | Mechanism | What it prevents |
|---|---|---|
| **Kafka consumer** | Offset tracking + `auto.offset.reset=earliest` | Lost events on crash; replay from last committed offset |
| **Bronze append** | Iceberg atomic commit + unique event ID | Partial writes visible to readers |
| **Silver dedup** | `drop_duplicates(subset=["txn_id"])` + latest-wins | Duplicate events from retries appearing twice |
| **Gold aggregate** | Iceberg snapshot isolation | Aggregates computed over consistent state, not mid-write |

**The idempotency pattern in Silver:**

```python
# Silver pipeline: idempotent by construction
# Running it 100 times produces the same result as running it once

def silver_transform(bronze_path: str, silver_table) -> int:
    """Idempotent: dedup by txn_id, latest-wins."""
    bronze = pd.read_csv(bronze_path)
    
    # dedup: if same txn_id appears 3 times (retries), keep the latest
    bronze = bronze.sort_values("ts").drop_duplicates(
        subset=["txn_id"], keep="last")
    
    # validate
    clean = bronze.dropna(subset=["amount"])
    clean = clean[clean["amount"] > 0]
    
    # append to Iceberg (atomic commit)
    silver_table.append(pa.Table.from_pandas(clean))
    return len(clean)
```

**Why retries are safe:** If the ETL job crashes after writing but before committing,
Iceberg's atomic pointer swap means the data is either fully visible or fully invisible.
On retry, the same data is written again; Silver's dedup collapses the duplicates.

**Watermark tracking (Kafka → Bronze):**

```python
# track the last processed Kafka offset per partition
# stored in a small metadata table (DuckDB or Iceberg)
last_offsets = {
    "partition_0": 1_234_567,
    "partition_1": 987_654,
}

# on restart: resume from last committed offset, not from beginning
# this gives at-least-once delivery; dedup in Silver makes it effectively exactly-once
```

### 2.2 Quality gates between zones

Zone crossings are *contract checkpoints*, not just copies. Bronze→silver and silver→gold
pipelines assert before promoting:

| Gate type | Examples |
|---|---|
| Schema contracts | column names/types/nullability stable vs. yesterday (Flight SQL `get_schema` doubles as a CI probe - Lesson 09) |
| Content expectations | not-null on keys, `amount > 0`, currency ∈ ISO-4217, `txn_id` unique |
| Volume sanity | today's row count within ±x% of trailing average (catches silent upstream truncation) |

Failures go to a **quarantine** zone with rejection reasons and alerts - never silently
dropped, never promoted (exercise 1 builds exactly this). The medallion diagram's
"contract" rows are enforced HERE, not in code review.

## 3. The catalog: one map to rule them all

Lesson 06 introduced catalogs as pointer-swap authorities. At platform scale:

```sql
-- pseudo-SQL of the governance catalog
bank.silver.card_txns      -> metadata.json v8412
bank.gold.fraud_features   -> metadata.json v12093
bank.gold.regulatory_mart  -> metadata.json v551
```

Properties you must design for:

1. **Atomicity** - pointer swap is the commit (Nessie gives git-like branches/tags:
   stage a whole release on branch `release/2026Q3`, tag `quarter-end` forever).
2. **Access control** - REST catalogs (Polaris-style) authorize per principal/table/
   column before returning pointers.
3. **Discovery** - `list_tables(namespace)` is your data catalog UI's backend.

## 4. Small-file disease & the maintenance cadence

Streaming ingest writes tiny files (one per micro-batch). 100k files of 4KB will kill
any engine - planning alone dwarfs reading. The lifecycle:

```
ingest (small files) ──▶ COMPACTION (bin-pack to ~128-512MB)
                              │
stale snapshots ◀── expire_snapshots (after N days)
leaked files     ◀── remove_orphan_files (weekly, careful!)
fragmented manifests ◀── rewrite_manifests
```

| Symptom | Cause | Fix |
|---|---|---|
| queries slow, planning >> scanning | thousands of tiny files | compaction (`rewrite_data_files`) |
| metadata.json grows unboundedly | snapshot history never pruned | `expire_snapshots` |
| disk usage > table size | orphaned files from crashed jobs | `remove_orphan_files` |
| time travel suddenly fails | snapshots expired | set retention to audit horizon |

Bank policy example: card events stream in → compact nightly at 02:00 → expire
snapshots older than 35 days EXCEPT tagged quarter-end ones (regulators!) → orphan
sweep weekly with 72h safety window.

### The small-file problem, quantified

Small files are not just "slightly slower" — they can make queries 100× worse:

| Metric | 10 files × 512 MB | 10,000 files × 512 KB |
|---|---|---|
| **S3 LIST requests** | 1 LIST page | 10 LIST pages (10× overhead) |
| **Footer reads** | 10 × 5 KB = 50 KB | 10,000 × 5 KB = **50 MB** |
| **Planning time (S3)** | ~50 ms | ~5–30 seconds |
| **File handles open** | 10 | 10,000 (may hit OS limits!) |
| **S3 request cost** | $0.0005 | $0.05 (100× cost) |
| **Parquet row groups** | 10 × 5 = 50 | 10,000 × 1 = 10,000 |
| **Manifest entries (Iceberg)** | 10 | 10,000 (slow planning) |

**The compounding effect:** planning time is O(files), and on object stores each file
requires a separate HTTP GET for the footer. A 100K-file table with 5KB footers means
500 MB of metadata downloads *before a single data byte is read.*

**Real-world example at Meridian:** card events stream at 10K events/second. After 24 hours:
- Without compaction: 86,400 micro-batch files × 50 KB each = 4.3 GB total, but
  planning takes 45 seconds just to list and read footers.
- After nightly compaction: 24 files × 180 MB each = 4.3 GB total, planning takes 200 ms.
- Same data, same size, **225× faster planning**.

### Compaction strategy deep dive: bin-pack vs sort-merge

Not all compaction is the same. The strategy depends on your access patterns:

| Strategy | What it does | When to use |
|---|---|---|
| **Bin-pack** | Merge small files into target-size files; no reordering | Streaming ingest (many tiny files, order doesn't matter) |
| **Sort-merge** | Bin-pack AND sort rows within each file by a key | Range queries on sort key (e.g., `ts` for time-series) |
| **Z-order** | Bin-pack AND cluster by multiple columns | Multi-dimensional predicates (e.g., `card_id` AND `ts`) |

**Bin-pack (default):**
```
Before:  [64KB][64KB][64KB][64KB][64KB]...  (100 files)
After:   [256MB][256MB][256MB]...           (3 files)
Rows within each file: arbitrary order
```

**Sort-merge (better for time-series):**
```
Before:  [64KB day1+day3 mixed][64KB day2+day1 mixed]...
After:   [256MB sorted by ts][256MB sorted by ts]...
Rows within each file: ts-ordered → delta encoding + predicate pushdown win
```

**When to use sort-merge:**
- Most queries filter by timestamp (`WHERE ts BETWEEN X AND Y`)
- You want better compression (sorted timestamps → delta encoding)
- You want better min/max pruning (sorted = tighter min/max ranges)

**When bin-pack is enough:**
- Queries filter by non-sort columns (card_id, merchant)
- Ingest order is already good (Kafka partitions by card_id)
- You just need to reduce file count, not improve data layout

**Banking example:** Meridian compacts card_txns nightly with **sort-merge by `ts`** because
the fraud team's primary query pattern is "transactions in the last N minutes." The sorted
layout means each file's min/max timestamps are tight, and most files are pruned entirely.

```python
# PyIceberg: sort-merge compaction (conceptual)
# In Spark: rewrite_data_files(..., sort_order=["ts ASC"])
# In PyIceberg: manual overwrite with sorted data
all_data = table.scan().to_arrow()
sorted_data = all_data.sort_by("ts")  # sort before overwrite
table.overwrite(sorted_data)          # atomic swap: old files → new sorted files
```

### Compaction timing: when NOT to compact

| Situation | Why not | What to do |
|---|---|---|
| During quarter-end freeze | Regulatory prohibition on data changes | Compact before freeze; tag the snapshot |
| While analysts are running long queries | Compaction rewrites files; old snapshots stay valid but new ones appear | Compact during off-hours (02:00–05:00) |
| Right after a GDPR delete | Delete files are fresh; compaction merges them into rewrites | Wait 24h for delete file accumulation |
| When storage is tight | Compaction creates new files before old ones are expired | Expire first, then compact |

## 5. End-to-end example: a miniature medallion pipeline

Runnable with pyarrow + duckdb + pyiceberg: bronze CSV drop → silver dedup/validate
(Parquet) → gold Iceberg mart → maintain it → serve counts. ~80 lines.

```python
"""
lesson10_medallion_mini.py
Bronze -> Silver -> Gold pipeline with maintenance, all local.
Deps: pip install pyarrow pandas duckdb "pyiceberg[pyarrow,sql-sqlite]"
"""
import os, shutil
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog import load_catalog

W = "/tmp/opencode/lakehouse"
shutil.rmtree(W, ignore_errors=True)
os.makedirs(f"{W}/bronze", exist_ok=True)

# ---------- BRONZE: raw drop (as-received, warts included) ----------------------
rng = np.random.default_rng(11)
n = 20_000
raw = pd.DataFrame({
    "txn_id":   np.arange(n),
    "card_id":  rng.integers(300_000, 305_000, n),
    "ts":       pd.to_datetime("2026-08-01") +
               pd.to_timedelta(rng.integers(0, 14 * 86400, n), unit="s"),
    "amount":   np.round(rng.gamma(2, 40, n), 2),
})
raw.loc[::500, "amount"] = np.nan                 # upstream junk
raw = pd.concat([raw, raw.iloc[:250]])            # duplicates from retries!
raw.to_csv(f"{W}/bronze/drop_2026-08-04.csv", index=False)

# ---------- SILVER: validate & dedupe into typed Parquet -------------------------
b = pd.read_csv(f"{W}/bronze/drop_2026-08-04.csv", parse_dates=["ts"])
silver = (b.drop_duplicates(subset=["txn_id"])
           .dropna(subset=["amount"])
           .astype({"txn_id": "int64", "card_id": "int64",
                    "amount": "float64", "ts": "datetime64[us]"}))
silver_table = pa.Table.from_pandas(silver, preserve_index=False)
pq.write_table(silver_table, f"{W}/silver.parquet")
print("bronze rows:", len(b), "-> silver rows:", len(silver_table))

# ---------- GOLD: Iceberg mart with day partitioning -----------------------------
catalog = load_catalog(
    "meridian",
    **{"type": "sql", "uri": f"sqlite:///{W}/catalog.db",
       "warehouse": f"file://{W}/warehouse"})
catalog.create_namespace_if_not_exists("gold")
try:
    gold = catalog.load_table("gold.daily_volume")
except Exception:
    from pyiceberg.schema import Schema
    from pyiceberg.types import NestedField, TimestampType, LongType, DoubleType
    from pyiceberg.partitioning import PartitionSpec, PartitionField
    from pyiceberg.transforms import DayTransform
    gold = catalog.create_table(
        "gold.daily_volume",
        schema=Schema(
            NestedField(1, "day",    TimestampType(), required=True),
            NestedField(2, "txns",   LongType(),      required=True),
            NestedField(3, "volume", DoubleType(),    required=True)),
        partition_spec=PartitionSpec(
            PartitionField(source_id=1, field_id=1000,
                           transform=DayTransform(), name="day_bucket")),
    )

daily = silver
daily["day"] = daily["ts"].dt.floor("D")
mart = (daily.groupby("day", as_index=False)
        .agg(txns=("txn_id", "size"), volume=("amount", "sum")))
mart["volume"] = mart["volume"].round(2)

# Arrow side must mirror Iceberg's required=True <=> nullable=False (Lesson 06!)
GOLD_ARROW = pa.schema([
    pa.field("day",    pa.timestamp("us"), nullable=False),
    pa.field("txns",   pa.int64(),         nullable=False),
    pa.field("volume", pa.float64(),       nullable=False),
])

# ---------- MAINTENANCE: streaming-style micro-batches = small-file disease -------
for i in range(len(mart)):                 # one tiny append per day, like a stream
    tiny = mart.iloc[i:i + 1]
    gold.append(pa.Table.from_pandas(tiny, schema=GOLD_ARROW, preserve_index=False))
print("gold snapshots:", len(gold.snapshots()))
files_before = sum(len(files) for _, _, files in os.walk(
    f"{W}/warehouse/gold/daily_volume/data"))
print("data files after micro-batches:", files_before)   # ~1 file per day!

# ---------- SERVE: DuckDB reads gold through the metadata pointer ------------------
import duckdb, glob
meta = max(glob.glob(f"{W}/warehouse/gold/daily_volume/metadata/*.metadata.json"))
con = duckdb.connect()
try:
    con.execute("LOAD iceberg;")
except Exception:
    con.execute("INSTALL iceberg; LOAD iceberg;")
print(con.sql(f"""
    SELECT count(*) AS days, round(sum(volume), 2) AS total_volume
    FROM iceberg_scan('{meta}')
""").df().to_string(index=False))
```

Output:

```
bronze rows: 20250 -> silver rows: 19960
gold snapshots: 14
data files after micro-batches: 14
 days  total_volume
   14    1586180.28
```

What just happened:

1. **Bronze** preserved reality - junk and duplicate retries included.
2. **Silver** enforced the contract: deduped by `txn_id`, null amounts rejected,
   types pinned. Reproducible from bronze any time.
3. **Gold** is transactional: every micro-append is an atomic snapshot (14 of them).
4. Streaming-style appends reproduced **small-file disease**: one file per day,
   none bigger than a few KB - compaction would bin-pack these tomorrow at 02:00.
5. DuckDB served the mart through the Iceberg pointer - zero coupling to PyIceberg.

## 6. Governance & security checklist

| Concern | Mechanism |
|---|---|
| Who may query what | catalog-level authZ (REST catalog policies) + gateway roles (L09) |
| Row/column filtering | views or dynamic policies at the engine; never "security by path" |
| Audit trail | every query logged at gateway: principal, SQL, bytes served |
| Provable history | Iceberg snapshots/tagged quarter-ends (L06) |
| Right-to-erasure | DELETE + compaction + snapshot expiry discipline |
| Schema contracts | `get_schema` over Flight SQL wired into consumers' CI |
| Cost control | compaction + expiry schedules; storage truth in ONE place |

### Data lineage tracking: where did this data come from?

Regulators don't just ask "what does the data say?" — they ask "where did it come from,
was it transformed correctly, and who accessed it?" Lineage answers the full chain:

```
SOURCE              BRONZE           SILVER           GOLD              CONSUMER
─────────────────────────────────────────────────────────────────────────────────
Oracle OLTP  ──▶  raw_events.csv  ──▶  card_txns.parquet  ──▶  fraud_features  ──▶  Flight SQL
                   │                    │                     │                     │
                ingestion job       dedup+validate        aggregate             query
                2026-08-01 02:00    2026-08-01 02:05      2026-08-01 02:10     2026-08-01 14:30
                svc_etl             svc_etl               svc_etl               risk_analyst
```

**What to track at each hop:**

| Hop | Metadata to capture |
|---|---|
| Source → Bronze | source system, extraction timestamp, offset/watermark, row count, file hash |
| Bronze → Silver | input file path, output table, row counts (in/out/rejected), validation results |
| Silver → Gold | input snapshot, output snapshot, aggregation logic, filter criteria |
| Gold → Consumer | principal, query, bytes served, timestamp (Flight SQL audit log) |

**Implementation patterns:**

1. **Iceberg table properties** — store lineage metadata in table properties:
   ```python
   # tag table with provenance
   with table.update_properties() as update:
       update.set("created_by", "svc_etl")
       update.set("source_system", "oracle_core_banking")
       update.set("pipeline", "bronze_to_silver_v2.3")
       update.set("last_refresh", "2026-08-01T02:05:00Z")
   ```

2. **OpenLineage / Marquez** — industry-standard lineage API:
   ```python
   # emit lineage event (conceptual)
   emit_lineage(
       job="silver_dedup",
       inputs=["s3://meridian/bronze/drop_2026-08-01.csv"],
       outputs=["iceberg://gold.card_txns?snapshot=8412"],
       run_id="abc-123"
   )
   ```

3. **Iceberg snapshot summaries** — every commit carries metadata:
   ```python
   # snapshot.summary contains operation stats
   for snap in table.snapshots():
       print(snap.summary)
       # {'added-files-size-bytes': '524288', 'total-records': '19960',
       #  'operation': 'append', 'appended-rows': '19960'}
   ```

**Banking scenario:** A regulator asks: "This fraud alert was triggered on August 5th.
Show me the exact data lineage from source to alert."

The lineage chain:
1. Source: Oracle OLTP `card_authorizations` table, extracted at 2026-08-05 02:00 UTC
2. Bronze: `s3://meridian/bronze/auth_2026-08-05.csv` (47,382 rows, SHA-256: `a3f2...`)
3. Silver: `iceberg://gold.card_txns` snapshot 9104 (47,380 rows after dedup, 2 rejected)
4. Gold feature: `fraud_features` snapshot 12093 (aggregated per-card velocity)
5. Alert: Flight SQL query by `risk_analyst` at 14:30 UTC → 3 cards flagged

The auditor verifies: source row count matches Bronze; Silver dedup log shows 2 rejected
rows with reasons; Gold aggregation is reproducible from Silver snapshot 9104; the alert
query is in the Flight SQL audit log.

## 7. Exercises

1. Extend the mini-pipeline: add a `quarantine` zone capturing silver-rejected rows
   with reasons; report quarantine rate per day.
2. Simulate a failed writer: create a fake `.parquet` in gold's data dir not referenced
   by any manifest; explain what `remove_orphan_files` would do (don't run destructive
   cleanup without a safety window!).
3. Tag the current gold snapshot as `quarter-end`; expire everything else older than
   0 seconds except the tag; verify the tagged state still reads correctly.
4. Put the mini-gateway from Lesson 09 in front of the gold table: analyst queries go
   through Flight SQL, ingestion through `do_put`.
5. Measure end-to-end latency: bronze drop → gold readable via DuckDB. Where does the
   time actually go? (Spoiler: almost never the columnar layers.)
6. Simulate the small-file problem: write 1000 tiny Parquet files to a directory, then
   compact them to 10 files. Measure scan planning time before and after using DuckDB's
   `read_parquet` with `hive_partitioning=true`.
7. Implement idempotency: write the same Silver transform function, run it twice on
   the same Bronze input, and prove the Silver row count doesn't double.
8. Build a lineage chain: Bronze → Silver → Gold with metadata tags at each step.
   Query the Gold table and trace back to the exact Bronze source file.

## 8. Cheat sheet

| Concept | Fact |
|---|---|
| Lakehouse | open files + table format + catalog + multi-engine compute |
| Medallion | bronze(raw) → silver(clean) → gold(curated); each zone has a contract |
| Ingest patterns | batch drop / micro-batch / log-based CDC (I/U/D events, idempotent, watermark-tracked) |
| Exactly-once | at-least-once delivery + atomic Iceberg commits + Silver idempotent dedup |
| Idempotency | dedup by unique key with latest-wins; retries produce same result |
| Quality gates | schema + expectation + volume checks at zone borders; rejects quarantined with reasons |
| Iceberg placement | wherever ACID/time-travel matter (usually gold) |
| Catalog | atomic pointer swap; authZ + discovery; Nessie adds branches/tags |
| Small-file cost | 10K files × 5KB footer = 50MB metadata; planning 100× slower than compacted |
| Compaction: bin-pack | merge small files to target size; no reordering; default choice |
| Compaction: sort-merge | bin-pack + sort by key; better for time-series range queries |
| Compaction timing | avoid during quarter-end freezes, active queries, or right after deletes |
| Lineage | track source → bronze → silver → gold with metadata at each hop |
| Metadata bloat | expire_snapshots on schedule; keep audit-required tags forever |
| Orphans | remove_orphan_files weekly with generous safety windows |
| Governance | catalog authZ + gateway roles + audit logs + snapshot proofs |

**Next:** Lesson 11 - the capstone. Ingest, store, transact, audit, and serve:
you build Meridian's miniature lakehouse end to end and answer four real banking
questions with it.
