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

## 8. Cheat sheet

| Concept | Fact |
|---|---|
| Lakehouse | open files + table format + catalog + multi-engine compute |
| Medallion | bronze(raw) → silver(clean) → gold(curated); each zone has a contract |
| Ingest patterns | batch drop / micro-batch / log-based CDC (I/U/D events, idempotent, watermark-tracked) |
| Quality gates | schema + expectation + volume checks at zone borders; rejects quarantined with reasons |
| Iceberg placement | wherever ACID/time-travel matter (usually gold) |
| Catalog | atomic pointer swap; authZ + discovery; Nessie adds branches/tags |
| Small-file fix | bin-pack compaction to 128-512MB targets |
| Metadata bloat | expire_snapshots on schedule; keep audit-required tags forever |
| Orphans | remove_orphan_files weekly with generous safety windows |
| Governance | catalog authZ + gateway roles + audit logs + snapshot proofs |

**Next:** Lesson 11 - the capstone. Ingest, store, transact, audit, and serve:
you build Meridian's miniature lakehouse end to end and answer four real banking
questions with it.
