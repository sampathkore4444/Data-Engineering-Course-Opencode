# Lesson 11 — Capstone: Meridian Trust Lakehouse, End to End

> **Meridian Trust Bank case study, Part 11**: One working system, all five layers.
> Stream card transactions into **Iceberg** tables; catch velocity-fraud with
> **DuckDB**; pull the kill switch on a compromised card with a row-level delete;
> prove quarter-end state to the regulator via **time travel**; and serve BI +
> regulators from one governed endpoint over **Arrow Flight SQL** - columnar from
> disk to dashboard.

---

## Table of Contents

| Section | Topic |
|---|---|
| [1](#1-the-assignment) | The assignment |
| [2](#2-the-complete-capstone) | The complete capstone |
| [3](#3-where-each-lesson-shows-up) | Where each lesson shows up |
| [4](#4-production-hardening-checklist) | Production hardening checklist |
| [5](#5-exercises) | Exercises |
| [6](#6-cheat-sheet-the-whole-stack-in-one-table) | Cheat sheet |

---

## 1. The assignment

Build a miniature lakehouse that answers four real banking questions:

| # | Question | Technology under test |
|---|---|---|
| Q1 | How do we ingest millions of events without breaking tables? | Iceberg ACID appends (L06/07) |
| Q2 | Which cards show burst behavior right now? | DuckDB vectorized SQL over Arrow scans (L03-05) |
| Q3 | Can you PROVE the ledger as of July 2nd, after days of changes and one erasure? | Iceberg snapshots / time travel (L06) |
| Q4 | How do BI and the regulator consume results? | Flight SQL gateway (L08/09) |

Architecture of what we build:

```
 make_day(1..3)                    ┌────────────────────────────────┐
   320k txns ═══append═══▶  ICEBERG│ bank.card_txns                 │
                        snapshots  │ hidden day partitioning        │
                                   └───────┬────────────────────────┘
                                           │ scan().to_arrow() zero-copy
                                           ▼
                                    DUCKDB ENGINE
                       velocity SQL ──▶ fraud suspects view
                       time-travel scan ──▶ quarter-end replay
                                           │ registered views
                                           ▼
                              FLIGHT SQL GATEWAY :31400
                     BI tool ◀── b"fraud_suspects"    regulator ◀── ad-hoc SQL
```

## 2. The complete capstone

Save as `lesson11_capstone.py` and run it.

```python
"""
lesson11_capstone.py - Meridian Trust Bank miniature lakehouse
ingest -> Iceberg storage -> fraud & regulatory analytics -> Flight SQL serving
Deps: pip install pyarrow pandas numpy duckdb "pyiceberg[pyarrow,sql-sqlite]"
"""
import os, shutil, threading, time
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.flight as fl
import duckdb
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, LongType, TimestampType, DoubleType, StringType
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform

W = "/tmp/opencode/capstone"
shutil.rmtree(W, ignore_errors=True)
os.makedirs(f"{W}/warehouse", exist_ok=True)

catalog = load_catalog("meridian", **{
    "type": "sql", "uri": f"sqlite:///{W}/catalog.db",
    "warehouse": f"file://{W}/warehouse"})
catalog.create_namespace("bank")

TXN_SCHEMA = Schema(
    NestedField(1, "txn_id",   LongType(),      required=True),
    NestedField(2, "card_id",  LongType(),      required=True),
    NestedField(3, "ts",       TimestampType(), required=True),
    NestedField(4, "amount",   DoubleType(),    required=True),
    NestedField(5, "currency", StringType(),     required=False),
)
ARROW_TXN = pa.schema([
    pa.field("txn_id",   pa.int64(),         nullable=False),
    pa.field("card_id",  pa.int64(),         nullable=False),
    pa.field("ts",       pa.timestamp("us"), nullable=False),
    pa.field("amount",   pa.float64(),       nullable=False),
    pa.field("currency", pa.string()),
])

txns = catalog.create_table(
    "bank.card_txns", schema=TXN_SCHEMA,
    partition_spec=PartitionSpec(
        PartitionField(source_id=3, field_id=1000,
                       transform=DayTransform(), name="ts_day")))

# ============================ STAGE 1: INGEST ==================================
def make_day(day, n, seed):
    rng = np.random.default_rng(seed)
    return pa.Table.from_pydict({
        "txn_id":   np.arange(n, dtype="int64") + day * 1_000_000,
        "card_id":  rng.integers(300_000, 306_000, n),
        "ts":       pd.to_datetime(f"2026-07-{day:02d}") +
                   pd.to_timedelta(rng.integers(0, 86400, n), unit="s"),
        "amount":   np.round(rng.gamma(2.0, 45.0, n), 2),
        "currency": rng.choice(["EUR", "USD"], n),
    }, schema=ARROW_TXN)

for d in (1, 2):
    txns.append(make_day(d, 120_000, seed=d))
quarter_end_snapshot = txns.current_snapshot().snapshot_id     # <-- AUDIT ANCHOR
qe_rows = len(txns.scan().to_arrow())
print(f"[ingest] quarter-end state: {qe_rows} rows")

txns.append(make_day(3, 80_000, seed=3))                        # life goes on...
print("[ingest] snapshots:", len(txns.snapshots()))

# ============================ STAGE 2: FRAUD ANALYTICS =========================
con = duckdb.connect()
con.register("txns_now", txns.scan().to_arrow())

velocity = con.sql("""
    WITH bursts AS (
      SELECT card_id, date_trunc('hour', ts) AS hr,
             count(*) AS hits, sum(amount) AS volume
      FROM txns_now
      GROUP BY 1, 2
    )
    SELECT card_id, max(hits) AS max_hourly_hits, round(max(volume), 2) AS peak_vol
    FROM bursts GROUP BY card_id ORDER BY max_hourly_hits DESC LIMIT 5
""").df()
print("[fraud] velocity suspects:\n", velocity.to_string(index=False))

suspect_card = int(velocity.iloc[0]["card_id"])

# ---- STAGE 2b: risk ops pulls the kill switch (row-level delete, MoR) ----------
txns.delete(f"card_id = {suspect_card}")
after_kill = len(txns.scan().to_arrow())
print(f"[riskops] card {suspect_card} erased: "
      f"{qe_rows + 80_000} -> {after_kill} live rows")

# ==================== STAGE 3: REGULATORY PROOF (TIME TRAVEL) ==================
past = txns.scan(snapshot_id=quarter_end_snapshot).to_arrow()
con.register("txns_qe", past)
proof = con.sql("""
    SELECT count(*) AS rows, round(sum(amount), 2) AS total
    FROM txns_qe
""").df()
print("[regulator] quarter-end replay:", proof.to_dict("records"))
assert len(past) == qe_rows, "quarter-end state must be immutable!"

# ============================ STAGE 4: SERVE ===================================
class Gateway(fl.FlightServerBase):
    def __init__(self, location):
        super().__init__(location)
        self._location = location
        self.views = {"fraud_suspects": velocity}

    def _result(self, q):
        if q in self.views:
            return pa.Table.from_pandas(self.views[q])
        return con.sql(q).to_arrow_table()

    def get_flight_info(self, context, descriptor):
        q = descriptor.command.decode()
        return fl.FlightInfo(self._result(q).schema, descriptor,
                             [fl.FlightEndpoint(fl.Ticket(descriptor.command),
                                                [self._location])],
                             -1, -1)

    def do_get(self, context, ticket):
        return fl.RecordBatchStream(self._result(ticket.ticket.decode()))

gateway = Gateway("grpc://127.0.0.1:31400")
threading.Thread(target=gateway.serve, daemon=True).start()
time.sleep(0.5)

client = fl.FlightClient("grpc://127.0.0.1:31400")
info = client.get_flight_info(fl.FlightDescriptor.for_command(b"fraud_suspects"))
served = client.do_get(info.endpoints[0].ticket).read_all()
print("[serve] BI tool received:\n", served.to_pandas().to_string(index=False))

adhoc = b"SELECT currency, count(*) n FROM txns_qe GROUP BY currency"
info = client.get_flight_info(fl.FlightDescriptor.for_command(adhoc))
served = client.do_get(info.endpoints[0].ticket).read_all()
print("[serve] regulator feed:\n", served.to_pandas().to_string(index=False))
```

Sample run (random data - your numbers will differ):

```
[ingest] quarter-end state: 240000 rows
[ingest] snapshots: 3
[fraud] velocity suspects:
  card_id  max_hourly_hits  peak_vol
  304306                8    637.90
  305916                8    827.02
  ...
[riskops] card 304306 erased: 320000 -> 319950 live rows
[regulator] quarter-end replay: [{'rows': 240000, 'total': 21575804.55}]
[serve] BI tool received: ...same suspect table...
[serve] regulator feed:
 currency      n
     USD 120358
     EUR 119642
```

## 3. Where each lesson shows up

| Stage | Line of code that matters | Lesson |
|---|---|---|
| Schema | `NestedField(...)` field IDs survive renames forever | 06 |
| Partition | `DayTransform()` on `ts` - queries never mention partitions | 06 |
| Ingest | `txns.append(day_table)` = atomic snapshot, safe concurrent readers | 06/07 |
| Scan | `txns.scan().to_arrow()` → `duckdb.register` - zero-copy into engine | 03/04/05 |
| Fraud SQL | CTE + `date_trunc('hour')` burst aggregation, vectorized | 05 |
| Kill switch | `txns.delete(f"card_id = {id}")` - MoR delete file, new snapshot | 07 |
| Audit | `scan(snapshot_id=quarter_end_snapshot)` - bit-for-bit history | 06 |
| Contract | `get_flight_info` returns schema before data moves | 08 |
| Serving | `RecordBatchStream` - Arrow bytes to any client, no serialization | 08/09 |

## 4. Production hardening checklist

What you would add before this runs for real money:

1. **Auth**: token handshake + per-principal policy at the gateway (Lesson 09) -
   the regulator gets `txns_qe` views, never raw table access.
2. **Compaction & expiry**: nightly `rewrite_data_files`, snapshot retention with
   tagged quarter-ends kept forever (Lesson 10).
3. **Streaming ingest**: replace batch `append` with micro-batches from Kafka/Flink;
   watch small-file metrics and let compaction heal them (Lessons 07/10).
4. **Multiple engines**: Spark for training features, Trino for BI, DuckDB embedded
   in risk tools - same catalog, same truth (Lesson 10).
5. **Observability**: snapshot summaries as metrics; gateway audit log per query;
   data-quality gates between bronze/silver/gold zones.
6. **DR**: catalog backups + warehouse versioning; Iceberg metadata is small -
   replicating pointers is cheap insurance.

## 5. Exercises

1. Add an AML stage: find circular flows (A→B→C→A within 48h) using a self-join;
   serve results as a third named endpoint.
2. Parameterize the gateway: accept `{"sql": ..., "as_of_snapshot": ...}` and use
   PyIceberg scans server-side so clients can time travel through the API safely.
3. Simulate two writers: append day-4 data from two processes simultaneously and
   verify optimistic concurrency (one retries) without corruption.
4. Benchmark: DuckDB direct Parquet vs Iceberg scan vs Flight round trip for the
   fraud query. Quantify the cost of each layer's guarantees.
5. Write the auditor's one-pager: given only this codebase, how do you prove no row
   was silently altered between two snapshots? (Hint: manifests carry per-file stats;
   snapshot lineage is hash-chained.)

---

## 6. Interview questions: capstone and full-stack integration

### Concept 1: End-to-end lakehouse

**Q1: Walk through the complete data flow from card authorization to fraud alert.**

A: (1) Card switch sends authorization → Kafka → Bronze (raw Parquet). (2) Silver: deduplicate, validate schema, reject bad rows. (3) Gold: aggregate per-card velocity features. (4) DuckDB queries Gold for fraud patterns. (5) Flight SQL serves results to analysts. (6) Iceberg provides ACID, time travel, GDPR. (7) Catalog provides governance, audit trail. The key: each layer has a clear responsibility.

**Q2: How does each technology in the stack earn its salary?**

A: Parquet: compressed columnar storage. Arrow: zero-copy in-memory format. DuckDB: vectorized SQL engine. Iceberg: ACID, time travel, schema evolution. Flight SQL: governed network protocol. S3: durable cheap storage. Each layer solves a specific problem — no layer is redundant.

**Q3: What's the production hardening checklist for a lakehouse?**

A: (1) Auth: JWT tokens, role-based access, (2) Compaction: nightly rewrite_data_files, (3) Expiry: expire_snapshots with tagged quarter-ends, (4) Orphan cleanup: weekly remove_orphan_files, (5) Monitoring: file count, snapshot count, query latency, (6) DR: catalog backups, warehouse versioning, (7) Testing: data quality gates, pipeline tests.

**Q4: How do you handle a 100TB lakehouse in production?**

A: (1) Partitioning: date-based (low cardinality), (2) Compaction: nightly, target 512MB files, (3) Pruning: predicate pushdown, partition pruning, (4) Caching: DuckDB results cache, Flight SQL response cache, (5) Scaling: Spark for heavy transforms, DuckDB for interactive, (6) Monitoring: query latency, file count, storage cost.

**Q5: How do you migrate from a legacy data warehouse to a lakehouse?**

A: (1) Assess: what queries run, what SLAs exist, (2) Design: medallion zones, catalog, governance, (3) Build: Bronze/Silver/Gold pipelines, (4) Migrate: dual-run (warehouse + lakehouse), compare results, (5) Cutover: redirect queries to lakehouse, (6) Decommission: shut down warehouse. The key: dual-run ensures correctness before cutover.

### Concept 2: Capstone integration

**Q1: How does the capstone demonstrate all five layers working together?**

A: Parquet (storage) → Arrow (memory) → DuckDB (compute) → Iceberg (ACID) → Flight SQL (serving). Each layer is used: Parquet files in Bronze, Arrow for zero-copy between DuckDB and Flight, DuckDB for SQL queries, Iceberg for time travel and GDPR, Flight SQL for governed access. The key: no layer is skipped.

**Q2: How do you prove quarter-end state to a regulator?**

A: (1) Tag the quarter-end snapshot: `create_tag(snapshot_id, "quarter_end_2026Q2")`. (2) Query: `SELECT * FROM txns VERSION AS OF tag_quarter_end_2026Q2`. (3) Show: snapshot ID, row count, metadata. (4) Verify: same query, same result, anytime. The key: Iceberg time travel makes this instant and reproducible.

**Q3: How do you handle GDPR erasure across the capstone pipeline?**

A: (1) Identify card_ids for the customer. (2) DELETE from Silver and Gold tables. (3) Archive pre-deletion data to compliance table. (4) Tag pre-deletion snapshot (never expires). (5) Compact affected partitions. (6) Log the operation. The key: atomic deletes + archived data + audit trail = compliant.

**Q4: How does the Flight SQL gateway serve the capstone results?**

A: Gateway reads Iceberg Gold table via DuckDB. Analysts query via Flight SQL (Python, JDBC, ODBC). Gateway logs: principal, query, bytes. Results stream as Arrow batches (zero-parse). The key: one governed endpoint for all consumers.

**Q5: What's the production hardening story for the capstone?**

A: (1) Auth: JWT tokens, role-based access at gateway, (2) Compaction: nightly, (3) Expiry: 7-day retention, tagged quarter-ends forever, (4) Monitoring: file count, snapshot count, query latency, (5) DR: catalog backups, warehouse versioning. The key: the capstone is a production-ready miniature.

### Concept 3: Banking-specific patterns

**Q1: How do you handle regulatory reporting in a lakehouse?**

A: (1) Gold zone stores pre-aggregated regulatory marts, (2) Time travel proves historical state, (3) Tags pin quarter-end snapshots, (4) Flight SQL serves reports, (5) Audit log tracks all access. The key: regulatory reporting is a first-class use case, not an afterthought.

**Q2: How do you handle PCI-DSS compliance?**

A: (1) Tokenize PANs at ingestion (never store raw PANs), (2) Column masking via Iceberg views, (3) Role-based access (fraud team sees masked PAN, compliance sees audit metadata), (4) Audit logging (who accessed what, when), (5) Encryption at rest (S3 SSE). The key: PCI-DSS is about access control and audit, not just encryption.

**Q3: How do you handle Basel III/SOX compliance?**

A: (1) Retain transaction data for 7+ years (regulatory requirement), (2) Time travel proves historical state, (3) Tags pin audit snapshots (never expire), (4) Lineage tracking (source → Bronze → Silver → Gold), (5) Audit logs (all access tracked). The key: compliance is about reproducibility and retention.

**Q4: How do you handle multi-jurisdiction data residency?**

A: (1) Partition data by region (EU data in EU S3, US data in US S3), (2) Catalog enforces regional access (EU analysts see EU data only), (3) Flight SQL gateway per region (low latency), (4) Cross-region replication for DR. The key: data residency is a partitioning and access control problem.

**Q5: How do you handle real-time fraud detection in a batch lakehouse?**

A: (1) Bronze: streaming ingest (Kafka → Parquet), (2) Silver: real-time validation, (3) Gold: batch-aggregated features (hourly/daily), (4) Real-time layer: Redis/DuckDB for streaming features, (5) Lambda architecture: batch + real-time. The key: lakehouse is for batch; real-time needs a separate layer.

### Concept 4: Interview preparation

**Q1: How do you explain the lakehouse architecture to a non-technical stakeholder?**

A: "A lakehouse is like a data warehouse but cheaper and more flexible. It stores data in open formats (Parquet) so any tool can read it. It has ACID transactions (like a database) so data is always consistent. It supports time travel (like git) so we can prove historical state. And it has governance (like a bank vault) so only authorized people see sensitive data."

**Q2: What's the most important design decision in a lakehouse?**

A: The table format (Iceberg vs Delta vs Hudi). This decision affects: (1) engine support (which tools can read the data), (2) feature set (time travel, schema evolution, partition evolution), (3) operational model (compaction, expiry, maintenance). The key: choose an open format (Iceberg) for maximum flexibility.

**Q3: How do you handle a production incident in a lakehouse?**

A: (1) Identify: which table, which snapshot, what changed, (2) Diagnose: was it a write failure, data corruption, or performance degradation?, (3) Fix: rollback to good snapshot (Iceberg), or compact (small files), or expire (metadata bloat), (4) Verify: query the fixed table, confirm results, (5) Post-mortem: what caused it, how to prevent. The key: Iceberg's time travel makes rollback instant.

**Q4: What's the biggest mistake you see in lakehouse implementations?**

A: (1) Skipping governance (no catalog, no access control, no audit), (2) Ignoring maintenance (no compaction, no expiry, small files accumulate), (3) Over-engineering (building custom when managed services exist), (4) Under-testing (no data quality checks, no pipeline tests). The key: lakehouse is not just Parquet + S3 — it's the governance and maintenance that make it production-ready.

**Q5: How do you justify the cost of a lakehouse migration?**

A: (1) Storage cost: Parquet on S3 is 10× cheaper than proprietary warehouse, (2) Compute cost: DuckDB/Spark are open-source, no license fees, (3) Flexibility: any tool can read the data, no vendor lock-in, (4) Compliance: time travel, audit trails, GDPR support. The key: lakehouse reduces both cost and risk.

---

## 7. Cheat sheet: the whole stack in one table

| Layer | Tool | Key call |
|---|---|---|
| Physical | Parquet | `pq.write_table(t, path)` |
| Memory/wire | Arrow | `table.to_batches()`, IPC, Flight streams |
| Engine | DuckDB | `con.register("v", arrow_tbl); con.sql(...)` |
| Tables | Iceberg | `create_table/append/delete/scan(snapshot_id=)` |
| Protocol | Flight SQL | descriptors/tickets/actions over gRPC |
| Governance | catalog + gateway | atomic pointer swap + roles + audit logs |

You built every layer yourself. The file swamp is dead; long live the lakehouse.

---

**Next:** Lesson 12 - the lakehouse leaves the laptop. Object storage semantics
(flat keys, no renames, footer-only reads) and what they change about everything
you just built.

*Revisit lessons 02, 05 and 06 whenever a production incident makes you wonder why
any of this exists - there is always a banking story there.*
