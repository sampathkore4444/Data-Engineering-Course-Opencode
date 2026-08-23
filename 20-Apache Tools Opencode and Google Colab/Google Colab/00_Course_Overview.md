# 🏦 Modern Data Engineering Masterclass — Banking Analytics Stack

> **Goal:** Make you a master of the modern analytical data stack: **Columnar Storage → Apache Arrow → DuckDB → Apache Iceberg → Apache Flight SQL → Apache Spark**.
>
> **Theme:** Every lesson uses a realistic **banking scenario** ("FinBank") with end-to-end, runnable Python code.

### 🎯 What You'll Be Able to Do After This Course

- Explain *why* columnar storage beats row storage for analytics — and when it doesn't.
- Move tabular data between Pandas, NumPy, DuckDB, Polars, Parquet and network clients **without serialization copies** (Arrow).
- Run SQL directly over files, Arrow tables and DataFrames from a laptop (DuckDB).
- Give a data lake database-grade guarantees: ACID commits, time travel for auditors, GDPR deletes, safe concurrent writers (Iceberg).
- Build your own high-performance data service consumed by BI tools, JDBC clients and notebooks (Flight SQL).
- Know exactly which workloads to scale out with Spark — and how the same lake serves all engines.

---

## 📚 The Learning Path (Read in this order)

| # | Lesson | File | One-liner |
|---|--------|------|-----------|
| 1 | **Columnar Data Storage** | `01_Columnar_Data_Storage.md` | *Why* analytics is 100x faster when data is stored column-by-column. The foundation for everything else. |
| 2 | **Apache Arrow** | `02_Apache_Arrow.md` | The universal **in-memory columnar format** that lets every tool speak the same language with zero-copy transfers. |
| 3 | **DuckDB** | `03_DuckDB.md` | An embedded **analytical SQL engine** (the "SQLite of analytics"). Run SQL on Parquet/Arrow/Pandas at insane speed. |
| 4 | **Apache Iceberg** | `04_Apache_Iceberg.md` | The **table format** that turns a data lake into a reliable database: ACID transactions, time travel, schema evolution. |
| 5 | **Apache Flight SQL** | `05_Apache_Flight_SQL.md` | The modern **network protocol** for moving tabular data between systems at wire speed using Arrow + gRPC. |
| 6 | **Apache Spark** | `06_Apache_Spark.md` | The **distributed compute engine** that scales everything from Lessons 1–4 beyond one machine: DataFrames, SQL, and Iceberg on a cluster. |
| 7 | **Capstone Walkthrough** | `07_Capstone_FinBank_Platform.md` | Build the **FinBank Fraud Analytics Platform** end-to-end, wiring all six technologies into one working system. |

---

## 🧭 Which Tool When? (The 30-Second Decision Table)

| Situation | Reach for |
|---|---|
| Store billions of rows for cheap scans & aggregations | **Parquet files** (L1) — ZSTD, dictionary-encoded, partitioned by date |
| Pass tables between Pandas/NumPy/DuckDB/Polars in one process | **Apache Arrow** (L2) |
| Ad-hoc SQL over Parquet/lake from a laptop, no servers | **DuckDB** (L3) |
| Many writers + readers, auditors, GDPR deletes, schema evolution on the lake | **Iceberg table format** (L4) |
| Serve typed data to BI tools / JDBC / notebooks over the network | **Flight SQL server** (L5) |
| Data or compute outgrows one machine (TBs–PBs, scheduled ETL) | **Spark** (L6) |

These compose rather than compete: a bank typically runs *all six at once* — see the diagram below.

## 🧠 The Big Picture — How It All Fits Together

```
                        ┌─────────────────────────────────────────────┐
                        │              FINBANK DATA PLATFORM           │
                        └─────────────────────────────────────────────┘

   OLTP Systems          Ingest / Serve         Lakehouse Layer        Analytics
 ┌─────────────┐      ┌──────────────┐       ┌────────────────┐    ┌──────────┐
 │ Core Banking│      │ Flight SQL   │       │ Apache Iceberg │    │ DuckDB   │
 │ (PostgreSQL)│◄────►│ Server       │◄─────►│ Tables on S3   │───►│ Local SQL│
 │ Cards/Txn DB│      │ (Arrow over  │       │ (Parquet files │    │ Notebooks│
 └─────────────┘      │  gRPC)       │       │  + metadata)   │    └──────────┘
                      └──────▲───────┘       └───────┬────────┘    ┌──────────┐
                             │                       │             │ Spark /  │
                      ┌──────┴───────┐               │             │ Trino    │
                      │ Apache Arrow │◄──────────────┘             │ Engines  │
                      │ (zero-copy   │      reads/writes           └──────────┘
                      │  in-memory)  │
                      └──────▲───────┘
                             │
                ┌────────────┴─────────────┐
                │  COLUMNAR STORAGE LAYER   │
                │  (Parquet on disk/object  │
                │   store, Arrow in memory) │
                └───────────────────────────┘
```

**The story in one paragraph:**

FinBank's core banking system writes millions of transactions/day into PostgreSQL (row-oriented, great for OLTP). For fraud detection and regulatory reporting we copy that data into the lake as **Parquet**, a **columnar file format** (Lesson 1). Inside processes, we hold it as **Apache Arrow** arrays so Pandas, DuckDB, and Spark share it with zero copies (Lesson 2). Analysts query billions of rows from their laptops using **DuckDB** (Lesson 3). On the lake, tables are registered as **Apache Iceberg** tables giving us ACID guarantees, time travel for audits, and safe concurrent writes (Lesson 4). Finally, instead of slow CSV-over-REST APIs, our risk team pulls data through an **Apache Flight SQL** server that streams Arrow record batches over gRPC (Lesson 5).

---

## 🎓 How to Use This Course

Each lesson follows the same structure:

1. **Concepts** — what it is, why it exists, how it works internally.
2. **Banking scenario** — a concrete FinBank problem this technology solves.
3. **End-to-end Python** — runnable code you can execute locally (`pip install` instructions included).
4. **Cheat sheet** — quick reference commands/APIs.
5. **Exercises + Quiz** — to actually become a master, do them.

### Prerequisites

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

*(Each lesson lists exactly which packages it needs. Lesson 6 additionally needs `pip install pyspark`.)*

### Suggested pace

- Day 1–2: Lesson 1 & 2 (storage fundamentals)
- Day 3–4: Lesson 3 (DuckDB hands-on)
- Day 5–7: Lesson 4 (Iceberg + catalog setup)
- Day 8–9: Lesson 5 (Flight SQL client/server)
- Day 10–12: Lesson 6 (Spark at cluster scale)
- Day 13–14: Lesson 7 (capstone) — build it, then extend it

---

## 📖 Glossary (Terms Used Throughout)

| Term | Meaning |
|---|---|
| **OLTP / OLAP** | Online Transaction Processing (many small reads/writes, row stores) vs. Online Analytical Processing (big scans/aggregations, column stores). |
| **Columnar format** | Data laid out column-by-column so scans touch only needed fields and compress well (Parquet, ORC). |
| **Predicate pushdown** | Engine pushes `WHERE` filters down to storage; whole files/row-groups get skipped via min/max stats. |
| **Projection pushdown** | Reading only the columns a query references. |
| **Vectorized execution** | Processing values in batches/vectors (~2048–64k) per operator step instead of one row at a time — cache/SIMD friendly. |
| **Zero-copy** | Sharing memory buffers between systems without duplicating or re-parsing bytes (Arrow's superpower). |
| **IPC** | Inter-Process Communication — Arrow's streaming/file format for moving record batches. |
| **CDC** | Change Data Capture — streaming inserts/updates/deletes from an OLTP DB into the lake (e.g., Debezium). |
| **ACID** | Atomicity, Consistency, Isolation, Durability — transaction guarantees Iceberg brings to object storage. |
| **Table format** | Spec for data files + metadata that turns a folder of Parquet into a versioned, transactional table (Iceberg/Delta/Hudi). |
| **Time travel** | Querying a table *as of* an earlier snapshot/version. |
| **Catalog** | Service that tracks the current metadata pointer for each table (REST, Glue, Nessie, Hive…). |
| **Lakehouse** | Lake storage + table format + multiple engines = warehouse-like reliability on cheap object storage. |
| **gRPC / HTTP2** | RPC framework Flight uses for multiplexed, backpressured streams of Arrow batches. |
| **Shuffle** | Spark's redistribution of data between executors for joins/group-bys — the main thing to minimize. |

---

## 🏁 Capstone Project

Build **"FinBank Fraud Analytics Platform"** combining all six lessons:

1. Generate synthetic card transactions (Pandas).
2. Convert to Arrow, export Parquet partitioned by date (L1+L2).
3. Register an Iceberg table over the warehouse; run inserts/upserts with ACID (L4).
4. Query with DuckDB directly against Parquet/Iceberg metadata (L3).
5. Serve results through your own Flight SQL server; consume them from a Python/JDBC client (L5).
6. Know which steps collapse into the Spark job when data outgrows one machine (L6).

---

*Start with `01_Columnar_Data_Storage.md` ➡️*
