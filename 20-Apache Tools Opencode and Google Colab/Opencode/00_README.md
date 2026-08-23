# Mastering Modern Analytics Data Infrastructure

### Columnar Storage · Apache Parquet · Apache Arrow · DuckDB · Apache Iceberg · Apache Flight SQL · Lakehouse Architecture · S3 Object Storage · Apache Spark

> A complete course built around **real-world banking scenarios** with runnable **Python code**.
>
> Tested with: Python 3.12, `pyarrow 25.x`, `pandas`, `duckdb 1.5.x`.
> Install everything you need: `pip install pyarrow pandas duckdb`
> (lessons 12-13 add: `pip install boto3 "moto[s3]" pyspark`)

---

## Why this course exists

Modern data engineering has quietly replaced the old "dump CSVs into a warehouse" model with a
**decoupled lakehouse stack**:

| Layer | Technology | One-line job |
|---|---|---|
| On-disk format | **Parquet** (columnar) | Compress & organize data physically |
| In-memory format | **Apache Arrow** | Zero-copy columnar data movement between systems |
| Query engine (embedded) | **DuckDB** | Serverless OLAP: SQL over Parquet/Arrow at laptop or cluster scale |
| Table format | **Apache Iceberg** | ACID transactions, time travel, schema/partition evolution on raw files |
| Wire protocol / API | **Flight SQL** | Fast, standardized SQL data access over gRPC (Arrow-native) |
| Storage substrate | **S3-compatible object storage** | Durable, cheap "disk" the lake physically lives on |
| Scale-out engine | **Apache Spark** | Same open files, fleet-sized joins, windows and features |

A bank that masters this stack can run fraud analytics over billions of transactions,
prove "what did the ledger look like at quarter end?" to an auditor, and serve query results
to any language without serialization bottlenecks.

## The learning path

Read in order — each lesson builds on the previous one.

| # | Lesson | What you will master |
|---|--------|----------------------|
| 01 | [`01_columnar_storage_fundamentals.md`](01_columnar_storage_fundamentals.md) | Row vs column layout, why OLAP loves columns, encodings (RLE, dictionary, delta), compression |
| 02 | [`02_parquet_file_format.md`](02_parquet_file_format.md) | Parquet internals: row groups, column chunks, pages, footer metadata, predicate pushdown, partitioned datasets |
| 03 | [`03_arrow_in_memory_format.md`](03_arrow_in_memory_format.md) | The Arrow memory model: buffers, validity bitmaps, offset arrays, RecordBatch/Table, zero-copy IPC |
| 04 | [`04_pyarrow_hands_on_lab.md`](04_pyarrow_hands_on_lab.md) | PyArrow in practice: arrays, compute kernels, pandas interop, datasets API, streaming batches |
| 05 | [`05_duckdb_analytical_engine.md`](05_duckdb_analytical_engine.md) | Embedded OLAP with DuckDB: vectorized execution, direct Parquet queries, Arrow interchange, ASOF joins, window functions |
| 06 | [`06_iceberg_table_format.md`](06_iceberg_table_format.md) | Why "just files" breaks; Iceberg snapshots, manifests, time travel, hidden partitioning, schema/partition evolution, catalogs |
| 07 | [`07_iceberg_hands_on_lab.md`](07_iceberg_hands_on_lab.md) | Iceberg with PyIceberg + DuckDB + Spark-style SQL: create tables, upserts, time travel for audits, compaction, maintenance |
| 08 | [`08_flight_sql_protocol.md`](08_flight_sql_protocol.md) | gRPC fundamentals, Flight data model, Flight SQL commands, auth, streaming result sets |
| 09 | [`09_flight_sql_hands_on_lab.md`](09_flight_sql_hands_on_lab.md) | Build a production-style Flight SQL server (DuckDB backend) + clients in Python and Java-style patterns |
| 10 | [`10_lakehouse_architecture.md`](10_lakehouse_architecture.md) | Medallion architecture, catalogs, small-file problem, compaction/orphan cleanup, engine interop, security |
| 11 | [`11_capstone_banking_project.md`](11_capstone_banking_project.md) | End-to-end capstone: ingest → Parquet/Iceberg → fraud & regulatory analytics → serve via Flight SQL |
| 12 | [`12_object_storage_data_lake.md`](12_object_storage_data_lake.md) | Object storage semantics: flat keys, no renames, footer-only planning over HTTP range GETs, DuckDB httpfs on S3 |
| 13 | [`13_spark_distributed_analytics.md`](13_spark_distributed_analytics.md) | Apache Spark: lazy DAGs and shuffles, pushdown proof via EXPLAIN, sliding velocity windows at scale, hive-partitioned write-back |

## Where DuckDB fits

DuckDB is the **engine** that makes this stack tangible on any machine:

```
   Parquet files  ──query──▶  DuckDB  ──zero-copy──▶  Arrow  ──stream──▶  Flight SQL clients
        ▲                        │
        └── written by PyArrow   └── reads/writes Iceberg (extension), pandas, CSV, JSON
```

You will use it as the analytical brain in lessons 05, 07, 09, 11 and 12.

## The running banking case study

Every lesson uses the same fictional bank: **Meridian Trust Bank**, with:

- A **core banking ledger** (row-based OLTP database): accounts, payments, transfers
- A **card payments stream**: ~2 billion events/year
- **Regulatory obligations**: central-bank reporting must reproduce quarter-end ledger state exactly
- A **fraud team** that needs sub-second scans across years of card transactions
- An **AML (anti-money-laundering)** team doing graph-like pattern queries over histories

These requirements map *perfectly* onto the five technologies:

- Fraud scans → **columnar storage** (read 5 of 60 columns, not all)
- Ledger archives → **Parquet**
- Cross-system data movement without CPU burn → **Arrow zero-copy / IPC**
- Ad-hoc risk/fraud SQL over files, no servers → **DuckDB**
- Quarter-end reproducibility & schema changes without downtime → **Iceberg**
- BI tools, notebooks, and risk engines querying one governed endpoint → **Flight SQL**

## How to use this repo

```bash
pip install pyarrow pandas duckdb
python3 <any code block from lessons>   # every snippet is self-contained unless stated
```

Each lesson follows the same rhythm:

1. **Concept** — theory explained from first principles
2. **Architecture diagram** — ASCII wiring diagrams
3. **Banking scenario** — how Meridian Trust uses it in production
4. **End-to-end example** — complete runnable Python code
5. **Exercises** — to make you a master, not a tourist
6. **Cheat sheet** — quick reference

By lesson 11 you will have designed and built a miniature lakehouse for a bank with your own hands.
