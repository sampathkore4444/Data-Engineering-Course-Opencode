# Parquet and Data Lakes

## Table of Contents
1. [Detailed Explanation](#1-detailed-explanation)
2. [Example](#2-example)
3. [Real-World Banking Scenario 1](#3-banking-scenario-1-data-lake-architecture)
4. [Python Code - Scenario 1](#4-python-code---scenario-1)
5. [Real-World Banking Scenario 2](#5-banking-scenario-2-data-lakehouse)
6. [Python Code - Scenario 2](#6-python-code---scenario-2)
7. [Interview Questions](#7-interview-questions)

---

## 1. Detailed Explanation

### Parquet in the Data Lake

Parquet is the **de facto standard** for data lake storage:

> **Data lakes store vast amounts of raw and processed data in object storage (S3, GCS, ADLS). Parquet is the primary file format because of its columnar storage, compression, and query optimization capabilities.**

### Data Lake Architecture with Parquet

```
Data Sources
     |
     +-- Operational Databases (Oracle, MySQL)
     +-- Streaming (Kafka, Kinesis)
     +-- APIs (REST, GraphQL)
     +-- Files (CSV, JSON)
     |
     v
  Ingestion Layer
     |
     v
  Processing Layer (Spark, Flink)
     |
     v
  Storage Layer (S3, GCS, ADLS)
     |
     +-- Raw Zone (Bronze)
     |     +-- Parquet files (raw data)
     |
     +-- Processed Zone (Silver)
     |     +-- Parquet files (cleaned, enriched)
     |
     +-- Curated Zone (Gold)
     |     +-- Parquet files (aggregated, BI-ready)
     |
     v
  Query Layer
     |
     +-- DuckDB (ad-hoc)
     +-- Spark (batch processing)
     +-- Trino (interactive SQL)
     +-- BI Tools (Tableau, PowerBI)
```

### Why Parquet for Data Lakes?

| Feature | Benefit for Data Lakes |
|---------|----------------------|
| Columnar storage | Efficient analytical queries |
| Compression | 5-10x storage savings |
| Predicate pushdown | Fast queries on large data |
| Schema evolution | Handle changing data structures |
| Splittable | Parallel processing |
| Language-neutral | Works with every tool |
| Self-describing | No external schema registry needed |

### Data Lake Zones with Parquet

```
Bronze (Raw):
  s3://data-lake/bronze/transactions/
    year=2026/
      month=08/
        day=24/
          transactions_001.parquet
          transactions_002.parquet

Silver (Cleaned):
  s3://data-lake/silver/transactions/
    year=2026/
      month=08/
        day=24/
          transactions_clean.parquet

Gold (Aggregated):
  s3://data-lake/gold/
    daily_summary/
      year=2026/
        month=08/
          day=24/
            summary.parquet
    customer_360/
      customer_360.parquet
```

### Parquet File Organization Best Practices

**File sizing:**
```
Optimal file size: 256MB - 1GB
Too small: < 64MB (metadata overhead)
Too large: > 1GB (poor parallelism)
```

**Partitioning:**
```
Partition by: date, region, category
Avoid: high-cardinality columns (user_id, transaction_id)
Rule of thumb: Partition should create 10-100K files, not millions
```

**Naming conventions:**
```
Good: transactions_2026_08_24_001.parquet
Bad:  part-00000-abc123.parquet (not human-readable)
```

---

## 2. Example

### Data Lake Structure Example

```
s3://banking-data-lake/
├── bronze/
│   ├── transactions/
│   │   ├── year=2026/
│   │   │   ├── month=08/
│   │   │   │   ├── day=24/
│   │   │   │   │   ├── transactions_001.parquet (512 MB)
│   │   │   │   │   └── transactions_002.parquet (488 MB)
│   │   │   │   └── day=25/
│   │   │   └── month=09/
│   │   └── year=2025/
│   ├── customers/
│   └── accounts/
├── silver/
│   ├── transactions_clean/
│   ├── customers_enriched/
│   └── accounts_verified/
├── gold/
│   ├── daily_summary/
│   ├── customer_360/
│   └── branch_performance/
└── sandbox/
    └── ad_hoc_analysis/
```

---

## 3. Banking Scenario 1: Data Lake Architecture

### Problem
A bank is building a data lake to consolidate data from:
- Core banking system (Oracle)
- Card processing system (MySQL)
- Online banking (PostgreSQL)
- Mobile app events (Kafka)
- Third-party data (CSV feeds)

Requirements:
- 100 TB initial storage, growing 10 TB/month
- Support for batch and real-time analytics
- Compliance with data retention policies
- Cost-effective storage

### Why Parquet?
- 5-10x compression vs raw formats
- Works with every analytics tool
- Supports partitioning for efficient queries
- Enables schema evolution as requirements change

### Architecture
```
Core Banking (Oracle)
       |
Card Processing (MySQL)
       |  Debezium CDC
Online Banking (PostgreSQL)  →  Kafka  →  Spark/Flink
Mobile Events (Kafka)
       |
Third-party Data (CSV)
       |
       v
  ETL Pipeline
       |
       v
  S3 Data Lake (Parquet)
       |
       +-- Bronze (raw)
       +-- Silver (cleaned)
       +-- Gold (aggregated)
       |
       v
  Analytics Layer
```

---

## 4. Python Code - Scenario 1

```python
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
import numpy as np
from datetime import datetime, timedelta
import random
import os
import tempfile

# ============================================================
# BANKING SCENARIO: Data Lake Architecture
# ============================================================

def create_data_lake_structure(base_path):
    """Create the data lake directory structure."""
    zones = ["bronze", "silver", "gold"]
    domains = ["transactions", "customers", "accounts"]

    for zone in zones:
        for domain in domains:
            path = os.path.join(base_path, zone, domain)
            os.makedirs(path, exist_ok=True)

    print(f"Created data lake structure at {base_path}")


def generate_bronze_transactions(base_path, num_days=30, txns_per_day=10_000):
    """Generate raw transaction data (Bronze zone)."""
    random.seed(42)
    np.random.seed(42)

    for day_offset in range(num_days):
        date = datetime(2026, 8, 1) + timedelta(days=day_offset)
        date_str = date.strftime("%Y-%m-%d")

        # Create day partition
        day_path = os.path.join(base_path, "bronze", "transactions", f"date={date_str}")
        os.makedirs(day_path, exist_ok=True)

        # Generate raw data (with some quality issues)
        amounts = np.random.lognormal(6, 2, txns_per_day).round(2)
        amounts[np.random.choice(txns_per_day, 50)] = np.nan  # Nulls
        amounts[np.random.choice(txns_per_day, 20)] = -100.0  # Negatives

        table = pa.table({
            "transaction_id": pa.array(list(range(day_offset * txns_per_day + 1,
                                                     (day_offset + 1) * txns_per_day + 1)), type=pa.int64()),
            "account_id": pa.array([f"ACC{random.randint(100000, 999999)}" for _ in range(txns_per_day)], type=pa.string()),
            "amount": pa.array(amounts, type=pa.float64()),
            "currency": pa.array(np.random.choice(["USD", "EUR", "GBP", ""], txns_per_day), type=pa.string()),
            "status": pa.array(np.random.choice(
                ["COMPLETED", "PENDING", "FAILED", ""], txns_per_day
            ), type=pa.string()),
            "channel": pa.array(np.random.choice(
                ["ONLINE", "MOBILE", "BRANCH", "ATM", "POS"], txns_per_day
            ), type=pa.string()),
            "raw_timestamp": pa.array([
                f"{date_str} {random.randint(0,23):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}"
                for _ in range(txns_per_day)
            ], type=pa.string()),
        })

        # Write as raw Parquet (no optimization)
        pq.write_table(table, os.path.join(day_path, "raw.parquet"), compression="snappy")

    print(f"Generated {num_days} days of bronze data")


def process_silver_transactions(base_path, num_days=30):
    """Process bronze data to silver (cleaned, enriched)."""
    for day_offset in range(num_days):
        date = datetime(2026, 8, 1) + timedelta(days=day_offset)
        date_str = date.strftime("%Y-%m-%d")

        # Read bronze
        bronze_path = os.path.join(base_path, "bronze", "transactions", f"date={date_str}", "raw.parquet")
        if not os.path.exists(bronze_path):
            continue

        table = pq.read_table(bronze_path)

        # Clean: Remove nulls and negatives
        mask = pc.and_(
            pc.is_valid(table.column("amount")),
            pc.greater(table.column("amount"), pa.scalar(0.0))
        )
        cleaned = pc.filter(table, mask)

        # Clean: Remove empty statuses
        status_mask = pc.and_(
            pc.is_valid(cleaned.column("status")),
            pc.not_equal(cleaned.column("status"), pa.scalar(""))
        )
        cleaned = pc.filter(cleaned, status_mask)

        # Write silver (optimized)
        silver_path = os.path.join(base_path, "silver", "transactions", f"date={date_str}")
        os.makedirs(silver_path, exist_ok=True)

        pq.write_table(
            cleaned,
            os.path.join(silver_path, "clean.parquet"),
            compression="zstd",
            use_dictionary=True,
            write_statistics=True,
        )

    print(f"Processed {num_days} days to silver")


def generate_gold_aggregates(base_path, num_days=30):
    """Generate gold zone aggregates."""
    all_daily_summaries = []

    for day_offset in range(num_days):
        date = datetime(2026, 8, 1) + timedelta(days=day_offset)
        date_str = date.strftime("%Y-%m-%d")

        # Read silver
        silver_path = os.path.join(base_path, "silver", "transactions", f"date={date_str}", "clean.parquet")
        if not os.path.exists(silver_path):
            continue

        table = pq.read_table(silver_path)

        # Convert to pandas for aggregation
        df = table.to_pandas()

        # Daily summary
        summary = {
            "date": date_str,
            "total_transactions": len(df),
            "total_amount": df["amount"].sum(),
            "avg_amount": df["amount"].mean(),
            "completed_pct": (df["status"] == "COMPLETED").mean() * 100,
        }
        all_daily_summaries.append(summary)

    # Write gold summary
    import pandas as pd
    summary_df = pd.DataFrame(all_daily_summaries)

    gold_path = os.path.join(base_path, "gold", "daily_summary")
    os.makedirs(gold_path, exist_ok=True)

    pq.write_table(
        pa.Table.from_pandas(summary_df),
        os.path.join(gold_path, "summary.parquet"),
        compression="zstd",
    )

    print(f"Generated gold zone summary ({len(all_daily_summaries)} days)")


def query_data_lake(base_path):
    """Query across data lake zones."""
    import duckdb

    con = duckdb.connect()

    # Query bronze (raw)
    print("\n=== Bronze Zone (Raw Data) ===")
    result = con.execute(f"""
        SELECT COUNT(*) as total_rows, SUM(amount) as total_amount
        FROM read_parquet('{base_path}/bronze/transactions/date=2026-08-24/raw.parquet')
    """).fetchdf()
    print(result.to_string(index=False))

    # Query silver (cleaned)
    print("\n=== Silver Zone (Cleaned Data) ===")
    result = con.execute(f"""
        SELECT COUNT(*) as total_rows, SUM(amount) as total_amount
        FROM read_parquet('{base_path}/silver/transactions/date=2026-08-24/clean.parquet')
    """).fetchdf()
    print(result.to_string(index=False))

    # Query gold (aggregated)
    print("\n=== Gold Zone (Aggregated) ===")
    result = con.execute(f"""
        SELECT * FROM read_parquet('{base_path}/gold/daily_summary/summary.parquet')
        LIMIT 10
    """).fetchdf()
    print(result.to_string(index=False))


# ============================================================
# RUN THE SCENARIO
# ============================================================
if __name__ == "__main__":
    base_path = os.path.join(tempfile.gettempdir(), "banking_data_lake")

    # Create structure
    create_data_lake_structure(base_path)

    # Generate bronze data
    print("\nGenerating bronze data...")
    generate_bronze_transactions(base_path, num_days=7, txns_per_day=5000)

    # Process to silver
    print("\nProcessing to silver...")
    process_silver_transactions(base_path, num_days=7)

    # Generate gold
    print("\nGenerating gold aggregates...")
    generate_gold_aggregates(base_path, num_days=7)

    # Query data lake
    query_data_lake(base_path)
```

---

## 5. Banking Scenario 2: Data Lakehouse

### Problem
A bank wants to combine data lake flexibility with data warehouse reliability:
- ACID transactions on Parquet data
- Schema evolution without data loss
- Time travel for auditing
- Multi-engine support (Spark, DuckDB, Trino)

### Why Lakehouse?
- Data lake: Cheap storage, flexible schemas
- Data warehouse: ACID, time travel, governance
- Lakehouse: Best of both worlds

### Architecture
```
Data Sources
       |
       v
  ETL Pipeline
       |
       v
  Iceberg / Delta Lake (Table Format)
       |
       +-- ACID transactions
       +-- Schema evolution
       +-- Time travel
       +-- Partition evolution
       |
       v
  Parquet Files (S3)
       |
       v
  Query Engines
       |
       +-- Spark (batch)
       +-- DuckDB (ad-hoc)
       +-- Trino (interactive)
       +-- Flink (streaming)
```

---

## 6. Python Code - Scenario 2

```python
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
from datetime import datetime, timedelta
import random
import os
import tempfile
import time

# ============================================================
# BANKING SCENARIO: Data Lakehouse with Parquet
# ============================================================

def create_lakehouse_schema():
    """Define schema for lakehouse transaction table."""
    return pa.schema([
        ("transaction_id", pa.int64()),
        ("account_id", pa.string()),
        ("customer_id", pa.string()),
        ("amount", pa.decimal128(18, 2)),
        ("currency", pa.string()),
        ("transaction_type", pa.string()),
        ("status", pa.string()),
        ("channel", pa.string()),
        ("transaction_date", pa.date32()),
        ("created_at", pa.timestamp("us")),
        ("updated_at", pa.timestamp("us")),
        ("version", pa.int32()),
    ])


def generate_lakehouse_data(num_rows=100_000):
    """Generate transaction data for lakehouse."""
    random.seed(42)
    np.random.seed(42)

    from decimal import Decimal

    table = pa.table({
        "transaction_id": pa.array(list(range(1, num_rows + 1)), type=pa.int64()),
        "account_id": pa.array([f"ACC{random.randint(100000, 999999)}" for _ in range(num_rows)], type=pa.string()),
        "customer_id": pa.array([f"CUST{random.randint(10000, 99999)}" for _ in range(num_rows)], type=pa.string()),
        "amount": pa.array([Decimal(str(round(random.uniform(1.0, 100000.0), 2))) for _ in range(num_rows)], type=pa.decimal128(18, 2)),
        "currency": pa.array(np.random.choice(["USD", "EUR", "GBP"], num_rows), type=pa.string()),
        "transaction_type": pa.array(np.random.choice(["DEBIT", "CREDIT", "TRANSFER", "WIRE"], num_rows), type=pa.string()),
        "status": pa.array(np.random.choice(["COMPLETED", "PENDING", "FAILED"], num_rows), type=pa.string()),
        "channel": pa.array(np.random.choice(["ONLINE", "MOBILE", "BRANCH", "ATM"], num_rows), type=pa.string()),
        "transaction_date": pa.array([
            (datetime(2026, 8, 1) + timedelta(days=random.randint(0, 30))).strftime("%Y-%m-%d")
            for _ in range(num_rows)
        ], type=pa.date32()),
        "created_at": pa.array([datetime.now()] * num_rows, type=pa.timestamp("us")),
        "updated_at": pa.array([datetime.now()] * num_rows, type=pa.timestamp("us")),
        "version": pa.array([1] * num_rows, type=pa.int32()),
    })

    return table


def store_lakehouse_snapshot(table, base_path, snapshot_id):
    """Store a snapshot of the lakehouse table."""
    snapshot_path = os.path.join(base_path, f"snapshot={snapshot_id}")
    os.makedirs(snapshot_path, exist_ok=True)

    pq.write_to_dataset(
        table,
        root_path=snapshot_path,
        partition_cols=["transaction_date"],
        compression="zstd",
        use_dictionary=True,
        write_statistics=True,
    )

    # Write metadata
    metadata = {
        "snapshot_id": snapshot_id,
        "timestamp": datetime.now().isoformat(),
        "num_rows": table.num_rows,
        "num_columns": table.num_columns,
    }

    print(f"Snapshot {snapshot_id}: {table.num_rows:,} rows written")


def simulate_update(table, account_id, new_amount):
    """Simulate an UPDATE operation (creates new snapshot)."""
    from decimal import Decimal

    # Find the row
    mask = pc.equal(table.column("account_id"), pa.scalar(account_id))
    indices = pc.filter(pa.array(range(len(table))), mask)

    if len(indices) == 0:
        return table

    # Create updated table
    amounts = table.column("amount").to_pylist()
    idx = indices[0].as_py()
    amounts[idx] = Decimal(str(new_amount))

    updated_table = table.set_column(
        table.schema.get_field_index("amount"),
        "amount",
        pa.array(amounts, type=pa.decimal128(18, 2))
    )

    # Update version and timestamp
    versions = updated_table.column("version").to_pylist()
    versions[idx] = versions[idx] + 1
    updated_table = updated_table.set_column(
        updated_table.schema.get_field_index("version"),
        "version",
        pa.array(versions, type=pa.int32())
    )

    return updated_table


def simulate_delete(table, transaction_id):
    """Simulate a DELETE operation (creates new snapshot)."""
    mask = pc.not_equal(table.column("transaction_id"), pa.scalar(transaction_id))
    return pc.filter(table, mask)


def query_snapshot(base_path, snapshot_id):
    """Query a specific snapshot."""
    import duckdb

    con = duckdb.connect()

    snapshot_path = os.path.join(base_path, f"snapshot={snapshot_id}")

    result = con.execute(f"""
        SELECT 
            COUNT(*) as total_rows,
            SUM(CAST(amount AS DOUBLE)) as total_amount,
            COUNT(DISTINCT account_id) as unique_accounts
        FROM read_parquet('{snapshot_path}/**/*.parquet')
    """).fetchdf()

    return result


def compare_snapshots(base_path, snapshot_id_1, snapshot_id_2):
    """Compare two snapshots."""
    import duckdb

    con = duckdb.connect()

    path1 = os.path.join(base_path, f"snapshot={snapshot_id_1}")
    path2 = os.path.join(base_path, f"snapshot={snapshot_id_2}")

    result = con.execute(f"""
        SELECT 
            'Snapshot {snapshot_id_1}' as snapshot,
            COUNT(*) as rows,
            SUM(CAST(amount AS DOUBLE)) as total_amount
        FROM read_parquet('{path1}/**/*.parquet')
        UNION ALL
        SELECT 
            'Snapshot {snapshot_id_2}' as snapshot,
            COUNT(*) as rows,
            SUM(CAST(amount AS DOUBLE)) as total_amount
        FROM read_parquet('{path2}/**/*.parquet')
    """).fetchdf()

    return result


# ============================================================
# RUN THE SCENARIO
# ============================================================
if __name__ == "__main__":
    base_path = os.path.join(tempfile.gettempdir(), "lakehouse")
    os.makedirs(base_path, exist_ok=True)

    # Generate initial data
    print("Generating initial data...")
    table = generate_lakehouse_data(num_rows=100_000)

    # Snapshot 1: Initial load
    store_lakehouse_snapshot(table, base_path, snapshot_id=1)

    # Snapshot 2: After updates
    print("\nSimulating updates...")
    updated_table = simulate_update(table, "ACC100000", 999999.99)
    updated_table = simulate_delete(updated_table, 42)
    store_lakehouse_snapshot(updated_table, base_path, snapshot_id=2)

    # Compare snapshots
    print("\n=== Snapshot Comparison ===")
    comparison = compare_snapshots(base_path, 1, 2)
    print(comparison.to_string(index=False))

    # Query specific snapshot
    print("\n=== Query Snapshot 1 ===")
    result = query_snapshot(base_path, 1)
    print(result.to_string(index=False))
```

---

## 7. Interview Questions

### Q1: What is a data lake and why is Parquet the preferred format?

**Answer:**

A data lake is a centralized repository for storing structured and unstructured data at scale, typically on object storage (S3, GCS, ADLS).

**Why Parquet is preferred:**

1. **Columnar storage**: Efficient for analytical queries (read only needed columns)
2. **Compression**: 5-10x compression reduces storage costs
3. **Predicate pushdown**: Skip irrelevant data for fast queries
4. **Self-describing**: Schema embedded in files
5. **Splittable**: Enables parallel processing
6. **Tool-agnostic**: Works with Spark, DuckDB, Trino, Pandas, etc.

**Example**:
```
Raw CSV: 100 GB, queries take 30 minutes
Parquet: 10 GB, queries take 30 seconds
```

---

### Q2: What are the Bronze/Silver/Gold zones in a data lake?

**Answer:**

| Zone | Purpose | Data State | Example |
|------|---------|------------|---------|
| **Bronze** | Raw ingestion | As-is from source | Raw CSV/JSON from APIs |
| **Silver** | Cleaned & validated | Deduplicated, typed, validated | Cleaned transactions |
| **Gold** | Business-ready | Aggregated, enriched, BI-ready | Daily summaries, KPIs |

**Benefits:**
- **Data lineage**: Track data from raw to refined
- **Reproducibility**: Re-process from any zone
- **Quality gates**: Validate at each stage
- **Cost optimization**: Hot (Gold) vs Cold (Bronze) storage tiers

**Example**:
```
Bronze: Raw API responses (100 GB)
Silver: Cleaned, typed transactions (20 GB)
Gold: Daily branch summaries (1 GB)
```

---

### Q3: How do you organize Parquet files in a data lake?

**Answer:**

**Partitioning strategy:**
```
s3://data-lake/transactions/
  year=2026/
    month=08/
      day=24/
        transactions_001.parquet
        transactions_002.parquet
```

**Best practices:**

1. **Partition by low-cardinality columns**: date, region, status
2. **Avoid over-partitioning**: Each partition should have > 100K rows
3. **File sizing**: 256MB - 1GB per file
4. **Naming**: Include meaningful identifiers
5. **Compression**: Use Zstd for best balance

**Anti-patterns:**
- Partitioning by user_id (millions of partitions)
- Files < 64MB (metadata overhead)
- Files > 2GB (poor parallelism)

---

### Q4: How do you handle schema evolution in a data lake?

**Answer:**

**Strategies:**

1. **Add columns**: Parquet supports this natively
```python
# Old schema: [id, name]
# New schema: [id, name, email]
# Old files: email column appears as NULL
```

2. **Use Iceberg/Delta Lake**: Table format manages schema evolution
```python
# Iceberg handles schema across files
ALTER TABLE transactions ADD COLUMN email STRING
```

3. **Version your schemas**: Include version in metadata
```python
schema_version = "v3"
```

**Best practices:**
- Add new columns at the end
- Never remove columns (mark as deprecated)
- Test schema compatibility before production
- Use Iceberg for complex evolution

---

### Q5: What is the difference between a data lake and a lakehouse?

**Answer:**

| Feature | Data Lake | Lakehouse |
|---------|-----------|-----------|
| Storage | Object storage (S3) | Object storage (S3) |
| Format | Parquet, CSV, JSON | Parquet (managed) |
| ACID | ❌ | ✅ |
| Schema evolution | ⚠️ Limited | ✅ Full |
| Time travel | ❌ | ✅ |
| Governance | Manual | Built-in |
| Query performance | Good | Better (optimized metadata) |

**Lakehouse adds:**
- **Table format**: Iceberg, Delta Lake, Hudi
- **ACID transactions**: Consistent writes
- **Time travel**: Query historical snapshots
- **Schema evolution**: Safe schema changes
- **Partition evolution**: Change partitioning without rewriting

**When to use which:**
- **Data lake**: Simple storage, cost optimization, flexible schemas
- **Lakehouse**: Production analytics, compliance, multi-engine access
