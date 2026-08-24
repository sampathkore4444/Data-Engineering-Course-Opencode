# Concept 16: DuckDB vs Iceberg - Complementary, Not Competitors

## 📚 Detailed Explanation

One of the most common misconceptions in modern data architecture is thinking **DuckDB** and **Iceberg** are competing technologies. They are not. They solve fundamentally different problems and are designed to work together.

### The Core Distinction

> **DuckDB = Query Engine** (How do I query the data?)
> **Iceberg = Table Management Layer** (What files constitute my table?)

### What DuckDB Does

DuckDB is an **embedded analytical SQL engine**. It excels at:

| Capability | Description |
|------------|-------------|
| **SQL Execution** | Full SQL support with analytics functions |
| **Parquet Reading** | Direct Parquet file scanning |
| **Vectorized Execution** | SIMD-optimized columnar processing |
| **In-Memory Processing** | Fast analytical queries |
| **Embedded** | No server required |
| ** lightweight** | Easy to deploy and use |

**DuckDB answers:** "How do I query this data efficiently?"

### What Iceberg Does

Iceberg is a **table format** that manages metadata and transactions. It excels at:

| Capability | Description |
|------------|-------------|
| **Table Management** | Which files belong to the table |
| **Snapshots** | Immutable versions of table state |
| **Time Travel** | Query historical data |
| **Schema Evolution** | Change schema without rewriting files |
| **Partition Evolution** | Change partitioning strategy |
| **ACID Transactions** | Atomic commits |
| **Concurrent Writers** | Safe multi-writer access |

**Iceberg answers:** "What is the current state of my table and how has it changed?"

### The Library Analogy

```
Parquet Files = Books (actual data)
Iceberg = Library Catalog + Version System (metadata, history)
DuckDB = Librarian (queries and analyzes)
```

You wouldn't ask: "Why do I need a library catalog? The librarian can search books."

For a tiny library, maybe. For 10 million books with multiple librarians, constant additions, and historical versions, you need both.

---

## 💡 Example: DuckDB vs Iceberg in Action

### Scenario: Simple Query (DuckDB Alone is Fine)

```python
import duckdb

# DuckDB directly queries Parquet files
con = duckdb.connect()
result = con.execute("""
    SELECT branch_id, SUM(amount)
    FROM read_parquet('s3://banking/transactions/*.parquet')
    GROUP BY branch_id
""").df()
```

**DuckDB works great here because:**
- Read-only queries
- Simple file structure
- No concurrent writes needed
- No versioning required

### Scenario: Complex Table Management (Need Iceberg)

```python
from pyiceberg.catalog import load_catalog

# Iceberg manages the table
catalog = load_catalog("banking", uri="http://catalog:8181")
table = catalog.load_table("banking.transactions")

# DuckDB queries the Iceberg table
con = duckdb.connect()
result = con.execute("""
    SELECT branch_id, SUM(amount)
    FROM banking.transactions
    WHERE transaction_date >= '2026-08-01'
    GROUP BY branch_id
""").df()
```

**Now you need Iceberg because:**
- Multiple writers (Spark, Flink)
- Need snapshots for time travel
- Schema evolution required
- ACID transactions needed

---

## 🏦 Real-World Banking Scenario 1: DuckDB for Local Analytics

### Scenario
A bank's **data science team** needs to analyze **50 GB** of transaction data for a fraud detection model. The data is in Parquet files on a shared drive. They need:
- Fast SQL queries
- Ad-hoc exploration
- Local processing
- No production infrastructure

### Problem
- Traditional data warehouse is slow for ad-hoc queries
- Setting up Spark is overkill for 50 GB
- Need immediate results

### Solution
DuckDB alone is perfect for this use case:
- Embedded (no server)
- Fast Parquet reading
- SQL support
- Local processing

### Python Code

```python
"""
Banking Scenario 1: DuckDB for Local Analytics
Using DuckDB for Data Science Exploration
"""

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import random
from datetime import datetime, timedelta
import os
import time

# ============================================================
# STEP 1: Create Sample Parquet Files (Simulate Data Lake)
# ============================================================

print("=== DUCKDB FOR LOCAL ANALYTICS ===\n")

# Create temporary directory
data_dir = "/tmp/banking_analytics"
os.makedirs(data_dir, exist_ok=True)

print("--- Creating Sample Parquet Files ---")

def generate_transaction_files(num_files: int, rows_per_file: int) -> list:
    """Generate multiple Parquet files."""
    
    files = []
    
    for file_num in range(num_files):
        # Generate data
        data = {
            "transaction_id": [f"TXN-{file_num:04d}-{i:06d}" for i in range(rows_per_file)],
            "account_id": [f"ACC-{1000 + (i % 100):06d}" for i in range(rows_per_file)],
            "branch_id": [f"BR-{10 + (i % 20):03d}" for i in range(rows_per_file)],
            "amount": [round(random.uniform(100, 100000), 2) for _ in range(rows_per_file)],
            "transaction_date": [
                (datetime(2026, 1, 1) + timedelta(days=random.randint(0, 240))).date()
                for _ in range(rows_per_file)
            ],
            "status": [random.choice(["COMPLETED", "PENDING", "DECLINED"]) for _ in range(rows_per_file)],
        }
        
        # Write to Parquet
        table = pa.table(data)
        file_path = os.path.join(data_dir, f"transactions_{file_num:04d}.parquet")
        pq.write_table(table, file_path)
        files.append(file_path)
    
    return files

# Generate 10 files with 50,000 rows each (500,000 total)
files = generate_transaction_files(10, 50000)
print(f"Generated {len(files)} Parquet files")

# Get total size
total_size = sum(os.path.getsize(f) for f in files)
print(f"Total data size: {total_size / 1024 / 1024:.2f} MB")

# ============================================================
# STEP 2: DuckDB Query - Simple Aggregation
# ============================================================

print("\n--- DuckDB Query: Branch Analysis ---")

con = duckdb.connect()

# Query 1: Branch-wise aggregation
start = time.time()
result1 = con.execute(f"""
    SELECT 
        branch_id,
        COUNT(*) as transaction_count,
        SUM(amount) as total_amount,
        AVG(amount) as avg_amount
    FROM read_parquet('{data_dir}/transactions_*.parquet')
    GROUP BY branch_id
    ORDER BY total_amount DESC
    LIMIT 10
""").df()
query1_time = time.time() - start

print(f"\nTop 10 Branches by Total Amount:")
print(result1.to_string(index=False))
print(f"\nQuery Time: {query1_time:.3f} seconds")

# ============================================================
# STEP 3: DuckDB Query - Date Range Filter
# ============================================================

print("\n--- DuckDB Query: Date Range Analysis ---")

start = time.time()
result2 = con.execute(f"""
    SELECT 
        transaction_date,
        COUNT(*) as transactions,
        SUM(amount) as daily_total
    FROM read_parquet('{data_dir}/transactions_*.parquet')
    WHERE transaction_date >= '2026-06-01'
      AND transaction_date < '2026-09-01'
    GROUP BY transaction_date
    ORDER BY transaction_date
""").df()
query2_time = time.time() - start

print(f"\nQ3 2026 Daily Transactions:")
print(result2.head(10).to_string(index=False))
print(f"\nQuery Time: {query2_time:.3f} seconds")

# ============================================================
# STEP 4: DuckDB Query - Window Functions
# ============================================================

print("\n--- DuckDB Query: Running Totals ---")

start = time.time()
result3 = con.execute(f"""
    SELECT 
        branch_id,
        transaction_date,
        daily_total,
        SUM(daily_total) OVER (
            PARTITION BY branch_id 
            ORDER BY transaction_date
        ) as running_total
    FROM (
        SELECT 
            branch_id,
            transaction_date,
            SUM(amount) as daily_total
        FROM read_parquet('{data_dir}/transactions_*.parquet')
        GROUP BY branch_id, transaction_date
    ) daily_agg
    WHERE branch_id = 'BR-010'
    ORDER BY transaction_date
    LIMIT 10
""").df()
query3_time = time.time() - start

print(f"\nRunning Totals for Branch BR-010:")
print(result3.to_string(index=False))
print(f"\nQuery Time: {query3_time:.3f} seconds")

# ============================================================
# STEP 5: DuckDB Query - Complex Analytics
# ============================================================

print("\n--- DuckDB Query: Fraud Detection Features ---")

start = time.time()
result4 = con.execute(f"""
    WITH branch_stats AS (
        SELECT 
            branch_id,
            AVG(amount) as avg_amount,
            STDDEV(amount) as stddev_amount
        FROM read_parquet('{data_dir}/transactions_*.parquet')
        GROUP BY branch_id
    )
    SELECT 
        t.transaction_id,
        t.branch_id,
        t.amount,
        bs.avg_amount,
        bs.stddev_amount,
        (t.amount - bs.avg_amount) / NULLIF(bs.stddev_amount, 0) as z_score
    FROM read_parquet('{data_dir}/transactions_*.parquet') t
    JOIN branch_stats bs ON t.branch_id = bs.branch_id
    WHERE ABS((t.amount - bs.avg_amount) / NULLIF(bs.stddev_amount, 0)) > 2
    ORDER BY z_score DESC
    LIMIT 10
""").df()
query4_time = time.time() - start

print(f"\nPotential Fraud Transactions (Z-Score > 2):")
print(result4.to_string(index=False))
print(f"\nQuery Time: {query4_time:.3f} seconds")

# ============================================================
# STEP 6: DuckDB Benefits Summary
# ============================================================

print("\n--- DuckDB Benefits Summary ---")

print("""
DUCKDB BENEFITS FOR LOCAL ANALYTICS:

1. EMBEDDED
   - No server required
   - Run anywhere
   - Easy deployment

2. FAST
   - Vectorized execution
   - SIMD optimizations
   - Parallel processing

3. SQL SUPPORT
   - Full SQL syntax
   - Window functions
   - CTEs
   - Joins

4. PARQUET NATIVE
   - Direct Parquet reading
   - No conversion needed
   - Efficient scanning

5. LIGHTWEIGHT
   - Small binary
   - Low memory usage
   - Fast startup

USE CASES:
  ✓ Data exploration
  ✓ Ad-hoc queries
  ✓ Notebooks
  ✓ Local analytics
  ✓ Small/medium datasets
  ✓ ETL transformations
""")
```

---

## 🏦 Real-World Banking Scenario 2: Iceberg + DuckDB for Production

### Scenario
A bank's **production analytics platform** needs:
- **Iceberg**: Manage 10 TB of transaction data with multiple writers
- **DuckDB**: Provide fast SQL queries for BI dashboards
- **Spark/Flink**: Handle batch and streaming writes

### Problem
- Need ACID transactions for data integrity
- Need time travel for auditing
- Need fast queries for dashboards
- Multiple engines writing simultaneously

### Solution
**Iceberg + DuckDB architecture:**
- Iceberg manages table metadata and transactions
- DuckDB queries Iceberg tables efficiently
- Spark/Flink write to Iceberg tables

### Python Code

```python
"""
Banking Scenario 2: Iceberg + DuckDB for Production
Using Complementary Technologies
"""

from pyiceberg.catalog import load_catalog
from pyiceberg.table import Table
from datetime import datetime, timedelta
import pyarrow as pa
import duckdb
import random
import time

# ============================================================
# STEP 1: Setup Iceberg Catalog
# ============================================================

print("=== ICEBERG + DUCKDB FOR PRODUCTION ===\n")

catalog = load_catalog(
    "banking_production",
    **{
        "uri": "http://catalog-service:8181",
        "warehouse": "s3a://banking-production-lakehouse/"
    }
)

# ============================================================
# STEP 2: Create Iceberg Table
# ============================================================

print("--- Creating Iceberg Table ---")

schema = pa.schema([
    pa.field("transaction_id", pa.string(), nullable=False),
    pa.field("account_id", pa.string(), nullable=False),
    pa.field("branch_id", pa.string(), nullable=False),
    pa.field("amount", pa.decimal128(18, 2), nullable=False),
    pa.field("transaction_date", pa.date32(), nullable=False),
    pa.field("status", pa.string(), nullable=False),
])

try:
    iceberg_table = catalog.create_table(
        identifier="production.transactions",
        schema=schema,
        partition_spec={"transform": "month", "source": "transaction_date"},
        properties={
            "write.format.default": "parquet",
            "write.parquet.compression-codec": "zstd",
        }
    )
    print("Created Iceberg table: production.transactions")
except Exception:
    iceberg_table = catalog.load_table("production.transactions")
    print("Loaded existing Iceberg table")

# ============================================================
# STEP 3: Load Data via Iceberg (Simulate Spark/Flink)
# ============================================================

print("\n--- Loading Data via Iceberg ---")

def load_monthly_data(iceberg_table: Table, year: int, month: int, rows: int) -> dict:
    """
    Simulate Spark/Flink loading data to Iceberg.
    In production, this would be a Spark job or Flink job.
    """
    start_time = datetime.now()
    
    # Generate data
    data = pa.table({
        "transaction_id": [f"TXN-{year}{month:02d}-{i:06d}" for i in range(1, rows + 1)],
        "account_id": [f"ACC-{1000 + (i % 100):06d}" for i in range(rows)],
        "branch_id": [f"BR-{10 + (i % 20):03d}" for i in range(rows)],
        "amount": [round(random.uniform(100, 100000), 2) for _ in range(rows)],
        "transaction_date": [
            datetime(year, month, random.randint(1, 28)).date()
            for _ in range(rows)
        ],
        "status": ["COMPLETED"] * rows,
    })
    
    # Append to Iceberg (atomic commit)
    iceberg_table.append(data)
    
    return {
        "engine": "Spark",
        "rows": len(data),
        "snapshot_id": iceberg_table.metadata.current_snapshot_id,
        "duration": (datetime.now() - start_time).total_seconds()
    }

# Load 6 months of data
print("Loading 6 months of data (simulating Spark)...")
for month in range(1, 7):
    result = load_monthly_data(iceberg_table, 2026, month, 100000)
    print(f"  Month {month:02d}: {result['rows']:,} rows, Snapshot {result['snapshot_id']}")

# ============================================================
# STEP 4: Query Iceberg Table with DuckDB
# ============================================================

print("\n--- Querying Iceberg Table with DuckDB ---")

# In production, DuckDB connects to Iceberg via REST catalog
# Here we simulate by reading from Iceberg table

def query_with_duckdb(iceberg_table: Table, query_name: str) -> dict:
    """
    Query Iceberg table using DuckDB.
    In production, use duckdb.iceberg extension.
    """
    start_time = time.time()
    
    # Read data from Iceberg
    data = iceberg_table.scan().to_arrow()
    
    # Create DuckDB connection and query
    con = duckdb.connect()
    
    # Register Arrow table
    con.register("transactions", data)
    
    # Execute query
    result = con.execute("""
        SELECT 
            branch_id,
            COUNT(*) as transaction_count,
            SUM(amount) as total_amount,
            AVG(amount) as avg_amount
        FROM transactions
        GROUP BY branch_id
        ORDER BY total_amount DESC
        LIMIT 10
    """).df()
    
    query_time = time.time() - start_time
    
    return {
        "query": query_name,
        "rows_returned": len(result),
        "query_time": query_time,
        "result": result
    }

# Execute query
query_result = query_with_duckdb(iceberg_table, "Branch Analysis")
print(f"\n{query_result['query']}:")
print(query_result['result'].to_string(index=False))
print(f"\nQuery Time: {query_result['query_time']:.3f} seconds")

# ============================================================
# STEP 5: Demonstrate Time Travel with DuckDB
# ============================================================

print("\n--- Time Travel with DuckDB ---")

# Get snapshot before latest load
snapshots = iceberg_table.metadata.snapshots
if len(snapshots) > 3:
    target_snapshot = snapshots[-3]
    target_time = datetime.fromtimestamp(target_snapshot.timestamp_ms / 1000)
    
    print(f"\nQuerying historical snapshot:")
    print(f"  Snapshot ID: {target_snapshot.snapshot_id}")
    print(f"  Timestamp: {target_time}")
    
    # In production, use time travel SQL with DuckDB
    # Here we simulate by scanning with filter
    
    historical_data = iceberg_table.scan(
        row_filter=f"transaction_date >= '2026-01-01' AND transaction_date < '2026-02-01'"
    ).to_arrow()
    
    con = duckdb.connect()
    con.register("historical", historical_data)
    
    result = con.execute("""
        SELECT 
            COUNT(*) as total_transactions,
            SUM(amount) as total_amount
        FROM historical
    """).df()
    
    print(f"\nHistorical Data (Jan 2026):")
    print(result.to_string(index=False))

# ============================================================
# STEP 6: Demonstrate Concurrent Writers
# ============================================================

print("\n--- Concurrent Writers with Iceberg ---")

print("""
PRODUCTION ARCHITECTURE:

                    Kafka
                      │
            ┌─────────┴─────────┐
            │                   │
         Flink               Spark
      (Streaming)            (Batch)
            │                   │
            └─────────┬─────────┘
                      │
                      ▼
                 ┌─────────┐
                 │ Iceberg │  ◄── Table Management
                 └────┬────┘
                      │
                   Parquet
                      │
                   S3 / MinIO
                      │
          ┌───────────┼───────────┐
          │           │           │
       DuckDB      Trino       Spark
    (Dashboards) (Ad-hoc)  (ML Training)
          │           │           │
          └───────────┼───────────┘
                      │
                      ▼
                   BI / ML

KEY POINTS:
  ✓ Iceberg manages table state
  ✓ Multiple writers supported
  ✓ DuckDB queries efficiently
  ✓ Time travel available
  ✓ ACID transactions guaranteed
""")

# ============================================================
# STEP 7: Comparison Table
# ============================================================

print("\n--- DuckDB vs Iceberg Comparison ---")

print("""
| Capability                          | DuckDB                    | Iceberg                    |
|-------------------------------------|---------------------------|----------------------------|
| SQL Engine                          | ✅ Yes                     | ❌ No                       |
| Query Parquet                       | ✅ Yes                     | ❌ No (uses it)             |
| Table Format                        | ❌ No                      | ✅ Yes                      |
| Snapshots                           | ❌ No                      | ✅ Yes                      |
| Time Travel                         | Limited                   | ✅ Full support             |
| Schema Evolution                    | Query-side                | ✅ Table-level              |
| Partition Evolution                 | ❌ No                      | ✅ Yes                      |
| ACID Transactions                   | ❌ No                      | ✅ Yes                      |
| Concurrent Writers                  | ❌ No                      | ✅ Yes                      |
| Embedded                            | ✅ Yes                     | ❌ No                       |
| Vectorized Execution                | ✅ Yes                     | ❌ No                       |
| Metadata Management                 | ❌ No                      | ✅ Yes                      |

RECOMMENDATION:
  ✓ Use DuckDB for: Querying, analytics, dashboards
  ✓ Use Iceberg for: Table management, versioning, ACID
  ✓ Use Both: DuckDB queries Iceberg tables
""")

# ============================================================
# STEP 8: Architecture Recommendations
# ============================================================

print("\n--- Architecture Recommendations ---")

print("""
WHEN TO USE DUCKDB ALONE:

  Data Size: < 100 GB
  Writers: Single/few users
  Use Case: Local analytics, notebooks
  Infrastructure: Minimal

  Example:
    DuckDB → Parquet → Local/S3

WHEN TO USE ICEBERG + DUCKDB:

  Data Size: > 1 TB
  Writers: Multiple (Spark, Flink)
  Use Case: Production analytics
  Infrastructure: Cloud data lake

  Example:
    Spark/Flink → Iceberg → Parquet → S3
    DuckDB → Iceberg → Query

WHEN TO USE ICEBERG + SPARK/TRINO:

  Data Size: > 10 PB
  Writers: Many concurrent writers
  Use Case: Enterprise data platform
  Infrastructure: Full lakehouse

  Example:
    Multiple writers → Iceberg → Parquet → S3
    Spark/Trino → Iceberg → Query
""")
```

---

## 🎯 5 Real-World Interview Questions

### Question 1: What is the difference between DuckDB and Iceberg?

**Answer:**

**DuckDB:**
- Embedded analytical SQL engine
- Reads and queries Parquet files
- Provides SQL execution
- Vectorized, parallel processing
- No table management

**Iceberg:**
- Table format for data lakes
- Manages metadata and transactions
- Provides snapshots and time travel
- Schema and partition evolution
- ACID transactions

**Key Difference:**
- DuckDB = Query Engine (how to query)
- Iceberg = Table Management (what is the table)

**Example:**
```
DuckDB: "Give me SQL to query this data"
Iceberg: "These 1000 files are the current table state"
```

---

### Question 2: Can you use DuckDB without Iceberg?

**Answer:**

**Yes, DuckDB works without Iceberg:**

```python
import duckdb
con = duckdb.connect()
result = con.execute("""
    SELECT * FROM read_parquet('s3://data/*.parquet')
""")
```

**When DuckDB alone is sufficient:**
- Data size < 100 GB
- Read-only queries
- Single/few users
- No concurrent writes needed
- No time travel required

**When you need Iceberg:**
- Data size > 1 TB
- Multiple writers
- Need ACID transactions
- Need time travel
- Schema evolution required

---

### Question 3: How do DuckDB and Iceberg work together?

**Answer:**

**Integration Architecture:**

```
DuckDB (Query Engine)
    ↓
Iceberg (Table Format)
    ↓
Parquet (File Format)
    ↓
Object Storage (S3/GCS)
```

**How it works:**
1. Iceberg manages table metadata
2. DuckDB connects to Iceberg catalog
3. DuckDB queries Iceberg tables
4. Results returned via DuckDB

**Example:**
```python
# DuckDB with Iceberg extension
con = duckdb.connect()
con.execute("""
    SELECT * FROM iceberg_scan('s3://lake/table/metadata/v1.json')
""")
```

**Benefits:**
- Iceberg provides ACID, snapshots, time travel
- DuckDB provides fast SQL execution
- Best of both worlds

---

### Question 4: When would you choose DuckDB over Iceberg?

**Answer:**

**Choose DuckDB when:**

1. **Small Data**: < 100 GB
2. **Local Analytics**: Notebooks, exploration
3. **Simple Queries**: Ad-hoc analysis
4. **No Infrastructure**: Embedded, no server
5. **Fast Prototyping**: Quick results

**Use Cases:**
- Data science exploration
- ETL transformations
- Local analytics
- Ad-hoc queries
- Prototyping

**Example:**
```python
# Perfect for DuckDB
con = duckdb.connect()
result = con.execute("""
    SELECT branch_id, SUM(amount)
    FROM read_parquet('transactions/*.parquet')
    GROUP BY branch_id
""")
```

---

### Question 5: Why not replace Iceberg with DuckDB for table management?

**Answer:**

**DuckDB cannot replace Iceberg because:**

1. **No Table Format**: DuckDB doesn't manage table state
2. **No Snapshots**: No version history
3. **No ACID**: No transaction guarantees
4. **No Concurrent Writers**: Not designed for multi-writer
5. **No Metadata Management**: No manifest/statistics management

**Iceberg provides what DuckDB cannot:**

| Feature | DuckDB | Iceberg |
|---------|--------|---------|
| Table Management | ❌ | ✅ |
| Snapshots | ❌ | ✅ |
| Time Travel | Limited | Full |
| ACID | ❌ | ✅ |
| Concurrent Writers | ❌ | ✅ |
| Schema Evolution | Query-side | Table-level |

**Example:**
```sql
-- Iceberg enables this
UPDATE transactions SET amount = 500 WHERE id = 123;
-- Atomic commit, snapshot created, metadata updated

-- DuckDB cannot do this on Parquet files directly
-- Would need to rewrite files manually
```

---

## 📝 Summary

| Aspect | DuckDB | Iceberg | Together |
|--------|--------|---------|----------|
| **Role** | Query Engine | Table Format | Complete Solution |
| **Strength** | SQL Execution | Metadata Management | Best of Both |
| **Use Case** | Analytics | Data Lake | Production Lakehouse |
| **Data Size** | Small-Medium | Any | Any |
| **Writers** | Single | Multiple | Multiple |
| **Features** | SQL, Parquet | ACID, Snapshots | Full Stack |

**Key Takeaway:**
> DuckDB and Iceberg are **complementary**, not competitors. Use DuckDB to query, Iceberg to manage. Together they provide a complete analytics solution.
