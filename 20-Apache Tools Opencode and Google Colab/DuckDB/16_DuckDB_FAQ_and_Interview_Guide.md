# DuckDB FAQ & Interview Guide

## Table of Contents
1. [Detailed Explanation](#1-detailed-explanation)
2. [Example](#2-example)
3. [Real-World Banking Scenario 1](#3-banking-scenario-1-technical-interview)
4. [Python Code - Scenario 1](#4-python-code---scenario-1)
5. [Real-World Banking Scenario 2](#5-banking-scenario-2-system-design)
6. [Python Code - Scenario 2](#6-python-code---scenario-2)
7. [Interview Questions](#7-interview-questions)

---

## 1. Detailed Explanation

### Frequently Asked Questions

#### Q: What is DuckDB?
**A:** DuckDB is an **embedded analytical SQL database** designed for fast query performance on data stored in files (Parquet, CSV, JSON). It's like SQLite for analytics.

#### Q: How is DuckDB different from SQLite?
**A:**

| Feature | DuckDB | SQLite |
|---------|--------|--------|
| **Type** | OLAP (analytical) | OLTP (transactional) |
| **Query focus** | Aggregations, analytics | CRUD, transactions |
| **Storage** | Queries external files | Internal database |
| **Execution** | Vectorized, columnar | Row-based |

#### Q: When should I use DuckDB?
**A:**
- Ad-hoc analysis on Parquet/CSV files
- Data science workflows
- Embedded analytics
- Quick prototyping
- Small to medium datasets (< 1 TB)

#### Q: How do I install DuckDB?
**A:**
```bash
pip install duckdb
```

#### Q: How do I query Parquet files?
**A:**
```python
import duckdb
con = duckdb.connect()
result = con.execute("""
    SELECT status, SUM(amount)
    FROM read_parquet('transactions.parquet')
    GROUP BY status
""").fetchdf()
```

#### Q: What SQL features does DuckDB support?
**A:**
- Full ANSI SQL
- Window functions
- CTEs (including recursive)
- PIVOT/UNPIVOT
- Lambda functions
- Array operations
- Regular expressions

#### Q: How does DuckDB achieve high performance?
**A:**
1. Vectorized execution (processes data in batches)
2. Columnar execution (works on columns, not rows)
3. Push-based execution (data flows from producers)
4. Morsel-driven parallelism (work split into small units)
5. Cache-conscious algorithms

#### Q: What are DuckDB's limitations?
**A:**
1. Single-node (not distributed)
2. Single-writer (only one process can write)
3. No ACID on external files
4. Memory constraints for very large datasets

#### Q: How does DuckDB compare to Spark?
**A:**

| Feature | DuckDB | Spark |
|---------|--------|-------|
| **Architecture** | Embedded | Distributed |
| **Scale** | GB to TB | TB to PB |
| **Setup** | pip install | Cluster setup |
| **Best for** | Ad-hoc analysis | Large-scale ETL |

---

## 2. Example

### DuckDB Interview Ready Example

```python
import duckdb
import pandas as pd
import numpy as np
import time

# Create sample banking data
np.random.seed(42)
num_rows = 100_000

transactions = pd.DataFrame({
    "transaction_id": range(1, num_rows + 1),
    "account_id": [f"ACC{np.random.randint(1, 5000):05d}" for _ in range(num_rows)],
    "amount": np.random.lognormal(6, 2, num_rows).round(2),
    "status": np.random.choice(["COMPLETED", "PENDING", "FAILED"], num_rows),
    "channel": np.random.choice(["ONLINE", "MOBILE", "BRANCH", "ATM"], num_rows),
    "date": pd.date_range("2026-01-01", periods=num_rows, freq="30s"),
})

con = duckdb.connect()
con.register("transactions", transactions)

# Demo 1: Basic aggregation
print("=== Basic Aggregation ===")
result = con.execute("""
    SELECT status, COUNT(*), SUM(amount), AVG(amount)
    FROM transactions
    GROUP BY status
""").fetchdf()
print(result.to_string(index=False))

# Demo 2: Window function
print("\n=== Window Function ===")
result = con.execute("""
    SELECT 
        transaction_id,
        amount,
        RANK() OVER (ORDER BY amount DESC) as rank
    FROM transactions
    LIMIT 10
""").fetchdf()
print(result.to_string(index=False))

# Demo 3: CTE
print("\n=== CTE ===")
result = con.execute("""
    WITH 
    customer_stats AS (
        SELECT account_id, COUNT(*) as tx_count, SUM(amount) as total
        FROM transactions
        GROUP BY account_id
    )
    SELECT * FROM customer_stats WHERE total > 50000
    LIMIT 10
""").fetchdf()
print(result.to_string(index=False))

con.close()
```

---

## 3. Banking Scenario 1: Technical Interview

### Common Interview Topics

1. **DuckDB fundamentals**: What it is, when to use
2. **SQL features**: Window functions, CTEs, aggregates
3. **Performance optimization**: Column pruning, predicate pushdown
4. **Data integration**: Pandas, Parquet, Arrow
5. **Architecture**: Vectorized execution, push-based model

---

## 4. Python Code - Scenario 1

```python
import duckdb
import pandas as pd
import numpy as np
import time

# ============================================================
# BANKING SCENARIO: Technical Interview Prep
# ============================================================

def interview_question_1():
    """Q: Explain DuckDB's architecture."""
    print("\n=== Q1: DuckDB Architecture ===")
    print("""
    DuckDB Architecture:
    
    SQL Query
        ↓
    Parser → Optimizer → Planner
        ↓
    Vectorized Execution Engine
        ↓
    Columnar Memory Format (Arrow-based)
        ↓
    Result (DataFrame / Arrow Table)
    
    Key features:
    - Vectorized execution (batch processing)
    - Columnar execution (works on columns)
    - Push-based execution (data flows from producers)
    - Morsel-driven parallelism (small work units)
    """)


def interview_question_2():
    """Q: How does DuckDB optimize queries?"""
    print("\n=== Q2: Query Optimization ===")
    print("""
    DuckDB applies these optimizations:
    
    1. Predicate Pushdown
       WHERE amount > 1000
       → Pushed to Parquet reader
    
    2. Column Pruning
       SELECT amount, status
       → Only reads 2 columns
    
    3. Join Reordering
       Small table first for hash join
    
    4. Aggregation Pushdown
       GROUP BY status
       → Partial aggregation before join
    
    5. Common Subexpression Elimination
       WITH cte AS (...) 
       → Computed once, reused
    """)


def interview_question_3():
    """Q: When to use DuckDB vs Pandas?"""
    print("\n=== Q3: DuckDB vs Pandas ===")
    print("""
    Use DuckDB when:
    - Running SQL queries
    - Data is larger than memory
    - Complex joins and aggregations
    - Need window functions
    
    Use Pandas when:
    - Data manipulation
    - Quick prototyping
    - Integration with ML libraries
    - Data fits in memory
    
    Example:
    # DuckDB: SQL interface
    con.execute("SELECT status, SUM(amount) FROM df GROUP BY status")
    
    # Pandas: Method chains
    df.groupby("status")["amount"].sum()
    """)


def interview_question_4():
    """Q: Explain window functions."""
    print("\n=== Q4: Window Functions ===")
    print("""
    Window functions perform calculations across rows
    related to the current row without collapsing results.
    
    Types:
    1. Ranking: RANK(), DENSE_RANK(), ROW_NUMBER()
    2. Aggregate: SUM(), AVG(), COUNT()
    3. Value: LAG(), LEAD(), FIRST_VALUE()
    
    Example:
    SELECT 
        id,
        amount,
        RANK() OVER (ORDER BY amount DESC) as rank,
        SUM(amount) OVER (ORDER BY date) as running_total,
        LAG(amount, 1) OVER (ORDER BY date) as prev_amount
    FROM transactions
    """)


def interview_question_5():
    """Q: How to optimize DuckDB performance?"""
    print("\n=== Q5: Performance Optimization ===")
    print("""
    Top 5 optimizations:
    
    1. Use Parquet files
       Better compression and performance
    
    2. Set memory limits
       SET memory_limit='4GB'
    
    3. Use column pruning
       SELECT amount, status FROM ...
    
    4. Use predicate pushdown
       WHERE amount > 1000
    
    5. Use CTEs for complex queries
       WITH stats AS (...) SELECT ...
    """)


def run_interview_prep():
    """Run all interview questions."""
    interview_question_1()
    interview_question_2()
    interview_question_3()
    interview_question_4()
    interview_question_5()


# ============================================================
# RUN THE SCENARIO
# ============================================================
if __name__ == "__main__":
    run_interview_prep()
```

---

## 5. Banking Scenario 2: System Design

### Problem
Design an analytics platform for a bank:
- 100 GB daily transaction data
- Real-time dashboard (5 second refresh)
- Ad-hoc analysis by analysts
- Cost-effective storage

### Architecture Design

```
┌─────────────────────────────────────────────────────────┐
│                    Data Sources                         │
├─────────────────────────────────────────────────────────┤
│ Core Banking (Oracle) ──► ETL ──► Parquet (S3)         │
│ Card Processing (MySQL) ──► ETL ──► Parquet (S3)       │
│ Online Banking ──► Kafka ──► Spark ──► Parquet (S3)     │
└─────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────┐
│              Storage Layer (S3)                          │
│  Parquet files (256MB - 1GB each)                       │
│  Partitioned by date                                    │
│  Zstd compression                                       │
└─────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────┐
│              Query Layer                                 │
│  DuckDB (embedded, ad-hoc)                              │
│  Spark (batch processing)                               │
│  Trino (interactive SQL)                                │
└─────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────┐
│              Application Layer                          │
│  Dashboard (real-time)                                  │
│  Reports (batch)                                        │
│  Ad-hoc analysis (analysts)                             │
└─────────────────────────────────────────────────────────┘
```

---

## 6. Python Code - Scenario 2

```python
import duckdb
import pandas as pd
import numpy as np
import time
import pyarrow.parquet as pq
import os
import tempfile

# ============================================================
# BANKING SCENARIO: System Design
# ============================================================

def design_analytics_platform():
    """Present analytics platform design for interview."""
    print("""
    ╔══════════════════════════════════════════════════════════════╗
    ║         BANKING ANALYTICS PLATFORM DESIGN                    ║
    ╠══════════════════════════════════════════════════════════════╣
    ║                                                              ║
    ║  Requirements:                                               ║
    ║  - 100 GB daily transaction data                             ║
    ║  - Real-time dashboard (5 second refresh)                    ║
    ║  - Ad-hoc analysis by analysts                               ║
    ║  - Cost-effective storage                                    ║
    ║                                                              ║
    ║  Design Decisions:                                           ║
    ║  1. Format: Parquet (columnar, compressed)                   ║
    ║  2. Storage: S3 (object storage, lifecycle policies)         ║
    ║  3. Query: DuckDB (embedded, fast)                           ║
    ║  4. Partitioning: By date (low cardinality)                  ║
    ║  5. File size: 256MB - 1GB (optimal parallelism)            ║
    ║                                                              ║
    ║  Architecture:                                               ║
    ║  ETL → Parquet (S3) → DuckDB → Dashboard                    ║
    ║                                                              ║
    ║  Storage Calculation:                                        ║
    ║  - 100 GB/day × 365 days = 36.5 TB/year                     ║
    ║  - Parquet (5x compression) = 7.3 TB/year                   ║
    ║  - S3 Standard: ~$168/month                                  ║
    ║                                                              ║
    ╚══════════════════════════════════════════════════════════════╝
    """)


def calculate_costs():
    """Calculate platform costs for interview."""
    print("\n=== Cost Calculation ===")

    # Parameters
    daily_gb = 100
    compression_ratio = 5
    retention_years = 3

    # Calculations
    daily_compressed_gb = daily_gb / compression_ratio
    annual_tb = (daily_compressed_gb * 365) / 1024
    total_tb = annual_tb * retention_years

    print(f"Daily raw: {daily_gb} GB")
    print(f"Daily compressed: {daily_compressed_gb:.1f} GB (5x compression)")
    print(f"Annual storage: {annual_tb:.1f} TB")
    print(f"Total storage ({retention_years} years): {total_tb:.1f} TB")

    # S3 costs
    s3_standard_per_tb = 23.00  # $/month
    monthly_cost = total_tb * s3_standard_per_tb
    annual_cost = monthly_cost * 12

    print(f"\nS3 costs:")
    print(f"  Monthly: ${monthly_cost:,.2f}")
    print(f"  Annual: ${annual_cost:,.2f}")
    print(f"  Per GB: ${monthly_cost / (total_tb * 1024):.4f}")


def show_code_example():
    """Show code example for interview."""
    print("\n=== Code Example ===")
    print("""
    # DuckDB query on Parquet
    import duckdb
    
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    
    result = con.execute(\"\"\"
        SELECT 
            status,
            COUNT(*) as count,
            SUM(amount) as total
        FROM read_parquet('s3://bucket/transactions/*.parquet')
        WHERE date >= '2026-08-01'
        GROUP BY status
    \"\"\").fetchdf()
    
    # Pandas integration
    import pandas as pd
    df = con.execute("SELECT * FROM result").fetchdf()
    
    # Visualization
    df.plot(kind='bar', x='status', y='total')
    """)


# ============================================================
# RUN THE SCENARIO
# ============================================================
if __name__ == "__main__":
    design_analytics_platform()
    calculate_costs()
    show_code_example()
```

---

## 7. Interview Questions

### Q1: Explain DuckDB's vectorized execution.

**Answer:**

DuckDB processes data in **vectors** (batches of ~2048 values) instead of row-by-row:

```
Traditional (row-by-row):
Row 1: process(amount=100)
Row 2: process(amount=250)
Row 3: process(amount=500)

DuckDB (vectorized):
Vector 1: process([100, 250, 500, 75, 1000])
Vector 2: process([200, 150, 300, 800, 50])
```

**Benefits:**
1. CPU cache efficiency
2. SIMD operations (multiple values per instruction)
3. Reduced function call overhead
4. Better pipelining

---

### Q2: What is predicate pushdown and why is it important?

**Answer:**

Predicate pushdown moves filter conditions to the storage layer:

```sql
WHERE amount > 1000
```

**Without pushdown:** Read all 100 GB → Filter → Return 1 GB
**With pushdown:** Check statistics → Read 1 GB → Return 1 GB

**Speedup: 100x**

**How it works:**
1. Parquet stores min/max statistics per row group
2. DuckDB checks statistics first
3. Skips row groups where filter cannot match

---

### Q3: Design a data pipeline using DuckDB.

**Answer:**

```
Architecture:
  Data Sources (CSV, API, Database)
       ↓
  Python (Pandas)
       ↓
  DuckDB (SQL transforms)
       ↓
  Parquet (Output)
       ↓
  Analytics (Dashboard, Reports)

Key components:
1. Extraction: Pandas reads from sources
2. Transformation: DuckDB SQL transforms
3. Loading: Write to Parquet
4. Validation: DuckDB queries for quality checks

Example:
  con.execute(\"\"\"
      CREATE TABLE transformed AS
      SELECT 
          customer_id,
          SUM(amount) as total
      FROM read_parquet('input/*.parquet')
      GROUP BY 1
  \"\"\")
  con.execute(\"\"\"
      COPY (SELECT * FROM transformed) 
      TO 'output.parquet' (FORMAT PARQUET)
  \"\"\")
```

---

### Q4: How do you optimize DuckDB queries?

**Answer:**

1. **Column pruning**: SELECT specific columns
```sql
SELECT amount, status FROM table  -- Not SELECT *
```

2. **Predicate pushdown**: WHERE clauses
```sql
WHERE amount > 1000  -- Pushed to Parquet reader
```

3. **Use CTEs**: For readability and optimization
```sql
WITH stats AS (...) SELECT * FROM stats
```

4. **Set memory limits**: Prevent OOM
```sql
SET memory_limit='4GB'
```

5. **Use Parquet**: Better compression and performance

---

### Q5: Compare DuckDB with other databases.

**Answer:**

| Feature | DuckDB | PostgreSQL | SQLite | Spark |
|---------|--------|------------|--------|-------|
| **Type** | OLAP | OLTP | OLTP | OLAP |
| **Deployment** | Embedded | Server | Embedded | Distributed |
| **Scale** | GB-TB | GB-TB | GB | TB-PB |
| **Best for** | Analytics | Web apps | Mobile | ETL |

**Use DuckDB for:**
- Ad-hoc analysis
- Data science workflows
- Embedded analytics
- Small to medium datasets

**Use PostgreSQL for:**
- Transactional workloads
- Multi-user access
- Complex schemas

**Use Spark for:**
- Large-scale ETL
- Distributed processing
- Cluster computing
