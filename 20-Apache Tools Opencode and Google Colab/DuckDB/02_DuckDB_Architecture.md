# DuckDB Architecture

## Table of Contents
1. [Detailed Explanation](#1-detailed-explanation)
2. [Example](#2-example)
3. [Real-World Banking Scenario 1](#3-banking-scenario-1-query-optimization)
4. [Python Code - Scenario 1](#4-python-code---scenario-1)
5. [Real-World Banking Scenario 2](#5-banking-scenario-2-concurrent-analytics)
6. [Python Code - Scenario 2](#6-python-code---scenario-2)
7. [Interview Questions](#7-interview-questions)

---

## 1. Detailed Explanation

### DuckDB Architecture Overview

> **DuckDB uses a modern analytical database architecture with vectorized execution, push-based processing, and columnar memory format for high-performance queries.**

### The Query Processing Pipeline

```
SQL Query
    ↓
┌─────────────────────────┐
│       Parser            │  Parse SQL into AST
└─────────────────────────┘
    ↓
┌─────────────────────────┐
│       Binder            │  Resolve tables, columns, types
└─────────────────────────┘
    ↓
┌─────────────────────────┐
│       Planner           │  Create logical plan
└─────────────────────────┘
    ↓
┌─────────────────────────┐
│       Optimizer         │  Apply optimizations
│  - Predicate pushdown   │
│  - Column pruning       │
│  - Join reordering      │
│  - Aggregation pushdown │
└─────────────────────────┘
    ↓
┌─────────────────────────┐
│       Planner           │  Create physical plan
└─────────────────────────┘
    ↓
┌─────────────────────────┐
│   Execution Engine      │  Vectorized, columnar
│  - Morsel-driven        │
│  - Push-based           │
│  - Parallel             │
└─────────────────────────┘
    ↓
Result (Arrow / DataFrame)
```

### Vectorized Execution

Traditional databases process data row-by-row:
```
Row 1: process(amount=100)
Row 2: process(amount=250)
Row 3: process(amount=500)
...
```

DuckDB processes data in **vectors** (batches):
```
Vector 1: process([100, 250, 500, 75, 1000])
Vector 2: process([200, 150, 300, 800, 50])
...
```

**Benefits:**
1. **CPU cache efficiency**: Sequential access patterns
2. **SIMD operations**: Process multiple values per instruction
3. **Reduced function call overhead**: One call per vector, not per row

### Push-Based Execution

Traditional databases use **pull-based** execution:
```
Iterator pulls data from child operator
  Iterator pulls from its child
    Iterator pulls from scan
```

DuckDB uses **push-based** execution:
```
Scan pushes data to filter
  Filter pushes to aggregation
    Aggregation pushes to result
```

**Benefits:**
1. **Better pipelining**: Data flows continuously
2. **Less overhead**: No iterator management
3. **Better parallelism**: Multiple producers can push simultaneously

### Morsel-Driven Parallelism

DuckDB splits work into **morsels** (small units of work):

```
Query: SELECT status, SUM(amount) FROM transactions GROUP BY status

Morsel 1: Read rows 1-10000 → Partial aggregation
Morsel 2: Read rows 10001-20000 → Partial aggregation
Morsel 3: Read rows 20001-30000 → Partial aggregation
Morsel 4: Read rows 30001-40000 → Partial aggregation

Final: Merge partial aggregations → Result
```

**Benefits:**
1. **Load balancing**: Work distributed evenly
2. **Cache efficiency**: Small working sets
3. **Fault tolerance**: One morsel failure doesn't kill query

### Columnar Memory Format

DuckDB uses Arrow-compatible columnar memory:

```
Row-based (Pandas):
Row 1: [1, "Alice", 100.0]
Row 2: [2, "Bob", 250.0]
Row 3: [3, "Charlie", 500.0]

Columnar (DuckDB):
id:    [1, 2, 3]
name:  ["Alice", "Bob", "Charlie"]
amount: [100.0, 250.0, 500.0]
```

**Benefits:**
1. **Compression**: Similar values together
2. **Vectorization**: Process entire columns at once
3. **Zero-copy**: Arrow integration without conversion

### Query Optimization Techniques

```
1. Predicate Pushdown
   WHERE amount > 1000
   → Push filter to scan (read less data)

2. Column Pruning
   SELECT amount, status
   → Read only 2 columns, skip others

3. Join Reordering
   A JOIN B JOIN C
   → Reorder based on selectivity

4. Aggregation Pushdown
   GROUP BY status
   → Partial aggregation before join

5. Late Materialization
   SELECT * WHERE amount > 1000
   → Delay creating full rows until final step
```

---

## 2. Example

### DuckDB Internal Performance Demo

```python
import duckdb
import pandas as pd
import numpy as np
import time

# Create large dataset
np.random.seed(42)
num_rows = 10_000_000

df = pd.DataFrame({
    "id": range(num_rows),
    "status": np.random.choice(["COMPLETED", "PENDING", "FAILED"], num_rows),
    "amount": np.random.uniform(1.0, 100000.0, num_rows),
    "category": np.random.choice(["A", "B", "C", "D", "E"], num_rows),
})

# Create DuckDB connection
con = duckdb.connect()

# Register DataFrame
con.register("transactions", df)

# Benchmark queries
queries = [
    ("Simple aggregation", "SELECT status, COUNT(*), SUM(amount) FROM transactions GROUP BY status"),
    ("Filtered aggregation", "SELECT status, AVG(amount) FROM transactions WHERE amount > 10000 GROUP BY status"),
    ("Window function", "SELECT id, amount, RANK() OVER (PARTITION BY status ORDER BY amount DESC) as rank FROM transactions LIMIT 1000"),
    ("Complex join", """
        SELECT a.status, a.total, b.count
        FROM (SELECT status, SUM(amount) as total FROM transactions GROUP BY status) a
        JOIN (SELECT status, COUNT(*) as count FROM transactions GROUP BY status) b
        ON a.status = b.status
    """),
]

for name, query in queries:
    start = time.time()
    result = con.execute(query).fetchdf()
    elapsed = time.time() - start
    print(f"{name}: {elapsed:.3f}s")

con.close()
```

---

## 3. Banking Scenario 1: Query Optimization

### Problem
A bank's analytics queries are slow:
- Dashboard takes 30 seconds to load
- Monthly reports take 2 hours
- Ad-hoc queries timeout

Root causes:
- Poor query structure
- No index optimization
- Large dataset scans

### Why DuckDB Architecture Matters?
- Vectorized execution: 10-100x faster than row-by-row
- Predicate pushdown: Skip irrelevant data
- Column pruning: Read only needed columns
- Morsel parallelism: Use all CPU cores

### Architecture
```
Slow Query
    ↓
DuckDB Optimizer
    ↓
┌─────────────────────────┐
│ 1. Predicate pushdown   │  → Read 1% of data
│ 2. Column pruning       │  → Read 2 columns instead of 20
│ 3. Join reordering      │  → Smaller intermediate results
│ 4. Aggregation pushdown │  → Partial aggregation early
└─────────────────────────┘
    ↓
Fast Result (2 seconds instead of 30)
```

---

## 4. Python Code - Scenario 1

```python
import duckdb
import pandas as pd
import numpy as np
import time
import os
import tempfile
import pyarrow.parquet as pq

# ============================================================
# BANKING SCENARIO: Query Optimization
# ============================================================

def generate_large_dataset(num_rows=5_000_000):
    """Generate large transaction dataset."""
    np.random.seed(42)

    df = pd.DataFrame({
        "transaction_id": range(1, num_rows + 1),
        "account_id": [f"ACC{np.random.randint(100000, 999999)}" for _ in range(num_rows)],
        "customer_id": [f"CUST{np.random.randint(1, 100000):06d}" for _ in range(num_rows)],
        "branch_id": [f"BR{np.random.randint(1, 500):03d}" for _ in range(num_rows)],
        "amount": np.random.lognormal(6, 2, num_rows).round(2),
        "status": np.random.choice(["COMPLETED", "PENDING", "FAILED"], num_rows),
        "channel": np.random.choice(["ONLINE", "MOBILE", "BRANCH", "ATM", "POS"], num_rows),
        "date": pd.date_range("2024-01-01", periods=num_rows, freq="2s"),
    })

    return df


def benchmark_queries(df):
    """Benchmark different query patterns."""
    con = duckdb.connect()
    con.register("transactions", df)

    queries = [
        # 1. Full scan (slow)
        ("Full scan", "SELECT COUNT(*) FROM transactions"),
        
        # 2. With filter (faster)
        ("Filtered", "SELECT COUNT(*) FROM transactions WHERE status = 'COMPLETED'"),
        
        # 3. Column pruning (faster)
        ("Column pruning", "SELECT COUNT(*) FROM transactions WHERE status = 'COMPLETED' AND amount > 10000"),
        
        # 4. Aggregation (optimized)
        ("Aggregation", """
            SELECT status, COUNT(*), SUM(amount), AVG(amount)
            FROM transactions
            WHERE date >= '2026-01-01'
            GROUP BY status
        """),
        
        # 5. Window function
        ("Window function", """
            SELECT customer_id, amount,
                   RANK() OVER (PARTITION BY status ORDER BY amount DESC) as rank
            FROM transactions
            WHERE status = 'COMPLETED'
            LIMIT 1000
        """),
        
        # 6. Complex analytics
        ("Complex analytics", """
            WITH monthly AS (
                SELECT 
                    DATE_TRUNC('month', date) as month,
                    status,
                    COUNT(*) as tx_count,
                    SUM(amount) as total_amount
                FROM transactions
                GROUP BY 1, 2
            )
            SELECT 
                month,
                status,
                tx_count,
                total_amount,
                total_amount / SUM(total_amount) OVER (PARTITION BY month) * 100 as pct_of_month
            FROM monthly
            ORDER BY month, status
        """),
    ]

    print(f"\n=== Query Benchmark ({len(df):,} rows) ===")
    print(f"{'Query':<25} {'Time (s)':<12} {'Rows Processed':<15}")
    print("-" * 52)

    for name, query in queries:
        start = time.time()
        result = con.execute(query).fetchdf()
        elapsed = time.time() - start
        print(f"{name:<25} {elapsed:<12.3f} {len(df):<15,}")

    con.close()


def save_and_query_parquet(df, base_path):
    """Save to Parquet and query with DuckDB."""
    # Save as Parquet
    pq.write_table(pq.Table.from_pandas(df), os.path.join(base_path, "transactions.parquet"), compression="zstd")

    # Query Parquet directly
    con = duckdb.connect()

    start = time.time()
    result = con.execute(f"""
        SELECT 
            status,
            COUNT(*) as tx_count,
            SUM(amount) as total_amount
        FROM read_parquet('{base_path}/transactions.parquet')
        WHERE date >= '2026-01-01'
        GROUP BY status
    """).fetchdf()
    elapsed = time.time() - start

    print(f"\n=== Parquet Query Performance ===")
    print(f"Query time: {elapsed:.3f}s")
    print(result.to_string(index=False))

    con.close()


# ============================================================
# RUN THE SCENARIO
# ============================================================
if __name__ == "__main__":
    # Generate dataset
    print("Generating large dataset...")
    df = generate_large_dataset(num_rows=5_000_000)

    # Benchmark queries
    benchmark_queries(df)

    # Parquet query
    base_path = os.path.join(tempfile.gettempdir(), "duckdb_optimization")
    os.makedirs(base_path, exist_ok=True)
    save_and_query_parquet(df, base_path)
```

---

## 5. Banking Scenario 2: Concurrent Analytics

### Problem
A bank needs multiple analysts to query the same data simultaneously:
- 10 analysts running queries concurrently
- Each query must complete in < 30 seconds
- No query should block others

### Why DuckDB Architecture Matters?
- Single-writer, multi-reader: Multiple concurrent reads
- No lock contention: Read-only queries don't block
- Memory efficient: Each query gets its own memory space
- Vectorized execution: Fast even with concurrent loads

### Architecture
```
Analyst 1 ─┐
Analyst 2 ─┤
Analyst 3 ─┼──► DuckDB (multi-reader)
Analyst 4 ─┤         |
Analyst 5 ─┘         v
                 Parquet Files
                 (read-only)
```

---

## 6. Python Code - Scenario 2

```python
import duckdb
import pandas as pd
import numpy as np
import time
import os
import tempfile
import pyarrow.parquet as pq
from concurrent.futures import ThreadPoolExecutor, as_completed

# ============================================================
# BANKING SCENARIO: Concurrent Analytics
# ============================================================

def generate_analytics_data(num_rows=2_000_000):
    """Generate transaction data for concurrent queries."""
    np.random.seed(42)

    df = pd.DataFrame({
        "transaction_id": range(1, num_rows + 1),
        "account_id": [f"ACC{np.random.randint(100000, 999999)}" for _ in range(num_rows)],
        "amount": np.random.lognormal(6, 2, num_rows).round(2),
        "status": np.random.choice(["COMPLETED", "PENDING", "FAILED"], num_rows),
        "channel": np.random.choice(["ONLINE", "MOBILE", "BRANCH", "ATM"], num_rows),
        "date": pd.date_range("2026-01-01", periods=num_rows, freq="5s"),
    })

    return df


def run_concurrent_queries(base_path, num_analysts=5):
    """Simulate concurrent analyst queries."""
    queries = [
        # Query 1: Daily summary
        """
        SELECT DATE_TRUNC('day', date) as day, COUNT(*), SUM(amount)
        FROM read_parquet('{path}')
        GROUP BY 1 ORDER BY 1
        """,
        
        # Query 2: Status distribution
        """
        SELECT status, COUNT(*), AVG(amount)
        FROM read_parquet('{path}')
        GROUP BY 1
        """,
        
        # Query 3: Channel analysis
        """
        SELECT channel, COUNT(*), SUM(amount)
        FROM read_parquet('{path}')
        WHERE status = 'COMPLETED'
        GROUP BY 1
        """,
        
        # Query 4: Top accounts
        """
        SELECT account_id, COUNT(*), SUM(amount)
        FROM read_parquet('{path}')
        GROUP BY 1
        ORDER BY 3 DESC
        LIMIT 100
        """,
        
        # Query 5: Time series
        """
        SELECT DATE_TRUNC('hour', date) as hour, COUNT(*)
        FROM read_parquet('{path}')
        GROUP BY 1
        ORDER BY 1
        """,
    ]

    def execute_query(args):
        query_id, query_template, path = args
        query = query_template.format(path=path)
        
        start = time.time()
        con = duckdb.connect()
        result = con.execute(query).fetchdf()
        elapsed = time.time() - start
        con.close()
        
        return query_id, elapsed, len(result)

    # Prepare query arguments
    parquet_path = os.path.join(base_path, "transactions.parquet")
    query_args = [(i, q, parquet_path) for i, q in enumerate(queries)]

    # Run queries concurrently
    print(f"\n=== Concurrent Query Execution ({num_analysts} analysts) ===")
    
    with ThreadPoolExecutor(max_workers=num_analysts) as executor:
        futures = [executor.submit(execute_query, args) for args in query_args * (num_analysts // len(queries) + 1)]
        
        results = []
        for future in as_completed(futures):
            query_id, elapsed, rows = future.result()
            results.append((query_id, elapsed, rows))

    # Print results
    print(f"{'Query':<10} {'Time (s)':<12} {'Rows':<10}")
    print("-" * 32)
    for query_id, elapsed, rows in sorted(results):
        print(f"Q{query_id:<9} {elapsed:<12.3f} {rows:<10}")

    total_time = max(r[1] for r in results)
    print(f"\nTotal wall time: {total_time:.3f}s")


def benchmark_concurrent_vs_sequential(base_path):
    """Compare concurrent vs sequential query execution."""
    parquet_path = os.path.join(base_path, "transactions.parquet")
    
    query = f"""
        SELECT status, COUNT(*), AVG(amount)
        FROM read_parquet('{parquet_path}')
        GROUP BY 1
    """
    
    # Sequential execution
    start = time.time()
    for _ in range(5):
        con = duckdb.connect()
        con.execute(query).fetchdf()
        con.close()
    sequential_time = time.time() - start
    
    # Concurrent execution
    def run_query(_):
        con = duckdb.connect()
        con.execute(query).fetchdf()
        con.close()
    
    start = time.time()
    with ThreadPoolExecutor(max_workers=5) as executor:
        list(executor.map(run_query, range(5)))
    concurrent_time = time.time() - start
    
    print(f"\n=== Sequential vs Concurrent ===")
    print(f"Sequential (5 queries): {sequential_time:.3f}s")
    print(f"Concurrent (5 queries): {concurrent_time:.3f}s")
    print(f"Speedup: {sequential_time/concurrent_time:.1f}x")


# ============================================================
# RUN THE SCENARIO
# ============================================================
if __name__ == "__main__":
    base_path = os.path.join(tempfile.gettempdir(), "concurrent_analytics")
    os.makedirs(base_path, exist_ok=True)

    # Generate and save data
    print("Generating analytics data...")
    df = generate_analytics_data(num_rows=2_000_000)
    pq.write_table(pq.Table.from_pandas(df), os.path.join(base_path, "transactions.parquet"), compression="zstd")

    # Run concurrent queries
    run_concurrent_queries(base_path, num_analysts=5)

    # Benchmark
    benchmark_concurrent_vs_sequential(base_path)
```

---

## 7. Interview Questions

### Q1: Explain DuckDB's vectorized execution model.

**Answer:**

**Traditional row-by-row execution:**
```
For each row:
    Process row 1
    Process row 2
    Process row 3
    ...
```

**DuckDB's vectorized execution:**
```
For each vector (batch of ~2048 values):
    Process vector 1: [row1, row2, ..., row2048]
    Process vector 2: [row2049, row2050, ..., row4096]
    ...
```

**Benefits:**
1. **CPU cache efficiency**: Sequential access patterns
2. **SIMD operations**: Process multiple values per instruction
3. **Reduced function call overhead**: One call per vector
4. **Better pipelining**: Overlap I/O and computation

**Example:**
```sql
SELECT SUM(amount) FROM transactions
-- Processes 2048 amounts per vector operation
-- ~20x faster than row-by-row
```

---

### Q2: What is push-based execution and why is it better?

**Answer:**

**Pull-based (traditional):**
```
Result pulls from Aggregation
  Aggregation pulls from Filter
    Filter pulls from Scan
```

**Push-based (DuckDB):**
```
Scan pushes to Filter
  Filter pushes to Aggregation
    Aggregation pushes to Result
```

**Benefits:**
1. **Better pipelining**: Data flows continuously
2. **Less overhead**: No iterator management
3. **Better parallelism**: Multiple producers can push simultaneously
4. **Cache efficiency**: Data processed as it arrives

---

### Q3: How does DuckDB handle memory management?

**Answer:**

**Memory management strategies:**

1. **Buffer manager**: Manages data pages in memory
2. **Memory limit**: Configurable per-query memory limit
3. **Spilling**: Results to disk when memory exceeded
4. **Morsel-based**: Small working sets per thread

**Configuration:**
```python
import duckdb
con = duckdb.connect()
con.execute("SET memory_limit='4GB'")
con.execute("SET threads=4")
```

**Best practices:**
- Set memory limit appropriately
- Use streaming for large results
- Monitor memory usage in production

---

### Q4: What query optimizations does DuckDB apply automatically?

**Answer:**

1. **Predicate pushdown**: Filters pushed to scan
```sql
WHERE amount > 1000  -- Pushed to Parquet reader
```

2. **Column pruning**: Only read needed columns
```sql
SELECT amount, status  -- Only reads 2 columns
```

3. **Join reordering**: Smallest table first
```sql
A JOIN B JOIN C  -- Reordered by size
```

4. **Aggregation pushdown**: Partial aggregation early
```sql
GROUP BY status  -- Aggregated before join
```

5. **Common subexpression elimination**: Avoid duplicate computation
```sql
WITH cte AS (...)  -- Computed once, reused
```

---

### Q5: How does DuckDB differ from PostgreSQL for analytics?

**Answer:**

| Feature | DuckDB | PostgreSQL |
|---------|--------|------------|
| **Type** | OLAP | OLTP |
| **Execution** | Vectorized, columnar | Row-based |
| **Storage** | Queries external files | Internal storage |
| **Setup** | Embedded, zero-config | Server-based |
| **Concurrency** | Single-writer, multi-reader | Full ACID |
| **Best for** | Analytics, reporting | Web apps, transactions |

**When to use DuckDB:**
- Ad-hoc analysis on files
- Embedded analytics
- Data science workflows
- Quick prototyping

**When to use PostgreSQL:**
- Transactional workloads
- Multi-user concurrent writes
- ACID requirements
- Complex schemas with constraints
