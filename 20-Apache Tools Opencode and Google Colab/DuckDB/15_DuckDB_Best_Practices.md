# DuckDB Best Practices

## Table of Contents
1. [Detailed Explanation](#1-detailed-explanation)
2. [Example](#2-example)
3. [Real-World Banking Scenario 1](#3-banking-scenario-1-production-deployment)
4. [Python Code - Scenario 1](#4-python-code---scenario-1)
5. [Real-World Banking Scenario 2](#5-banking-scenario-2-performance-tuning)
6. [Python Code - Scenario 2](#6-python-code---scenario-2)
7. [Interview Questions](#7-interview-questions)

---

## 1. Detailed Explanation

### DuckDB Best Practices

> **Following best practices ensures optimal performance, reliability, and maintainability when using DuckDB in production.**

### Query Best Practices

```
1. Use column pruning
   SELECT amount, status FROM table  -- Good
   SELECT * FROM table               -- Bad

2. Use predicate pushdown
   WHERE amount > 1000               -- Good
   (no filter)                       -- Bad

3. Use CTEs for readability
   WITH stats AS (...) SELECT ...    -- Good
   (complex nested subqueries)       -- Bad

4. Use appropriate data types
   DECIMAL(18,2) for money           -- Good
   FLOAT for money                   -- Bad

5. Avoid SELECT * in production
   SELECT specific columns           -- Good
   SELECT *                          -- Bad
```

### Memory Management

```python
# Set memory limit
con.execute("SET memory_limit='4GB'")

# Set thread count
con.execute("SET threads=4")

# Monitor memory usage
con.execute("PRAGMA database_size")
```

### Connection Management

```python
# Use context manager
with duckdb.connect() as con:
    result = con.execute("SELECT ...").fetchdf()

# Close connections when done
con.close()

# One connection per thread
def query_in_thread():
    con = duckdb.connect()  # New connection per thread
    # ... query ...
    con.close()
```

### Error Handling

```python
try:
    result = con.execute("SELECT ...").fetchdf()
except duckdb.Error as e:
    print(f"DuckDB error: {e}")
except Exception as e:
    print(f"General error: {e}")
finally:
    con.close()
```

### Performance Tuning

```
1. Set appropriate memory limits
   SET memory_limit='4GB'

2. Optimize thread usage
   SET threads=4

3. Use vectorized operations
   Avoid row-by-row processing

4. Leverage automatic optimizations
   Predicate pushdown, column pruning

5. Use Parquet for large datasets
   Better compression and performance
```

### Production Checklist

```
□ Set memory limits
□ Set thread count
□ Use parameterized queries
□ Handle errors gracefully
□ Close connections properly
□ Monitor performance
□ Use appropriate data types
□ Enable progress bars for long queries
```

---

## 2. Example

### Best Practices Demo

```python
import duckdb
import pandas as pd
import numpy as np

# Create sample data
np.random.seed(42)
num_rows = 100_000

df = pd.DataFrame({
    "id": range(1, num_rows + 1),
    "customer_id": [f"CUST{np.random.randint(1, 1000):04d}" for _ in range(num_rows)],
    "amount": np.random.uniform(1, 10000, num_rows).round(2),
    "status": np.random.choice(["COMPLETED", "PENDING", "FAILED"], num_rows),
    "date": pd.date_range("2026-01-01", periods=num_rows, freq="30s"),
})

# Best practice: Use context manager
with duckdb.connect() as con:
    # Best practice: Set memory limit
    con.execute("SET memory_limit='2GB'")
    
    # Best practice: Register DataFrame
    con.register("transactions", df)
    
    # Best practice: Use column pruning
    result = con.execute("""
        SELECT status, COUNT(*), AVG(amount)
        FROM transactions
        GROUP BY status
    """).fetchdf()
    print(result.to_string(index=False))
    
    # Best practice: Use CTEs
    result = con.execute("""
        WITH 
        customer_stats AS (
            SELECT customer_id, SUM(amount) as total
            FROM transactions
            GROUP BY customer_id
        )
        SELECT * FROM customer_stats WHERE total > 5000
        LIMIT 10
    """).fetchdf()
    print("\nHigh-value customers:")
    print(result.to_string(index=False))
```

---

## 3. Banking Scenario 1: Production Deployment

### Problem
A bank wants to deploy DuckDB in production:
- Embedded analytics service
- Handle concurrent requests
- Ensure reliability
- Monitor performance

### Why Best Practices?
- Ensure reliability
- Optimize performance
- Handle errors gracefully
- Enable monitoring

---

## 4. Python Code - Scenario 1

```python
import duckdb
import pandas as pd
import numpy as np
import threading
import time
from contextlib import contextmanager

# ============================================================
# BANKING SCENARIO: Production Deployment
# ============================================================

class DuckDBService:
    """Production-ready DuckDB service."""

    def __init__(self, memory_limit='4GB', threads=4):
        self.memory_limit = memory_limit
        self.threads = threads
        self._local = threading.local()

    def get_connection(self):
        """Get thread-local connection."""
        if not hasattr(self._local, 'con') or self._local.con is None:
            self._local.con = duckdb.connect()
            self._local.con.execute(f"SET memory_limit='{self.memory_limit}'")
            self._local.con.execute(f"SET threads={self.threads}")
        return self._local.con

    @contextmanager
    def query_context(self):
        """Context manager for queries."""
        con = self.get_connection()
        try:
            yield con
        except duckdb.Error as e:
            print(f"DuckDB error: {e}")
            raise
        except Exception as e:
            print(f"General error: {e}")
            raise

    def execute_query(self, query, params=None):
        """Execute query with error handling."""
        with self.query_context() as con:
            if params:
                result = con.execute(query, params).fetchdf()
            else:
                result = con.execute(query).fetchdf()
            return result

    def close(self):
        """Close all connections."""
        if hasattr(self._local, 'con') and self._local.con:
            self._local.con.close()
            self._local.con = None


def run_concurrent_queries(service, num_threads=5):
    """Run concurrent queries to test thread safety."""
    results = []

    def query_thread(thread_id):
        try:
            result = service.execute_query(f"""
                SELECT 
                    {thread_id} as thread_id,
                    COUNT(*) as count,
                    AVG(amount) as avg_amount
                FROM transactions
                GROUP BY status
            """)
            results.append((thread_id, "SUCCESS", len(result)))
        except Exception as e:
            results.append((thread_id, "FAILED", str(e)))

    # Generate sample data
    np.random.seed(42)
    df = pd.DataFrame({
        "id": range(1, 100001),
        "amount": np.random.uniform(1, 10000, 100000).round(2),
        "status": np.random.choice(["A", "B", "C"], 100000),
    })

    # Register data
    with service.query_context() as con:
        con.register("transactions", df)

    # Run concurrent queries
    threads = []
    for i in range(num_threads):
        t = threading.Thread(target=query_thread, args=(i,))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    # Print results
    print("\n=== Concurrent Query Results ===")
    for thread_id, status, result in results:
        print(f"Thread {thread_id}: {status} ({result})")


def demonstrate_best_practices():
    """Demonstrate DuckDB best practices."""
    # Create service
    service = DuckDBService(memory_limit='2GB', threads=4)

    # Run concurrent queries
    run_concurrent_queries(service, num_threads=5)

    service.close()


# ============================================================
# RUN THE SCENARIO
# ============================================================
if __name__ == "__main__":
    demonstrate_best_practices()
```

---

## 5. Banking Scenario 2: Performance Tuning

### Problem
A bank's DuckDB queries are slow:
- Dashboard takes 10 seconds to load
- Must load in < 2 seconds
- Large dataset (10 GB Parquet)

### Why Tuning?
- Meet performance requirements
- Improve user experience
- Reduce infrastructure costs
- Enable real-time analytics

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
# BANKING SCENARIO: Performance Tuning
# ============================================================

def generate_large_dataset(num_rows=2_000_000):
    """Generate large dataset for tuning."""
    np.random.seed(42)

    df = pd.DataFrame({
        "transaction_id": range(1, num_rows + 1),
        "account_id": [f"ACC{np.random.randint(1, 100000):06d}" for _ in range(num_rows)],
        "amount": np.random.lognormal(6, 2, num_rows).round(2),
        "status": np.random.choice(["COMPLETED", "PENDING", "FAILED"], num_rows),
        "channel": np.random.choice(["ONLINE", "MOBILE", "BRANCH", "ATM"], num_rows),
        "date": pd.date_range("2024-01-01", periods=num_rows, freq="2s"),
    })

    return df


def tune_performance(df):
    """Tune DuckDB performance."""
    # Save as Parquet
    parquet_path = os.path.join(tempfile.mkdtemp(), "transactions.parquet")
    pq.write_table(pq.Table.from_pandas(df), parquet_path, compression="zstd")

    print("=== Performance Tuning ===\n")

    # 1. Default settings
    print("1. Default settings...")
    con = duckdb.connect()
    start = time.time()
    con.execute(f"SELECT COUNT(*) FROM read_parquet('{parquet_path}')").fetchone()
    default_time = time.time() - start
    print(f"   Default time: {default_time:.3f}s")
    con.close()

    # 2. Optimized settings
    print("\n2. Optimized settings...")
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    con.execute("SET threads=4")
    con.execute("SET enable_progress_bar=true")
    
    start = time.time()
    con.execute(f"SELECT COUNT(*) FROM read_parquet('{parquet_path}')").fetchone()
    optimized_time = time.time() - start
    print(f"   Optimized time: {optimized_time:.3f}s")
    con.close()

    # 3. Query optimization
    print("\n3. Query optimization...")
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    con.execute("SET threads=4")

    queries = [
        ("Full scan", f"SELECT * FROM read_parquet('{parquet_path}')"),
        ("Column pruning", f"SELECT amount, status FROM read_parquet('{parquet_path}')"),
        ("Predicate pushdown", f"SELECT * FROM read_parquet('{parquet_path}') WHERE amount > 50000"),
        ("Both optimizations", f"SELECT amount, status FROM read_parquet('{parquet_path}') WHERE amount > 50000 AND status = 'COMPLETED'"),
    ]

    for name, query in queries:
        start = time.time()
        con.execute(query).fetchdf()
        elapsed = time.time() - start
        print(f"   {name}: {elapsed:.3f}s")

    con.close()

    # Summary
    print(f"\n=== Summary ===")
    print(f"Default: {default_time:.3f}s")
    print(f"Optimized: {optimized_time:.3f}s")
    print(f"Speedup: {default_time/optimized_time:.1f}x")


# ============================================================
# RUN THE SCENARIO
# ============================================================
if __name__ == "__main__":
    print("Generating large dataset...")
    df = generate_large_dataset(num_rows=2_000_000)

    tune_performance(df)
```

---

## 7. Interview Questions

### Q1: What are the key DuckDB best practices?

**Answer:**

1. **Use column pruning**: SELECT specific columns
2. **Use predicate pushdown**: WHERE clauses
3. **Use CTEs**: For readability
4. **Set memory limits**: SET memory_limit='4GB'
5. **Use appropriate data types**: DECIMAL for money
6. **Close connections**: Use context managers
7. **Handle errors**: Try-except blocks
8. **Use Parquet**: For large datasets

---

### Q2: How do you manage DuckDB connections in production?

**Answer:**

```python
# Use context manager
with duckdb.connect() as con:
    result = con.execute("SELECT ...").fetchdf()

# Thread-local connections
import threading
_local = threading.local()

def get_connection():
    if not hasattr(_local, 'con'):
        _local.con = duckdb.connect()
    return _local.con

# Connection pooling (for web apps)
from concurrent.futures import ThreadPoolExecutor
pool = ThreadPoolExecutor(max_workers=10)
```

---

### Q3: How do you handle errors in DuckDB?

**Answer:**

```python
import duckdb

try:
    result = con.execute("SELECT ...").fetchdf()
except duckdb.Error as e:
    print(f"DuckDB error: {e}")
except Exception as e:
    print(f"General error: {e}")
finally:
    con.close()
```

**Common errors:**
- `duckdb.Error`: SQL syntax errors
- `duckdb.DatabaseError`: Database connection issues
- `duckdb.OperationalError`: Query execution issues

---

### Q4: How do you optimize DuckDB for large datasets?

**Answer:**

1. **Use Parquet**: Better compression and performance
2. **Set memory limits**: Prevent OOM errors
3. **Use column pruning**: Read only needed columns
4. **Use predicate pushdown**: Filter at storage level
5. **Use streaming**: For very large results
6. **Partition data**: For date-based queries

```python
# Optimize settings
con.execute("SET memory_limit='8GB'")
con.execute("SET threads=8")

# Use Parquet
result = con.execute("""
    SELECT amount, status
    FROM read_parquet('large_file.parquet')
    WHERE amount > 1000
""").fetchdf()
```

---

### Q5: How do you monitor DuckDB performance?

**Answer:**

```python
# Enable progress bar
con.execute("SET enable_progress_bar=true")

# Time queries
import time
start = time.time()
result = con.execute("SELECT ...").fetchdf()
elapsed = time.time() - start

# Check database size
size = con.execute("PRAGMA database_size").fetchone()

# Profile queries
con.execute("EXPLAIN ANALYZE SELECT ...")
```

**Metrics to monitor:**
- Query execution time
- Memory usage
- Thread utilization
- Result set size
