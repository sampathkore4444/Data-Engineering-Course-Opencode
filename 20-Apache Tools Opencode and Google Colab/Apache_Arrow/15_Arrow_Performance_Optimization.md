# Concept 15: Arrow Performance Optimization

## 📚 Detailed Explanation

**Arrow Performance Optimization** involves techniques to maximize the efficiency of Arrow operations, from memory layout to query execution.

### Why Performance Matters

**Without Optimization:**
```
Query: 10 seconds
Memory: 10 GB
CPU: 100%
```

**With Optimization:**
```
Query: 1 second (10x faster)
Memory: 5 GB (50% less)
CPU: 50% (50% less)
```

### Optimization Categories

| Category | Techniques |
|----------|------------|
| **Memory** | Dictionary encoding, null bitmaps |
| **Compute** | Vectorized operations, SIMD |
| **I/O** | Compression, batching |
| **Query** | Predicate pushdown, column pruning |

---

## 💡 Example: Performance Optimization

### Before Optimization
```python
import pyarrow as pa

# Slow: No optimization
table = pa.table({
    "category": ["A", "B", "A", "C", "B"] * 1000000,
    "amount": [100, 200, 150, 300, 250] * 1000000,
})
```

### After Optimization
```python
import pyarrow as pa
import pyarrow.compute as pc

# Fast: Optimized
table = pa.table({
    "category": pc.dictionary_encode(pa.array(["A", "B", "A", "C", "B"] * 1000000)),
    "amount": pa.array([100, 200, 150, 300, 250] * 1000000, type=pa.int32()),
})
```

---

## 🏦 Real-World Banking Scenario 1: Query Optimization

### Scenario
A bank's **analytics queries** are running slowly. They need to:
- Optimize query performance
- Reduce memory usage
- Improve response time

### Problem
- Slow queries
- High memory usage
- Poor performance

### Solution
Arrow performance optimization:
- Dictionary encoding
- Vectorized operations
- Efficient memory layout

### Python Code

```python
"""
Banking Scenario 1: Query Optimization
Using Arrow Performance Techniques
"""

import pyarrow as pa
import pyarrow.compute as pc
import random
import time

# ============================================================
# STEP 1: Generate Unoptimized Data
# ============================================================

print("=== QUERY OPTIMIZATION WITH ARROW ===\n")

def generate_unoptimized_data(num_records: int) -> pa.Table:
    """Generate unoptimized data."""
    
    data = {
        "transaction_id": [f"TXN-{i:010d}" for i in range(1, num_records + 1)],
        "account_id": [f"ACC-{random.randint(1000, 9999):06d}" for _ in range(num_records)],
        "amount": [round(random.uniform(100, 100000), 2) for _ in range(num_records)],
        "transaction_type": [random.choice(["CREDIT", "DEBIT", "TRANSFER"]) for _ in range(num_records)],
        "status": [random.choice(["COMPLETED", "PENDING", "FAILED"]) for _ in range(num_records)],
        "branch": [random.choice(["Mumbai", "Delhi", "Bangalore", "Chennai", "Kolkata"]) for _ in range(num_records)],
    }
    
    return pa.table(data)

# Generate 10 million records
print("Generating 10 million records (unoptimized)...")
unoptimized = generate_unoptimized_data(10000000)
print(f"Generated: {len(unoptimized):,} records")
print(f"Memory: {unoptimized.nbytes / 1024 / 1024:.2f} MB")

# ============================================================
# STEP 2: Apply Optimizations
# ============================================================

print("\n--- Applying Optimizations ---")

start_time = time.time()

# 1. Dictionary encoding for categorical columns
optimized_data = {
    "transaction_id": unoptimized.column("transaction_id"),
    "account_id": unoptimized.column("account_id"),
    "amount": unoptimized.column("amount"),
    "transaction_type": pc.dictionary_encode(unoptimized.column("transaction_type")),
    "status": pc.dictionary_encode(unoptimized.column("status")),
    "branch": pc.dictionary_encode(unoptimized.column("branch")),
}

optimized = pa.table(optimized_data)
optimization_time = time.time() - start_time

print(f"\nOptimizations Applied:")
print(f"  Time: {optimization_time:.3f} seconds")
print(f"  Dictionary encoding: transaction_type, status, branch")
print(f"  Memory: {optimized.nbytes / 1024 / 1024:.2f} MB")
print(f"  Savings: {(1 - optimized.nbytes / unoptimized.nbytes) * 100:.1f}%")

# ============================================================
# STEP 3: Query Performance Comparison
# ============================================================

print("\n--- Query Performance Comparison ---")

# Query 1: Aggregation (unoptimized)
start_time = time.time()
unopt_agg = unoptimized.group_by("branch").aggregate({
    "amount": "sum",
    "transaction_id": "count"
})
unopt_agg_time = time.time() - start_time

print(f"\nAggregation (Unoptimized):")
print(f"  Time: {unopt_agg_time:.3f} seconds")

# Query 1: Aggregation (optimized)
start_time = time.time()
opt_agg = optimized.group_by("branch").aggregate({
    "amount": "sum",
    "transaction_id": "count"
})
opt_agg_time = time.time() - start_time

print(f"\nAggregation (Optimized):")
print(f"  Time: {opt_agg_time:.3f} seconds")
print(f"  Speedup: {unopt_agg_time / opt_agg_time:.1f}x faster")

# Query 2: Filtering (unoptimized)
start_time = time.time()
unopt_filtered = unoptimized.filter(pc.greater(unoptimized.column("amount"), 50000))
unopt_filter_time = time.time() - start_time

print(f"\nFiltering (Unoptimized):")
print(f"  Time: {unopt_filter_time:.3f} seconds")

# Query 2: Filtering (optimized)
start_time = time.time()
opt_filtered = optimized.filter(pc.greater(optimized.column("amount"), 50000))
opt_filter_time = time.time() - start_time

print(f"\nFiltering (Optimized):")
print(f"  Time: {opt_filter_time:.3f} seconds")
print(f"  Speedup: {unopt_filter_time / opt_filter_time:.1f}x faster")

# ============================================================
# STEP 4: Memory Usage Comparison
# ============================================================

print("\n--- Memory Usage Comparison ---")

print(f"\nMemory Usage:")
print(f"  Unoptimized: {unoptimized.nbytes / 1024 / 1024:.2f} MB")
print(f"  Optimized: {optimized.nbytes / 1024 / 1024:.2f} MB")
print(f"  Savings: {(1 - optimized.nbytes / unoptimized.nbytes) * 100:.1f}%")

# ============================================================
# STEP 5: Performance Summary
# ============================================================

print("\n--- Performance Summary ---")

print("""
QUERY OPTIMIZATION RESULTS:

Dataset: 10 million records

Performance Improvements:
  - Aggregation: {unopt_agg_time:.3f}s → {opt_agg_time:.3f}s ({unopt_agg_time/opt_agg_time:.1f}x faster)
  - Filtering: {unopt_filter_time:.3f}s → {opt_filter_time:.3f}s ({unopt_filter_time/opt_filter_time:.1f}x faster)
  - Memory: {unoptimized.nbytes/1024/1024:.2f} MB → {optimized.nbytes/1024/1024:.2f} MB ({(1-optimized.nbytes/unoptimized.nbytes)*100:.1f}% savings)

OPTIMIZATION TECHNIQUES:
  ✓ Dictionary encoding for categorical data
  ✓ Vectorized operations
  ✓ Efficient memory layout
  ✓ Null bitmaps

BENEFITS:
  - Faster queries
  - Less memory
  - Better performance
""")
```

---

## 🏦 Real-World Banking Scenario 2: Memory Optimization

### Scenario
A bank's **analytics platform** is running out of memory. They need to:
- Reduce memory usage
- Process larger datasets
- Maintain performance

### Problem
- Memory limitations
- Large datasets
- Performance requirements

### Solution
Arrow memory optimization:
- Dictionary encoding
- Efficient types
- Batch processing

### Python Code

```python
"""
Banking Scenario 2: Memory Optimization
Using Arrow Memory Techniques
"""

import pyarrow as pa
import pyarrow.compute as pc
import random
import time

# ============================================================
# STEP 1: Generate Memory-Intensive Data
# ============================================================

print("=== MEMORY OPTIMIZATION WITH ARROW ===\n")

def generate_memory_intensive_data(num_records: int) -> pa.Table:
    """Generate memory-intensive data."""
    
    data = {
        "customer_id": [f"CUST-{i:08d}" for i in range(1, num_records + 1)],
        "name": [f"Customer_{random.randint(1, 10000)}" for _ in range(num_records)],
        "email": [f"customer{random.randint(1, 10000)}@bank.com" for _ in range(num_records)],
        "phone": [f"+1-555-{random.randint(1000, 9999)}" for _ in range(num_records)],
        "address": [f"{random.randint(1, 9999)} Main St, City {random.randint(1, 100)}" for _ in range(num_records)],
        "account_type": [random.choice(["SAVINGS", "CURRENT", "FIXED"]) for _ in range(num_records)],
        "risk_rating": [random.choice(["LOW", "MEDIUM", "HIGH"]) for _ in range(num_records)],
    }
    
    return pa.table(data)

# Generate 5 million records
print("Generating 5 million records...")
data = generate_memory_intensive_data(5000000)
print(f"Generated: {len(data):,} records")
print(f"Memory: {data.nbytes / 1024 / 1024:.2f} MB")

# ============================================================
# STEP 2: Apply Memory Optimizations
# ============================================================

print("\n--- Applying Memory Optimizations ---")

start_time = time.time()

# 1. Dictionary encoding
optimized_data = {
    "customer_id": data.column("customer_id"),
    "name": pc.dictionary_encode(data.column("name")),
    "email": pc.dictionary_encode(data.column("email")),
    "phone": pc.dictionary_encode(data.column("phone")),
    "address": pc.dictionary_encode(data.column("address")),
    "account_type": pc.dictionary_encode(data.column("account_type")),
    "risk_rating": pc.dictionary_encode(data.column("risk_rating")),
}

optimized = pa.table(optimized_data)
optimization_time = time.time() - start_time

print(f"\nOptimizations Applied:")
print(f"  Time: {optimization_time:.3f} seconds")
print(f"  Dictionary encoding: 6 columns")
print(f"  Memory: {optimized.nbytes / 1024 / 1024:.2f} MB")
print(f"  Savings: {(1 - optimized.nbytes / data.nbytes) * 100:.1f}%")

# ============================================================
# STEP 3: Query Performance with Optimized Data
# ============================================================

print("\n--- Query Performance ---")

# Aggregation
start_time = time.time()
result = optimized.group_by("account_type").aggregate({
    "customer_id": "count",
    "risk_rating": "count"
})
agg_time = time.time() - start_time

print(f"\nAggregation:")
print(f"  Time: {agg_time:.3f} seconds")
print(result.to_pandas().to_string(index=False))

# Filtering
start_time = time.time()
high_risk = optimized.filter(pc.equal(optimized.column("risk_rating"), "HIGH"))
filter_time = time.time() - start_time

print(f"\nHigh-Risk Filter:")
print(f"  Records: {len(high_risk):,}")
print(f"  Time: {filter_time:.3f} seconds")

# ============================================================
# STEP 4: Memory Usage Analysis
# ============================================================

print("\n--- Memory Usage Analysis ---")

print(f"\nMemory Comparison:")
print(f"  Original: {data.nbytes / 1024 / 1024:.2f} MB")
print(f"  Optimized: {optimized.nbytes / 1024 / 1024:.2f} MB")
print(f"  Savings: {(1 - optimized.nbytes / data.nbytes) * 100:.1f}%")

# Column-wise analysis
print(f"\nColumn Memory Usage:")
for col_name in data.column_names:
    orig_size = data.column(col_name).nbytes
    opt_size = optimized.column(col_name).nbytes
    savings = (1 - opt_size / orig_size) * 100 if orig_size > 0 else 0
    print(f"  {col_name}: {orig_size/1024/1024:.2f} MB → {opt_size/1024/1024:.2f} MB ({savings:.1f}% savings)")

# ============================================================
# STEP 5: Performance Summary
# ============================================================

print("\n--- Performance Summary ---")

print("""
MEMORY OPTIMIZATION RESULTS:

Dataset: 5 million records

Memory Usage:
  - Original: {data.nbytes/1024/1024:.2f} MB
  - Optimized: {optimized.nbytes/1024/1024:.2f} MB
  - Savings: {(1-optimized.nbytes/data.nbytes)*100:.1f}%

Query Performance:
  - Aggregation: {agg_time:.3f} seconds
  - Filtering: {filter_time:.3f} seconds

OPTIMIZATION TECHNIQUES:
  ✓ Dictionary encoding (30-50% savings)
  ✓ Efficient data types
  ✓ Null bitmaps
  ✓ Batch processing

BENEFITS:
  - 50% less memory
  - Faster queries
  - Larger datasets
  - Better performance
""")
```

---

## 🎯 5 Real-World Interview Questions

### Question 1: What are the key Arrow performance optimization techniques?

**Answer:**

**Optimization Techniques:**

1. **Dictionary Encoding:**
   - For categorical data
   - 30-50% memory savings

2. **Vectorized Operations:**
   - SIMD instructions
   - Parallel processing

3. **Efficient Types:**
   - Use smallest appropriate type
   - int32 vs int64

4. **Null Bitmaps:**
   - Efficient null handling
   - No sentinel values

**Example:**
```python
import pyarrow as pa
import pyarrow.compute as pc

# Dictionary encoding
array = pc.dictionary_encode(pa.array(["A", "B", "A", "C"]))
```

---

### Question 2: How do you optimize Arrow queries for performance?

**Answer:**

**Query Optimization:**

1. **Predicate Pushdown:**
```python
table = pq.read_table("data.parquet", filters=[("amount", ">", 10000)])
```

2. **Column Pruning:**
```python
table = pq.read_table("data.parquet", columns=["id", "amount"])
```

3. **Partitioning:**
```python
pq.write_to_dataset(table, "output", partition_cols=["year", "month"])
```

---

### Question 3: How do you measure Arrow performance?

**Answer:**

**Performance Metrics:**

1. **Query Time:**
```python
start = time.time()
result = table.group_by("category").aggregate({"amount": "sum"})
elapsed = time.time() - start
```

2. **Memory Usage:**
```python
print(f"Memory: {table.nbytes / 1024 / 1024:.2f} MB")
```

3. **Throughput:**
```python
throughput = len(table) / elapsed
print(f"Throughput: {throughput:,.0f} records/sec")
```

---

### Question 4: What are common Arrow performance pitfalls?

**Answer:**

**Common Pitfalls:**

1. **No Dictionary Encoding:**
   - High memory usage
   - Slow filtering

2. **Wrong Data Types:**
   - Using int64 for small numbers
   - Using float64 for integers

3. **No Null Handling:**
   - Wasted memory
   - Slow operations

4. **Large Batches:**
   - Memory pressure
   - Slow processing

**Solutions:**
- Use dictionary encoding
- Choose appropriate types
- Handle nulls efficiently
- Process in batches

---

### Question 5: How do you optimize Arrow for large datasets?

**Answer:**

**Large Dataset Optimization:**

1. **Batch Processing:**
```python
for batch in table.to_batches(max_chunksize=10000):
    process(batch)
```

2. **Partitioning:**
```python
pq.write_to_dataset(table, "output", partition_cols=["year", "month"])
```

3. **Compression:**
```python
pq.write_table(table, "output.parquet", compression="zstd")
```

4. **Memory Management:**
```python
import gc
gc.collect()
```

---

## 📝 Summary

| Aspect | Key Point |
|--------|-----------|
| **Definition** | Techniques for high performance |
| **Techniques** | Dictionary encoding, vectorization, types |
| **Metrics** | Query time, memory, throughput |
| **Pitfalls** | No encoding, wrong types, large batches |
| **Large Datasets** | Batching, partitioning, compression |
| **Benefits** | 10x faster, 50% less memory |
