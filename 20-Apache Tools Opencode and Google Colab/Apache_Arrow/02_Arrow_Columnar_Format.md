# Concept 02: Arrow Columnar Format

## 📚 Detailed Explanation

The **Arrow Columnar Format** is the memory layout specification that defines how data is stored in RAM. Understanding this format is crucial for optimizing performance and memory usage.

### Why Columnar Format?

**Traditional Row-Based (CSV, JSON):**
```
Row 1: [1001, "Alice", 50000.00, "2026-08-24"]
Row 2: [1002, "Bob", 75000.00, "2026-08-24"]
Row 3: [1003, "Charlie", 60000.00, "2026-08-24"]

Problem: Mixed types in memory, poor cache utilization
```

**Arrow Columnar:**
```
IDs:     [1001, 1002, 1003]           ← All integers
Names:   ["Alice", "Bob", "Charlie"]  ← All strings
Amounts: [50000.00, 75000.00, 60000.00] ← All floats
Dates:   ["2026-08-24", "2026-08-24", "2026-08-24"] ← All dates

Benefit: Same types together, excellent cache utilization
```

### Arrow Memory Layout

**Primitive Types (Int, Float, etc.):**
```
Values Buffer:
[1001][1002][1003][1004][1005]
 ↑
 Contiguous memory, same type
```

**Variable-Length Types (String, Binary):**
```
Offsets Buffer: [0][5][9][14][20]
Values Buffer:  [A][l][i][c][e][B][o][b][C][h][a][r][l][i][e][D][a][v][i][d]

Offsets tell where each string starts/ends
```

**Null Bitmap:**
```
Validity Bitmap: [1][1][0][1][1]
                 ↑  ↑  ↑  ↑  ↑
                OK OK NULL OK OK
```

### Arrow Data Types

| Category | Types | Description |
|----------|-------|-------------|
| **Integer** | int8, int16, int32, int64 | Signed integers |
| **Unsigned** | uint8, uint16, uint32, uint64 | Unsigned integers |
| **Float** | float16, float32, float64 | Floating point |
| **String** | utf8, large_utf8 | Variable-length strings |
| **Binary** | binary, large_binary | Variable-length bytes |
| **Boolean** | boolean | True/False |
| **Date** | date32, date32day, date32month | Date types |
| **Time** | time32, time64, timestamp | Time types |
| **Decimal** | decimal128, decimal256 | Precise decimals |
| **Nested** | struct, list, map | Complex types |

### Dictionary Encoding

**Without Dictionary:**
```
Colors: ["red", "blue", "red", "green", "red", "blue"]
Memory: 6 strings × ~10 bytes = 60 bytes
```

**With Dictionary:**
```
Dictionary: ["red", "blue", "green"]  ← 3 strings
Indices:    [0, 1, 0, 2, 0, 1]        ← 6 integers
Memory: 3 strings + 6 integers = ~36 bytes (40% savings)
```

---

## 💡 Example: Memory Layout in Banking

### Scenario: Transaction Table

```python
import pyarrow as pa

# Create transaction table
table = pa.table({
    "id": [1001, 1002, 1003, 1004, 1005],
    "customer": ["Alice", "Bob", "Charlie", "David", "Eve"],
    "amount": [50000.00, 75000.00, 60000.00, 80000.00, 55000.00],
    "status": ["active", "active", "inactive", "active", "active"]
})
```

**Memory Layout in RAM Memory:**
```
id Column (int64):
  [1001][1002][1003][1004][1005]  ← 40 bytes (5 × 8 bytes)

customer Column (utf8):
  Offsets: [0][5][8][15][20][23]  ← 24 bytes (6 × 4 bytes)
  Values:  [A][l][i][c][e][B][o][b][C][h][a][r][l][i][e][D][a][v][i][d][E][v][e]  ← 23 bytes

amount Column (float64):
  [50000.00][75000.00][60000.00][80000.00][55000.00]  ← 40 bytes

status Column (utf8 with dictionary encoding):
  Dictionary: ["active", "inactive"]  ← 14 bytes
  Indices:    [0, 0, 1, 0, 0]         ← 20 bytes (5 × 4 bytes)
  Total: 34 bytes (vs 50 bytes without dictionary)
```

---

## 🏦 Real-World Banking Scenario 1: Memory Optimization

### Scenario
A bank stores **100 million customer records**. Memory is expensive, and they need to optimize storage while maintaining query performance.

### Problem
- High memory usage
- Expensive infrastructure
- Need fast analytics

### Solution
Arrow's columnar format with dictionary encoding:
- Dictionary encoding for categorical data
- Null bitmaps for sparse data
- Efficient compression

### Python Code

```python
"""
Banking Scenario 1: Memory Optimization
Using Arrow Columnar Format
"""

import pyarrow as pa
import pyarrow.compute as pc
import random
import sys

# ============================================================
# STEP 1: Generate Customer Data
# ============================================================

print("=== MEMORY OPTIMIZATION WITH ARROW ===\n")

def generate_customer_data(num_records: int) -> pa.Table:
    """Generate realistic customer data."""
    
    # Generate customer IDs
    customer_ids = [f"CUST-{i:08d}" for i in range(1, num_records + 1)]
    
    # Generate names (limited set - good for dictionary encoding)
    first_names = ["Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace", "Henry"]
    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis"]
    names = [f"{random.choice(first_names)} {random.choice(last_names)}" for _ in range(num_records)]
    
    # Generate account types (limited set)
    account_types = [random.choice(["SAVINGS", "CURRENT", "FIXED_DEPOSIT", "LOAN"]) for _ in range(num_records)]
    
    # Generate balances
    balances = [round(random.uniform(1000, 1000000), 2) for _ in range(num_records)]
    
    # Generate cities (limited set)
    cities = [random.choice(["Mumbai", "Delhi", "Bangalore", "Chennai", "Kolkata"]) for _ in range(num_records)]
    
    # Generate risk ratings (limited set)
    risk_ratings = [random.choice(["LOW", "MEDIUM", "HIGH"]) for _ in range(num_records)]
    
    # Create Arrow Table
    table = pa.table({
        "customer_id": customer_ids,
        "name": names,
        "account_type": account_types,
        "balance": balances,
        "city": cities,
        "risk_rating": risk_ratings,
    })
    
    return table

# Generate 10 million customers
print("Generating 10 million customer records...")
customers = generate_customer_data(10000000)

print(f"Generated: {len(customers):,} records")
print(f"Schema: {customers.schema}")

# ============================================================
# STEP 2: Analyze Memory Usage
# ============================================================

print("\n--- Analyzing Memory Usage ---")

# Get memory usage for each column
print("\nColumn Memory Usage:")
total_memory = 0

for col_name in customers.column_names:
    col = customers.column(col_name)
    col_memory = col.nbytes
    total_memory += col_memory
    print(f"  {col_name}: {col_memory / 1024 / 1024:.2f} MB")

print(f"\nTotal Arrow Table Memory: {total_memory / 1024 / 1024:.2f} MB")

# ============================================================
# STEP 3: Dictionary Encoding Analysis
# ============================================================

print("\n--- Dictionary Encoding Analysis ---")

# Analyze dictionary-encoded columns
dictionary_columns = ["account_type", "city", "risk_rating"]

for col_name in dictionary_columns:
    col = customers.column(col_name)
    
    # Check if dictionary encoded
    if hasattr(col, 'dictionary'):
        dictionary = col.dictionary
        indices = col.indices
        print(f"\n{col_name} (Dictionary Encoded):")
        print(f"  Unique values: {len(dictionary)}")
        print(f"  Dictionary size: {dictionary.nbytes / 1024:.2f} KB")
        print(f"  Indices size: {indices.nbytes / 1024 / 1024:.2f} MB")
        print(f"  Total: {(dictionary.nbytes + indices.nbytes) / 1024 / 1024:.2f} MB")
    else:
        print(f"\n{col_name} (Not Dictionary Encoded):")
        print(f"  Size: {col.nbytes / 1024 / 1024:.2f} MB")

# ============================================================
# STEP 4: Memory Comparison
# ============================================================

print("\n--- Memory Comparison ---")

# Compare with Pandas
import pandas as pd

pandas_df = customers.to_pandas()
pandas_memory = pandas_df.memory_usage(deep=True).sum()

print(f"\nMemory Usage Comparison:")
print(f"  Arrow Table: {total_memory / 1024 / 1024:.2f} MB")
print(f"  Pandas DataFrame: {pandas_memory / 1024 / 1024:.2f} MB")
print(f"  Savings: {(1 - total_memory / pandas_memory) * 100:.1f}%")

# ============================================================
# STEP 5: Optimize with Dictionary Encoding
# ============================================================

print("\n--- Optimizing with Dictionary Encoding ---")

# Convert columns to dictionary type for better compression
optimized_table = pa.table({
    "customer_id": customers.column("customer_id"),
    "name": customers.column("name"),
    "account_type": pc.dictionary_encode(customers.column("account_type")),
    "balance": customers.column("balance"),
    "city": pc.dictionary_encode(customers.column("city")),
    "risk_rating": pc.dictionary_encode(customers.column("risk_rating")),
})

# Compare sizes
optimized_memory = optimized_table.nbytes
print(f"\nOptimized Memory Usage:")
print(f"  Original: {total_memory / 1024 / 1024:.2f} MB")
print(f"  Optimized: {optimized_memory / 1024 / 1024:.2f} MB")
print(f"  Additional Savings: {(1 - optimized_memory / total_memory) * 100:.1f}%")

# ============================================================
# STEP 6: Query Performance
# ============================================================

print("\n--- Query Performance ---")

# Query 1: Filter by account type
start_time = time.time()
savings_customers = optimized_table.filter(
    pc.equal(optimized_table.column("account_type"), "SAVINGS")
)
filter_time = time.time() - start_time

print(f"\nFilter by Account Type = 'SAVINGS':")
print(f"  Results: {len(savings_customers):,}")
print(f"  Time: {filter_time:.3f} seconds")

# Query 2: Aggregation by city
start_time = time.time()
city_agg = optimized_table.group_by("city").aggregate({
    "balance": "sum",
    "customer_id": "count"
})
agg_time = time.time() - start_time

print(f"\nAggregation by City:")
print(f"  Time: {agg_time:.3f} seconds")

# ============================================================
# STEP 7: Benefits Summary
# ============================================================

print("\n--- Benefits Summary ---")

print("""
ARROW COLUMNAR FORMAT BENEFITS:

1. MEMORY EFFICIENCY
   - Contiguous memory per column
   - Dictionary encoding for categorical data
   - Null bitmaps for sparse data

2. CACHE EFFICIENCY
   - Sequential memory access
   - CPU cache line optimization
   - Predictable memory patterns

3. SIMD VECTORIZATION
   - Process multiple values simultaneously
   - Hardware-accelerated operations
   - Parallel computation

4. COMPRESSION
   - Similar values compress better
   - Run-length encoding
   - Dictionary encoding

5. ANALYTICS OPTIMIZATION
   - Fast aggregations
   - Efficient filtering
   - Predicate pushdown

MEMORY SAVINGS:
  - Dictionary encoding: 30-50% savings
  - Null bitmaps: 50-70% savings for sparse data
  - Overall: 40-60% less memory than row-based formats
""")
```

---

## 🏦 Real-World Banking Scenario 2: High-Performance Analytics

### Scenario
A bank's **analytics team** runs complex queries on **500 million transactions**. They need:
- Fast query response (< 1 second)
- Complex aggregations
- Real-time dashboards

### Problem
- Slow query performance
- High memory usage
- Expensive infrastructure

### Solution
Arrow's columnar format with:
- Vectorized operations
- Efficient memory layout
- Parallel processing

### Python Code

```python
"""
Banking Scenario 2: High-Performance Analytics
Using Arrow Columnar Format
"""

import pyarrow as pa
import pyarrow.compute as pc
import random
from datetime import datetime, timedelta
import time

# ============================================================
# STEP 1: Generate Large Transaction Dataset
# ============================================================

print("=== HIGH-PERFORMANCE ANALYTICS ===\n")

def generate_large_transactions(num_records: int) -> pa.Table:
    """Generate large transaction dataset."""
    
    # Generate transaction IDs
    transaction_ids = [f"TXN-{i:010d}" for i in range(1, num_records + 1)]
    
    # Generate account IDs (limited set)
    account_ids = [f"ACC-{random.randint(1000, 9999):06d}" for _ in range(num_records)]
    
    # Generate amounts
    amounts = [round(random.uniform(10, 1000000), 2) for _ in range(num_records)]
    
    # Generate transaction types (limited set)
    txn_types = [random.choice(["CREDIT", "DEBIT", "TRANSFER", "PAYMENT"]) for _ in range(num_records)]
    
    # Generate channels (limited set)
    channels = [random.choice(["ATM", "MOBILE", "WEB", "BRANCH", "UPI"]) for _ in range(num_records)]
    
    # Generate statuses (limited set)
    statuses = [random.choice(["COMPLETED", "PENDING", "FAILED", "REVERSED"]) for _ in range(num_records)]
    
    # Create Arrow Table
    table = pa.table({
        "transaction_id": transaction_ids,
        "account_id": account_ids,
        "amount": amounts,
        "transaction_type": txn_types,
        "channel": channels,
        "status": statuses,
    })
    
    return table

# Generate 500 million transactions
print("Generating 500 million transactions...")
start_time = time.time()
transactions = generate_large_transactions(500000000)
generation_time = time.time() - start_time

print(f"Generated in {generation_time:.2f} seconds")
print(f"Table size: {transactions.nbytes / 1024 / 1024 / 1024:.2f} GB")

# ============================================================
# STEP 2: Vectorized Aggregations
# ============================================================

print("\n--- Vectorized Aggregations ---")

# Aggregation 1: Total by transaction type
start_time = time.time()
type_agg = transactions.group_by("transaction_type").aggregate({
    "amount": "sum",
    "transaction_id": "count"
})
agg_time = time.time() - start_time

print(f"\nTransaction Type Aggregation:")
print(f"  Time: {agg_time:.3f} seconds")
print(f"  Results:")
for i in range(len(type_agg)):
    txn_type = type_agg.column("transaction_type")[i].as_py()
    total = type_agg.column("amount_sum")[i].as_py()
    count = type_agg.column("transaction_id_count")[i].as_py()
    print(f"    {txn_type}: ${total:,.2f} ({count:,} transactions)")

# Aggregation 2: Total by channel
start_time = time.time()
channel_agg = transactions.group_by("channel").aggregate({
    "amount": "sum",
    "transaction_id": "count"
})
agg_time = time.time() - start_time

print(f"\nChannel Aggregation:")
print(f"  Time: {agg_time:.3f} seconds")
print(f"  Results:")
for i in range(len(channel_agg)):
    channel = channel_agg.column("channel")[i].as_py()
    total = channel_agg.column("amount_sum")[i].as_py()
    count = channel_agg.column("transaction_id_count")[i].as_py()
    print(f"    {channel}: ${total:,.2f} ({count:,} transactions)")

# ============================================================
# STEP 3: Filtering Performance
# ============================================================

print("\n--- Filtering Performance ---")

# Filter 1: High-value transactions
start_time = time.time()
high_value_mask = pc.greater(transactions.column("amount"), 100000)
high_value = transactions.filter(high_value_mask)
filter_time = time.time() - start_time

print(f"\nHigh-Value Transactions (> $100,000):")
print(f"  Count: {len(high_value):,}")
print(f"  Time: {filter_time:.3f} seconds")

# Filter 2: Multi-condition filter
start_time = time.time()
condition1 = pc.greater(transactions.column("amount"), 50000)
condition2 = pc.equal(transactions.column("status"), "COMPLETED")
condition3 = pc.equal(transactions.column("channel"), "MOBILE")

combined_mask = pc.and_(pc.and_(condition1, condition2), condition3)
filtered = transactions.filter(combined_mask)
filter_time = time.time() - start_time

print(f"\nMulti-Condition Filter:")
print(f"  Conditions: amount > 50000 AND status = COMPLETED AND channel = MOBILE")
print(f"  Count: {len(filtered):,}")
print(f"  Time: {filter_time:.3f} seconds")

# ============================================================
# STEP 4: Statistics Computation
# ============================================================

print("\n--- Statistics Computation ---")

amounts = transactions.column("amount")

# Compute comprehensive statistics
start_time = time.time()

stats = {
    "count": pc.count(amounts).as_py(),
    "sum": pc.sum(amounts).as_py(),
    "mean": pc.mean(amounts).as_py(),
    "min": pc.min(amounts).as_py(),
    "max": pc.max(amounts).as_py(),
    "std": pc.stddev(amounts).as_py(),
    "median": pc.median(amounts).as_py(),
}

stats_time = time.time() - start_time

print(f"\nAmount Statistics:")
print(f"  Count: {stats['count']:,}")
print(f"  Sum: ${stats['sum']:,.2f}")
print(f"  Mean: ${stats['mean']:,.2f}")
print(f"  Min: ${stats['min']:,.2f}")
print(f"  Max: ${stats['max']:,.2f}")
print(f"  Std Dev: ${stats['std']:,.2f}")
print(f"  Median: ${stats['median']:,.2f}")
print(f"\n  Computed in {stats_time:.3f} seconds")

# ============================================================
# STEP 5: Performance Summary
# ============================================================

print("\n--- Performance Summary ---")

print("""
ARROW PERFORMANCE METRICS:

Dataset: 500 million transactions
Size: ~40 GB (in-memory)

Operations:
  - Generation: {generation_time:.2f} seconds
  - Aggregation: {agg_time:.3f} seconds
  - Filtering: {filter_time:.3f} seconds
  - Statistics: {stats_time:.3f} seconds

Performance Characteristics:
  ✓ Vectorized operations (SIMD)
  ✓ Cache-efficient memory layout
  ✓ Parallel processing
  ✓ Zero-copy operations

vs Pandas:
  - 3-5x faster aggregations
  - 2-3x faster filtering
  - 50% less memory usage

vs Traditional Databases:
  - 10-100x faster for in-memory analytics
  - No I/O overhead
  - No query planning overhead
""")
```

---

## 🎯 5 Real-World Interview Questions

### Question 1: Explain the Arrow columnar format and its memory layout.

**Answer:**

**Arrow Columnar Format:**
- Data organized by columns, not rows
- Each column stored contiguously in memory
- Same type data stored together

**Memory Layout:**

1. **Primitive Types (int, float)**:
   ```
   [value1][value2][value3]...[valueN]
   ```

2. **Variable-Length (string, binary)**:
   ```
   Offsets: [0][5][9][14]
   Values:  [A][l][i][c][e][B][o][b][C][h][a][r][l][i][e]
   ```

3. **Null Bitmap**:
   ```
   [1][1][0][1][1]  ← Validity flags
   ```

**Benefits:**
- Cache-efficient (sequential access)
- SIMD vectorization possible
- Efficient compression

---

### Question 2: What is dictionary encoding and when should you use it?

**Answer:**

**Dictionary Encoding:**
- Stores unique values in dictionary
- Uses integer indices to reference values
- Reduces memory for categorical data

**Example:**
```
Without Dictionary:
  ["red", "blue", "red", "green", "red"]  ← 5 strings

With Dictionary:
  Dictionary: ["red", "blue", "green"]  ← 3 strings
  Indices: [0, 1, 0, 2, 0]             ← 5 integers
```

**When to Use:**
- Low cardinality columns (few unique values)
- Categorical data (status, type, category)
- Repeated string values

**When NOT to Use:**
- High cardinality (many unique values)
- Unique identifiers
- Free-text fields

**Example:**
```python
# Good for dictionary encoding
account_type = ["SAVINGS", "CURRENT", "SAVINGS", "FIXED"]
status = ["ACTIVE", "ACTIVE", "INACTIVE", "ACTIVE"]

# Bad for dictionary encoding
customer_id = ["CUST-001", "CUST-002", "CUST-003"]
transaction_id = ["TXN-001", "TXN-002", "TXN-003"]
```

---

### Question 3: How does Arrow's memory layout improve CPU cache performance?

**Answer:**

**Cache Performance:**

1. **Sequential Access**: Column data stored contiguously
2. **Predictable Patterns**: CPU can prefetch data
3. **Same-Type Data**: No type switching overhead
4. **SIMD Instructions**: Process multiple values simultaneously

**Comparison:**
```
Row-Based:
  [1001, "Alice", 50000] → Mixed types, poor cache
  [1002, "Bob", 75000]   → Cache misses

Columnar:
  [1001, 1002, 1003]     → Same type, excellent cache
  ["Alice", "Bob", "Charlie"] → Sequential strings
```

**Performance Impact:**
- 10-100x faster for analytics
- 50-70% less cache misses
- SIMD vectorization enabled

---

### Question 4: Compare Arrow's memory usage with row-based formats.

**Answer:**

**Memory Comparison:**

| Aspect | Row-Based | Arrow Columnar |
|--------|-----------|----------------|
| **Storage** | Mixed types | Same type |
| **Null Handling** | Separate | Bitmap |
| **String Storage** | Padded | Dictionary/Offsets |
| **Compression** | Poor | Excellent |
| **Memory Usage** | 100% | 40-60% |

**Example:**
```
Row-Based (CSV/JSON):
  Row 1: [1001, "Alice", 50000.00, "2026-08-24"]
  Row 2: [1002, "Bob", 75000.00, "2026-08-24"]
  Memory: ~200 bytes (with padding)

Arrow Columnar:
  IDs: [1001, 1002]  ← 16 bytes
  Names: ["Alice", "Bob"]  ← 16 bytes
  Amounts: [50000, 75000]  ← 16 bytes
  Memory: ~48 bytes (75% less)
```

---

### Question 5: How does Arrow handle nested data types?

**Answer:**

**Nested Types:**

1. **Struct**: Fixed set of named fields
   ```
   {name: "Alice", age: 30, address: {city: "NYC", zip: "10001"}}
   ```

2. **List**: Variable-length list of values
   ```
   [1, 2, 3], [4, 5], [6, 7, 8, 9]
   ```

3. **Map**: Key-value pairs
   ```
   {"city": "NYC", "state": "NY"}
   ```

**Memory Layout:**
```
Struct:
  Field 1 (name): ["Alice", "Bob"]
  Field 2 (age): [30, 25]
  Field 3 (address): Struct{city, zip}

List:
  Offsets: [0][3][5][8]
  Values: [1, 2, 3, 4, 5, 6, 7, 8, 9]
```

**Example:**
```python
import pyarrow as pa

# Struct type
struct_type = pa.struct([
    pa.field("name", pa.string()),
    pa.field("age", pa.int32()),
    pa.field("address", pa.struct([
        pa.field("city", pa.string()),
        pa.field("zip", pa.string()),
    ])),
])
```

---

## 📝 Summary

| Aspect | Key Point |
|--------|-----------|
| **Definition** | Memory layout specification for columnar data |
| **Layout** | Contiguous columns, null bitmaps, offsets |
| **Dictionary Encoding** | Reduces memory for categorical data |
| **Cache Efficiency** | Sequential access, SIMD vectorization |
| **Memory Savings** | 40-60% less than row-based formats |
| **Performance** | 10-100x faster for analytics |
| **Nested Types** | Struct, List, Map support |
