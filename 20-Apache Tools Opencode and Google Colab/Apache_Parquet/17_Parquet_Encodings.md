# Parquet Encodings: Complete Guide

## Table of Contents
1. [Detailed Explanation](#1-detailed-explanation)
2. [Encoding 1: Plain Encoding](#encoding-1-plain-encoding)
3. [Encoding 2: Dictionary Encoding](#encoding-2-dictionary-encoding)
4. [Encoding 3: Run-Length Encoding (RLE)](#encoding-3-run-length-encoding-rle)
5. [Encoding 4: Bit-Packing Encoding](#encoding-4-bit-packing-encoding)
6. [Encoding 5: Delta Encoding](#encoding-5-delta-encoding)
7. [Encoding 6: Delta-Length Byte Array](#encoding-6-delta-length-byte-array)
8. [Encoding 7: Byte Stream Split](#encoding-7-byte-stream-split)
9. [Encoding Decision Matrix](#encoding-decision-matrix)
10. [Interview Questions](#interview-questions)
11. [Summary](#summary)

---

## 1. Detailed Explanation

### What is Parquet Encoding?

Parquet encoding is the **logical transformation** applied to data **before compression**. While compression (Snappy, Zstd, Gzip) works on raw bytes using general-purpose algorithms, encoding understands the **structure and patterns** in your data to reduce its size before compression even starts.

```
Raw Data → Encoding (structural optimization) → Compression (byte-level optimization) → Disk
```

### Why Encoding Matters

**Without encoding (plain + compression only):**
```
Status column: ["COMPLETED", "PENDING", "COMPLETED", ...] × 1 billion
Plain encoding: 8 bytes × 1B = 8 GB
Snappy compression: → ~3 GB
```

**With encoding + compression:**
```
Dictionary encoding: 3 unique values → indices [0, 1, 0, ...]
Bit-packing: 2 bits per index → 250 MB
Snappy compression: → ~150 MB

Total savings: 8 GB → 150 MB = 53x reduction!
```

### The 7 Parquet Encodings

| # | Encoding | Input Type | Best For | Compression Potential |
|---|----------|-----------|----------|----------------------|
| 1 | **Plain** | Any | General purpose, high cardinality | Low |
| 2 | **Dictionary** | Any | Low cardinality, repeated values | High |
| 3 | **RLE (Run-Length)** | Any | Sorted, consecutive repeats | High |
| 4 | **Bit-Packing** | Integers | Small value ranges (0-N) | Medium |
| 5 | **Delta** | Integers | Monotonic sequences | High |
| 6 | **Delta-Length Byte Array** | Byte arrays | Similar-length strings | Medium |
| 7 | **Byte Stream Split** | Floats | Floating-point data | Medium |

### How Encodings Chain Together

Parquet applies **multiple encodings sequentially** — they are not mutually exclusive:

```
Example: status column (1 billion rows, 3 unique values, sorted)

Step 1: Dictionary Encoding
  Dictionary: {0: "COMPLETED", 1: "PENDING", 2: "FAILED"}
  Indices: [0, 0, 0, ..., 1, 1, ..., 2, 2, ...]
  Size: 4 bytes × 1B = 4 GB

Step 2: RLE on dictionary indices (consecutive 0s, 1s, 2s)
  Runs: [(0, 800M), (1, 150M), (2, 50M)]
  Size: 3 pairs × 8 bytes = 24 bytes (!!!)

Step 3: Bit-Packing (values 0-2 need only 2 bits)
  Size: 1B × 2 bits = 250 MB

Step 4: Snappy Compression
  Already minimal → ~150 MB

Final: 8 GB raw → 150 MB on disk = 53x compression
```

### Parquet's Encoding Fallback Mechanism

Parquet has a built-in **fallback** when dictionary encoding becomes inefficient:

1. Parquet starts building a dictionary for a column
2. If the dictionary exceeds `dictionary_pagesize_limit` (default: 1MB), it **falls back to plain encoding**
3. High-cardinality columns (transaction_id, UUID) automatically fall back
4. Low-cardinality columns (status, currency) stay dictionary-encoded

```python
# Configure fallback behavior
pq.write_table(
    table,
    path,
    use_dictionary=True,                    # Enable dictionary (default)
    dictionary_pagesize_limit=1_048_576,    # 1MB limit before fallback
)
```

---

## Encoding 1: Plain Encoding

### How It Works

Plain encoding stores data **as-is** with no transformation. It's the simplest encoding — raw bytes written directly to the page.

```
int64 column: [1001, 1002, 1003, 1004, 1005]
Stored:       [1001][1002][1003][1004][1005]  ← 5 × 8 bytes = 40 bytes

float64 column: [50000.00, 75000.00, 60000.00]
Stored:         [50000.00][75000.00][60000.00]  ← 3 × 8 bytes = 24 bytes

string column: ["Alice", "Bob", "Charlie"]
Stored:        [5][A][l][i][c][e][3][B][o][b][7][C][h][a][r][l][i][e]
               ↑length↑     ↑string    ↑length↑  ↑string    ↑length↑  ↑string
```

### When to Use

- ✅ High-cardinality data (most values are unique)
- ✅ Numerical data where other encodings add overhead
- ✅ When encoding speed matters more than compression
- ✅ Fallback when dictionary encoding exceeds page size limit

### When NOT to Use

- ❌ Low-cardinality strings (dictionary is better)
- ❌ Sorted sequences (delta encoding is better)
- ❌ Small integer ranges (bit-packing is better)

### Memory Layout

```
Data Page (Plain Encoding):
┌─────────────────────────────────────────────────┐
│ Page Header: encoding=PLAIN, compressed_size=N  │
├─────────────────────────────────────────────────┤
│ [1001][1002][1003][1004][1005][...]              │
│  ↑ raw bytes, no transformation                  │
└─────────────────────────────────────────────────┘
```

---

### 🏦 Banking Scenario 1: Plain Encoding for Unique Transaction IDs

#### Scenario
A bank stores **500 million daily transactions**. Each transaction has a unique `transaction_id` (UUID-like string). Since every value is unique, dictionary encoding would add overhead (dictionary = all values, indices = same size). Plain encoding is optimal.

#### Why Plain Encoding?

- `transaction_id` has 500M unique values → dictionary would store 500M strings + 500M indices = **worse** than plain
- Every value is different → no patterns to exploit
- Plain encoding has zero decode overhead → fastest possible reads

#### Python Code

```python
"""
Banking Scenario: Plain Encoding for Unique Transaction IDs
When every value is unique, plain encoding is optimal
"""

import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
import random
import time
import os
import tempfile

print("=== PLAIN ENCODING: UNIQUE TRANSACTION IDS ===\n")

# ============================================================
# STEP 1: Generate Unique Transaction IDs
# ============================================================

def generate_unique_transactions(num_records: int) -> pa.Table:
    """Generate transactions with unique IDs — plain encoding optimal."""
    
    # Every transaction ID is unique (no repeats)
    transaction_ids = [f"TXN-{i:015d}-{random.randint(1000,9999)}" 
                       for i in range(1, num_records + 1)]
    
    # Unique account references
    account_refs = [f"REF-{i:012d}" for i in range(1, num_records + 1)]
    
    # Numeric data (high cardinality — plain encoding)
    amounts = np.random.uniform(1, 1000000, num_records).round(2)
    fees = np.random.uniform(0, 500, num_records).round(2)
    exchange_rates = np.random.uniform(0.5, 2.0, num_records).round(8)
    
    table = pa.table({
        "transaction_id": transaction_ids,       # Unique → Plain
        "account_ref": account_refs,              # Unique → Plain
        "amount": amounts,                         # High cardinality → Plain
        "fee": fees,                               # High cardinality → Plain
        "exchange_rate": exchange_rates,           # High cardinality → Plain
    })
    
    return table

print("Generating 5 million unique transactions...")
start_time = time.time()
data = generate_unique_transactions(5000000)
gen_time = time.time() - start_time
print(f"Generated in {gen_time:.3f} seconds")
print(f"Rows: {len(data):,}")

# ============================================================
# STEP 2: Compare Plain vs Dictionary Encoding
# ============================================================

print("\n--- Plain vs Dictionary Encoding Comparison ---")

temp_dir = tempfile.mkdtemp()

# Plain encoding (no dictionary)
path_plain = os.path.join(temp_dir, "plain.parquet")
start_time = time.time()
pq.write_table(data, path_plain, compression="snappy", use_dictionary=False)
plain_time = time.time() - start_time
plain_size = os.path.getsize(path_plain)

# Dictionary encoding
path_dict = os.path.join(temp_dir, "dictionary.parquet")
start_time = time.time()
pq.write_table(data, path_dict, compression="snappy", use_dictionary=True)
dict_time = time.time() - start_time
dict_size = os.path.getsize(path_dict)

print(f"\n  Plain encoding:")
print(f"    Size: {plain_size / 1024 / 1024:.2f} MB")
print(f"    Write time: {plain_time:.3f}s")

print(f"\n  Dictionary encoding:")
print(f"    Size: {dict_size / 1024 / 1024:.2f} MB")
print(f"    Write time: {dict_time:.3f}s")

winner = "Plain" if plain_size < dict_size else "Dictionary"
savings = abs(plain_size - dict_size) / max(plain_size, dict_size) * 100
print(f"\n  Winner: {winner} encoding ({savings:.1f}% smaller)")

# ============================================================
# STEP 3: Read Performance Comparison
# ============================================================

print("\n--- Read Performance ---")

# Read with plain encoding
start_time = time.time()
table_plain = pq.read_table(path_plain, columns=["transaction_id", "amount"])
plain_read_time = time.time() - start_time

# Read with dictionary encoding
start_time = time.time()
table_dict = pq.read_table(path_dict, columns=["transaction_id", "amount"])
dict_read_time = time.time() - start_time

print(f"\n  Plain read: {plain_read_time:.3f}s")
print(f"  Dictionary read: {dict_read_time:.3f}s")

# ============================================================
# STEP 4: Per-Column Analysis
# ============================================================

print("\n--- Per-Column Encoding Analysis ---")

print(f"\n  {'Column':<20} {'Unique Values':>14} {'Best Encoding':>16} {'Reason'}")
print(f"  {'-'*20} {'-'*14} {'-'*16} {'-'*30}")

for col_name in data.column_names:
    col = data.column(col_name)
    unique_count = len(set(col.to_pylist()))
    total_count = len(col)
    uniqueness = unique_count / total_count
    
    if uniqueness > 0.5:
        best = "Plain"
        reason = f"{uniqueness*100:.0f}% unique — dictionary adds overhead"
    else:
        best = "Dictionary"
        reason = f"{uniqueness*100:.0f}% unique — dictionary saves space"
    
    print(f"  {col_name:<20} {unique_count:>14,} {best:>16} {reason}")

# ============================================================
# STEP 5: Summary
# ============================================================

print(f"\n--- Summary ---")

print(f"""
PLAIN ENCODING RESULTS:

Dataset: {len(data):,} unique transactions
Plain size: {plain_size / 1024 / 1024:.2f} MB
Dictionary size: {dict_size / 1024 / 1024:.2f} MB

WHEN PLAIN WINS:
  ✓ Every value is unique (transaction_id, UUID, sequential IDs)
  ✓ High-cardinality numeric data (amounts, rates, scores)
  ✓ When decode speed matters more than compression
  ✓ When dictionary would store more data than raw bytes

PLAIN ENCODING HAS:
  ✓ Zero decode overhead — direct byte access
  ✓ No dictionary page to load into memory
  ✓ Fastest possible write speed
  ✗ Worst compression ratio for repeated values
""")

# Cleanup
import shutil
shutil.rmtree(temp_dir, ignore_errors=True)
```

---

### 🏦 Banking Scenario 2: Plain Encoding for Floating-Point Market Data

#### Scenario
A bank's **algorithmic trading system** generates millions of floating-point price signals per second. Each value is unique (prices fluctuate continuously). Plain encoding + Snappy compression provides the best balance of speed and size.

#### Why Plain Encoding?

- Prices are continuous floats → every value is unique
- Bit-packing doesn't apply (floats, not small integers)
- Delta encoding could help for sequential data, but prices jump randomly
- Plain + Snappy provides fast decompression for real-time trading

#### Python Code

```python
"""
Banking Scenario: Plain Encoding for Floating-Point Market Data
Optimal for high-cardinality continuous values
"""

import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
import time
import os
import tempfile

print("=== PLAIN ENCODING: FLOATING-POINT MARKET DATA ===\n")

# ============================================================
# STEP 1: Generate Market Price Data
# ============================================================

def generate_market_prices(num_records: int) -> pa.Table:
    """Generate continuous floating-point price data."""
    
    np.random.seed(42)
    
    symbols = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "JPM", "BAC", "GS"]
    
    # Continuous prices — every value is unique
    bid_prices = np.random.uniform(50, 500, num_records).round(4)
    ask_prices = bid_prices + np.random.uniform(0.01, 2.0, num_records).round(4)
    last_prices = bid_prices + np.random.uniform(0, 1.0, num_records).round(4)
    vwap = ((bid_prices + ask_prices) / 2).round(4)
    spread = (ask_prices - bid_prices).round(6)
    volatility = np.random.uniform(0.01, 0.5, num_records).round(6)
    
    table = pa.table({
        "symbol": np.random.choice(symbols, num_records).tolist(),
        "bid_price": bid_prices.tolist(),
        "ask_price": ask_prices.tolist(),
        "last_price": last_prices.tolist(),
        "vwap": vwap.tolist(),
        "spread": spread.tolist(),
        "volatility": volatility.tolist(),
    })
    
    return table

print("Generating 10 million market price records...")
start_time = time.time()
prices = generate_market_prices(10000000)
gen_time = time.time() - start_time
print(f"Generated in {gen_time:.3f} seconds")
print(f"Rows: {len(prices):,}")

# ============================================================
# STEP 2: Plain Encoding Benchmark
# ============================================================

print("\n--- Plain Encoding Benchmark ---")

temp_dir = tempfile.mkdtemp()

# Plain encoding
path_plain = os.path.join(temp_dir, "plain.parquet")
start_time = time.time()
pq.write_table(prices, path_plain, compression="snappy", use_dictionary=False)
plain_write_time = time.time() - start_time
plain_size = os.path.getsize(path_plain)

# Dictionary encoding (for comparison)
path_dict = os.path.join(temp_dir, "dict.parquet")
start_time = time.time()
pq.write_table(prices, path_dict, compression="snappy", use_dictionary=True)
dict_write_time = time.time() - start_time
dict_size = os.path.getsize(path_dict)

print(f"\n  Plain encoding:")
print(f"    Size: {plain_size / 1024 / 1024:.2f} MB")
print(f"    Write time: {plain_write_time:.3f}s")

print(f"\n  Dictionary encoding:")
print(f"    Size: {dict_size / 1024 / 1024:.2f} MB")
print(f"    Write time: {dict_write_time:.3f}s")

# ============================================================
# STEP 3: Read & Query Performance
# ============================================================

print("\n--- Query Performance ---")

# Query: Read specific columns only
start_time = time.time()
result = pq.read_table(
    path_plain,
    columns=["symbol", "bid_price", "ask_price", "spread"],
    filters=[("symbol", "=", "JPM")]
)
query_time = time.time() - start_time

print(f"\n  Filtered query (symbol=JPM):")
print(f"    Rows: {result.num_rows:,}")
print(f"    Time: {query_time:.3f}s")

# Aggregate query
start_time = time.time()
result = pq.read_table(path_plain, columns=["symbol", "bid_price"])
df = result.to_pandas()
agg = df.groupby("symbol")["bid_price"].agg(["mean", "min", "max"])
agg_time = time.time() - start_time

print(f"\n  Aggregation by symbol:")
print(f"    Time: {agg_time:.3f}s")
print(agg.to_string())

# ============================================================
# STEP 4: Why Plain Wins for Floats
# ============================================================

print("\n--- Why Plain Wins for Floating-Point Data ---")

print("""
ANALYSIS:
  Float64 values: every value is unique (continuous distribution)
  Dictionary would store: 10M unique floats + 10M indices = WORSE
  
  Plain encoding:
    10M × 8 bytes = 80 MB raw
    + Snappy compression = ~60 MB
    
  Dictionary encoding:
    10M unique dictionary entries × 8 bytes = 80 MB dictionary
    + 10M × 4 bytes indices = 40 MB indices  
    + Snappy compression = ~70 MB (WORSE!)
    
  Plain wins because:
    ✓ No dictionary overhead for unique values
    ✓ Snappy finds patterns in raw float bytes
    ✓ Zero decode overhead (direct byte access)
    ✓ Fastest possible write speed
""")

# Cleanup
import shutil
shutil.rmtree(temp_dir, ignore_errors=True)
```

---

## Encoding 2: Dictionary Encoding

### How It Works

Dictionary encoding builds a **lookup table** of unique values, then stores **integer indices** instead of the original values. This is the most powerful encoding for columns with **few unique values repeated millions of times**.

```
Original: ["COMPLETED", "COMPLETED", "PENDING", "COMPLETED", "FAILED"]
           5 strings × 8 bytes avg = 40 bytes

Dictionary: {0: "COMPLETED", 1: "PENDING", 2: "FAILED"}
            3 strings × 8 bytes = 24 bytes

Indices: [0, 0, 1, 0, 2]
         5 integers × 4 bytes = 20 bytes

Total: 24 + 20 = 44 bytes (worse for 5 rows!)

But for 1 million rows:
  Without: 1M × 8 bytes = 8 MB
  With:    3 × 8 + 1M × 4 = 4 MB (50% savings!)
```

### Parquet Dictionary Page Structure

```
Column Chunk (status column):
  ├── Dictionary Page (max 1 per chunk)
  │   └── ["COMPLETED", "PENDING", "FAILED"]  ← unique values
  │
  ├── Data Page 0
  │   └── [0, 0, 1, 0, 2, 0, 1, 0, ...]     ← indices
  │
  ├── Data Page 1
  │   └── [0, 0, 0, 2, 1, 0, ...]            ← indices
  │
  └── Data Page 2
      └── [0, 1, 0, 0, 0, 2, ...]            ← indices
```

### When to Use

- ✅ **Low cardinality** (< 30% unique values): status, currency, country, type
- ✅ **Categorical data**: fixed set of options
- ✅ **Boolean-like columns**: True/False, Yes/No

### When NOT to Use

- ❌ **High cardinality** (> 50% unique): transaction_id, UUID, email
- ❌ **Free text**: descriptions, comments
- ❌ **When dictionary exceeds page size limit** (auto-fallback to plain)

### Configuration

```python
pq.write_table(
    table,
    path,
    use_dictionary=True,                    # Enable (default: True)
    dictionary_pagesize_limit=1_048_576,    # 1MB max dictionary page
    write_statistics=True,                  # Enable min/max stats
)
```

---

### 🏦 Banking Scenario 1: Dictionary Encoding for Payment Status Tracking

#### Scenario
A bank processes **1 billion payment transactions monthly**. The `status` column has only 5 unique values but 1 billion rows. Dictionary encoding reduces the status column from 8 GB to ~4 GB (50% savings).

#### Why Dictionary Encoding?

- 5 unique values × 1B rows = 99.9999995% repetition
- Dictionary: 5 strings (40 bytes) + 1B indices (4 GB) = 4 GB
- Plain: 1B × 8 bytes = 8 GB
- **50% reduction** on status alone

#### Python Code

```python
"""
Banking Scenario: Dictionary Encoding for Payment Status
1 billion rows, 5 unique values — dictionary encoding is essential
"""

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
import numpy as np
import random
import time
import os
import tempfile

print("=== DICTIONARY ENCODING: PAYMENT STATUS ===\n")

# ============================================================
# STEP 1: Generate Payment Data with Status Column
# ============================================================

def generate_payment_data(num_records: int) -> pa.Table:
    """Generate payment data — status column is low cardinality."""
    
    statuses = ["COMPLETED", "COMPLETED", "COMPLETED", "COMPLETED", "COMPLETED",
                "PENDING", "PENDING", "PROCESSING", "FAILED", "REVERSED"]
    channels = ["ACH", "WIRE", "SWIFT", "SEPA", "RTGS", "CHIPS", "BACS"]
    currencies = ["USD", "EUR", "GBP", "JPY", "CHF"]
    
    np.random.seed(42)
    
    table = pa.table({
        "payment_id": pa.array(range(1, num_records + 1), type=pa.int64()),
        "status": pa.array(np.random.choice(statuses, num_records), type=pa.string()),
        "channel": pa.array(np.random.choice(channels, num_records), type=pa.string()),
        "currency": pa.array(np.random.choice(currencies, num_records), type=pa.string()),
        "amount": pa.array(np.random.uniform(100, 1000000, num_records).round(2), type=pa.float64()),
    })
    
    return table

print("Generating 5 million payment records...")
start_time = time.time()
payments = generate_payment_data(5000000)
gen_time = time.time() - start_time
print(f"Generated in {gen_time:.3f} seconds")

# ============================================================
# STEP 2: Analyze Cardinality
# ============================================================

print("\n--- Cardinality Analysis ---")

print(f"\n  {'Column':<15} {'Unique':>10} {'Total':>10} {'Cardinality':>12} {'Best Encoding'}")
print(f"  {'-'*15} {'-'*10} {'-'*10} {'-'*12} {'-'*16}")

for col_name in payments.column_names:
    col = payments.column(col_name)
    unique = len(set(col.to_pylist()))
    total = len(col)
    card = unique / total
    
    if card < 0.01:
        best = "Dictionary ★"
    elif card < 0.3:
        best = "Dictionary"
    else:
        best = "Plain"
    
    print(f"  {col_name:<15} {unique:>10,} {total:>10,} {card:>11.4f} {best}")

# ============================================================
# STEP 3: Dictionary Encoding Impact
# ============================================================

print("\n--- Dictionary Encoding Impact ---")

temp_dir = tempfile.mkdtemp()

# With dictionary (default)
path_dict = os.path.join(temp_dir, "with_dict.parquet")
pq.write_table(payments, path_dict, compression="snappy", use_dictionary=True)
size_dict = os.path.getsize(path_dict)

# Without dictionary
path_plain = os.path.join(temp_dir, "without_dict.parquet")
pq.write_table(payments, path_plain, compression="snappy", use_dictionary=False)
size_plain = os.path.getsize(path_plain)

print(f"\n  With dictionary:    {size_dict / 1024 / 1024:.2f} MB")
print(f"  Without dictionary: {size_plain / 1024 / 1024:.2f} MB")
print(f"  Dictionary saves:   {(1 - size_dict / size_plain) * 100:.1f}%")

# ============================================================
# STEP 4: Per-Column Dictionary Analysis
# ============================================================

print("\n--- Per-Column Dictionary Analysis ---")

print(f"\n  {'Column':<15} {'Without Dict':>14} {'With Dict':>12} {'Savings':>10}")
print(f"  {'-'*15} {'-'*14} {'-'*12} {'-'*10}")

for col_name in payments.column_names:
    col = payments.column(col_name)
    
    temp_table = pa.table({col_name: col})
    
    path1 = os.path.join(temp_dir, f"{col_name}_dict.parquet")
    path2 = os.path.join(temp_dir, f"{col_name}_plain.parquet")
    
    pq.write_table(temp_table, path1, compression="snappy", use_dictionary=True)
    pq.write_table(temp_table, path2, compression="snappy", use_dictionary=False)
    
    s_dict = os.path.getsize(path1)
    s_plain = os.path.getsize(path2)
    savings = (1 - s_dict / s_plain) * 100 if s_plain > 0 else 0
    
    print(f"  {col_name:<15} {s_plain/1024:>12.1f}KB {s_dict/1024:>10.1f}KB {savings:>8.1f}%")

# ============================================================
# STEP 5: Query Performance with Dictionary
# ============================================================

print("\n--- Query Performance ---")

# Status filter (dictionary makes this fast)
start_time = time.time()
result = pq.read_table(
    path_dict,
    columns=["payment_id", "status", "amount"],
    filters=[("status", "=", "COMPLETED")]
)
filter_time = time.time() - start_time

print(f"\n  Filter status=COMPLETED:")
print(f"    Rows: {result.num_rows:,}")
print(f"    Time: {filter_time:.3f}s")

# Multi-condition filter
start_time = time.time()
result = pq.read_table(
    path_dict,
    columns=["payment_id", "status", "channel", "amount"],
    filters=[("status", "=", "COMPLETED"), ("channel", "=", "WIRE")]
)
multi_time = time.time() - start_time

print(f"\n  Filter status=COMPLETED AND channel=WIRE:")
print(f"    Rows: {result.num_rows:,}")
print(f"    Time: {multi_time:.3f}s")

# ============================================================
# STEP 6: Summary
# ============================================================

print(f"\n--- Summary ---")

print(f"""
DICTIONARY ENCODING RESULTS:

Dataset: {len(payments):,} records
Overall savings: {(1 - size_dict / size_plain) * 100:.1f}%

WHEN DICTIONARY WINS:
  ✓ status: 5 unique values → 50%+ savings
  ✓ channel: 7 unique values → significant savings
  ✓ currency: 5 unique values → significant savings
  ✓ Any categorical column with <30% unique values

HOW IT WORKS IN PARQUET:
  1. Dictionary page stores unique values ONCE
  2. Data pages store integer indices (4 bytes each)
  3. Dictionary page is loaded once, cached for all data pages
  4. Predicate pushdown uses dictionary for fast equality checks

FALLBACK BEHAVIOR:
  If dictionary exceeds 1MB → falls back to plain encoding
  This handles high-cardinality columns automatically
""")

# Cleanup
import shutil
shutil.rmtree(temp_dir, ignore_errors=True)
```

---

### 🏦 Banking Scenario 2: Dictionary Encoding for Credit Card Merchant Categories

#### Scenario
A bank's **credit card division** stores 2 billion transactions monthly. The `merchant_category` column has only 50 unique values (GROCERY, RESTAURANT, GAS_STATION, etc.) across 2B rows. Dictionary encoding reduces this column from 16 GB to ~800 MB.

#### Why Dictionary Encoding?

- 50 unique categories × 2B rows = extreme repetition
- Dictionary: 50 strings (~500 bytes) + 2B indices (8 GB) = 8 GB
- Plain: 2B × 8 bytes = 16 GB
- **50% reduction** just for merchant_category

#### Python Code

```python
"""
Banking Scenario: Dictionary Encoding for Merchant Categories
2 billion rows, 50 unique categories — massive memory savings
"""

import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
import time
import os
import tempfile

print("=== DICTIONARY ENCODING: MERCHANT CATEGORIES ===\n")

# ============================================================
# STEP 1: Generate Credit Card Transaction Data
# ============================================================

def generate_card_data(num_records: int) -> pa.Table:
    """Generate credit card data with low-cardinality categories."""
    
    np.random.seed(42)
    
    merchant_categories = [
        "GROCERY", "RESTAURANT", "GAS_STATION", "ONLINE_RETAIL", "HOTEL",
        "AIRLINE", "SUBSCRIPTION", "HEALTHCARE", "EDUCATION", "ENTERTAINMENT",
        "CLOTHING", "ELECTRONICS", "HOME_IMPROVEMENT", "PHARMACY", "UTILITIES",
        "COFFEE_SHOP", "FAST_FOOD", "DEPARTMENT_STORE", "SPORTING_GOODS", "BOOKSTORE",
        "AUTO_REPAIR", "INSURANCE", "TAXI_RIDE", "PARKING", "LAUNDRY",
        "GYM", "SALON", "VETERINARY", "PET_STORE", "GARDEN_CENTER",
        "FURNITURE", "JEWELRY", "TOY_STORE", "HARDWARE_STORE", "ART_SUPPLY",
        "MUSIC_STORE", "VIDEO_RENTAL", "TRAVEL_AGENCY", "CRUISE_LINE", "CAR_RENTAL",
        "HARDWARE_SOFTWARE", "CLOUD_SERVICES", "DOMAIN_HOSTING", "ADVERTISING", "MARKETING",
        "LEGAL_SERVICES", "ACCOUNTING", "CONSULTING", "REAL_ESTATE", "CONSTRUCTION"
    ]
    
    card_networks = ["VISA", "MASTERCARD", "AMEX", "DISCOVER"]
    channels = ["POS", "ONLINE", "MOBILE", "ATM", "RECURRING"]
    
    table = pa.table({
        "transaction_id": pa.array(range(1, num_records + 1), type=pa.int64()),
        "merchant_category": pa.array(np.random.choice(merchant_categories, num_records), type=pa.string()),
        "card_network": pa.array(np.random.choice(card_networks, num_records), type=pa.string()),
        "channel": pa.array(np.random.choice(channels, num_records), type=pa.string()),
        "amount": pa.array(np.random.exponential(50, num_records).round(2), type=pa.float64()),
    })
    
    return table

print("Generating 10 million card transactions...")
start_time = time.time()
card_data = generate_card_data(10000000)
gen_time = time.time() - start_time
print(f"Generated in {gen_time:.3f} seconds")

# ============================================================
# STEP 2: Dictionary Encoding for merchant_category
# ============================================================

print("\n--- Dictionary Encoding Analysis ---")

temp_dir = tempfile.mkdtemp()

# With dictionary
path_dict = os.path.join(temp_dir, "with_dict.parquet")
pq.write_table(card_data, path_dict, compression="snappy", use_dictionary=True)
size_dict = os.path.getsize(path_dict)

# Without dictionary
path_plain = os.path.join(temp_dir, "without_dict.parquet")
pq.write_table(card_data, path_plain, compression="snappy", use_dictionary=False)
size_plain = os.path.getsize(path_plain)

print(f"\n  With dictionary:    {size_dict / 1024 / 1024:.2f} MB")
print(f"  Without dictionary: {size_plain / 1024 / 1024:.2f} MB")
print(f"  Savings:            {(1 - size_dict / size_plain) * 100:.1f}%")

# ============================================================
# STEP 3: Per-Column Savings Breakdown
# ============================================================

print("\n--- Per-Column Savings Breakdown ---")

print(f"\n  {'Column':<20} {'Unique':>10} {'Without':>12} {'With':>12} {'Savings':>10}")
print(f"  {'-'*20} {'-'*10} {'-'*12} {'-'*12} {'-'*10}")

for col_name in card_data.column_names:
    col = card_data.column(col_name)
    unique = len(set(col.to_pylist()))
    
    temp = pa.table({col_name: col})
    p1 = os.path.join(temp_dir, f"{col_name}_d.parquet")
    p2 = os.path.join(temp_dir, f"{col_name}_p.parquet")
    
    pq.write_table(temp, p1, compression="snappy", use_dictionary=True)
    pq.write_table(temp, p2, compression="snappy", use_dictionary=False)
    
    s1 = os.path.getsize(p1)
    s2 = os.path.getsize(p2)
    savings = (1 - s1 / s2) * 100 if s2 > 0 else 0
    
    print(f"  {col_name:<20} {unique:>10,} {s2/1024:>10.1f}KB {s1/1024:>10.1f}KB {savings:>8.1f}%")

# ============================================================
# STEP 4: Aggregation Performance with Dictionary
# ============================================================

print("\n--- Aggregation Performance ---")

start_time = time.time()
result = pq.read_table(path_dict, columns=["merchant_category", "amount"])
df = result.to_pandas()
agg = df.groupby("merchant_category")["amount"].agg(["sum", "count", "mean"])
agg = agg.sort_values("sum", ascending=False)
agg_time = time.time() - start_time

print(f"\n  Top 10 merchant categories by volume ({agg_time:.3f}s):")
print(agg.head(10).to_string())

# ============================================================
# STEP 5: Summary
# ============================================================

print(f"\n--- Summary ---")

print(f"""
DICTIONARY ENCODING FOR MERCHANT CATEGORIES:

Dataset: {len(card_data):,} card transactions
Merchant categories: 50 unique values

Memory Impact:
  Without dictionary: {size_plain / 1024 / 1024:.2f} MB
  With dictionary:    {size_dict / 1024 / 1024:.2f} MB
  Savings:            {(1 - size_dict / size_plain) * 100:.1f}%

SCALING TO 2 BILLION ROWS:
  Without dictionary: ~16 GB (2B × 8 bytes)
  With dictionary:    ~8 GB (2B × 4 bytes index + 500 bytes dict)
  Savings:            ~8 GB (50% reduction)

BENEFITS:
  ✓ 50% less storage for categorical columns
  ✓ Faster filtering (compare integers, not strings)
  ✓ Better compression (integer patterns compress well)
  ✓ Parquet auto-applies dictionary to all string columns by default
""")

# Cleanup
import shutil
shutil.rmtree(temp_dir, ignore_errors=True)
```

---

## Encoding 3: Run-Length Encoding (RLE)

### How It Works

RLE compresses **consecutive runs of identical values** into (value, count) pairs. Instead of storing the same value N times, store it once with a count.

```
Original: [A, A, A, A, B, B, B, C, C, C, C, C]
            ── 4 A's ── ── 3 B's ── ── 5 C's ──

RLE: [(A, 4), (B, 3), (C, 5)]
      12 values → 3 pairs

For integers:
Original: [5, 5, 5, 5, 5, 3, 3, 3, 7, 7, 7, 7, 7, 7]
RLE:      [(5, 5), (3, 3), (7, 6)]
           14 values → 3 pairs
```

### When RLE Is Most Effective

```
Best case — sorted data:
  [0, 0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2]
  → [(0, 5), (1, 4), (2, 3)]  — massive compression

Worst case — no consecutive repeats:
  [0, 1, 0, 1, 0, 1, 0, 1]
  → [(0,1), (1,1), (0,1), (1,1), (0,1), (1,1), (0,1), (1,1)]
  — no compression at all (actually LARGER)
```

### When to Use

- ✅ **Sorted columns** (timestamps, IDs in order)
- ✅ **Batch-processed data** (same type in each batch)
- ✅ **Time-series** with periodic patterns
- ✅ **Log files** with repeated event types

### When NOT to Use

- ❌ Random/unsorted data (no consecutive repeats)
- ❌ High-entropy data (every value different)
- ❌ Data with frequent value changes

---

### 🏦 Banking Scenario 1: RLE for Sorted Audit Log Events

#### Scenario
A bank's **audit log** records every system event chronologically. Events are logged in batches — hundreds of "TRANSFER" events, then "PAYMENT" events, then "WITHDRAWAL" events. The `event_type` column has long consecutive runs perfect for RLE.

#### Why RLE?

- Events are logged in batches → long consecutive runs
- 5 event types × 10M rows → each type appears in 2M-row blocks
- RLE compresses 10M values into ~20 pairs

#### Python Code

```python
"""
Banking Scenario: RLE for Sorted Audit Log Events
Consecutive batch-processed events compress massively
"""

import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
import time
import os
import tempfile

print("=== RLE ENCODING: SORTED AUDIT LOG ===\n")

# ============================================================
# STEP 1: Generate Sorted Audit Log Data
# ============================================================

def generate_sorted_audit_log(num_records: int) -> pa.Table:
    """Generate audit log with clustered event types (batch processing)."""
    
    np.random.seed(42)
    
    event_types = ["TRANSFER", "PAYMENT", "WITHDRAWAL", "DEPOSIT", "QUERY"]
    
    # Create clustered events (batch processing simulation)
    events = []
    batch_size = num_records // 10  # 10 batches
    for batch_idx in range(10):
        event = event_types[batch_idx % len(event_types)]
        events.extend([event] * batch_size)
    events = events[:num_records]
    
    # Sequential event IDs (sorted)
    event_ids = list(range(1, num_records + 1))
    
    # Timestamps in order
    base_timestamp = 1692844800  # epoch
    timestamps = [base_timestamp + i for i in range(num_records)]
    
    # Amounts (some nulls)
    amounts = [round(np.random.uniform(1, 100000), 2) if np.random.random() > 0.2 
               else None for _ in range(num_records)]
    
    table = pa.table({
        "event_id": pa.array(event_ids, type=pa.int64()),
        "event_type": pa.array(events, type=pa.string()),
        "timestamp": pa.array(timestamps, type=pa.int64()),
        "amount": amounts,
    })
    
    return table

print("Generating 5 million sorted audit log records...")
start_time = time.time()
audit_log = generate_sorted_audit_log(5000000)
gen_time = time.time() - start_time
print(f"Generated in {gen_time:.3f} seconds")

# ============================================================
# STEP 2: Analyze Consecutive Runs
# ============================================================

print("\n--- Consecutive Run Analysis ---")

event_types = audit_log.column("event_type").to_pylist()

# Count runs
current_event = None
run_count = 0
runs = []
for event in event_types:
    if event == current_event:
        run_count += 1
    else:
        if current_event is not None:
            runs.append((current_event, run_count))
        current_event = event
        run_count = 1
if current_event:
    runs.append((current_event, run_count))

print(f"\n  Total records: {len(event_types):,}")
print(f"  Total runs: {len(runs)}")
print(f"  Average run length: {len(event_types) / len(runs):,.0f}")

print(f"\n  Run details:")
for event, count in runs:
    bar = "█" * min(count // 10000, 50)
    print(f"    {event:<15} {count:>10,} consecutive  {bar}")

# ============================================================
# STEP 3: RLE Compression Analysis
# ============================================================

print("\n--- RLE Compression Analysis ---")

# Simulate RLE on event_type
print(f"\n  Without RLE:")
print(f"    Storage: {len(event_types)} strings × ~10 bytes = {len(event_types) * 10 / 1024 / 1024:.1f} MB")

print(f"\n  With RLE:")
print(f"    Storage: {len(runs)} pairs × (10 bytes + 4 bytes) = {len(runs) * 14 / 1024:.1f} KB")
print(f"    Compression: {len(event_types) * 10 / (len(runs) * 14):.0f}x smaller!")

# ============================================================
# STEP 4: RLE Impact on Parquet File Size
# ============================================================

print("\n--- Parquet File Size Comparison ---")

temp_dir = tempfile.mkdtemp()

# Unsorted version (random order — RLE won't help)
import random
random.seed(42)
unsorted_events = random.sample(event_types, len(event_types))
unsorted_log = pa.table({
    "event_id": pa.array(event_ids, type=pa.int64()),
    "event_type": pa.array(unsorted_events, type=pa.string()),
    "timestamp": pa.array(timestamps, type=pa.int64()),
    "amount": audit_log.column("amount"),
})

path_unsorted = os.path.join(temp_dir, "unsorted.parquet")
pq.write_table(unsorted_log, path_unsorted, compression="snappy")
size_unsorted = os.path.getsize(path_unsorted)

# Sorted version (RLE helps)
path_sorted = os.path.join(temp_dir, "sorted.parquet")
pq.write_table(audit_log, path_sorted, compression="snappy")
size_sorted = os.path.getsize(path_sorted)

print(f"\n  Unsorted (RLE ineffective): {size_unsorted / 1024 / 1024:.2f} MB")
print(f"  Sorted (RLE effective):     {size_sorted / 1024 / 1024:.2f} MB")
print(f"  RLE savings:                {(1 - size_sorted / size_unsorted) * 100:.1f}%")

# ============================================================
# STEP 5: Query Performance on Sorted Data
# ============================================================

print("\n--- Query Performance ---")

# Filter by event type (RLE helps skip entire runs)
start_time = time.time()
result = pq.read_table(
    path_sorted,
    columns=["event_id", "event_type", "timestamp"],
    filters=[("event_type", "=", "TRANSFER")]
)
filter_time = time.time() - start_time

print(f"\n  Filter event_type=TRANSFER:")
print(f"    Rows: {result.num_rows:,}")
print(f"    Time: {filter_time:.3f}s")

# ============================================================
# STEP 6: Summary
# ============================================================

print(f"\n--- Summary ---")

print(f"""
RLE ENCODING RESULTS:

Dataset: {len(audit_log):,} sorted audit records
Consecutive runs: {len(runs)} (avg {len(event_types) // len(runs):,} per run)

Compression:
  Unsorted: {size_unsorted / 1024 / 1024:.2f} MB
  Sorted:   {size_sorted / 1024 / 1024:.2f} MB
  RLE savings: {(1 - size_sorted / size_unsorted) * 100:.1f}%

WHEN RLE WINS:
  ✓ Sorted/grouped data (long consecutive runs)
  ✓ Batch-processed events (same type in each batch)
  ✓ Time-series data with periodic patterns
  ✓ Log files with repeated event types

WHEN RLE LOSES:
  ✗ Random/unsorted data (no consecutive repeats)
  ✗ High-entropy data (every value different)
  ✗ Data with frequent value alternation

ARROW/PARQUET INTEGRATION:
  Arrow automatically applies RLE to dictionary indices
  when data is sorted — you get both for free!
""")

# Cleanup
import shutil
shutil.rmtree(temp_dir, ignore_errors=True)
```

---

### 🏦 Banking Scenario 2: RLE for Compliance Status in Regulatory Reports

#### Scenario
A bank generates **monthly regulatory reports** with compliance status for each transaction. Reports are generated in batch — all "COMPLIANT" transactions first, then "NON_COMPLIANT", then "UNDER_REVIEW". RLE compresses the sorted compliance status from 8 GB to < 100 KB.

#### Why RLE?

- Batch-generated reports → perfect clustering
- 3 compliance statuses × 1B rows → each type in 333M-row blocks
- RLE: 3 pairs × 8 bytes = 24 bytes total!

#### Python Code

```python
"""
Banking Scenario: RLE for Compliance Status in Regulatory Reports
Batch-generated compliance data — extreme RLE compression
"""

import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
import time
import os
import tempfile

print("=== RLE ENCODING: COMPLIANCE STATUS ===\n")

# ============================================================
# STEP 1: Generate Batch-Processed Compliance Data
# ============================================================

def generate_compliance_data(num_records: int) -> pa.Table:
    """Generate compliance data with batch-clustered statuses."""
    
    np.random.seed(42)
    
    compliance_statuses = ["COMPLIANT", "NON_COMPLIANT", "UNDER_REVIEW"]
    
    # Batch processing: each status in large consecutive blocks
    statuses = []
    batch_size = num_records // 3
    for status in compliance_statuses:
        statuses.extend([status] * batch_size)
    statuses = statuses[:num_records]
    
    table = pa.table({
        "transaction_id": pa.array(range(1, num_records + 1), type=pa.int64()),
        "compliance_status": pa.array(statuses, type=pa.string()),
        "amount": pa.array(np.random.uniform(100, 1000000, num_records).round(2), type=pa.float64()),
        "risk_score": pa.array(np.random.uniform(0, 1, num_records).round(4), type=pa.float64()),
    })
    
    return table

print("Generating 10 million compliance records...")
start_time = time.time()
compliance_data = generate_compliance_data(10000000)
gen_time = time.time() - start_time
print(f"Generated in {gen_time:.3f} seconds")

# ============================================================
# STEP 2: RLE Compression Analysis
# ============================================================

print("\n--- RLE Compression Analysis ---")

statuses = compliance_data.column("compliance_status").to_pylist()

# Count runs
runs = []
current = None
count = 0
for s in statuses:
    if s == current:
        count += 1
    else:
        if current:
            runs.append((current, count))
        current = s
        count = 1
if current:
    runs.append((current, count))

print(f"\n  Consecutive runs: {len(runs)}")
print(f"  Run details:")
for status, cnt in runs:
    print(f"    {status:<20} {cnt:>12,} consecutive records")

# ============================================================
# STEP 3: Parquet File Size Comparison
# ============================================================

print("\n--- Parquet Size Comparison ---")

temp_dir = tempfile.mkdtemp()

# Sorted (RLE effective)
path_sorted = os.path.join(temp_dir, "sorted.parquet")
pq.write_table(compliance_data, path_sorted, compression="snappy")
size_sorted = os.path.getsize(path_sorted)

# Random order (RLE ineffective)
import random
random.seed(42)
shuffled = compliance_data.to_pandas().sample(frac=1, random_state=42).reset_index(drop=True)
shuffled_arrow = pa.Table.from_pandas(shuffled)

path_random = os.path.join(temp_dir, "random.parquet")
pq.write_table(shuffled_arrow, path_random, compression="snappy")
size_random = os.path.getsize(path_random)

print(f"\n  Sorted (RLE):   {size_sorted / 1024 / 1024:.2f} MB")
print(f"  Random (no RLE): {size_random / 1024 / 1024:.2f} MB")
print(f"  RLE savings:     {(1 - size_sorted / size_random) * 100:.1f}%")

# ============================================================
# STEP 4: Query: Find Non-Compliant Transactions
# ============================================================

print("\n--- Query: Non-Compliant Transactions ---")

start_time = time.time()
result = pq.read_table(
    path_sorted,
    columns=["transaction_id", "compliance_status", "amount", "risk_score"],
    filters=[("compliance_status", "=", "NON_COMPLIANT")]
)
query_time = time.time() - start_time

print(f"\n  Non-compliant transactions:")
print(f"    Rows: {result.num_rows:,}")
print(f"    Time: {query_time:.3f}s")

df = result.to_pandas()
print(f"    Total amount: ${df['amount'].sum():,.2f}")
print(f"    Avg risk score: {df['risk_score'].mean():.4f}")

# ============================================================
# STEP 5: Summary
# ============================================================

print(f"\n--- Summary ---")

print(f"""
RLE ENCODING FOR COMPLIANCE STATUS:

Dataset: {len(compliance_data):,} records
Consecutive runs: {len(runs)} (avg {len(statuses) // len(runs):,} per run)

File Sizes:
  Sorted (RLE):    {size_sorted / 1024 / 1024:.2f} MB
  Random (no RLE): {size_random / 1024 / 1024:.2f} MB
  Savings:         {(1 - size_sorted / size_random) * 100:.1f}%

WHY THIS WORKS:
  ✓ Batch processing creates perfect clustering
  ✓ Each compliance status occupies ~333M consecutive rows
  ✓ RLE compresses 333M values into a single (value, count) pair
  ✓ Combined with dictionary encoding: extreme compression

SCALING TO 1 BILLION ROWS:
  Sorted + RLE:  ~50 KB (3 pairs × 8 bytes + dictionary)
  Random:        ~8 GB (1B × 8 bytes)
  Compression:   160,000x smaller!
""")

# Cleanup
import shutil
shutil.rmtree(temp_dir, ignore_errors=True)
```

---

## Encoding 4: Bit-Packing Encoding

### How It Works

Bit-packing stores small integers using the **minimum number of bits** needed, rather than a full byte (8 bits) or int32 (32 bits) per value.

```
Original: [0, 3, 1, 2, 0, 3, 1, 2]  (values range 0-3, need only 2 bits)

Plain encoding: 8 bytes (1 byte per value)
Bit-packed:     2 bytes (8 values × 2 bits = 16 bits = 2 bytes)

Savings: 75%!
```

### How It Works in Detail

```
Values: [0, 1, 2, 3, 0, 1, 2, 3]
         Need 2 bits each (0-3 range)

Binary (8 bits each, plain):
  00000000 00000001 00000010 00000011 00000000 00000001 00000010 00000011
  = 8 bytes

Bit-packed (2 bits each):
  00 01 10 11 00 01 10 11
  = 2 bytes

How to decode:
  Read 2 bits → value 00 = 0
  Read 2 bits → value 01 = 1
  Read 2 bits → value 10 = 2
  Read 2 bits → value 11 = 3
  ...and so on
```

### Bit Width Selection

| Value Range | Bits Needed | Max Value | Example Use |
|-------------|-------------|-----------|-------------|
| 0-1 | 1 bit | 1 | Boolean flags |
| 0-3 | 2 bits | 3 | 4-state status |
| 0-7 | 3 bits | 7 | 8-state enum |
| 0-15 | 4 bits | 15 | 16-state enum |
| 0-31 | 5 bits | 31 | Rating scale (0-5) |
| 0-63 | 6 bits | 63 | Month number |
| 0-255 | 8 bits | 255 | Byte-range values |

### When to Use

- ✅ **Small integer ranges** (0-7, 0-15, 0-31)
- ✅ **Encoded/coded values** (status codes, flags, ratings)
- ✅ **Dictionary indices** (Parquet applies bit-packing to dictionary indices automatically)
- ✅ **Boolean arrays** (1 bit per value)

### When NOT to Use

- ❌ Large integers (int32, int64) — bit-packing adds overhead
- ❌ Floating-point numbers — not applicable
- ❌ Strings — not applicable

---

### 🏦 Banking Scenario 1: Bit-Packing for Risk Rating Scores

#### Scenario
A bank assigns **risk ratings** to each transaction on a scale of 0-7 (8 levels). With 500 million transactions, plain encoding uses 4 bytes per rating (int32), but bit-packing uses only 3 bits — a **93% reduction**.

#### Why Bit-Packing?

- Risk ratings: 0-7 → only 3 bits needed (not 32 bits)
- 500M rows × 3 bits = 187 MB (vs 2 GB with int32)
- Combined with dictionary encoding for string categories → maximum compression

#### Python Code

```python
"""
Banking Scenario: Bit-Packing for Risk Rating Scores
Small integer ranges (0-7) — 93% reduction with bit-packing
"""

import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
import time
import os
import tempfile

print("=== BIT-PACKING: RISK RATING SCORES ===\n")

# ============================================================
# STEP 1: Generate Risk Rating Data
# ============================================================

def generate_risk_data(num_records: int) -> pa.Table:
    """Generate transaction data with small integer risk ratings."""
    
    np.random.seed(42)
    
    # Risk ratings: 0-7 (8 levels)
    risk_ratings = np.random.randint(0, 8, num_records)
    
    # Risk categories (derived from rating)
    risk_categories = []
    for r in risk_ratings:
        if r <= 2:
            risk_categories.append("LOW")
        elif r <= 5:
            risk_categories.append("MEDIUM")
        else:
            risk_categories.append("HIGH")
    
    table = pa.table({
        "transaction_id": pa.array(range(1, num_records + 1), type=pa.int64()),
        "risk_rating": pa.array(risk_ratings, type=pa.int8()),   # int8 = 1 byte
        "risk_category": pa.array(risk_categories, type=pa.string()),
        "amount": pa.array(np.random.uniform(100, 1000000, num_records).round(2), type=pa.float64()),
    })
    
    return table

print("Generating 5 million risk-rated transactions...")
start_time = time.time()
risk_data = generate_risk_data(5000000)
gen_time = time.time() - start_time
print(f"Generated in {gen_time:.3f} seconds")

# ============================================================
# STEP 2: Analyze Bit-Packing Potential
# ============================================================

print("\n--- Bit-Packing Analysis ---")

risk_ratings = risk_data.column("risk_rating").to_pylist()
min_val = min(risk_ratings)
max_val = max(risk_ratings)

import math
bits_needed = math.ceil(math.log2(max_val + 1)) if max_val > 0 else 1

print(f"\n  Risk rating range: {min_val} - {max_val}")
print(f"  Bits needed per value: {bits_needed}")
print(f"  Values representable: {2**bits_needed}")

print(f"\n  Storage comparison (per value):")
print(f"    Plain int8:    8 bits (1 byte)")
print(f"    Plain int32:   32 bits (4 bytes)")
print(f"    Bit-packed:    {bits_needed} bits")
print(f"    Savings vs int32: {(1 - bits_needed / 32) * 100:.1f}%")
print(f"    Savings vs int8:  {(1 - bits_needed / 8) * 100:.1f}%")

# ============================================================
# STEP 3: Parquet File Size with Bit-Packing
# ============================================================

print("\n--- Parquet Size with Different Types ---")

temp_dir = tempfile.mkdtemp()

# int8 (1 byte per value)
path_int8 = os.path.join(temp_dir, "int8.parquet")
table_int8 = pa.table({
    "transaction_id": risk_data.column("transaction_id"),
    "risk_rating": risk_data.column("risk_rating").cast(pa.int8()),
    "risk_category": risk_data.column("risk_category"),
    "amount": risk_data.column("amount"),
})
pq.write_table(table_int8, path_int8, compression="snappy")
size_int8 = os.path.getsize(path_int8)

# int32 (4 bytes per value)
path_int32 = os.path.join(temp_dir, "int32.parquet")
table_int32 = pa.table({
    "transaction_id": risk_data.column("transaction_id"),
    "risk_rating": risk_data.column("risk_rating").cast(pa.int32()),
    "risk_category": risk_data.column("risk_category"),
    "amount": risk_data.column("amount"),
})
pq.write_table(table_int32, path_int32, compression="snappy")
size_int32 = os.path.getsize(path_int32)

print(f"\n  risk_rating as int32: {size_int32 / 1024 / 1024:.2f} MB")
print(f"  risk_rating as int8:  {size_int8 / 1024 / 1024:.2f} MB")
print(f"  Savings with int8:    {(1 - size_int8 / size_int32) * 100:.1f}%")

# ============================================================
# STEP 4: Query: High-Risk Transactions
# ============================================================

print("\n--- Query: High-Risk Transactions ---")

start_time = time.time()
result = pq.read_table(
    path_int8,
    columns=["transaction_id", "risk_rating", "risk_category", "amount"],
    filters=[("risk_rating", ">=", 6)]
)
query_time = time.time() - start_time

print(f"\n  High-risk transactions (rating >= 6):")
print(f"    Rows: {result.num_rows:,}")
print(f"    Time: {query_time:.3f}s")

df = result.to_pandas()
print(f"    Total amount: ${df['amount'].sum():,.2f}")
print(f"    Avg risk rating: {df['risk_rating'].mean():.1f}")

# ============================================================
# STEP 5: Summary
# ============================================================

print(f"\n--- Summary ---")

print(f"""
BIT-PACKING ENCODING RESULTS:

Dataset: {len(risk_data):,} transactions
Risk rating range: 0-7 (3 bits needed)

Storage:
  int32 (plain):  {size_int32 / 1024 / 1024:.2f} MB
  int8 (smaller): {size_int8 / 1024 / 1024:.2f} MB
  Savings:        {(1 - size_int8 / size_int32) * 100:.1f}%

PARQUET BIT-PACKING:
  Parquet automatically applies bit-packing to:
  ✓ Dictionary indices (always small integers)
  ✓ Boolean arrays (1 bit per value)
  ✓ Small-range integers when encoding is set to BIT_PACKED

WHEN TO USE:
  ✓ Ratings, scores, levels (0-N where N < 32)
  ✓ Status codes, flags, enums
  ✓ Boolean arrays (1 bit per value)
  ✓ Any small integer range

SCALING TO 500 MILLION ROWS:
  int32: 2 GB
  int8:  500 MB (75% savings)
  Bit-packed (3 bits): 187 MB (91% savings)
""")

# Cleanup
import shutil
shutil.rmtree(temp_dir, ignore_errors=True)
```

---

### 🏦 Banking Scenario 2: Bit-Packing for Transaction Channel Flags

#### Scenario
A bank uses **4-bit channel codes** to identify transaction channels (ATM=0, MOBILE=1, WEB=2, BRANCH=3, UPI=4, NEFT=5, RTGS=6, SWIFT=7). With 100 million transactions, bit-packing reduces the channel column from 400 MB to 50 MB.

#### Why Bit-Packing?

- 8 channel codes → 3 bits per value (0-7)
- 100M rows × 3 bits = 37.5 MB (vs 400 MB with int32)
- Combined with dictionary for channel names → maximum efficiency

#### Python Code

```python
"""
Banking Scenario: Bit-Packing for Transaction Channel Codes
8 channel codes (0-7) — 3 bits per value, 87.5% reduction
"""

import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
import time
import os
import tempfile

print("=== BIT-PACKING: CHANNEL CODES ===\n")

# ============================================================
# STEP 1: Generate Channel Code Data
# ============================================================

def generate_channel_data(num_records: int) -> pa.Table:
    """Generate transaction data with small integer channel codes."""
    
    np.random.seed(42)
    
    # 8 channels → codes 0-7 (3 bits)
    channel_codes = np.random.randint(0, 8, num_records)
    channel_names = ["ATM", "MOBILE", "WEB", "BRANCH", "UPI", "NEFT", "RTGS", "SWIFT"]
    
    table = pa.table({
        "transaction_id": pa.array(range(1, num_records + 1), type=pa.int64()),
        "channel_code": pa.array(channel_codes, type=pa.int8()),
        "channel_name": pa.array([channel_names[c] for c in channel_codes], type=pa.string()),
        "amount": pa.array(np.random.uniform(100, 1000000, num_records).round(2), type=pa.float64()),
    })
    
    return table

print("Generating 10 million channel-coded transactions...")
start_time = time.time()
channel_data = generate_channel_data(10000000)
gen_time = time.time() - start_time
print(f"Generated in {gen_time:.3f} seconds")

# ============================================================
# STEP 2: Bit-Packing Analysis
# ============================================================

print("\n--- Bit-Packing Analysis ---")

print(f"\n  Channel codes: 0-7")
print(f"  Bits needed: 3 (log2(8) = 3)")
print(f"  Values per byte: 2 (8 bits / 3 bits ≈ 2.67, packed as 2 per byte)")

print(f"\n  Storage per 100M values:")
print(f"    int32 (plain): 400 MB (100M × 4 bytes)")
print(f"    int8 (plain):  100 MB (100M × 1 byte)")
print(f"    Bit-packed:     37.5 MB (100M × 3 bits)")
print(f"    Savings vs int32: 90.6%")
print(f"    Savings vs int8:  62.5%")

# ============================================================
# STEP 3: Parquet File Size Comparison
# ============================================================

print("\n--- Parquet File Size ---")

temp_dir = tempfile.mkdtemp()

# int8 version
path_int8 = os.path.join(temp_dir, "int8.parquet")
table_int8 = pa.table({
    "transaction_id": channel_data.column("transaction_id"),
    "channel_code": channel_data.column("channel_code").cast(pa.int8()),
    "channel_name": channel_data.column("channel_name"),
    "amount": channel_data.column("amount"),
})
pq.write_table(table_int8, path_int8, compression="snappy")
size_int8 = os.path.getsize(path_int8)

# int32 version
path_int32 = os.path.join(temp_dir, "int32.parquet")
table_int32 = pa.table({
    "transaction_id": channel_data.column("transaction_id"),
    "channel_code": channel_data.column("channel_code").cast(pa.int32()),
    "channel_name": channel_data.column("channel_name"),
    "amount": channel_data.column("amount"),
})
pq.write_table(table_int32, path_int32, compression="snappy")
size_int32 = os.path.getsize(path_int32)

print(f"\n  channel_code as int32: {size_int32 / 1024 / 1024:.2f} MB")
print(f"  channel_code as int8:  {size_int8 / 1024 / 1024:.2f} MB")
print(f"  Savings:               {(1 - size_int8 / size_int32) * 100:.1f}%")

# ============================================================
# STEP 4: Query: UPI Transactions
# ============================================================

print("\n--- Query: UPI Transactions ---")

start_time = time.time()
result = pq.read_table(
    path_int8,
    columns=["transaction_id", "channel_name", "amount"],
    filters=[("channel_code", "=", 4)]  # UPI = code 4
)
query_time = time.time() - start_time

print(f"\n  UPI transactions (code=4):")
print(f"    Rows: {result.num_rows:,}")
print(f"    Time: {query_time:.3f}s")

# ============================================================
# STEP 5: Summary
# ============================================================

print(f"\n--- Summary ---")

print(f"""
BIT-PACKING FOR CHANNEL CODES:

Dataset: {len(channel_data):,} transactions
Channel codes: 0-7 (3 bits needed)

Storage:
  int32: {size_int32 / 1024 / 1024:.2f} MB
  int8:  {size_int8 / 1024 / 1024:.2f} MB
  Savings: {(1 - size_int8 / size_int32) * 100:.1f}%

KEY INSIGHT:
  Use the smallest integer type that fits your data:
  ✓ Risk ratings (0-7): int8 (1 byte)
  ✓ Channel codes (0-7): int8 (1 byte)
  ✓ Boolean flags (0/1): bool (1 bit in Parquet!)
  ✓ Ratings (0-5): int8 (1 byte)

PARQUET AUTOMATICALLY:
  ✓ Bit-packs dictionary indices
  ✓ Bit-packs boolean arrays
  ✓ Applies BIT_PACKED encoding when configured
""")

# Cleanup
import shutil
shutil.rmtree(temp_dir, ignore_errors=True)
```

---

## Encoding 5: Delta Encoding

### How It Works

Delta encoding stores **differences between consecutive values** instead of the values themselves. For monotonically increasing data (timestamps, sequential IDs), deltas are small numbers that compress extremely well.

```
Original:  [1000, 1005, 1003, 1008, 1012]
Deltas:    [1000, +5, -2, +5, +4]
            ↑base  ↑ differences

For timestamps:
Original:  [1692844800, 1692844801, 1692844802, 1692844803]
Deltas:    [1692844800, +1, +1, +1]
            ↑base       ↑ tiny deltas!
```

### How It Works in Detail

```
Sequential IDs: [1001, 1002, 1003, 1004, 1005]
                  ↓
Delta base: 1001
Deltas:    [0, 1, 1, 1, 1]    ← all deltas = 1

Bit-packing: values 0-1 need only 1 bit each
  5 values × 1 bit = 5 bits (vs 40 bytes with int64!)

Compression: [1001, 1, 1, 1, 1] → extremely compressible
```

### When to Use

- ✅ **Monotonically increasing sequences**: IDs, timestamps, row numbers
- ✅ **Sequential data**: time-series, ordered logs
- ✅ **Near-sequential data**: IDs with small gaps

### When NOT to Use

- ❌ **Random data**: deltas are large, no savings
- ❌ **Decreasing sequences**: negative deltas don't compress as well
- ❌ **Data with large jumps**: deltas larger than original values

---

### 🏦 Banking Scenario 1: Delta Encoding for Sequential Transaction IDs

#### Scenario
A bank generates **sequential transaction IDs** (1001, 1002, 1003, ...). Delta encoding reduces the ID column from 8 GB (int64) to < 100 KB by storing the base value + deltas of 1.

#### Why Delta Encoding?

- IDs are sequential: deltas are all 1
- Base: 1001, deltas: [1, 1, 1, 1, ...]
- Bit-pack deltas: 1 bit each → 125 MB for 1B rows (vs 8 GB int64)

#### Python Code

```python
"""
Banking Scenario: Delta Encoding for Sequential Transaction IDs
Sequential IDs compress 80x with delta encoding
"""

import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
import time
import os
import tempfile

print("=== DELTA ENCODING: SEQUENTIAL TRANSACTION IDS ===\n")

# ============================================================
# STEP 1: Generate Sequential Transaction IDs
# ============================================================

def generate_sequential_data(num_records: int) -> pa.Table:
    """Generate data with sequential IDs — delta encoding optimal."""
    
    np.random.seed(42)
    
    # Sequential IDs (delta encoding perfect)
    transaction_ids = list(range(1001, 1001 + num_records))
    
    # Sequential timestamps (delta encoding optimal)
    base_timestamp = 1692844800  # 2023-08-24 epoch
    timestamps = [base_timestamp + i for i in range(num_records)]
    
    table = pa.table({
        "transaction_id": pa.array(transaction_ids, type=pa.int64()),
        "timestamp": pa.array(timestamps, type=pa.int64()),
        "amount": pa.array(np.random.uniform(100, 1000000, num_records).round(2), type=pa.float64()),
        "status": pa.array(np.random.choice(["COMPLETED", "PENDING"], num_records), type=pa.string()),
    })
    
    return table

print("Generating 5 million sequential transactions...")
start_time = time.time()
seq_data = generate_sequential_data(5000000)
gen_time = time.time() - start_time
print(f"Generated in {gen_time:.3f} seconds")

# ============================================================
# STEP 2: Delta Encoding Analysis
# ============================================================

print("\n--- Delta Encoding Analysis ---")

ids = seq_data.column("transaction_id").to_pylist()

# Calculate deltas
deltas = [ids[0]] + [ids[i] - ids[i-1] for i in range(1, len(ids))]
unique_deltas = set(deltas)

print(f"\n  ID range: {ids[0]} - {ids[-1]}")
print(f"  Unique delta values: {unique_deltas}")
print(f"  All deltas are: {deltas[1]} (sequential!)")

print(f"\n  Storage comparison:")
print(f"    Plain int64: {len(ids) * 8 / 1024 / 1024:.1f} MB ({len(ids)} × 8 bytes)")
print(f"    Delta base + deltas: 8 bytes + {len(ids) * 4 / 1024 / 1024:.1f} MB (if int32 deltas)")
print(f"    Delta + bit-pack (1 bit): 8 bytes + {len(ids) * 1 / 8 / 1024 / 1024:.1f} MB")

# ============================================================
# STEP 3: Parquet File Size Comparison
# ============================================================

print("\n--- Parquet Size: Delta vs Plain ---")

temp_dir = tempfile.mkdtemp()

# Plain encoding
path_plain = os.path.join(temp_dir, "plain.parquet")
pq.write_table(seq_data, path_plain, compression="snappy", use_dictionary=False)
size_plain = os.path.getsize(path_plain)

# Default Parquet (with dictionary + auto encodings)
path_default = os.path.join(temp_dir, "default.parquet")
pq.write_table(seq_data, path_default, compression="snappy")
size_default = os.path.getsize(path_default)

print(f"\n  Plain (no encoding):  {size_plain / 1024 / 1024:.2f} MB")
print(f"  Default (auto):       {size_default / 1024 / 1024:.2f} MB")
print(f"  Auto encoding savings: {(1 - size_default / size_plain) * 100:.1f}%")

# ============================================================
# STEP 4: Query: Time-Range Scan
# ============================================================

print("\n--- Query: Time-Range Scan ---")

start_time = time.time()
result = pq.read_table(
    path_default,
    columns=["transaction_id", "timestamp", "amount"],
    filters=[("timestamp", ">=", 1692845000), ("timestamp", "<=", 1692846000)]
)
query_time = time.time() - start_time

print(f"\n  Time-range scan (1000 seconds):")
print(f"    Rows: {result.num_rows:,}")
print(f"    Time: {query_time:.3f}s")

# ============================================================
# STEP 5: Summary
# ============================================================

print(f"\n--- Summary ---")

print(f"""
DELTA ENCODING FOR SEQUENTIAL IDS:

Dataset: {len(seq_data):,} sequential transactions
ID range: {ids[0]} - {ids[-1]} (delta = 1 for all)

Storage:
  Plain:    {size_plain / 1024 / 1024:.2f} MB
  Default:  {size_default / 1024 / 1024:.2f} MB

WHEN DELTA WINS:
  ✓ Sequential IDs (1, 2, 3, 4, ...)
  ✓ Timestamps in order (epoch, epoch+1, epoch+2, ...)
  ✓ Auto-incrementing primary keys
  ✓ Time-series data with regular intervals

HOW PARQUET USES DELTA:
  Parquet has DELTA_BINARY_PACKED and DELTA_LENGTH_BYTE_ARRAY
  encodings that apply delta transformation automatically
  when configured:
  
  pq.write_table(table, path, column_encoding={
      "transaction_id": "DELTA_BINARY_PACKED",
      "timestamp": "DELTA_BINARY_PACKED",
  })
""")

# Cleanup
import shutil
shutil.rmtree(temp_dir, ignore_errors=True)
```

---

### 🏦 Banking Scenario 2: Delta Encoding for Timestamps in Trade Records

#### Scenario
A bank's **trading system** logs trades with timestamps that increase by 1-10 microseconds each. Delta encoding stores the base timestamp + small microsecond deltas, reducing the timestamp column from 8 GB to < 200 MB for 1 billion trades.

#### Why Delta Encoding?

- Timestamps are sequential with tiny gaps (1-10 µs)
- Deltas: 1-10 (need only 4 bits each)
- 1B × 4 bits = 500 MB (vs 8 GB with int64)

#### Python Code

```python
"""
Banking Scenario: Delta Encoding for Trade Timestamps
Microsecond-sequential timestamps — 94% compression
"""

import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
import time
import os
import tempfile

print("=== DELTA ENCODING: TRADE TIMESTAMPS ===\n")

# ============================================================
# STEP 1: Generate Sequential Trade Timestamps
# ============================================================

def generate_trade_timestamps(num_records: int) -> pa.Table:
    """Generate trades with microsecond-sequential timestamps."""
    
    np.random.seed(42)
    
    # Base timestamp: 2023-08-24 09:30:00 UTC (market open)
    base_ts = 1692877800000000  # microseconds
    
    # Sequential timestamps with 1-10 µs gaps
    gaps = np.random.randint(1, 11, num_records)
    timestamps = np.cumsum(gaps) + base_ts
    
    table = pa.table({
        "trade_id": pa.array(range(1, num_records + 1), type=pa.int64()),
        "timestamp_us": pa.array(timestamps, type=pa.int64()),
        "price": pa.array(np.random.uniform(100, 500, num_records).round(4), type=pa.float64()),
        "volume": pa.array(np.random.randint(1, 10000, num_records), type=pa.int32()),
    })
    
    return table

print("Generating 5 million sequential trade timestamps...")
start_time = time.time()
trades = generate_trade_timestamps(5000000)
gen_time = time.time() - start_time
print(f"Generated in {gen_time:.3f} seconds")

# ============================================================
# STEP 2: Delta Analysis
# ============================================================

print("\n--- Delta Analysis ---")

timestamps = trades.column("timestamp_us").to_pylist()
deltas = [timestamps[i] - timestamps[i-1] for i in range(1, len(timestamps))]

print(f"\n  Timestamp range: {timestamps[0]:,} - {timestamps[-1]:,}")
print(f"  Gap range: {min(deltas)} - {max(deltas)} µs")
print(f"  Avg gap: {np.mean(deltas):.1f} µs")
print(f"  All deltas fit in: {max(deltas).bit_length()} bits")

print(f"\n  Storage comparison:")
print(f"    Plain int64: {len(timestamps) * 8 / 1024 / 1024:.1f} MB")
print(f"    Delta + bit-pack ({max(deltas).bit_length()} bits): ~{len(timestamps) * max(deltas).bit_length() / 8 / 1024 / 1024:.1f} MB")

# ============================================================
# STEP 3: Parquet File Size
# ============================================================

print("\n--- Parquet Size ---")

temp_dir = tempfile.mkdtemp()

# Plain encoding
path_plain = os.path.join(temp_dir, "plain.parquet")
pq.write_table(trades, path_plain, compression="snappy", use_dictionary=False)
size_plain = os.path.getsize(path_plain)

# Default (auto encodings)
path_default = os.path.join(temp_dir, "default.parquet")
pq.write_table(trades, path_default, compression="snappy")
size_default = os.path.getsize(path_default)

print(f"\n  Plain:   {size_plain / 1024 / 1024:.2f} MB")
print(f"  Default: {size_default / 1024 / 1024:.2f} MB")
print(f"  Savings: {(1 - size_default / size_plain) * 100:.1f}%")

# ============================================================
# STEP 4: Query: Time-Windowed Trade Lookup
# ============================================================

print("\n--- Query: Time-Windowed Lookup ---")

# Find trades in a 1-second window
window_start = timestamps[1000000]
window_end = window_start + 1000000  # 1 second

start_time = time.time()
result = pq.read_table(
    path_default,
    columns=["trade_id", "timestamp_us", "price", "volume"],
    filters=[("timestamp_us", ">=", window_start), ("timestamp_us", "<=", window_end)]
)
query_time = time.time() - start_time

print(f"\n  1-second window query:")
print(f"    Window: {window_start:,} - {window_end:,}")
print(f"    Rows: {result.num_rows:,}")
print(f"    Time: {query_time:.3f}s")

# ============================================================
# STEP 5: Summary
# ============================================================

print(f"\n--- Summary ---")

print(f"""
DELTA ENCODING FOR TRADE TIMESTAMPS:

Dataset: {len(trades):,} sequential trades
Timestamp gaps: {min(deltas)}-{max(deltas)} µs (fits in {max(deltas).bit_length()} bits)

Storage:
  Plain:   {size_plain / 1024 / 1024:.2f} MB
  Default: {size_default / 1024 / 1024:.2f} MB
  Savings: {(1 - size_default / size_plain) * 100:.1f}%

WHEN DELTA WINS:
  ✓ Microsecond-sequential timestamps
  ✓ Sequential trade IDs
  ✓ Ordered log entries
  ✓ Auto-incrementing sequences

KEY INSIGHT:
  Delta encoding transforms LARGE values (int64 timestamps)
  into SMALL deltas (4-bit integers), which then benefit
  from bit-packing AND compression — triple optimization!
""")

# Cleanup
import shutil
shutil.rmtree(temp_dir, ignore_errors=True)
```

---

## Encoding 6: Delta-Length Byte Array

### How It Works

Delta-Length Byte Array stores variable-length byte arrays by encoding the **lengths as deltas** and the **values contiguously**.

```
Original: ["NYC", "LA", "Chicago", "NYC"]
Lengths:  [3, 2, 7, 3]
Deltas:   [3, -1, +5, -4]  ← differences between consecutive lengths
Values:   [NYCLACHICAGONYC]  ← all bytes concatenated

Parquet stores:
  Length deltas + concatenated values
```

### When to Use

- ✅ **Strings with similar lengths** (city names, country codes)
- ✅ **Binary data with similar sizes**
- ✅ When strings are roughly the same length across rows

### When NOT to Use

- ❌ Highly variable string lengths (use plain + dictionary instead)
- ❌ Very short strings (overhead not worth it)

---

### 🏦 Banking Scenario: Delta-Length for Country Codes in International Transfers

#### Scenario
A bank processes **200 million international wire transfers**. The `origin_country` and `destination_country` columns store 2-3 character ISO codes. Delta-Length encoding stores the length deltas (all 0s for 2-char codes, +1 for 3-char codes) efficiently.

#### Python Code

```python
"""
Banking Scenario: Delta-Length Byte Array for Country Codes
Short, similar-length strings — delta-length encoding optimal
"""

import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
import time
import os
import tempfile

print("=== DELTA-LENGTH: COUNTRY CODES ===\n")

# ============================================================
# STEP 1: Generate International Transfer Data
# ============================================================

def generate_intl_transfers(num_records: int) -> pa.Table:
    """Generate international transfer data with country codes."""
    
    np.random.seed(42)
    
    # 2-3 character country codes (ISO 3166-1 alpha-2/3)
    countries_2char = ["US", "GB", "DE", "FR", "JP", "AU", "CA", "SG", "HK", "CH"]
    countries_3char = ["IND", "BRA", "MEX", "ZAF", "ARE", "SAU", "TUR", "IDN"]
    
    all_countries = countries_2char + countries_3char
    country_weights = [0.15] * len(countries_2char) + [0.05] * len(countries_3char)
    
    origin_countries = np.random.choice(all_countries, num_records, p=country_weights)
    dest_countries = np.random.choice(all_countries, num_records, p=country_weights)
    
    table = pa.table({
        "transfer_id": pa.array(range(1, num_records + 1), type=pa.int64()),
        "origin_country": pa.array(origin_countries, type=pa.string()),
        "dest_country": pa.array(dest_countries, type=pa.string()),
        "amount_usd": pa.array(np.random.uniform(1000, 50000000, num_records).round(2), type=pa.float64()),
        "currency": pa.array(np.random.choice(["USD", "EUR", "GBP", "JPY"], num_records), type=pa.string()),
    })
    
    return table

print("Generating 10 million international transfers...")
start_time = time.time()
intl_data = generate_intl_transfers(10000000)
gen_time = time.time() - start_time
print(f"Generated in {gen_time:.3f} seconds")

# ============================================================
# STEP 2: String Length Analysis
# ============================================================

print("\n--- String Length Analysis ---")

origin_lengths = [len(s) for s in intl_data.column("origin_country").to_pylist()]
dest_lengths = [len(s) for s in intl_data.column("dest_country").to_pylist()]

print(f"\n  origin_country:")
print(f"    Length distribution: {set(origin_lengths)}")
print(f"    Most common length: {max(set(origin_lengths), key=origin_lengths.count)} chars")

print(f"\n  dest_country:")
print(f"    Length distribution: {set(dest_lengths)}")
print(f"    Most common length: {max(set(dest_lengths), key=dest_lengths.count)} chars")

# Delta-length analysis
length_deltas = [origin_lengths[i] - origin_lengths[i-1] for i in range(1, len(origin_lengths))]
print(f"\n  Length delta range: {min(length_deltas)} to {max(length_deltas)}")
print(f"  Most common delta: {max(set(length_deltas), key=length_deltas.count)} (same length as previous)")

# ============================================================
# STEP 3: Parquet File Size
# ============================================================

print("\n--- Parquet Size ---")

temp_dir = tempfile.mkdtemp()

path_default = os.path.join(temp_dir, "default.parquet")
pq.write_table(intl_data, path_default, compression="snappy")
size_default = os.path.getsize(path_default)

path_dict = os.path.join(temp_dir, "dict_only.parquet")
pq.write_table(intl_data, path_dict, compression="snappy", use_dictionary=True)
size_dict = os.path.getsize(path_dict)

print(f"\n  Default encoding:    {size_default / 1024 / 1024:.2f} MB")
print(f"  Dictionary encoding: {size_dict / 1024 / 1024:.2f} MB")

# ============================================================
# STEP 4: Query: Transfers to Specific Country
# ============================================================

print("\n--- Query: Transfers to India ---")

start_time = time.time()
result = pq.read_table(
    path_default,
    columns=["transfer_id", "dest_country", "amount_usd"],
    filters=[("dest_country", "=", "IND")]
)
query_time = time.time() - start_time

print(f"\n  Transfers to IND:")
print(f"    Rows: {result.num_rows:,}")
print(f"    Time: {query_time:.3f}s")

df = result.to_pandas()
print(f"    Total USD: ${df['amount_usd'].sum():,.2f}")

# ============================================================
# STEP 5: Summary
# ============================================================

print(f"\n--- Summary ---")

print(f"""
DELTA-LENGTH BYTE ARRAY RESULTS:

Dataset: {len(intl_data):,} international transfers
Country code lengths: 2-3 characters (very similar)

WHEN DELTA-LENGTH WINS:
  ✓ Strings with similar lengths (ISO codes, abbreviations)
  ✓ Binary data with similar sizes
  ✓ Length deltas are small (0, ±1, ±2)

PARQUET AUTO-APPLIES:
  Delta-Length Byte Array is one of Parquet's built-in
  encodings for byte array columns. It's automatically
  considered when writing byte array / string data.
""")

# Cleanup
import shutil
shutil.rmtree(temp_dir, ignore_errors=True)
```

---

## Encoding 7: Byte Stream Split

### How It Works

Byte Stream Split takes each byte of every value and groups them into separate streams. This helps compression algorithms find patterns within each byte position.

```
Original float32 values: [1.5, 2.5, 3.5, 4.5]

Binary representation:
  1.5 → 0x3FC00000 → bytes: [3F, C0, 00, 00]
  2.5 → 0x40200000 → bytes: [40, 20, 00, 00]
  3.5 → 0x40600000 → bytes: [40, 60, 00, 00]
  4.5 → 0x40900000 → bytes: [40, 90, 00, 00]

Without split: [3F,C0,00,00, 40,20,00,00, 40,60,00,00, 40,90,00,00]
  → mixed byte patterns, hard to compress

With byte stream split:
  Byte stream 0: [3F, 40, 40, 40]  ← high bytes (similar!)
  Byte stream 1: [C0, 20, 60, 90]  ← next bytes
  Byte stream 2: [00, 00, 00, 00]  ← zero bytes (compress to nothing!)
  Byte stream 3: [00, 00, 00, 00]  ← zero bytes (compress to nothing!)
  → each stream has similar bytes → compression finds patterns
```

### When to Use

- ✅ **Floating-point columns** (prices, rates, amounts)
- ✅ **When compression ratio matters more than speed**
- ✅ **Columns with limited precision** (many trailing zeros)

### When NOT to Use

- ❌ Integer data (delta encoding is better)
- ❌ When decompression speed is critical
- ❌ Short data (< 1KB) — overhead not worth it

---

### 🏦 Banking Scenario: Byte Stream Split for Floating-Point Financial Rates

#### Scenario
A bank stores **interest rates and exchange rates** with limited decimal precision. Many values share byte patterns (e.g., all rates start with 0x40 for values 2.0-3.99). Byte Stream Split groups these bytes for better compression.

#### Python Code

```python
"""
Banking Scenario: Byte Stream Split for Floating-Point Rates
Limited-precision floats compress well with byte stream split
"""

import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
import time
import os
import tempfile

print("=== BYTE STREAM SPLIT: FLOATING-POINT RATES ===\n")

# ============================================================
# STEP 1: Generate Financial Rate Data
# ============================================================

def generate_rate_data(num_records: int) -> pa.Table:
    """Generate financial rate data with limited precision."""
    
    np.random.seed(42)
    
    # Interest rates: 0.5% - 15% with 2 decimal places
    interest_rates = np.round(np.random.uniform(0.5, 15.0, num_records), 2)
    
    # Exchange rates: 0.5 - 150 with 4 decimal places
    exchange_rates = np.round(np.random.uniform(0.5, 150.0, num_records), 4)
    
    # Volatility: 0.01 - 0.5 with 4 decimal places
    volatility = np.round(np.random.uniform(0.01, 0.5, num_records), 4)
    
    # Spread: 0.001 - 0.1 with 6 decimal places
    spread = np.round(np.random.uniform(0.001, 0.1, num_records), 6)
    
    table = pa.table({
        "rate_id": pa.array(range(1, num_records + 1), type=pa.int64()),
        "interest_rate": pa.array(interest_rates, type=pa.float64()),
        "exchange_rate": pa.array(exchange_rates, type=pa.float64()),
        "volatility": pa.array(volatility, type=pa.float64()),
        "spread": pa.array(spread, type=pa.float64()),
    })
    
    return table

print("Generating 10 million financial rate records...")
start_time = time.time()
rate_data = generate_rate_data(10000000)
gen_time = time.time() - start_time
print(f"Generated in {gen_time:.3f} seconds")

# ============================================================
# STEP 2: Byte Pattern Analysis
# ============================================================

print("\n--- Byte Pattern Analysis ---")

# Analyze byte patterns for interest_rate
rates = rate_data.column("interest_rate").to_pylist()[:1000]

print(f"\n  Interest rate byte patterns (first 10 values):")
for rate in rates[:10]:
    import struct
    bytes_repr = struct.pack('>d', rate)
    hex_str = ' '.join(f'{b:02X}' for b in bytes_repr)
    print(f"    {rate:>8.2f}% → {hex_str}")

# Count byte frequency for first byte position
first_bytes = [struct.pack('>d', r)[0] for r in rates]
byte_counts = {}
for b in first_bytes:
    byte_counts[b] = byte_counts.get(b, 0) + 1

print(f"\n  First byte frequency (top 5):")
for byte_val, count in sorted(byte_counts.items(), key=lambda x: -x[1])[:5]:
    print(f"    0x{byte_val:02X}: {count} occurrences ({count/len(rates)*100:.1f}%)")

# ============================================================
# STEP 3: Parquet Size Comparison
# ============================================================

print("\n--- Parquet Size Comparison ---")

temp_dir = tempfile.mkdtemp()

# With byte stream split (Parquet default for floats)
path_split = os.path.join(temp_dir, "split.parquet")
pq.write_table(rate_data, path_split, compression="snappy")
size_split = os.path.getsize(path_split)

# Without byte stream split
path_nosplit = os.path.join(temp_dir, "nosplit.parquet")
pq.write_table(rate_data, path_nosplit, compression="snappy")
size_nosplit = os.path.getsize(path_nosplit)

print(f"\n  With byte stream split:    {size_split / 1024 / 1024:.2f} MB")
print(f"  Without byte stream split: {size_nosplit / 1024 / 1024:.2f} MB")

if size_nosplit > size_split:
    print(f"  Byte stream split saves:   {(1 - size_split / size_nosplit) * 100:.1f}%")
else:
    print(f"  Byte stream split overhead: {(size_split / size_nosplit - 1) * 100:.1f}%")

# ============================================================
# STEP 4: Query: Rate Lookup
# ============================================================

print("\n--- Query: Rate Lookup ---")

start_time = time.time()
result = pq.read_table(
    path_split,
    columns=["rate_id", "interest_rate", "exchange_rate"],
    filters=[("interest_rate", ">", 10.0)]
)
query_time = time.time() - start_time

print(f"\n  Interest rates > 10%:")
print(f"    Rows: {result.num_rows:,}")
print(f"    Time: {query_time:.3f}s")

# ============================================================
# STEP 5: Summary
# ============================================================

print(f"\n--- Summary ---")

print(f"""
BYTE STREAM SPLIT RESULTS:

Dataset: {len(rate_data):,} financial rate records
Float precision: 2-6 decimal places

Storage:
  With split:    {size_split / 1024 / 1024:.2f} MB
  Without split: {size_nosplit / 1024 / 1024:.2f} MB

WHEN BYTE STREAM SPLIT WINS:
  ✓ Floating-point columns with limited precision
  ✓ Rates, prices, amounts with many trailing zeros
  ✓ Large float columns (>1MB)
  ✓ When compression ratio matters more than speed

HOW IT WORKS:
  1. Each float64 = 8 bytes
  2. Split into 8 byte streams (one per byte position)
  3. Each stream contains similar bytes → compresses well
  4. Zero bytes compress to almost nothing

PARQUET DEFAULT:
  Byte Stream Split is automatically applied to
  FLOAT and DOUBLE columns in Parquet.
""")

# Cleanup
import shutil
shutil.rmtree(temp_dir, ignore_errors=True)
```

---

## Encoding Decision Matrix

| Data Pattern | Best Encoding | Why | Savings |
|-------------|---------------|-----|---------|
| Unique IDs (sequential) | Delta | Deltas = 1 | 90%+ |
| Unique IDs (random) | Plain | No pattern to exploit | 0% |
| Low-cardinality strings | Dictionary | Store values once | 50-70% |
| High-cardinality strings | Plain | Dictionary adds overhead | 0% |
| Sorted consecutive repeats | RLE | (value, count) pairs | 80-99% |
| Small integers (0-N) | Bit-Packing | Fewer bits per value | 50-90% |
| Monotonic sequences | Delta | Small differences | 80-95% |
| Similar-length strings | Delta-Length | Length deltas = 0 | 20-40% |
| Floating-point data | Byte Stream Split | Byte patterns compress | 10-30% |
| Boolean arrays | Bit-Packing | 1 bit per value | 87.5% |

### How Parquet Chains Encodings

```
Example: status column (sorted, 1B rows, 3 unique values)

Step 1: Dictionary Encoding
  → 3 unique strings + 1B integer indices
  → 24 bytes + 4 GB = 4 GB

Step 2: RLE on dictionary indices
  → 3 runs: [(0, 800M), (1, 150M), (2, 50M)]
  → 24 bytes (!!!)

Step 3: Bit-Packing (values 0-2 need 2 bits)
  → 1B × 2 bits = 250 MB

Step 4: Snappy/Zstd Compression
  → ~150 MB

Total: 8 GB raw → 150 MB on disk = 53x compression
```

---

## Interview Questions

### Q1: What is the difference between encoding and compression in Parquet?

**Answer:**

**Encoding** is a logical transformation that exploits data structure:
- Dictionary: replaces strings with integer IDs
- RLE: stores (value, count) pairs
- Bit-Packing: packs small integers into fewer bits
- Delta: stores differences between consecutive values

**Compression** is a general-purpose algorithm applied after encoding:
- Snappy, Gzip, Zstd, LZ4, Brotli
- Works on encoded bytes, not original data

**Pipeline**: Raw Data → Encoding → Compression → Disk

**Example**:
```
Status column: ["COMPLETED", "COMPLETED", "PENDING", ...]
  ↓ Dictionary Encoding
Indices: [0, 0, 1, ...]  (4 bytes each)
  ↓ Bit-Packing
Packed: [00, 00, 01, ...]  (2 bits each)
  ↓ Snappy Compression
Compressed bytes (smallest)
```

---

### Q2: When would you choose dictionary encoding over plain encoding?

**Answer:**

**Choose dictionary when:**
- Column has < 30% unique values (low cardinality)
- Examples: status, currency, country, channel, type

**Choose plain when:**
- Column has > 50% unique values (high cardinality)
- Examples: transaction_id, UUID, email, amount

**The math:**
```
Dictionary: unique_values × avg_string_bytes + total_rows × 4 bytes (index)
Plain: total_rows × avg_string_bytes

Dictionary wins when:
  unique_values × avg_string_bytes < total_rows × (avg_string_bytes - 4)
```

**Real example:**
```
status (3 unique, 1B rows):
  Dictionary: 3 × 8 + 1B × 4 = 4 GB
  Plain: 1B × 8 = 8 GB
  Dictionary wins by 50%

transaction_id (1B unique, 1B rows):
  Dictionary: 1B × 15 + 1B × 4 = 19 GB
  Plain: 1B × 15 = 15 GB
  Plain wins by 21%
```

---

### Q3: How does RLE help with sorted data in Parquet?

**Answer:**

Sorted data creates long consecutive runs of identical values:

```
Sorted event types: [TRANSFER, TRANSFER, ..., PAYMENT, PAYMENT, ...]
                     ←── 100M TRANSFER ──→←── 50M PAYMENT ──→
```

RLE compresses runs:
```
Without RLE: [0, 0, 0, ..., 0, 1, 1, ..., 1]  → 1B × 4 bytes = 4 GB
With RLE:    [(0, 100000000), (1, 50000000)]    → 2 × 8 bytes = 16 bytes
Compression ratio: 250,000,000x!
```

**Arrow/PARQUET integration:** Arrow automatically applies RLE to dictionary indices when writing Parquet. If your data is sorted, you get RLE compression for free.

---

### Q4: What is bit-packing and when is it useful?

**Answer:**

Bit-packing stores small integers using the minimum bits needed:

```
Values 0-3: need 2 bits each (not 8 bits)
Values 0-7: need 3 bits each
Values 0-15: need 4 bits each

Example (values 0-3):
  Plain: [00000000, 00000011, 00000001, 00000010]  = 4 bytes
  Packed: [00, 11, 01, 10]  = 1 byte (75% savings)
```

**Useful for:**
- Risk ratings (0-7): 3 bits instead of 32 bits
- Channel codes (0-7): 3 bits instead of 32 bits
- Boolean arrays: 1 bit instead of 8 bits
- Dictionary indices: always small integers

**Parquet applies bit-packing automatically** to dictionary indices and when BIT_PACKED encoding is configured.

---

### Q5: When would you use delta encoding in Parquet?

**Answer:**

**Delta encoding is optimal for monotonically increasing data:**

```
Sequential IDs: [1001, 1002, 1003, 1004]
Deltas: [1001, 1, 1, 1]  ← base + tiny deltas

Timestamps: [1692844800, 1692844801, 1692844802]
Deltas: [1692844800, 1, 1]  ← base + tiny deltas
```

**When to use:**
- Sequential primary keys
- Timestamps in chronological order
- Auto-incrementing row numbers
- Time-series data with regular intervals

**When NOT to use:**
- Random data (deltas are large)
- Data with large gaps (deltas > original values)
- Non-sequential data

**Configuration:**
```python
pq.write_table(table, path, column_encoding={
    "transaction_id": "DELTA_BINARY_PACKED",
    "timestamp": "DELTA_BINARY_PACKED",
})
```

---

### Q6: How does Parquet's encoding fallback mechanism work?

**Answer:**

Parquet has automatic fallback when dictionary encoding becomes inefficient:

1. Parquet starts building a dictionary for a column
2. If dictionary exceeds `dictionary_pagesize_limit` (default: 1MB), it falls back to **plain encoding**
3. The column chunk is written without dictionary encoding

**Configuration:**
```python
pq.write_table(
    table,
    path,
    use_dictionary=True,                    # Enable (default)
    dictionary_pagesize_limit=1_048_576,    # 1MB limit
)
```

**Why this matters:**
- High-cardinality columns (transaction_id, UUID) automatically fall back
- Low-cardinality columns (status, currency) stay dictionary-encoded
- You don't need to manually configure encoding per column
- Parquet handles the optimal encoding selection automatically

---

## Summary

| Aspect | Key Point |
|--------|-----------|
| **Plain Encoding** | Raw bytes, no transformation, fastest decode |
| **Dictionary Encoding** | Store values once + integer indices, 50-70% savings |
| **RLE Encoding** | (value, count) pairs for consecutive repeats, 80-99% savings |
| **Bit-Packing** | Fewer bits per small integer, 50-90% savings |
| **Delta Encoding** | Differences between values, 80-95% for sequential data |
| **Delta-Length** | Length deltas for similar-length strings, 20-40% savings |
| **Byte Stream Split** | Split float bytes for better compression, 10-30% savings |
| **Encoding Fallback** | Dictionary auto-falls back to plain when dictionary > 1MB |
| **Chaining** | Multiple encodings apply sequentially (dictionary → RLE → bit-pack → compress) |
| **Auto-Selection** | Parquet auto-selects optimal encoding based on data patterns |
