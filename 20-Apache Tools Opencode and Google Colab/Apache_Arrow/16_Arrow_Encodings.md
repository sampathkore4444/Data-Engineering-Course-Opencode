# Concept 16: Arrow Encodings

## 📚 Detailed Explanation

**Arrow Encodings** define how data is physically represented in memory (RAM). Arrow uses a set of encoding strategies to store columnar data efficiently. Each encoding is optimized for a specific data pattern — the right encoding can reduce memory usage by 30-70% while speeding up queries.

### What is an Encoding?

An encoding is a **rule for translating data into bytes** in memory. Arrow doesn't store data as raw text — it uses specialized binary layouts that the CPU can read directly.

```
Raw Python List:    ["active", "active", "inactive", "active"]
                        ↓ Arrow Encoding
Memory Layout:      Dictionary: {0: "active", 1: "inactive"}
                    Indices:    [0, 0, 1, 0]
```

### Why Encodings Matter

**Without encoding awareness:**
```
Memory: 100 GB
Query: 10 seconds
CPU: 100%
```

**With proper encoding:**
```
Memory: 40 GB (60% less)
Query: 2 seconds (5x faster)
CPU: 40% (60% less)
```

### The 5 Arrow In-Memory Encodings

| # | Encoding | Use Case | Memory Pattern |
|---|----------|----------|----------------|
| 1 | **Flat (Raw) Encoding** | Fixed-width primitives | Direct byte array |
| 2 | **Offset Encoding** | Variable-length data | Offsets + Values buffers |
| 3 | **Dictionary Encoding** | Low-cardinality strings | Dictionary + Indices |
| 4 | **Validity Bitmap** | Null value tracking | 1 bit per value |
| 5 | **Run-Length Encoding (RLE)** | Repeated consecutive values | Value + Count pairs |

> **Important:** Encodings 1-4 are native to Arrow's in-memory format. Encoding 5 (RLE) is used internally by Arrow for boolean arrays and by Parquet on disk. Arrow also uses RLE for dictionary indices internally.

---

## 🔍 Encoding 1: Flat (Flat Buffer) Encoding

### How It Works

Flat encoding stores fixed-width values **directly as contiguous bytes** — no offsets, no dictionary, no indirection. Each value occupies the same number of bytes.

```
int64 column (8 bytes per value):
  [1001][1002][1003][1004][1005]
  ↑     ↑     ↑     ↑     ↑
  8B    8B    8B    8B    8B   = 40 bytes total

float64 column (8 bytes per value):
  [50000.00][75000.00][60000.00]
  ↑         ↑         ↑
  8B        8B        8B        = 24 bytes total

bool column (1 byte per value):
  [1][0][1][1][0]
  ↑  ↑  ↑  ↑  ↑
  1B 1B 1B 1B 1B = 5 bytes total
```

### Supported Types

| Type | Bytes per Value | Example |
|------|----------------|---------|
| `int8` | 1 | Small integers (-128 to 127) |
| `int16` | 2 | Medium integers |
| `int32` | 4 | Standard integers |
| `int64` | 8 | Large integers |
| `uint8` | 1 | Unsigned small integers |
| `uint16` | 2 | Unsigned medium integers |
| `uint32` | 4 | Unsigned standard integers |
| `uint64` | 8 | Unsigned large integers |
| `float16` | 2 | Half precision |
| `float32` | 4 | Single precision |
| `float64` | 8 | Double precision |
| `bool` | 1 | True/False |

### When to Use

- ✅ All fixed-width numeric types (int, float, bool)
- ✅ Data where every value is unique (no repeats to exploit)
- ✅ When maximum read speed is needed (zero decode overhead)

### Memory Layout Diagram

```
Buffer (raw bytes):
┌────────┬────────┬────────┬────────┬────────┐
│ 1001   │ 1002   │ 1003   │ 1004   │ 1005   │
│ 8 bytes│ 8 bytes│ 8 bytes│ 8 bytes│ 8 bytes│
└────────┴────────┴────────┴────────┴────────┘
  byte 0   byte 8   byte 16  byte 24  byte 32

CPU reads: direct pointer dereference + type cast
No parsing. No lookup. Just read.
```

---

## 🔍 Encoding 2: Offset Encoding (Offsets + Values)

### How It Works

Offset encoding solves the problem of **variable-length data** (strings, binary, lists). Since each value has a different byte length, Arrow uses two buffers:

1. **Offsets Buffer**: Stores the starting position of each value
2. **Values Buffer**: Stores all values concatenated

```
Strings: ["Alice", "Bob", "Charlie", "David"]

Offsets Buffer: [0][5][8][15]
                 ↑  ↑  ↑   ↑
                 │  │  │   └─ "David" starts at byte 15
                 │  │  └───── "Charlie" starts at byte 8
                 │  └──────── "Bob" starts at byte 5
                 └─────────── "Alice" starts at byte 0
                 (N+1 entries for N values)

Values Buffer:  [A][l][i][c][e][B][o][b][C][h][a][r][l][i][e][D][a][v][i][d]
                  ←───────────── 19 bytes ──────────────────────→

To read "Charlie": 
  start = offsets[2] = 8
  end   = offsets[3] = 15
  value = values[8:15] = "Charlie"
```

### Supported Types

| Type | Description |
|------|-------------|
| `utf8` | Variable-length strings (up to 2GB) |
| `large_utf8` | Variable-length strings (up to 4GB) |
| `binary` | Variable-length byte arrays |
| `large_binary` | Large variable-length byte arrays |
| `list` | Variable-length lists |
| `large_list` | Large variable-length lists |
| `map` | Key-value pairs |

### When to Use

- ✅ Any variable-length data (strings, binary)
- ✅ When values have different sizes
- ✅ Always needed — there's no alternative for variable-length types

### Memory Layout Diagram

```
String Array: ["Alice", "Bob", "Charlie", "David"]

Offsets (int32, 4 bytes each):       Values (raw bytes):
┌─────┬─────┬─────┬─────┬─────┐     ┌─┬─┬─┬─┬─┬─┬─┬─┬─┬─┬─┬─┬─┬─┬─┬─┬─┬─┬─┐
│  0  │  5  │  8  │ 15  │  —  │     │A│l│i│c│e│B│o│b│C│h│a│r│l│i│e│D│a│v│i│d│
└─────┴─────┴─────┴─────┴─────┘     └─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┘
  5 entries (N+1) × 4 bytes          19 bytes
  = 20 bytes                         

Total: 20 + 19 = 39 bytes

How to decode:
  "Alice"   → offsets[0..1] = values[0:5]   = "Alice"
  "Bob"     → offsets[1..2] = values[5:8]   = "Bob"
  "Charlie" → offsets[2..3] = values[8:15]  = "Charlie"
  "David"   → offsets[3..4] = values[15:19] = "David"
```

---

## 🔍 Encoding 3: Dictionary Encoding

### How It Works

Dictionary encoding stores **unique values once** in a dictionary, then references them by integer indices. This is the most powerful encoding for **low-cardinality** data (few unique values repeated many times).

```
Original: ["active", "active", "inactive", "active", "active"]
           5 strings × 8 bytes avg = 40 bytes

With Dictionary:
  Dictionary: ["active", "inactive"]     ← 2 unique strings
  Indices:    [0, 0, 1, 0, 0]            ← 5 integers (4 bytes each)

  Dictionary: 2 × 8 bytes = 16 bytes
  Indices:    5 × 4 bytes = 20 bytes
  Total:      36 bytes (vs 40 bytes) — 10% savings

With 1 million rows (99% "active"):
  Without: 1,000,000 × 8 bytes = 8 MB
  With:    2 × 8 + 1,000,000 × 4 = 4 MB (50% savings!)
```

### When to Use

- ✅ **Low cardinality** (few unique values): status, country, category, type
- ✅ **High repetition**: same values appear thousands/millions of times
- ✅ Categorical data with fixed set of options

### When NOT to Use

- ❌ **High cardinality** (many unique values): customer_id, transaction_id
- ❌ **Unique identifiers**: every row has a different value
- ❌ **Free text**: descriptions, comments, emails

### Memory Layout Diagram

```
Status Column: ["active", "active", "inactive", "active", "active"]

WITHOUT Dictionary Encoding (Flat):
┌────────┬────────┬────────┬────────┬────────┐
│"active"│"active"│"inact" │"active"│"active"│
│ 8 bytes│ 8 bytes│ 8 bytes│ 8 bytes│ 8 bytes│
└────────┴────────┴────────┴────────┴────────┘
Total: 40 bytes

WITH Dictionary Encoding:
Dictionary Buffer:
┌──────────┬───────────┐
│ "active" │"inactive" │
│ 8 bytes  │ 9 bytes   │
└──────────┴───────────┘
= 17 bytes

Indices Buffer (int32):
┌─────┬─────┬─────┬─────┬─────┐
│  0  │  0  │  1  │  0  │  0  │
└─────┴─────┴─────┴─────┴─────┘
= 20 bytes

Total: 17 + 20 = 37 bytes (7% savings on 5 rows)

At scale (1M rows, 99% "active"):
  Without: ~8 MB
  With:    ~4 MB (50% savings!)
```

---

## 🔍 Encoding 4: Validity Bitmap (Null Bitmap)

### How It Works

Every Arrow array can have an optional **validity bitmap** that tracks which values are null. Each bit represents one value: `1` = valid, `0` = null.

```
Values:  [1001, NULL, 1003, NULL, 1005]
Bitmap:  [1,    0,    1,    0,    1   ]
          ↑     ↑     ↑     ↑     ↑
        valid null  valid null  valid
```

### When to Use

- ✅ Always — it's applied **on top of** any other encoding
- ✅ Sparse data (many nulls)
- ✅ When you need to distinguish NULL from 0 or empty string

### Memory Cost

- **1 bit per value** (not 1 byte!)
- 1 million values = 125 KB bitmap
- Very cheap: 125 KB for 1 million null flags

### Memory Layout Diagram

```
Values:  [1001, NULL, 1003, NULL, 1005]

Validity Bitmap (1 bit per value):
Bit position:  0    1    2    3    4
Binary:       [1][  0][  1][  0][  1]
              ↑    ↑    ↑    ↑    ↑
            OK  NULL   OK  NULL   OK

Stored as bytes:
  Byte 0: [1][0][1][0][1][0][0][0]  ← packed into 1 byte (5 bits used)
  (remaining 3 bits padded with 0)

Memory: 1 byte for 5 values (vs 5 bytes if using 1 byte per value)

At scale (1M values, 30% null):
  Bitmap: 1,000,000 bits = 125 KB
  vs naive: 1,000,000 bytes = 1 MB (8x more)
```

### How Arrow Combines Encodings

Arrow layers encodings together. A single column can have **multiple encodings simultaneously**:

```
Status Column (dictionary + validity):
  Dictionary: ["active", "inactive"]
  Indices:    [0, 0, 1, 0, 0]
  Bitmap:     [1, 1, 1, 1, 0]          ← last value is NULL

When reading value[4]:
  1. Check bitmap[4] = 0 → NULL (don't even look at indices)
  2. Return null
```

---

## 🔍 Encoding 5: Run-Length Encoding (RLE)

### How It Works

RLE compresses **consecutive runs of the same value** into (value, count) pairs. Instead of storing the same value N times, store it once with a count.

```
Original:   [5, 5, 5, 5, 5, 3, 3, 3, 7, 7, 7, 7, 7, 7]
              ─── 5 times ───  ── 3 times ── ── 7 times ──

RLE encoded: [(5, 5), (3, 3), (7, 6)]
              value,count pairs

14 values → 3 pairs (79% reduction)
```

### When to Use

- ✅ **Sorted or grouped data** where same values cluster together
- ✅ Boolean arrays (true/false runs)
- ✅ Internal use by Arrow for dictionary indices
- ✅ Parquet on-disk encoding (not primary in-memory encoding)

### Memory Layout Diagram

```
Boolean Array: [True, True, True, True, False, False, True]

Without RLE:
┌───┬───┬───┬───┬───┬───┬───┐
│ 1 │ 1 │ 1 │ 1 │ 0 │ 0 │ 1 │  ← 7 bytes (1 byte each)
└───┴───┴───┴───┴───┴───┴───┘

With RLE:
┌─────────┬─────────┬─────────┐
│(True, 4)│(False,2)│(True, 1)│  ← 3 pairs
└─────────┴─────────┴─────────┘

Savings: 7 bytes → ~6 bytes (modest for short runs)
At scale with long runs: massive savings
```

---

## 📊 Encoding Decision Matrix

| Data Pattern | Best Encoding | Savings |
|-------------|---------------|---------|
| Fixed-width numbers (all unique) | Flat | Baseline (fastest) |
| Variable-length strings | Offset | Required (no alternative) |
| Few unique values, many repeats | Dictionary | 30-70% |
| Sparse data (many nulls) | Validity Bitmap | 50-90% for null flags |
| Sorted/grouped consecutive repeats | RLE | 50-90% |
| Sorted + low cardinality | Dictionary + RLE | 70-95% |

---

## 💡 Example: Encodings in Banking

### Scenario: Transaction Table

```python
import pyarrow as pa
import pyarrow.compute as pc

# Create transaction table
table = pa.table({
    "id": [1001, 1002, 1003, 1004, 1005],                    # Flat
    "customer": ["Alice", "Bob", "Charlie", "David", "Eve"],  # Offset
    "amount": [50000.00, 75000.00, 60000.00, 80000.00, 55000.00],  # Flat
    "status": ["active", "active", "inactive", "active", "active"]  # Dictionary
})
```

**Memory Layout:**
```
id Column (Flat Encoding - int64):
  [1001][1002][1003][1004][1005]  ← 40 bytes (5 × 8 bytes)

customer Column (Offset Encoding - utf8):
  Offsets: [0][5][8][15][20][23]  ← 24 bytes (6 × 4 bytes)
  Values:  [A][l][i][c][e][B][o][b][C][h][a][r][l][i][e][D][a][v][i][d][E][v][e]  ← 23 bytes
  Total:   47 bytes

amount Column (Flat Encoding - float64):
  [50000.00][75000.00][60000.00][80000.00][55000.00]  ← 40 bytes

status Column (Dictionary Encoding):
  Dictionary: ["active", "inactive"]  ← 14 bytes (8 + 6)
  Indices:    [0, 0, 1, 0, 0]         ← 20 bytes (5 × 4 bytes)
  Total:      34 bytes (vs 40 bytes flat)
```

---

## 🏦 Real-World Banking Scenario 1: Flat Encoding — High-Performance Numeric Analytics

### Scenario
A bank's **risk analytics engine** processes millions of numeric交易指标 (交易金额, 利率, 汇率) in real-time. Every value is unique — there are no repeats to exploit with dictionary encoding. The system needs maximum read speed for SIMD-vectorized calculations.

### Problem
- Millions of unique float64/int64 values per column
- No repeated values → dictionary encoding would add overhead
- Need fastest possible CPU access for vectorized math
- Memory bandwidth is the bottleneck

### Solution
Flat encoding provides **zero-overhead** access — the CPU reads values directly with no decoding step. Combined with Arrow's columnar layout, this enables full SIMD utilization.

### Python Code

```python
"""
Banking Scenario 1: Flat Encoding for High-Performance Numeric Analytics
Using Arrow Flat Buffers for Maximum SIMD Performance
"""

import pyarrow as pa
import pyarrow.compute as pc
import random
import time

# ============================================================
# STEP 1: Generate High-Cardinality Numeric Data
# ============================================================

print("=== FLAT ENCODING: NUMERIC ANALYTICS ===\n")

def generate_trading_data(num_records: int) -> pa.Table:
    """Generate trading data where every value is unique."""
    
    # Transaction IDs (unique per row)
    transaction_ids = [f"TXN-{i:012d}" for i in range(1, num_records + 1)]
    
    # Trade amounts (unique floats - no repeats)
    trade_amounts = [round(random.uniform(100, 10000000), 2) for _ in range(num_records)]
    
    # Interest rates (unique floats)
    interest_rates = [round(random.uniform(0.01, 15.0), 6) for _ in range(num_records)]
    
    # Exchange rates (unique floats)
    exchange_rates = [round(random.uniform(0.5, 2.0), 8) for _ in range(num_records)]
    
    # Trade volumes (unique int64)
    trade_volumes = [random.randint(1, 1000000) for _ in range(num_records)]
    
    # Risk scores (unique float64)
    risk_scores = [round(random.uniform(0, 1), 6) for _ in range(num_records)]
    
    # Create Arrow Table — all Flat encoding
    table = pa.table({
        "transaction_id": transaction_ids,
        "trade_amount": trade_amounts,
        "interest_rate": interest_rates,
        "exchange_rate": exchange_rates,
        "trade_volume": trade_volumes,
        "risk_score": risk_scores,
    })
    
    return table

# Generate 5 million records
print("Generating 5 million trading records...")
start_time = time.time()
trading_data = generate_trading_data(5000000)
gen_time = time.time() - start_time

print(f"Generated in {gen_time:.3f} seconds")
print(f"Rows: {len(trading_data):,}")
print(f"Total memory: {trading_data.nbytes / 1024 / 1024:.2f} MB")

# ============================================================
# STEP 2: Analyze Flat Encoding Memory Layout
# ============================================================

print("\n--- Flat Encoding Memory Analysis ---")

for col_name in trading_data.column_names:
    col = trading_data.column(col_name)
    nbytes = col.nbytes
    
    # Check if dictionary encoded
    is_dict = hasattr(col, 'dictionary') and col.dictionary is not None
    encoding_type = "Dictionary" if is_dict else "Flat"
    
    print(f"\n  {col_name}:")
    print(f"    Encoding: {encoding_type}")
    print(f"    Type: {col.type}")
    print(f"    Memory: {nbytes / 1024 / 1024:.2f} MB")
    print(f"    Bytes/value: {nbytes / len(col):.1f}")

# ============================================================
# STEP 3: SIMD-Vectorized Aggregations (Flat Encoding Advantage)
# ============================================================

print("\n--- SIMD-Vectorized Aggregations ---")

# Aggregation 1: Sum of all trade amounts
start_time = time.time()
total_amount = pc.sum(trading_data.column("trade_amount")).as_py()
sum_time = time.time() - start_time

print(f"\n  Total Trade Amount: ${total_amount:,.2f}")
print(f"  Compute time: {sum_time:.6f} seconds")

# Aggregation 2: Mean interest rate
start_time = time.time()
mean_rate = pc.mean(trading_data.column("interest_rate")).as_py()
mean_time = time.time() - start_time

print(f"  Mean Interest Rate: {mean_rate:.6f}%")
print(f"  Compute time: {mean_time:.6f} seconds")

# Aggregation 3: Standard deviation of risk scores
start_time = time.time()
std_risk = pc.stddev(trading_data.column("risk_score")).as_py()
std_time = time.time() - start_time

print(f"  Risk Score Std Dev: {std_risk:.6f}")
print(f"  Compute time: {std_time:.6f} seconds")

# Aggregation 4: Min/Max of exchange rates
start_time = time.time()
min_rate = pc.min(trading_data.column("exchange_rate")).as_py()
max_rate = pc.max(trading_data.column("exchange_rate")).as_py()
minmax_time = time.time() - start_time

print(f"  Exchange Rate Range: {min_rate:.8f} — {max_rate:.8f}")
print(f"  Compute time: {minmax_time:.6f} seconds")

# ============================================================
# STEP 4: Vectorized Filtering (Flat Encoding Advantage)
# ============================================================

print("\n--- Vectorized Filtering ---")

# Filter: High-value trades
start_time = time.time()
high_value_mask = pc.greater(trading_data.column("trade_amount"), 5000000)
high_value_count = pc.sum(pa.array([1 if m else 0 for m in high_value_mask])).as_py()
filter_time = time.time() - start_time

print(f"\n  High-Value Trades (>$5M): {high_value_count:,}")
print(f"  Filter time: {filter_time:.3f} seconds")

# Filter: Multi-condition
start_time = time.time()
cond1 = pc.greater(trading_data.column("trade_amount"), 1000000)
cond2 = pc.greater(trading_data.column("risk_score"), 0.7)
combined = pc.and_(cond1, cond2)
combined_count = pc.sum(pa.array([1 if m else 0 for m in combined])).as_py()
multi_filter_time = time.time() - start_time

print(f"  High-Value + High-Risk: {combined_count:,}")
print(f"  Multi-filter time: {multi_filter_time:.3f} seconds")

# ============================================================
# STEP 5: Why Flat Encoding is Optimal Here
# ============================================================

print("\n--- Why Flat Encoding? ---")

print("""
FLAT ENCODING OPTIMAL WHEN:

1. Every value is UNIQUE (no repeats to exploit)
   → Dictionary would store N strings + N indices = MORE memory
   
2. Fixed-width types (int, float, bool)
   → Direct byte access, no offset lookup needed
   
3. SIMD vectorization needed
   → Contiguous, same-type bytes → CPU loads 4-8 values at once
   
4. Maximum read speed required
   → Zero decode overhead: ptr → value in one step

THIS SCENARIO:
  - 5M unique float64 values per column
  - Dictionary encoding would ADD overhead (no repeats)
  - Flat = fastest possible read path
  - SIMD processes 4 float64 per instruction (256-bit register)
""")

# ============================================================
# STEP 6: Performance Summary
# ============================================================

print("--- Performance Summary ---")

print(f"""
FLAT ENCODING RESULTS:

Dataset: {len(trading_data):,} records
Memory: {trading_data.nbytes / 1024 / 1024:.2f} MB

Aggregation Performance:
  Sum:    {sum_time:.6f}s
  Mean:   {mean_time:.6f}s
  StdDev: {std_time:.6f}s
  Min/Max: {minmax_time:.6f}s

Filtering Performance:
  Single condition: {filter_time:.3f}s
  Multi condition:  {multi_filter_time:.3f}s

WHY FLAT WINS HERE:
  ✓ Every value is unique — dictionary adds overhead
  ✓ Fixed-width types — direct byte access
  ✓ SIMD vectorized — 4-8 values per CPU instruction
  ✓ Zero decode overhead — fastest possible read
""")
```

---

## 🏦 Real-World Banking Scenario 2: Offset Encoding — Variable-Length Customer Data

### Scenario
A bank stores **customer master data** with names, emails, addresses, and transaction descriptions. All are variable-length strings — some are 3 characters, others are 100+. The system needs fast random access to any customer's data.

### Problem
- Strings have wildly different lengths (3 to 200+ bytes)
- Need random access to any row
- Must efficiently concatenate and slice strings
- Variable-length data is inherently complex

### Solution
Offset encoding provides O(1) random access to any string via the offsets buffer. The CPU reads two integers to find any string's start and end position.

### Python Code

```python
"""
Banking Scenario 2: Offset Encoding for Variable-Length Customer Data
Using Arrow Offset Buffers for Efficient String Storage
"""

import pyarrow as pa
import pyarrow.compute as pc
import random
import time

# ============================================================
# STEP 1: Generate Variable-Length Customer Data
# ============================================================

print("=== OFFSET ENCODING: VARIABLE-LENGTH DATA ===\n")

def generate_customer_data(num_records: int) -> pa.Table:
    """Generate customer data with variable-length strings."""
    
    first_names = ["Alice", "Bob", "Charlie", "David", "Eve", "Frank", 
                   "Grace", "Henry", "Ivy", "Jack", "Karen", "Leo"]
    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", 
                  "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"]
    
    streets = ["Main St", "Oak Ave", "Pine Rd", "Elm Blvd", "Cedar Ln",
               "Maple Dr", "Washington Ave", "Park Place"]
    cities = ["New York", "London", "Mumbai", "Tokyo", "Sydney", "Berlin"]
    
    customer_ids = [f"CUST-{i:010d}" for i in range(1, num_records + 1)]
    
    # Variable-length names
    names = [f"{random.choice(first_names)} {random.choice(last_names)}" 
             for _ in range(num_records)]
    
    # Variable-length emails
    emails = [f"{random.choice(first_names).lower()}.{random.choice(last_names).lower()}"
              f"{random.randint(1,999)}@bank.com" for _ in range(num_records)]
    
    # Variable-length addresses (highly variable length)
    addresses = [f"{random.randint(1, 9999)} {random.choice(streets)}, "
                 f"{random.choice(cities)} {random.randint(10000, 99999)}" 
                 for _ in range(num_records)]
    
    # Variable-length descriptions (very variable)
    descriptions = []
    for _ in range(num_records):
        length = random.choice([10, 25, 50, 100, 200])
        desc = ''.join(random.choices("abcdefghijklmnopqrstuvwxyz ", k=length)).strip()
        descriptions.append(desc)
    
    table = pa.table({
        "customer_id": customer_ids,
        "name": names,
        "email": emails,
        "address": addresses,
        "description": descriptions,
    })
    
    return table

# Generate 2 million records
print("Generating 2 million customer records...")
start_time = time.time()
customer_data = generate_customer_data(2000000)
gen_time = time.time() - start_time

print(f"Generated in {gen_time:.3f} seconds")
print(f"Rows: {len(customer_data):,}")

# ============================================================
# STEP 2: Analyze Offset Encoding Memory Layout
# ============================================================

print("\n--- Offset Encoding Memory Analysis ---")

for col_name in customer_data.column_names:
    col = customer_data.column(col_name)
    nbytes = col.nbytes
    
    # For string columns, estimate offsets vs values
    if col.type == pa.string():
        offsets_bytes = (len(col) + 1) * 4  # N+1 int32 offsets
        values_bytes = nbytes - offsets_bytes
        print(f"\n  {col_name} (Offset Encoding):")
        print(f"    Type: {col.type}")
        print(f"    Offsets buffer: {offsets_bytes / 1024 / 1024:.2f} MB ({(len(col)+1)} entries × 4 bytes)")
        print(f"    Values buffer:  {values_bytes / 1024 / 1024:.2f} MB")
        print(f"    Total:          {nbytes / 1024 / 1024:.2f} MB")
        print(f"    Avg string len: {values_bytes / len(col):.1f} bytes")
    else:
        print(f"\n  {col_name} (Flat Encoding):")
        print(f"    Type: {col.type}")
        print(f"    Memory: {nbytes / 1024 / 1024:.2f} MB")

# ============================================================
# STEP 3: Random Access Performance (Offset Encoding Strength)
# ============================================================

print("\n--- Random Access Performance ---")

# Access random rows — offset encoding enables O(1) string access
start_time = time.time()
random_indices = [random.randint(0, len(customer_data) - 1) for _ in range(10000)]
for idx in random_indices:
    _ = customer_data.column("name")[idx].as_py()
    _ = customer_data.column("email")[idx].as_py()
    _ = customer_data.column("address")[idx].as_py()
access_time = time.time() - start_time

print(f"\n  10,000 random row accesses (3 strings each):")
print(f"  Time: {access_time:.3f} seconds")
print(f"  Per access: {access_time / 10000 * 1000:.3f} ms")

# ============================================================
# STEP 4: String Operations (Offset Encoding Enables Fast Slicing)
# ============================================================

print("\n--- String Operations ---")

# Extract domain from emails using Arrow compute
start_time = time.time()
domains = pc.binary_slice(
    customer_data.column("email"),
    0,  # start not used directly — use replace
)
# Simpler: filter emails containing specific domain
bank_mask = pc.equal(
    pc.list_slice(pc.utf8_splitAscii(customer_data.column("email"), "@", 1), 0),
    "bank.com"
)
# Alternative: use substring to extract domains
start_time = time.time()
emails = customer_data.column("email")
# Find domain by splitting
at_positions = pc.utf8_split_ascii(emails, "@")
split_time = time.time() - start_time

print(f"\n  Split 2M emails by '@': {split_time:.3f} seconds")

# Count unique domains
start_time = time.time()
# Get domain for each email (second part after @)
domain_counts = customer_data.group_by("email").aggregate({"customer_id": "count"})
agg_time = time.time() - start_time

# ============================================================
# STEP 5: Comparison — Offset vs Naive Approach
# ============================================================

print("\n--- Offset vs Naive String Storage ---")

# Calculate what naive storage would cost
total_chars = sum(len(s) for s in customer_data.column("email").to_pylist())
naive_bytes = total_chars  # 1 byte per char (ASCII)
offset_bytes = customer_data.column("email").nbytes

print(f"\n  Email column:")
print(f"    Naive (1 byte per char, padded to max): ~{total_chars / 1024 / 1024:.2f} MB")
print(f"    Arrow Offset Encoding: {offset_bytes / 1024 / 1024:.2f} MB")
print(f"    Savings: {(1 - offset_bytes / naive_bytes) * 100:.1f}%")

# ============================================================
# STEP 6: Performance Summary
# ============================================================

print("\n--- Performance Summary ---")

print(f"""
OFFSET ENCODING RESULTS:

Dataset: {len(customer_data):,} records
Total memory: {customer_data.nbytes / 1024 / 1024:.2f} MB

Random Access: {access_time:.3f}s for 10,000 accesses
String Split:  {split_time:.3f}s for 2M emails

WHY OFFSET ENCODING IS ESSENTIAL:
  ✓ Variable-length strings CANNOT use flat encoding
  ✓ Offsets enable O(1) random access to any string
  ✓ Values buffer stores strings contiguously (cache-friendly)
  ✓ No padding waste — exact bytes per string
  ✓ Enables string operations (split, substring, concat)

MEMORY COMPOSITION:
  Offsets buffer: ~4 bytes per value (N+1 int32)
  Values buffer:  exact bytes of all strings combined
  Total:          offsets + values (no waste)
""")
```

---

## 🏦 Real-World Banking Scenario 3: Dictionary Encoding — Categorical Transaction Data

### Scenario
A bank's **transaction processing system** handles 50 million transactions/day. Columns like `status`, `channel`, `transaction_type`, and `currency` have very few unique values repeated millions of times. Memory is a critical constraint.

### Problem
- `status` has only 4 values but 50 million rows
- `channel` has only 6 values
- `transaction_type` has only 5 values
- Storing full strings per row wastes enormous memory
- Need fast filtering by category

### Solution
Dictionary encoding stores each unique value **once**, then uses tiny integer indices. This can reduce memory by 50-90% for categorical columns.

### Python Code

```python
"""
Banking Scenario 3: Dictionary Encoding for Categorical Transaction Data
Using Arrow Dictionary Encoding for Memory Optimization
"""

import pyarrow as pa
import pyarrow.compute as pc
import random
import time

# ============================================================
# STEP 1: Generate Categorical Transaction Data
# ============================================================

print("=== DICTIONARY ENCODING: CATEGORICAL DATA ===\n")

def generate_categorical_transactions(num_records: int) -> pa.Table:
    """Generate transactions with many categorical columns."""
    
    statuses = ["COMPLETED", "PENDING", "FAILED", "REVERSED"]
    channels = ["ATM", "MOBILE", "WEB", "BRANCH", "UPI", "NEFT"]
    txn_types = ["CREDIT", "DEBIT", "TRANSFER", "PAYMENT", "REFUND"]
    currencies = ["USD", "EUR", "GBP", "INR", "JPY"]
    risk_levels = ["LOW", "MEDIUM", "HIGH", "CRITICAL"]
    countries = ["US", "UK", "DE", "FR", "JP", "IN", "AU", "BR"]
    
    data = {
        "transaction_id": [f"TXN-{i:012d}" for i in range(1, num_records + 1)],
        "amount": [round(random.uniform(1, 100000), 2) for _ in range(num_records)],
        "status": [random.choice(statuses) for _ in range(num_records)],
        "channel": [random.choice(channels) for _ in range(num_records)],
        "transaction_type": [random.choice(txn_types) for _ in range(num_records)],
        "currency": [random.choice(currencies) for _ in range(num_records)],
        "risk_level": [random.choice(risk_levels) for _ in range(num_records)],
        "country": [random.choice(countries) for _ in range(num_records)],
    }
    
    return pa.table(data)

# Generate 10 million records
print("Generating 10 million transactions...")
start_time = time.time()
raw_data = generate_categorical_transactions(10000000)
gen_time = time.time() - start_time

print(f"Generated in {gen_time:.3f} seconds")
print(f"Rows: {len(raw_data):,}")
print(f"Raw memory: {raw_data.nbytes / 1024 / 1024:.2f} MB")

# ============================================================
# STEP 2: Apply Dictionary Encoding
# ============================================================

print("\n--- Applying Dictionary Encoding ---")

categorical_columns = ["status", "channel", "transaction_type", 
                       "currency", "risk_level", "country"]

start_time = time.time()

optimized_data = {
    "transaction_id": raw_data.column("transaction_id"),
    "amount": raw_data.column("amount"),
}

for col_name in categorical_columns:
    optimized_data[col_name] = pc.dictionary_encode(raw_data.column(col_name))

optimized = pa.table(optimized_data)
opt_time = time.time() - start_time

print(f"\nDictionary encoding applied in {opt_time:.3f} seconds")
print(f"Optimized memory: {optimized.nbytes / 1024 / 1024:.2f} MB")
print(f"Savings: {(1 - optimized.nbytes / raw_data.nbytes) * 100:.1f}%")

# ============================================================
# STEP 3: Analyze Each Dictionary-Encoded Column
# ============================================================

print("\n--- Dictionary Encoding Analysis ---")

for col_name in categorical_columns:
    col = optimized.column(col_name)
    
    if hasattr(col, 'dictionary') and col.dictionary is not None:
        dictionary = col.dictionary
        indices = col.indices
        
        # Calculate memory
        dict_memory = dictionary.nbytes
        indices_memory = indices.nbytes
        total_memory = dict_memory + indices_memory
        
        # What it would be without dictionary
        flat_memory = raw_data.column(col_name).nbytes
        
        print(f"\n  {col_name}:")
        print(f"    Unique values: {len(dictionary)}")
        print(f"    Dictionary: {dictionary.to_pylist()}")
        print(f"    Dictionary size: {dict_memory:,} bytes")
        print(f"    Indices size: {indices_memory:,} bytes ({len(indices)} × {indices_memory // len(indices)} bytes)")
        print(f"    Total (dict): {total_memory:,} bytes")
        print(f"    Flat (no dict): {flat_memory:,} bytes")
        print(f"    Savings: {(1 - total_memory / flat_memory) * 100:.1f}%")

# ============================================================
# STEP 4: Dictionary-Accelerated Filtering
# ============================================================

print("\n--- Dictionary-Accelerated Filtering ---")

# Filter by status — dictionary makes this fast
start_time = time.time()
completed = optimized.filter(pc.equal(optimized.column("status"), "COMPLETED"))
filter_time = time.time() - start_time

print(f"\n  Status = COMPLETED: {len(completed):,} transactions ({filter_time:.3f}s)")

# Multi-category filter
start_time = time.time()
cond1 = pc.equal(optimized.column("status"), "COMPLETED")
cond2 = pc.equal(optimized.column("channel"), "MOBILE")
cond3 = pc.equal(optimized.column("risk_level"), "LOW")
combined = pc.and_(pc.and_(cond1, cond2), cond3)
filtered = optimized.filter(combined)
multi_filter_time = time.time() - start_time

print(f"  COMPLETED + MOBILE + LOW risk: {len(filtered):,} ({multi_filter_time:.3f}s)")

# ============================================================
# STEP 5: Dictionary-Accelerated Aggregation
# ============================================================

print("\n--- Dictionary-Accelerated Aggregation ---")

start_time = time.time()
status_agg = optimized.group_by("status").aggregate({
    "amount": "sum",
    "transaction_id": "count"
})
agg_time = time.time() - start_time

print(f"\n  Aggregation by Status ({agg_time:.3f}s):")
for i in range(len(status_agg)):
    status = status_agg.column("status")[i].as_py()
    total = status_agg.column("amount_sum")[i].as_py()
    count = status_agg.column("transaction_id_count")[i].as_py()
    print(f"    {status}: ${total:,.2f} ({count:,} txns)")

# ============================================================
# STEP 6: Memory Comparison — With vs Without Dictionary
# ============================================================

print("\n--- Memory Comparison ---")

print(f"\n  {'Column':<20} {'Flat':>12} {'Dictionary':>12} {'Savings':>10}")
print(f"  {'-'*20} {'-'*12} {'-'*12} {'-'*10}")

for col_name in raw_data.column_names:
    flat_size = raw_data.column(col_name).nbytes
    dict_size = optimized.column(col_name).nbytes
    savings = (1 - dict_size / flat_size) * 100
    print(f"  {col_name:<20} {flat_size/1024/1024:>10.2f}MB {dict_size/1024/1024:>10.2f}MB {savings:>8.1f}%")

print(f"\n  {'TOTAL':<20} {raw_data.nbytes/1024/1024:>10.2f}MB {optimized.nbytes/1024/1024:>10.2f}MB {(1-optimized.nbytes/raw_data.nbytes)*100:>8.1f}%")

# ============================================================
# STEP 7: Performance Summary
# =================================================

print("\n--- Performance Summary ---")

print(f"""
DICTIONARY ENCODING RESULTS:

Dataset: {len(raw_data):,} transactions
Raw Memory: {raw_data.nbytes / 1024 / 1024:.2f} MB
Optimized: {optimized.nbytes / 1024 / 1024:.2f} MB
Total Savings: {(1 - optimized.nbytes / raw_data.nbytes) * 100:.1f}%

WHY DICTIONARY ENCODING WINS HERE:
  ✓ Status: 4 unique values × 10M rows = 99.99% savings on string storage
  ✓ Channel: 6 unique values × 10M rows = massive repetition
  ✓ All categorical columns have <10 unique values
  ✓ Integer indices (4 bytes) vs strings (6-12 bytes each)
  ✓ Filtering by category is faster (compare integers, not strings)

AT SCALE (1 billion rows):
  Status without dict: ~8 GB (1B × 8 bytes)
  Status with dict:    ~4 GB (1B × 4 bytes index + 28 bytes dict)
  Savings: ~4 GB (50% reduction)
""")
```

---

## 🏦 Real-World Banking Scenario 4: Validity Bitmap — Sparse Transaction Data

### Scenario
A bank's **regulatory reporting system** has a table with 50 columns, but most records only fill 10-15 columns. The rest are NULL. The system needs to store billions of records efficiently without wasting memory on NULL values.

### Problem
- 50 columns per record, but 70% are NULL in most rows
- Naive storage wastes memory on NULL values
- Need to distinguish NULL from 0 or empty string
- Must maintain fast access to non-null values

### Solution
Validity bitmap uses **1 bit per value** to track nulls, not 1 byte. For sparse data, this saves 87.5% of null-tracking overhead. Combined with other encodings, memory usage drops dramatically.

### Python Code

```python
"""
Banking Scenario 4: Validity Bitmap for Sparse Transaction Data
Using Arrow Null Bitmap for Efficient NULL Handling
"""

import pyarrow as pa
import pyarrow.compute as pc
import random
import time

# ============================================================
# STEP 1: Generate Sparse Transaction Data
# ============================================================

print("=== VALIDITY BITMAP: SPARSE DATA ===\n")

def generate_sparse_data(num_records: int) -> pa.Table:
    """Generate sparse transaction data with many nulls."""
    
    data = {
        "transaction_id": [f"TXN-{i:010d}" for i in range(1, num_records + 1)],
        "amount": [round(random.uniform(100, 100000), 2) for _ in range(num_records)],
        
        # 30% filled (70% null)
        "cross_border_fee": [
            round(random.uniform(1, 500), 2) if random.random() > 0.7 else None
            for _ in range(num_records)
        ],
        
        # 20% filled (80% null)
        "compliance_code": [
            f"COMP-{random.randint(1000, 9999)}" if random.random() > 0.8 else None
            for _ in range(num_records)
        ],
        
        # 10% filled (90% null)
        "regulatory_flag": [
            random.choice(["FLAG_A", "FLAG_B"]) if random.random() > 0.9 else None
            for _ in range(num_records)
        ],
        
        # 5% filled (95% null)
        "audit_notes": [
            f"Audit note {random.randint(1, 1000)}" if random.random() > 0.95 else None
            for _ in range(num_records)
        ],
        
        # 50% filled
        "internal_code": [
            f"INT-{random.randint(1, 50)}" if random.random() > 0.5 else None
            for _ in range(num_records)
        ],
        
        # Always filled
        "status": [random.choice(["COMPLETED", "PENDING"]) for _ in range(num_records)],
    }
    
    return pa.table(data)

# Generate 5 million records
print("Generating 5 million sparse records...")
start_time = time.time()
sparse_data = generate_sparse_data(5000000)
gen_time = time.time() - start_time

print(f"Generated in {gen_time:.3f} seconds")
print(f"Rows: {len(sparse_data):,}")
print(f"Total memory: {sparse_data.nbytes / 1024 / 1024:.2f} MB")

# ============================================================
# STEP 2: Analyze Null Distribution
# ============================================================

print("\n--- Null Distribution Analysis ---")

for col_name in sparse_data.column_names:
    col = sparse_data.column(col_name)
    null_count = col.null_count
    total_count = len(col)
    null_pct = (null_count / total_count) * 100
    valid_pct = 100 - null_pct
    
    # Calculate bitmap memory (1 bit per value)
    bitmap_memory = total_count / 8  # 1 bit per value
    
    print(f"\n  {col_name}:")
    print(f"    Valid: {total_count - null_count:,} ({valid_pct:.1f}%)")
    print(f"    NULL:  {null_count:,} ({null_pct:.1f}%)")
    print(f"    Bitmap size: {bitmap_memory / 1024 / 1024:.2f} MB (1 bit per value)")

# ============================================================
# STEP 3: Compare Memory — Naive vs Arrow Bitmap
# ============================================================

print("\n--- Memory: Naive vs Arrow Bitmap ---")

print(f"\n  {'Column':<20} {'Naive (1B/null)':>16} {'Arrow Bitmap':>14} {'Savings':>10}")
print(f"  {'-'*20} {'-'*16} {'-'*14} {'-'*10}")

total_naive = 0
total_arrow = 0

for col_name in sparse_data.column_names:
    col = sparse_data.column(col_name)
    total_count = len(col)
    null_count = col.null_count
    
    # Naive: 1 byte per null flag + value storage
    naive_null_bytes = total_count  # 1 byte per null
    arrow_bitmap_bytes = total_count / 8  # 1 bit per null
    
    naive_total = naive_null_bytes + col.nbytes
    arrow_total = arrow_bitmap_bytes + col.nbytes
    
    total_naive += naive_total
    total_arrow += arrow_total
    
    savings = (1 - arrow_total / naive_total) * 100 if naive_total > 0 else 0
    print(f"  {col_name:<20} {naive_total/1024/1024:>14.2f}MB {arrow_total/1024/1024:>12.2f}MB {savings:>8.1f}%")

total_savings = (1 - total_arrow / total_naive) * 100
print(f"\n  {'TOTAL':<20} {total_naive/1024/1024:>14.2f}MB {total_arrow/1024/1024:>12.2f}MB {total_savings:>8.1f}%")

# ============================================================
# STEP 4: Null-Aware Operations
# ============================================================

print("\n--- Null-Aware Operations ---")

# Count non-null values
start_time = time.time()
for col_name in sparse_data.column_names:
    col = sparse_data.column(col_name)
    valid_count = len(col) - col.null_count
    op_time = time.time() - start_time
    print(f"\n  {col_name}: {valid_count:,} non-null values")

# Sum only non-null values
start_time = time.time()
amount_sum = pc.sum(sparse_data.column("amount")).as_py()
sum_time = time.time() - start_time
print(f"\n  Amount sum (ignoring nulls): ${amount_sum:,.2f} ({sum_time:.3f}s)")

# Filter out nulls
start_time = time.time()
non_null_mask = pc.is_valid(sparse_data.column("cross_border_fee"))
non_null_data = sparse_data.filter(non_null_mask)
filter_time = time.time() - start_time

print(f"  Records with cross_border_fee: {len(non_null_data):,} ({filter_time:.3f}s)")

# ============================================================
# STEP 5: Combine with Dictionary Encoding
# ============================================================

print("\n--- Combining Bitmap + Dictionary Encoding ---")

# Apply dictionary encoding to sparse categorical columns
optimized = pa.table({
    "transaction_id": sparse_data.column("transaction_id"),
    "amount": sparse_data.column("amount"),
    "cross_border_fee": sparse_data.column("cross_border_fee"),
    "compliance_code": pc.dictionary_encode(sparse_data.column("compliance_code")),
    "regulatory_flag": pc.dictionary_encode(sparse_data.column("regulatory_flag")),
    "audit_notes": pc.dictionary_encode(sparse_data.column("audit_notes")),
    "internal_code": pc.dictionary_encode(sparse_data.column("internal_code")),
    "status": pc.dictionary_encode(sparse_data.column("status")),
})

print(f"\n  Original memory: {sparse_data.nbytes / 1024 / 1024:.2f} MB")
print(f"  Optimized memory: {optimized.nbytes / 1024 / 1024:.2f} MB")
print(f"  Total savings: {(1 - optimized.nbytes / sparse_data.nbytes) * 100:.1f}%")

# ============================================================
# STEP 6: Performance Summary
# ============================================================

print("\n--- Performance Summary ---")

print(f"""
VALIDITY BITMAP RESULTS:

Dataset: {len(sparse_data):,} records
Sparse columns: 5 out of 7 (70%+ null rate)

MEMORY SAVINGS:
  Bitmap approach: {total_arrow / 1024 / 1024:.2f} MB
  Naive approach:  {total_naive / 1024 / 1024:.2f} MB
  Savings: {total_savings:.1f}%

WHY VALIDITY BITMAP WINS:
  ✓ 1 bit per value (not 1 byte) — 8x smaller null flags
  ✓ Preserves NULL semantics (NULL ≠ 0 ≠ empty)
  ✓ Works ON TOP OF other encodings (flat, dictionary)
  ✓ Enables null-aware operations (sum ignores nulls)
  ✓ Critical for sparse data (regulatory, compliance)

AT SCALE (1 billion rows, 70% null):
  Naive null flags: 1 GB (1B bytes)
  Arrow bitmap:     125 MB (1B bits)
  Savings:          875 MB just for null flags!
""")
```

---

## 🏦 Real-World Banking Scenario 5: RLE + Dictionary — Sorted Transaction Log

### Scenario
A bank's **audit log** stores every transaction event in chronological order. The `event_type` column has only 5 values, and because events are logged in batches, the same event type repeats consecutively (e.g., 1000 "TRANSFER" events, then 500 "PAYMENT" events). RLE + Dictionary encoding can compress this dramatically.

### Problem
- 100 million event records
- Only 5 event types (low cardinality → dictionary)
- Events cluster by type (consecutive repeats → RLE)
- Need both memory savings AND fast sequential reads

### Solution
Dictionary encoding stores the 5 unique values once. RLE compresses the dictionary indices themselves (consecutive 0s, 0s, 0s become "0, count=1000"). Arrow applies RLE internally to dictionary indices automatically.

### Python Code

```python
"""
Banking Scenario 5: RLE + Dictionary Encoding for Sorted Audit Log
Using Arrow's Internal RLE for Dictionary Indices
"""

import pyarrow as pa
import pyarrow.compute as pc
import random
import time

# ============================================================
# STEP 1: Generate Sorted Audit Log Data
# ============================================================

print("=== RLE + DICTIONARY: SORTED AUDIT LOG ===\n")

def generate_sorted_audit_log(num_records: int) -> pa.Table:
    """Generate sorted audit log with clustered event types."""
    
    event_types = ["TRANSFER", "PAYMENT", "WITHDRAWAL", "DEPOSIT", "QUERY"]
    
    # Create clustered event types (batch processing simulation)
    events = []
    batch_size = num_records // 20  # 20 batches
    for batch in range(20):
        event = event_types[batch % len(event_types)]
        events.extend([event] * batch_size)
    
    # Ensure exact length
    events = events[:num_records]
    
    # Generate timestamps in order (sorted)
    base_time = 1692844800  # 2023-08-24 epoch
    timestamps = [base_time + i for i in range(num_records)]
    
    # Generate other columns
    data = {
        "event_id": [f"EVENT-{i:012d}" for i in range(1, num_records + 1)],
        "event_type": events,  # Clustered/sorted
        "account_id": [f"ACC-{random.randint(1000, 9999):06d}" for _ in range(num_records)],
        "amount": [round(random.uniform(1, 100000), 2) if random.random() > 0.3 else None 
                   for _ in range(num_records)],
        "status": [random.choice(["SUCCESS", "FAILED"]) for _ in range(num_records)],
    }
    
    return pa.table(data)

# Generate 20 million records
print("Generating 20 million sorted audit log records...")
start_time = time.time()
audit_data = generate_sorted_audit_log(20000000)
gen_time = time.time() - start_time

print(f"Generated in {gen_time:.3f} seconds")
print(f"Rows: {len(audit_data):,}")

# ============================================================
# STEP 2: Show Clustering (Why RLE Helps)
# ============================================================

print("\n--- Event Type Distribution (Clustered) ---")

# Show first 50 event types to demonstrate clustering
first_50 = audit_data.column("event_type")[:50].to_pylist()
print(f"\n  First 50 event types (notice clustering):")
for i in range(0, 50, 10):
    chunk = first_50[i:i+10]
    print(f"    [{i:2d}-{i+9:2d}]: {chunk}")

# Count consecutive runs
print(f"\n  Consecutive run analysis:")
current_event = None
run_count = 0
runs = []
for event in audit_data.column("event_type").to_pylist():
    if event == current_event:
        run_count += 1
    else:
        if current_event is not None:
            runs.append((current_event, run_count))
        current_event = event
        run_count = 1
if current_event:
    runs.append((current_event, run_count))

print(f"  Total runs: {len(runs)} (for {len(audit_data):,} records)")
print(f"  Average run length: {len(audit_data) / len(runs):,.0f}")
print(f"\n  Sample runs:")
for event, count in runs[:5]:
    print(f"    {event}: {count:,} consecutive events")

# ============================================================
# STEP 3: Apply Dictionary + RLE Encoding
# ============================================================

print("\n--- Applying Dictionary + RLE Encoding ---")

start_time = time.time()

# Dictionary encode the clustered column
encoded_events = pc.dictionary_encode(audit_data.column("event_type"))

# Show that Arrow internally uses RLE for dictionary indices
encoded_array = pa.array(encoded_events)

# Check the internal representation
if hasattr(encoded_array, 'indices'):
    indices = encoded_array.indices
    print(f"\n  Dictionary Indices (first 30):")
    print(f"    {indices[:30].to_pylist()}")
    print(f"  Notice: consecutive 0s, 0s, 0s — perfect for RLE")

opt_time = time.time() - start_time

optimized = pa.table({
    "event_id": audit_data.column("event_id"),
    "event_type": encoded_events,
    "account_id": audit_data.column("account_id"),
    "amount": audit_data.column("amount"),
    "status": pc.dictionary_encode(audit_data.column("status")),
})

print(f"\n  Encoding time: {opt_time:.3f} seconds")
print(f"  Original memory: {audit_data.nbytes / 1024 / 1024:.2f} MB")
print(f"  Optimized memory: {optimized.nbytes / 1024 / 1024:.2f} MB")
print(f"  Savings: {(1 - optimized.nbytes / audit_data.nbytes) * 100:.1f}%")

# ============================================================
# STEP 4: Analyze RLE Compression on Dictionary Indices
# ============================================================

print("\n--- RLE Compression Analysis ---")

event_col = optimized.column("event_type")
if hasattr(event_col, 'indices'):
    indices = event_col.indices.to_pylist()
    
    # Simulate RLE compression
    rle_pairs = []
    current_val = indices[0]
    current_count = 1
    for val in indices[1:]:
        if val == current_val:
            current_count += 1
        else:
            rle_pairs.append((current_val, current_count))
            current_val = val
            current_count = 1
    rle_pairs.append((current_val, current_count))
    
    # Calculate sizes
    flat_size = len(indices) * 4  # int32 per index
    rle_size = len(rle_pairs) * 8  # (value, count) pair = 8 bytes
    
    print(f"\n  Event Type Indices RLE Analysis:")
    print(f"    Total indices: {len(indices):,}")
    print(f"    Unique indices: {len(set(indices))}")
    print(f"    RLE pairs: {len(rle_pairs):,}")
    print(f"    Flat indices: {flat_size / 1024 / 1024:.2f} MB ({len(indices)} × 4 bytes)")
    print(f"    RLE compressed: {rle_size / 1024 / 1024:.2f} MB ({len(rle_pairs)} × 8 bytes)")
    print(f"    RLE compression ratio: {flat_size / rle_size:.1f}x")

# ============================================================
# STEP 5: Query Performance on Sorted Data
# ============================================================

print("\n--- Query Performance on Sorted Data ---")

# Sequential scan (benefits from RLE — CPU can skip runs)
start_time = time.time()
transfer_events = optimized.filter(pc.equal(optimized.column("event_type"), "TRANSFER"))
scan_time = time.time() - start_time

print(f"\n  Filter event_type = TRANSFER: {len(transfer_events):,} ({scan_time:.3f}s)")

# Aggregation by event type
start_time = time.time()
event_agg = optimized.group_by("event_type").aggregate({
    "event_id": "count",
    "amount": "sum"
})
agg_time = time.time() - start_time

print(f"\n  Aggregation by event type ({agg_time:.3f}s):")
for i in range(len(event_agg)):
    event = event_agg.column("event_type")[i].as_py()
    count = event_agg.column("event_id_count")[i].as_py()
    total = event_agg.column("amount_sum")[i].as_py()
    if total is not None:
        print(f"    {event}: {count:,} events, ${total:,.2f}")
    else:
        print(f"    {event}: {count:,} events, $0.00")

# ============================================================
# STEP 6: Performance Summary
# ============================================================

print("\n--- Performance Summary ---")

print(f"""
RLE + DICTIONARY ENCODING RESULTS:

Dataset: {len(audit_data):,} sorted audit records
Clustered event types: 5 unique values, long consecutive runs

MEMORY:
  Original: {audit_data.nbytes / 1024 / 1024:.2f} MB
  Optimized: {optimized.nbytes / 1024 / 1024:.2f} MB
  Savings: {(1 - optimized.nbytes / audit_data.nbytes) * 100:.1f}%

QUERY PERFORMANCE:
  Event type filter: {scan_time:.3f}s
  Aggregation: {agg_time:.3f}s

WHY RLE + DICTIONARY WINS:
  ✓ Dictionary: 5 unique values stored once (not 20M times)
  ✓ RLE: Consecutive indices compressed (0,0,0 → 0,count=3)
  ✓ Sorted data = long runs = massive RLE compression
  ✓ CPU can skip entire runs during sequential scans
  ✓ Best for batch-processed / time-ordered event logs

WHEN RLE IS MOST EFFECTIVE:
  ✓ Sorted or grouped data (long consecutive runs)
  ✓ Batch-processed events (same type in each batch)
  ✓ Time-series data with periodic patterns
  ✓ Log files with repeated event types
""")
```

---

## 🏦 Real-World Banking Scenario 6: Combining All Encodings — Complete Memory Optimization

### Scenario
A bank needs to store a **complete customer transaction history** with 100 million records. The table has a mix of all data types: unique IDs (flat), variable-length names (offset), categorical status (dictionary), sparse compliance fields (bitmap), and sorted timestamps (RLE-friendly). The goal: minimize total memory while maintaining fast analytics.

### Problem
- Mixed data patterns across columns
- Some columns are high-cardinality (flat), others low-cardinality (dictionary)
- Many columns have NULLs (bitmap needed)
- Total memory budget is tight
- Must support fast aggregation and filtering

### Solution
Apply the **optimal encoding per column** — flat for unique numerics, offset for strings, dictionary for categories, bitmap for nulls. Arrow handles all of these simultaneously.

### Python Code

```python
"""
Banking Scenario 6: Complete Memory Optimization — All Encodings Combined
Using Arrow's Full Encoding Toolkit for Maximum Efficiency
"""

import pyarrow as pa
import pyarrow.compute as pc
import random
import time

# ============================================================
# STEP 1: Generate Mixed-Type Transaction Data
# ============================================================

print("=== COMPLETE ENCODING OPTIMIZATION ===\n")

def generate_mixed_data(num_records: int) -> pa.Table:
    """Generate data requiring all encoding types."""
    
    statuses = ["COMPLETED", "PENDING", "FAILED", "REVERSED"]
    channels = ["ATM", "MOBILE", "WEB", "BRANCH", "UPI"]
    txn_types = ["CREDIT", "DEBIT", "TRANSFER", "PAYMENT"]
    currencies = ["USD", "EUR", "GBP", "INR"]
    countries = ["US", "UK", "DE", "FR", "JP", "IN"]
    
    data = {
        # Flat encoding — unique integers/floats
        "transaction_id": [f"TXN-{i:012d}" for i in range(1, num_records + 1)],
        "amount": [round(random.uniform(1, 100000), 2) for _ in range(num_records)],
        "fee": [round(random.uniform(0, 500), 2) for _ in range(num_records)],
        
        # Offset encoding — variable-length strings
        "customer_name": [f"Customer_{random.randint(1, 100000)}" for _ in range(num_records)],
        "description": [
            random.choice(["", "Salary transfer", "Rent payment", "Grocery shopping",
                          "Utility bill", "Insurance premium", "Loan EMI",
                          "Investment transfer", "Charity donation"])
            for _ in range(num_records)
        ],
        
        # Dictionary encoding — low cardinality
        "status": [random.choice(statuses) for _ in range(num_records)],
        "channel": [random.choice(channels) for _ in range(num_records)],
        "transaction_type": [random.choice(txn_types) for _ in range(num_records)],
        "currency": [random.choice(currencies) for _ in range(num_records)],
        "country": [random.choice(countries) for _ in range(num_records)],
        
        # Validity bitmap — sparse columns
        "compliance_code": [
            f"COMP-{random.randint(1000, 9999)}" if random.random() > 0.7 else None
            for _ in range(num_records)
        ],
        "regulatory_flag": [
            random.choice(["FLAG_A", "FLAG_B"]) if random.random() > 0.85 else None
            for _ in range(num_records)
        ],
        "audit_notes": [
            f"Note_{random.randint(1, 1000)}" if random.random() > 0.9 else None
            for _ in range(num_records)
        ],
    }
    
    return pa.table(data)

# Generate 10 million records
print("Generating 10 million mixed-type records...")
start_time = time.time()
raw_data = generate_mixed_data(10000000)
gen_time = time.time() - start_time

print(f"Generated in {gen_time:.3f} seconds")
print(f"Rows: {len(raw_data):,}")
print(f"Raw memory: {raw_data.nbytes / 1024 / 1024:.2f} MB")

# ============================================================
# STEP 2: Apply Optimal Encoding Per Column
# ============================================================

print("\n--- Applying Optimal Encodings ---")

start_time = time.time()

# Dictionary encode categorical columns
dict_columns = ["status", "channel", "transaction_type", "currency", "country",
                "compliance_code", "regulatory_flag", "audit_notes"]

optimized_data = {
    # Flat — already optimal for unique numerics
    "transaction_id": raw_data.column("transaction_id"),
    "amount": raw_data.column("amount"),
    "fee": raw_data.column("fee"),
    
    # Offset — already optimal for variable-length strings
    "customer_name": raw_data.column("customer_name"),
    "description": raw_data.column("description"),
}

# Dictionary encode categorical columns
for col_name in dict_columns:
    optimized_data[col_name] = pc.dictionary_encode(raw_data.column(col_name))

optimized = pa.table(optimized_data)
opt_time = time.time() - start_time

print(f"\n  Optimization time: {opt_time:.3f} seconds")

# ============================================================
# STEP 3: Column-by-Column Encoding Analysis
# ============================================================

print("\n--- Column Encoding Analysis ---")

print(f"\n  {'Column':<20} {'Encoding':<12} {'Raw':>10} {'Optimized':>10} {'Savings':>10}")
print(f"  {'-'*20} {'-'*12} {'-'*10} {'-'*10} {'-'*10}")

total_raw = 0
total_opt = 0

for col_name in raw_data.column_names:
    raw_size = raw_data.column(col_name).nbytes
    opt_size = optimized.column(col_name).nbytes
    total_raw += raw_size
    total_opt += opt_size
    
    # Determine encoding type
    if col_name in ["transaction_id", "amount", "fee"]:
        encoding = "Flat"
    elif col_name in ["customer_name", "description"]:
        encoding = "Offset"
    else:
        encoding = "Dictionary"
    
    savings = (1 - opt_size / raw_size) * 100 if raw_size > 0 else 0
    print(f"  {col_name:<20} {encoding:<12} {raw_size/1024/1024:>8.2f}MB {opt_size/1024/1024:>8.2f}MB {savings:>8.1f}%")

total_savings = (1 - total_opt / total_raw) * 100
print(f"\n  {'TOTAL':<20} {'—':<12} {total_raw/1024/1024:>8.2f}MB {total_opt/1024/1024:>8.2f}MB {total_savings:>8.1f}%")

# ============================================================
# STEP 4: Fast Analytics on Optimized Data
# ============================================================

print("\n--- Fast Analytics on Optimized Data ---")

# Aggregation by status
start_time = time.time()
status_agg = optimized.group_by("status").aggregate({
    "amount": "sum",
    "transaction_id": "count"
})
agg_time = time.time() - start_time

print(f"\n  By Status ({agg_time:.3f}s):")
for i in range(len(status_agg)):
    status = status_agg.column("status")[i].as_py()
    total = status_agg.column("amount_sum")[i].as_py()
    count = status_agg.column("transaction_id_count")[i].as_py()
    print(f"    {status}: ${total:,.2f} ({count:,})")

# Aggregation by channel + status
start_time = time.time()
channel_status_agg = optimized.group_by(["channel", "status"]).aggregate({
    "amount": "sum"
})
multi_agg_time = time.time() - start_time

print(f"\n  By Channel × Status ({multi_agg_time:.3f}s): {len(channel_status_agg)} groups")

# Filter high-value + specific status
start_time = time.time()
cond1 = pc.greater(optimized.column("amount"), 50000)
cond2 = pc.equal(optimized.column("status"), "COMPLETED")
cond3 = pc.equal(optimized.column("channel"), "MOBILE")
combined = pc.and_(pc.and_(cond1, cond2), cond3)
filtered = optimized.filter(combined)
filter_time = time.time() - start_time

print(f"\n  High-value COMPLETED MOBILE: {len(filtered):,} ({filter_time:.3f}s)")

# ============================================================
# STEP 5: Memory Budget Check
# ============================================================

print("\n--- Memory Budget Analysis ---")

print(f"""
  Memory Budget: 500 MB
  Raw data:      {total_raw / 1024 / 1024:.2f} MB {'EXCEEDS BUDGET ❌' if total_raw / 1024 / 1024 > 500 else 'within budget ✅'}
  Optimized:     {total_opt / 1024 / 1024:.2f} MB {'EXCEEDS BUDGET ❌' if total_opt / 1024 / 1024 > 500 else 'within budget ✅'}
  Savings:       {total_savings:.1f}%
""")

# ============================================================
# STEP 6: Performance Summary
# ============================================================

print("--- Performance Summary ---")

print(f"""
COMPLETE ENCODING OPTIMIZATION RESULTS:

Dataset: {len(raw_data):,} records
Columns: {len(raw_data.column_names)}

ENCODING BREAKDOWN:
  Flat (numeric):    transaction_id, amount, fee
  Offset (strings):  customer_name, description
  Dictionary:        status, channel, type, currency, country,
                     compliance_code, regulatory_flag, audit_notes

MEMORY:
  Raw:    {total_raw / 1024 / 1024:.2f} MB
  Optimal: {total_opt / 1024 / 1024:.2f} MB
  Saved:  {total_savings:.1f}%

QUERY PERFORMANCE:
  Single-group agg:  {agg_time:.3f}s
  Multi-group agg:   {multi_agg_time:.3f}s
  Multi-condition:   {filter_time:.3f}s

KEY INSIGHT:
  No single encoding works for all data.
  Arrow lets you choose the BEST encoding PER COLUMN.
  This is why columnar storage + proper encoding = massive wins.
""")
```

---

## 🎯 5 Real-World Interview Questions

### Question 1: What are Arrow's in-memory encodings and when do you use each?

**Answer:**

| Encoding | When to Use | Example |
|----------|-------------|---------|
| **Flat** | Fixed-width primitives (int, float, bool) | `amount`, `id`, `balance` |
| **Offset** | Variable-length data (strings, binary, lists) | `name`, `address`, `description` |
| **Dictionary** | Low-cardinality categorical data | `status`, `country`, `type` |
| **Validity Bitmap** | Any column with NULL values | Sparse compliance fields |
| **RLE** | Consecutive repeated values (sorted data) | Sorted event logs |

**Key insight:** These encodings **layer on top of each other**. A single column can have dictionary encoding + validity bitmap simultaneously.

```python
import pyarrow as pa
import pyarrow.compute as pc

# Dictionary + Null: status column with nulls
status = pa.array(["active", "active", None, "inactive", None])
encoded = pc.dictionary_encode(status)  # Dictionary encoding
# + validity bitmap tracks the None positions
```

---

### Question 2: Why is dictionary encoding only beneficial for low-cardinality data?

**Answer:**

**Low cardinality (few unique values):**
```
status: ["active", "active", "inactive", "active"]  → 2 unique values
Dictionary: 2 strings + 4 indices = LESS memory ✅
```

**High cardinality (many unique values):**
```
customer_id: ["CUST-001", "CUST-002", "CUST-003"]  → 3 unique values = 3 unique
Dictionary: 3 strings + 3 indices = SAME or MORE memory ❌
```

**The math:**
```
Without dict: N values × avg_string_bytes
With dict:    unique_values × avg_string_bytes + N × 4 bytes (int32 index)

Break-even point: unique_values ≈ N × 4 / avg_string_bytes
```

For `status` (avg 8 bytes, 2 unique): massive savings
For `customer_id` (avg 15 bytes, N unique): no savings

---

### Question 3: How does the validity bitmap work and why is it efficient?

**Answer:**

The validity bitmap uses **1 bit per value** (not 1 byte):

```
Values:  [1001, NULL, 1003, NULL, 1005]
Bitmap:  [1,    0,    1,    0,    1   ]

Stored as: 1 byte (bits packed) not 5 bytes
```

**Efficiency:**
- 1 million values = 125 KB bitmap
- Naive (1 byte per null) = 1 MB
- **8x smaller**

**Why not use sentinel values?**
```
Sentinel approach:  -999999 means NULL
  → Must check every value against sentinel
  → Can't have -999999 as a real value
  → Breaks type safety

Bitmap approach: separate validity flag
  → Any value can coexist with NULL
  → 1-bit check before accessing value
  → Type-safe, no special values needed
```

---

### Question 4: How does RLE help with sorted data in Arrow?

**Answer:**

**Sorted data creates long runs of identical values:**
```
Sorted event types: [TRANSFER, TRANSFER, TRANSFER, ..., PAYMENT, PAYMENT, ...]
                     ←── 1000 TRANSFER ──→←── 500 PAYMENT ──→
```

**RLE compresses runs:**
```
Without RLE: [0, 0, 0, ..., 0, 1, 1, ..., 1]  → 1M × 4 bytes = 4 MB
With RLE:    [(0, 1000000), (1, 500000)]        → 2 × 8 bytes = 16 bytes
Compression ratio: 250,000x!
```

**Arrow uses RLE internally for dictionary indices.** When you dictionary-encode a sorted column, Arrow automatically applies RLE to the indices buffer, getting both benefits.

---

### Question 5: When would you NOT use dictionary encoding?

**Answer:**

**Do NOT use dictionary encoding when:**

| Scenario | Why Not | Better Encoding |
|----------|---------|-----------------|
| Unique IDs (`TXN-001`, `TXN-002`) | Every value is unique — dictionary = more memory | Flat |
| Free-text descriptions | High cardinality, no repeats | Offset |
| Timestamps | Every value is unique | Flat |
| Emails/URLs | High cardinality, unique | Offset |
| Large numeric values | Dictionary adds index overhead | Flat |

**Rule of thumb:** If `unique_count / total_count > 0.3` (more than 30% unique), dictionary encoding likely **hurts** rather than helps.

---

## 📝 Summary

| Aspect | Key Point |
|--------|-----------|
| **Flat Encoding** | Fixed-width primitives, zero overhead, fastest reads |
| **Offset Encoding** | Variable-length data, O(1) random access via offsets |
| **Dictionary Encoding** | Low-cardinality data, 30-70% memory savings |
| **Validity Bitmap** | NULL tracking, 1 bit per value, 8x smaller than naive |
| **RLE Encoding** | Consecutive repeats, massive compression on sorted data |
| **Combining** | Multiple encodings layer on same column |
| **Decision** | Choose encoding per column based on data characteristics |
| **Key Insight** | No single encoding works for all — use the right one per column |
| **Memory Savings** | Proper encoding: 40-70% less memory than naive storage |
| **Performance** | Proper encoding: 2-10x faster queries |
