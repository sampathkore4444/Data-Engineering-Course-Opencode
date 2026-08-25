# Parquet Encoding & Compression

## Table of Contents
1. [Detailed Explanation](#1-detailed-explanation)
2. [Example](#2-example)
3. [Real-World Banking Scenario 1](#3-banking-scenario-1-payment-processing-analytics)
4. [Python Code - Scenario 1](#4-python-code---scenario-1)
5. [Real-World Banking Scenario 2](#5-banking-scenario-2-credit-card-transaction-storage)
6. [Python Code - Scenario 2](#6-python-code---scenario-2)
7. [Interview Questions](#7-interview-questions)

---

## 1. Detailed Explanation

### The Encoding-Compression Pipeline

In Parquet, data goes through a **two-stage reduction process**:

```
Raw Data
   ↓
Encoding (logical transformation)
   ↓
Compressed Bytes
   ↓
Written to Disk
```

Understanding both stages is critical because:
- **Encoding** reduces data by understanding the structure of the data
- **Compression** further reduces the encoded data using general-purpose algorithms

### Parquet Encodings

#### 1. Plain Encoding (No Encoding)

Data is stored as-is with minimal overhead.

```
amounts: [100.50, 250.75, 300.00, 150.25]
Stored:  [100.50, 250.75, 300.00, 150.25]  (raw bytes)
```

**When to use**: High-cardinality, unsorted data where other encodings don't help.

#### 2. Dictionary Encoding

Build a dictionary of unique values, replace each value with its index.

```
Original:  ["COMPLETED", "PENDING", "COMPLETED", "FAILED", "COMPLETED"]
Dictionary: {0: "COMPLETED", 1: "PENDING", 2: "FAILED"}
Encoded:   [0, 1, 0, 2, 0]
```

**When to use**: Low-cardinality columns (status, currency, country, channel).

**Parquet rules**:
- Dictionary page size default: 1MB
- If dictionary exceeds page size, falls back to plain encoding
- Maximum dictionary page size: 2GB

#### 3. Run-Length Encoding (RLE)

Store consecutive identical values as (value, count) pairs.

```
Original:  [A, A, A, A, B, B, B, C, C, C, C, C]
RLE:       [(A, 4), (B, 3), (C, 5)]
```

**When to use**: Sorted columns, columns with many repeated consecutive values.

#### 4. Bit-Packing Encoding

Pack small integers into the minimum number of bits.

```
Original:  [0, 3, 1, 2, 0, 3, 1, 2]  (values 0-3, need 2 bits each)
Plain:     8 bytes (1 byte per value)
Bit-packed: 2 bytes (8 values × 2 bits = 16 bits = 2 bytes)
```

**When to use**: Small integer ranges (flags, enums, status codes).

#### 5. Delta Encoding

Store differences between consecutive values.

```
Original:  [1000, 1005, 1003, 1008, 1012]
Delta:     [1000, +5, -2, +5, +4]
```

**When to use**: Monotonically increasing values (timestamps, IDs, sequences).

#### 6. Delta-Length Byte Array

Store byte arrays as (offset, length) pairs.

```
Original:  ["NYC", "LA", "Chicago", "NYC"]
Delta-Length: [(0,3), (3,2), (5,7), (0,3)]
```

#### 7. Byte Stream Split

Split byte streams by position for better compression.

### Encoding Summary Table

| Encoding | Input Type | Best For | Compression Potential |
|----------|-----------|----------|----------------------|
| Plain | Any | General purpose | Low |
| Dictionary | Any | Low cardinality | High |
| Run-Length | Any | Sorted/repeated values | High |
| Bit-Packing | Integers | Small ranges (0-N) | Medium |
| Delta | Integers | Monotonic sequences | High |
| Delta-Length | Byte arrays | Similar-length strings | Medium |

### Parquet Compression Codecs

After encoding, the data is compressed:

| Codec | Speed | Ratio | CPU Usage | Best For |
|-------|-------|-------|-----------|----------|
| **Snappy** | Fast | ~3-5x | Low | Default, interactive queries |
| **Gzip** | Slow | ~6-8x | High | Archival, cold storage |
| **Zstd** | Fast | ~6-8x | Medium | Best balance (recommended) |
| **LZ4** | Fastest | ~2-3x | Low | Latency-sensitive |
| **Brotli** | Slow | ~8-10x | High | Maximum compression |
| **None** | N/A | 1x | None | Debugging |

### How They Work Together

```
Column: status (1 billion rows)
  Values: 90% COMPLETED, 7% PENDING, 3% FAILED

Step 1: Dictionary Encoding
  Dictionary: {0: COMPLETED, 1: PENDING, 2: FAILED}
  Data: [0, 0, 0, 1, 0, 2, 0, 0, ...]  (1 billion integers, 0-2)
  Size: ~4 GB (4 bytes per int32)

Step 2: Bit-Packing
  Values only need 2 bits (0-2)
  Packed: ~250 MB (1 billion × 2 bits)

Step 3: Snappy Compression
  Many repeated patterns → ~150 MB

Total: 1 billion string values → 150 MB
Compression ratio: ~50x vs raw strings
```

---

## 2. Example

### Encoding Comparison on Sample Data

**Column: currency**
```
Original: ["USD", "USD", "USD", "EUR", "USD", "GBP", "USD", "USD", "EUR", "USD"]
```

**Plain Encoding:**
```
[3, 85, 83, 68, 3, 85, 83, 68, ...]  (3 bytes × 10 = 30 bytes)
```

**Dictionary Encoding:**
```
Dictionary: {0: "USD", 1: "EUR", 2: "GBP"}
Indices: [0, 0, 0, 1, 0, 2, 0, 0, 1, 0]  (4 bytes × 10 = 40 bytes)
```
Wait — for 10 values, dictionary is larger. But for 1 million values:
- Plain: 3 million bytes
- Dictionary: 4 bytes × 1,000,000 + 12 bytes = ~4 MB

For 100 million values:
- Plain: 300 MB
- Dictionary: 400 MB + 12 bytes

Dictionary wins when the dictionary is much smaller than the total data.

**Column: amount**
```
Original: [100.00, 100.50, 100.25, 100.75, 100.10, 100.90, 100.35, 100.60]
```

**Delta Encoding:**
```
Base: 100.00
Deltas: [0.00, +0.50, +0.25, +0.75, +0.10, +0.90, +0.35, +0.60]
```
Small deltas → better compression.

---

## 3. Banking Scenario 1: Payment Processing Analytics

### Problem
A bank processes **500 million payment transactions monthly** across 20 payment types (ACH, Wire, SWIFT, SEPA, etc.) and 50 status codes. The data must be:
- Compressed to minimize storage costs (budget: $0.023/GB/month on S3)
- Fast to query for daily payment reconciliation
- Retained for 7 years for regulatory compliance

### Why Encoding/Compression Matters?
- 20 payment types → dictionary encoding eliminates string storage
- 50 status codes → dictionary encoding reduces to 6-bit integers
- Timestamps in sequence → delta encoding for ~5x reduction
- Amounts with limited precision → better compression with Snappy/Zstd
- Total: 500M rows × 500 bytes = 250 GB raw → ~15 GB Parquet

### Architecture
```
Payment Gateway
     |
     v
  Payment Processing Engine
     |
     v
  Parquet Files (encoded + compressed)
     |
     +-- Snappy (hot data, 0-30 days)
     +-- Zstd (warm data, 30-365 days)
     +-- Gzip (cold data, 1-7 years)
     |
     v
  S3 Lifecycle Policy
```

---

## 4. Python Code - Scenario 1

```python
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
from datetime import datetime, timedelta
import random
import time
import os
import tempfile

# ============================================================
# BANKING SCENARIO: Payment Processing Analytics
# ============================================================

def generate_payment_data(num_payments=500_000):
    """Generate realistic payment transaction data."""
    random.seed(42)
    np.random.seed(42)

    payment_types = [
        "ACH_CREDIT", "ACH_DEBIT", "WIRE_DOMESTIC", "WIRE_INTERNATIONAL",
        "SWIFT_MT103", "SEPA_CREDIT", "SEPA_DEBIT", "RTGS", "CHIPS",
        "BACS", "FEDWIRE", "INST-pay", "CHECK_IMAGE", "EFT",
        "DIRECT_DEBIT", "Standing_Order", "BILL_PAY", "P2P_TRANSFER",
        "CARD_PAYMENT", "REFUND"
    ]

    statuses = [
        "COMPLETED", "COMPLETED", "COMPLETED", "COMPLETED", "COMPLETED",
        "COMPLETED", "COMPLETED", "PENDING", "PENDING", "PROCESSING",
        "FAILED", "CANCELLED", "REVERSED", "ON_HOLD", "REVIEW"
    ]

    currencies = ["USD", "USD", "USD", "USD", "EUR", "GBP", "JPY", "CHF"]
    channels = ["ONLINE", "MOBILE", "BRANCH", "API", "BATCH"]
    currencies_codes = ["USD", "EUR", "GBP", "JPY", "CHF", "CAD", "AUD"]

    # Generate timestamps (sequential for delta encoding optimization)
    start_time = datetime(2026, 8, 1)
    timestamps = np.array([
        start_time + timedelta(seconds=i * 0.5 + random.uniform(0, 0.3))
        for i in range(num_payments)
    ], dtype='datetime64[us]')

    table = pa.table({
        "payment_id": pa.array(list(range(1, num_payments + 1)), type=pa.int64()),
        "payment_type": pa.array(np.random.choice(payment_types, num_payments), type=pa.string()),
        "status": pa.array(np.random.choice(statuses, num_payments), type=pa.string()),
        "currency": pa.array(np.random.choice(currencies, num_payments), type=pa.string()),
        "source_currency": pa.array(np.random.choice(currencies_codes, num_payments), type=pa.string()),
        "target_currency": pa.array(np.random.choice(currencies_codes, num_payments), type=pa.string()),
        "channel": pa.array(np.random.choice(channels, num_payments), type=pa.string()),
        "amount": pa.array(np.random.uniform(1.0, 1000000.0, num_payments).round(2), type=pa.float64()),
        "fee": pa.array(np.random.uniform(0.5, 500.0, num_payments).round(2), type=pa.float64()),
        "exchange_rate": pa.array(np.random.uniform(0.7, 150.0, num_payments).round(6), type=pa.float64()),
        "originator_bic": pa.array([f"BIC{random.randint(100000, 999999)}" for _ in range(num_payments)], type=pa.string()),
        "beneficiary_bic": pa.array([f"BIC{random.randint(100000, 999999)}" for _ in range(num_payments)], type=pa.string()),
        "payment_timestamp": pa.array(timestamps, type=pa.timestamp("us")),
        "value_date": pa.array([
            (start_time + timedelta(days=random.randint(0, 5))).strftime("%Y-%m-%d")
            for _ in range(num_payments)
        ], type=pa.date32()),
    })

    return table


def benchmark_compression(table, base_path):
    """Benchmark different compression codecs."""
    codecs = ["snappy", "gzip", "zstd", "lz4", "brotli"]
    results = {}

    for codec in codecs:
        path = os.path.join(base_path, f"payments_{codec}.parquet")
        start = time.time()

        if codec == "none":
            pq.write_table(table, path, compression="none")
        else:
            pq.write_table(table, path, compression=codec)

        elapsed = time.time() - start
        size = os.path.getsize(path)
        results[codec] = {"size_mb": size / (1024*1024), "time_s": elapsed}

    # Raw size (no encoding, no compression)
    raw_path = os.path.join(base_path, "payments_raw.parquet")
    pq.write_table(table, raw_path, compression="none", use_dictionary=False)
    raw_size = os.path.getsize(raw_path)

    print(f"\n=== Compression Benchmark ({table.num_rows:,} rows) ===")
    print(f"Raw (no encoding/compression): {raw_size / (1024*1024):.1f} MB")
    print(f"\n{'Codec':<12} {'Size (MB)':<12} {'Ratio':<10} {'Time (s)':<10}")
    print("-" * 44)
    for codec, metrics in results.items():
        ratio = raw_size / (metrics["size_mb"] * 1024 * 1024)
        print(f"{codec:<12} {metrics['size_mb']:<12.1f} {ratio:<10.1f}x {metrics['time_s']:<10.3f}")


def benchmark_encoding(table, base_path):
    """Benchmark dictionary encoding impact."""
    # With dictionary encoding (default)
    path_dict = os.path.join(base_path, "with_dict.parquet")
    pq.write_table(table, path_dict, compression="snappy", use_dictionary=True)
    size_dict = os.path.getsize(path_dict)

    # Without dictionary encoding
    path_no_dict = os.path.join(base_path, "without_dict.parquet")
    pq.write_table(table, path_no_dict, compression="snappy", use_dictionary=False)
    size_no_dict = os.path.getsize(path_no_dict)

    print(f"\n=== Dictionary Encoding Impact ===")
    print(f"With dictionary:    {size_dict / (1024*1024):.1f} MB")
    print(f"Without dictionary: {size_no_dict / (1024*1024):.1f} MB")
    print(f"Dictionary saves:   {(1 - size_dict/size_no_dict)*100:.1f}%")

    # Show which columns benefit most
    print(f"\n=== Per-Column Dictionary Impact ===")
    for col_name in table.column_names:
        col = table.column(col_name)

        # With dictionary
        temp_dict = pa.table({col_name: col})
        pq.write_table(temp_dict, path_dict, compression="snappy", use_dictionary=True)
        s_dict = os.path.getsize(path_dict)

        # Without dictionary
        pq.write_table(temp_dict, path_no_dict, compression="snappy", use_dictionary=False)
        s_no_dict = os.path.getsize(path_no_dict)

        if s_no_dict > 0:
            savings = (1 - s_dict/s_no_dict) * 100
            if savings > 5:
                print(f"  {col_name:<25} {s_dict/1024:.1f}KB vs {s_no_dict/1024:.1f}KB → {savings:.0f}% savings")


def query_with_compression_comparison(base_path):
    """Query performance across different compression codecs."""
    codecs = ["snappy", "gzip", "zstd"]
    results = {}

    for codec in codecs:
        path = os.path.join(base_path, f"payments_{codec}.parquet")

        start = time.time()
        table = pq.read_table(
            path,
            columns=["payment_type", "status", "amount", "currency"],
            filters=[("status", "=", "COMPLETED"), ("amount", ">", 10000)],
        )
        elapsed = time.time() - start

        results[codec] = {"rows": table.num_rows, "time_s": elapsed}

    print(f"\n=== Query Performance by Codec ===")
    print(f"{'Codec':<12} {'Rows':<12} {'Time (s)':<10}")
    print("-" * 34)
    for codec, metrics in results.items():
        print(f"{codec:<12} {metrics['rows']:<12,} {metrics['time_s']:<10.3f}")


# ============================================================
# RUN THE SCENARIO
# ============================================================
if __name__ == "__main__":
    base_path = os.path.join(tempfile.gettempdir(), "payment_compression")
    os.makedirs(base_path, exist_ok=True)

    # Generate payment data
    print("Generating payment data...")
    payments = generate_payment_data(num_payments=500_000)

    # Benchmark compression codecs
    benchmark_compression(payments, base_path)

    # Benchmark dictionary encoding
    benchmark_encoding(payments, base_path)

    # Query performance comparison
    query_with_compression_comparison(base_path)
```

---

## 5. Banking Scenario 2: Credit Card Transaction Storage

### Problem
A bank issues **10 million credit cards** and processes **2 billion card transactions monthly**. Each transaction has:
- 30+ columns (card details, merchant info, amounts, timestamps, fraud flags)
- Heavy repetition in: card_network (Visa/MC/Amex), merchant_category, country_code, currency

Storage must support:
- Real-time fraud detection (hot data, last 7 days)
- Monthly billing statements (warm data, 30-90 days)
- Annual spend analysis (cold data, 90+ days)

### Why Encoding/Compression Matters?
- Card network: 3 values → dictionary encoding = 2 bits per row
- Merchant category: 50 values → dictionary encoding = 6 bits per row
- Country code: 200 values → dictionary encoding = 8 bits per row
- Timestamps: sequential → delta encoding = ~4 bytes vs 8 bytes
- Amounts: limited precision → Snappy/Zstd compress well
- 2 billion rows × 500 bytes = 1 TB raw → ~50 GB Parquet

### Architecture
```
POS Terminal / Online Gateway
     |
     v
  Card Processing System
     |
     v
  Kafka Stream
     |
     +-- Real-time: Last 7 days (in-memory / SSD)
     |
     v
  Parquet Files (Zstd compressed)
     |
     +-- Hot: Last 30 days (SSD-backed S3)
     +-- Warm: 30-90 days (S3 Standard)
     +-- Cold: 90+ days (S3 Glacier)
     |
     v
  Analytics Engine (DuckDB / Spark)
```

---

## 6. Python Code - Scenario 2

```python
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
from datetime import datetime, timedelta
import random
import time
import os
import tempfile

# ============================================================
# BANKING SCENARIO: Credit Card Transaction Storage
# ============================================================

def generate_card_transactions(num_transactions=1_000_000):
    """Generate realistic credit card transaction data."""
    random.seed(42)
    np.random.seed(42)

    card_networks = ["VISA", "MASTERCARD", "AMEX", "DISCOVER", "JCB"]
    merchant_categories = [
        "GROCERY", "RESTAURANT", "GAS_STATION", "ONLINE_RETAIL", "HOTEL",
        "AIRLINE", "SUBSCRIPTION", "HEALTHCARE", "EDUCATION", "ENTERTAINMENT",
        "CLOTHING", "ELECTRONICS", "HOME_IMPROVEMENT", "PHARMACY", "UTILITIES",
    ]
    countries = ["US", "US", "US", "US", "GB", "DE", "FR", "JP", "CA", "AU"]
    currencies = ["USD", "USD", "USD", "GBP", "EUR", "JPY", "CAD", "AUD"]
    channels = ["POS", "ONLINE", "MOBILE", "ATM", "RECURRING"]
    fraud_flags = [False] * 97 + [True] * 3  # 3% fraud rate

    # Sequential timestamps for delta encoding
    start_time = datetime(2026, 8, 1)
    timestamps = np.array([
        start_time + timedelta(seconds=i * 0.1)
        for i in range(num_transactions)
    ], dtype='datetime64[us]')

    amounts = np.concatenate([
        np.random.exponential(25, int(num_transactions * 0.7)),     # Small purchases
        np.random.exponential(200, int(num_transactions * 0.2)),    # Medium purchases
        np.random.exponential(2000, num_transactions - int(num_transactions * 0.9)),  # Large
    ]).round(2)
    np.random.shuffle(amounts)

    table = pa.table({
        "transaction_id": pa.array(list(range(1, num_transactions + 1)), type=pa.int64()),
        "card_number_last4": pa.array([f"{random.randint(1000, 9999)}" for _ in range(num_transactions)], type=pa.string()),
        "card_network": pa.array(np.random.choice(card_networks, num_transactions), type=pa.string()),
        "merchant_id": pa.array([f"M{random.randint(100000, 999999)}" for _ in range(num_transactions)], type=pa.string()),
        "merchant_name": pa.array(np.random.choice([
            "Walmart", "Amazon", "Starbucks", "Shell", "McDonald's",
            "Target", "Costco", "Best Buy", "Uber", "Netflix"
        ], num_transactions), type=pa.string()),
        "merchant_category": pa.array(np.random.choice(merchant_categories, num_transactions), type=pa.string()),
        "country_code": pa.array(np.random.choice(countries, num_transactions), type=pa.string()),
        "currency": pa.array(np.random.choice(currencies, num_transactions), type=pa.string()),
        "amount": pa.array(amounts, type=pa.float64()),
        "amount_usd": pa.array((amounts * np.random.uniform(0.8, 1.5, num_transactions)).round(2), type=pa.float64()),
        "channel": pa.array(np.random.choice(channels, num_transactions), type=pa.string()),
        "is_fraud": pa.array(np.random.choice(fraud_flags, num_transactions), type=pa.bool_()),
        "authorization_code": pa.array([f"AUTH{random.randint(100000, 999999)}" for _ in range(num_transactions)], type=pa.string()),
        "transaction_timestamp": pa.array(timestamps, type=pa.timestamp("us")),
        "settlement_date": pa.array([
            (datetime(2026, 8, 1) + timedelta(days=random.randint(0, 5))).strftime("%Y-%m-%d")
            for _ in range(num_transactions)
        ], type=pa.date32()),
    })

    return table


def store_card_transactions(table, base_path):
    """Store card transactions with optimal encoding/compression."""
    start = time.time()

    # Use Zstd for best balance of speed and compression
    pq.write_to_dataset(
        table,
        root_path=base_path,
        partition_cols=["settlement_date"],
        compression="zstd",
        compression_level=3,            # Zstd level 3: good speed + ratio
        use_dictionary=True,            # Critical for card_network, merchant_category, etc.
        write_statistics=True,
        data_page_size=1_048_576,       # 1MB pages
        dictionary_pagesize_limit=1_048_576,
        version="2.6",
    )

    elapsed = time.time() - start
    total_size = 0
    file_count = 0
    for root, dirs, files in os.walk(base_path):
        for f in files:
            if f.endswith(".parquet"):
                total_size += os.path.getsize(os.path.join(root, f))
                file_count += 1

    print(f"Stored {table.num_rows:,} card transactions")
    print(f"Files: {file_count}")
    print(f"Total size: {total_size / (1024*1024):.1f} MB")
    print(f"Write time: {elapsed:.3f}s")
    print(f"Compression: Zstd level 3")
    print(f"Dictionary encoding: enabled")


def query_fraud_analysis(base_path):
    """Query fraud transactions with column pruning and predicate pushdown."""
    start = time.time()

    fraud_columns = ["card_network", "merchant_category", "country_code",
                     "amount", "is_fraud", "channel"]

    filters = [("is_fraud", "=", True)]

    dataset = pq.ParquetDataset(
        base_path,
        filters=filters,
        use_legacy_dataset=False,
    )

    table = dataset.read(columns=fraud_columns)
    elapsed = time.time() - start

    df = table.to_pandas()

    print(f"\n=== Fraud Analysis ===")
    print(f"Query time: {elapsed:.3f}s")
    print(f"Fraud transactions: {len(df):,}")
    print(f"Total fraud amount: ${df['amount'].sum():,.2f}")

    print(f"\nFraud by card network:")
    print(df.groupby("card_network")["amount"].agg(["count", "sum"]).sort_values("sum", ascending=False).to_string())

    print(f"\nFraud by merchant category:")
    print(df.groupby("merchant_category")["amount"].agg(["count", "sum"]).sort_values("sum", ascending=False).head(5).to_string())

    return table


def query_monthly_summary(base_path):
    """Query monthly summary with minimal columns."""
    start = time.time()

    summary_columns = ["card_network", "amount", "currency"]

    dataset = pq.ParquetDataset(
        base_path,
        use_legacy_dataset=False,
    )

    table = dataset.read(columns=summary_columns)
    elapsed = time.time() - start

    df = table.to_pandas()

    print(f"\n=== Monthly Summary ===")
    print(f"Query time: {elapsed:.3f}s")
    print(f"Total transactions: {len(df):,}")
    print(f"Total volume: ${df['amount'].sum():,.2f}")

    print(f"\nBy card network:")
    print(df.groupby("card_network")["amount"].agg(["count", "sum", "mean"]).to_string())

    return table


# ============================================================
# RUN THE SCENARIO
# ============================================================
if __name__ == "__main__":
    base_path = os.path.join(tempfile.gettempdir(), "card_transactions")
    os.makedirs(base_path, exist_ok=True)

    # Generate and store card transactions
    print("Generating credit card transaction data...")
    transactions = generate_card_transactions(num_transactions=1_000_000)
    store_card_transactions(transactions, base_path)

    # Fraud analysis
    query_fraud_analysis(base_path)

    # Monthly summary
    query_monthly_summary(base_path)
```

---

## 7. Interview Questions

### Q1: What is the difference between encoding and compression in Parquet?

**Answer:**
**Encoding** is a logical transformation that exploits data patterns to reduce size:
- Dictionary encoding: replaces strings with integer IDs
- Run-length encoding: stores (value, count) pairs
- Bit-packing: packs small integers into fewer bits

**Compression** is a general-purpose algorithm applied after encoding:
- Snappy, Gzip, Zstd, LZ4, Brotli
- Works on the encoded bytes, not the original data

**Pipeline**: Raw Data → Encoding → Compression → Disk

**Example**:
```
Status column: ["COMPLETED", "COMPLETED", "PENDING", ...]
  ↓ Dictionary Encoding
Indices: [0, 0, 1, ...]  (4 bytes each)
  ↓ Snappy Compression
Compressed bytes (smaller)
```

The key insight: encoding understands the data semantics, compression finds general patterns in the encoded output.

---

### Q2: When would you choose Zstd over Snappy for Parquet compression?

**Answer:**

| Factor | Snappy | Zstd |
|--------|--------|------|
| Compression speed | Faster | Slightly slower |
| Decompression speed | Very fast | Fast |
| Compression ratio | ~3-5x | ~6-8x |
| CPU usage | Lower | Medium |

**Choose Snappy when:**
- Query latency is critical (real-time dashboards)
- CPU is the bottleneck
- Data is frequently written and read
- Interactive exploration workloads

**Choose Zstd when:**
- Storage cost matters (cloud storage bills)
- Data is read-heavy (write once, read many)
- You want the best balance of speed and ratio
- You can tune the compression level (1-22)

**Recommendation**: For most new projects, **Zstd level 3** is the modern default. It provides near-Snappy decompression speed with significantly better compression ratios.

---

### Q3: How does dictionary encoding affect query performance?

**Answer:**

**Positive effects:**
1. **Reduced I/O**: Dictionary pages are small and cached. Data pages contain small integers instead of strings.
2. **Better predicate pushdown**: Dictionary pages enable fast equality checks.
3. **Compression**: Integer indices compress better than strings.

**Negative effects:**
1. **Dictionary page overhead**: Must be loaded into memory before decoding data pages.
2. **High-cardinality penalty**: If every value is unique, dictionary encoding adds overhead (dictionary + indices > raw data).
3. **Update cost**: Any data modification requires rebuilding the dictionary.

**When it helps most:**
```
status column (3 unique values):
  Without dictionary: 7 bytes × 1B rows = 7 GB
  With dictionary: 4 bytes × 1B rows + 20 bytes = 4 GB
  Savings: 43%

transaction_id (1B unique values):
  Without dictionary: 8 bytes × 1B rows = 8 GB
  With dictionary: 4 bytes × 1B rows + 8 GB dictionary = 12 GB
  WORSE: 50% overhead
```

---

### Q4: What are the trade-offs between compression levels?

**Answer:**
Higher compression levels produce smaller files but require more CPU:

**Zstd example:**
```
Level 1:  Fast, ~5x ratio
Level 3:  Balanced, ~7x ratio  ← recommended
Level 9:  Slow, ~8x ratio
Level 15: Very slow, ~9x ratio
Level 22: Extremely slow, ~9.5x ratio
```

**Gzip example:**
```
Level 1:  Fast, ~4x ratio
Level 6:  Default, ~6x ratio
Level 9:  Slow, ~7x ratio
```

**Trade-off framework:**

| Use Case | Recommended Level | Rationale |
|----------|------------------|-----------|
| Hot data (frequent reads) | Low (Snappy/Zstd 1-3) | Speed matters more |
| Warm data (occasional reads) | Medium (Zstd 3-6) | Balance |
| Cold data (rare reads) | High (Gzip 9 / Zstd 9) | Storage savings |
| Archival (compliance) | Maximum (Brotli/Gzip 9) | Minimal reads |

---

### Q5: Can you explain the "encoding fallback" mechanism in Parquet?

**Answer:**
Parquet has a built-in fallback mechanism when dictionary encoding becomes inefficient:

**Dictionary page size limit** (default: 1MB):
1. Parquet starts building a dictionary
2. If the dictionary exceeds the page size limit (1MB), it **falls back** to plain encoding
3. The column chunk is written without dictionary encoding

**Why this matters:**
- High-cardinality columns (transaction_id, UUID) automatically fall back to plain encoding
- Low-cardinality columns (status, currency) stay dictionary-encoded
- You don't need to manually configure encoding per column

**Configuration options:**
```python
pq.write_table(
    table,
    path,
    use_dictionary=True,              # Enable dictionary (default: True)
    dictionary_pagesize_limit=1_048_576,  # 1MB limit
    write_statistics=True,
)
```

**Alternative**: Disable dictionary for specific columns by setting `use_dictionary=False` (applies to all columns) or use Iceberg/Delta Lake which can configure encoding per column.
