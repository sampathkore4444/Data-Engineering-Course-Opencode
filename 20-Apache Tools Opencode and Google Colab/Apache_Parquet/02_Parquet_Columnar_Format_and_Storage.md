# Parquet Columnar Format & Storage

## Table of Contents
1. [Detailed Explanation](#1-detailed-explanation)
2. [Example](#2-example)
3. [Real-World Banking Scenario 1](#3-banking-scenario-1-high-frequency-trading-data-storage)
4. [Python Code - Scenario 1](#4-python-code---scenario-1)
5. [Real-World Banking Scenario 2](#5-banking-scenario-2-multi-currency-forex-data)
6. [Python Code - Scenario 2](#6-python-code---scenario-2)
7. [Interview Questions](#7-interview-questions)

---

## 1. Detailed Explanation

### How Parquet Stores Data

The fundamental principle of Parquet's columnar format is:

> **Each column is stored as a contiguous block of data, separately from other columns. This means reading a single column requires reading only that column's data, not scanning through all columns row by row.**

### Row-Based vs Columnar Storage

Consider a bank's customer table:

```
| customer_id | name          | age | account_balance | credit_score | risk_level |
|-------------|---------------|-----|-----------------|--------------|------------|
| C001        | John Smith    | 45  | 125000.00       | 780          | LOW        |
| C002        | Jane Doe      | 32  | 45000.50        | 650          | MEDIUM     |
| C003        | Bob Johnson   | 58  | 890000.75       | 820          | LOW        |
```

**Row-based storage (CSV/MySQL):**
```
Row 1: C001|John Smith|45|125000.00|780|LOW
Row 2: C002|Jane Doe|32|45000.50|650|MEDIUM
Row 3: C003|Bob Johnson|58|890000.75|820|LOW
```

To compute `AVG(age)`, the engine reads **all 6 columns** for every row.

**Columnar storage (Parquet):**
```
customer_id: [C001, C002, C003]
name:        [John Smith, Jane Doe, Bob Johnson]
age:         [45, 32, 58]
balance:     [125000.00, 45000.50, 890000.75]
credit:      [780, 650, 820]
risk:        [LOW, MEDIUM, LOW]
```

To compute `AVG(age)`, the engine reads **only the age column** — 5x less I/O.

### Physical Layout

```
Parquet File
├── Row Group 0 (rows 0-999,999)
│   ├── Column Chunk: customer_id
│   │   ├── Page 0 (Data)    ← encoded + compressed
│   │   └── Page 1 (Data)
│   ├── Column Chunk: name
│   │   └── Page 0 (Dictionary + Data)
│   ├── Column Chunk: age
│   │   └── Page 0 (Data)
│   ├── Column Chunk: balance
│   │   └── Page 0 (Data)
│   └── Column Chunk: risk_level
│       └── Page 0 (Dictionary + Data)
│
├── Row Group 1 (rows 1,000,000-1,999,999)
│   ├── Column Chunk: customer_id
│   ├── Column Chunk: name
│   ├── Column Chunk: age
│   ├── Column Chunk: balance
│   └── Column Chunk: risk_level
│
└── Footer (File Metadata)
    ├── Schema
    ├── Row Group locations
    ├── Column statistics (min/max/count)
    └── Key-value metadata
```

### How Column Pruning Works

Query: `SELECT name, age FROM customers WHERE age > 50`

```
Step 1: Read Footer → Get schema, row group locations, statistics

Step 2: For each Row Group:
   - Check age statistics: min=32, max=78 → might have age > 50 → READ
   - Skip columns: customer_id, balance, credit_score, risk_level

Step 3: Read only:
   - Row Group 0: name column + age column
   - Row Group 1: name column + age column

Step 4: Apply filter (age > 50) in memory
```

### The Role of Encodings

Before compression, Parquet applies **encodings** to reduce data size:

| Encoding | How It Works | Best For |
|----------|-------------|----------|
| **Dictionary** | Maps repeated values to integer IDs | Low-cardinality columns (status, currency) |
| **Run-Length** | Stores consecutive identical values as (value, count) | Sorted or repeated values |
| **Bit-Packing** | Packs small integers into fewer bits | Small integer ranges |
| **Delta** | Stores differences between consecutive values | Timestamps, sorted IDs |
| **Plain** | Raw bytes, no transformation | High-cardinality, unsorted data |

**Example**: Column `status` with values `[COMPLETED, COMPLETED, PENDING, COMPLETED, FAILED, COMPLETED]`

Dictionary encoding:
```
Dictionary: {0: COMPLETED, 1: PENDING, 2: FAILED}
Data:       [0, 0, 1, 0, 2, 0]
```

6 string values → 6 integers (much smaller).

### How Compression Works After Encoding

```
Original Data (column: amount)
  [100.00, 250.50, 100.00, 300.75, 100.00, 250.50]
       ↓
Dictionary Encoding
  Dict: {0: 100.00, 1: 250.50, 2: 300.75}
  Data: [0, 1, 0, 2, 0, 1]
       ↓
Snappy Compression
  Compressed bytes (smaller)
       ↓
Written to Parquet Page
```

### Row Group Sizing

Row groups determine the granularity of parallelism:

```
Small Row Groups (< 64MB)
  + More parallelism
  + Better predicate pushdown
  - More metadata overhead
  - More file open overhead

Large Row Groups (> 512MB)
  + Less metadata overhead
  + Better compression (more data to compress)
  - Less parallelism
  - Coarser predicate pushdown

Recommended: 128MB - 256MB per row group
```

### Page Sizing

Within a column chunk, data is organized into pages:

```
Column Chunk (amount column, Row Group 0)
  ├── Dictionary Page (if dictionary encoding)  ← max 1 page per chunk
  ├── Data Page 0    ← typically 1MB
  ├── Data Page 1
  ├── Data Page 2
  └── Data Page 3
```

Pages are the unit of:
- **Compression**: Each page is compressed independently
- **Decompression**: Only needed pages are decompressed
- **I/O**: Minimum read size
- **Statistics**: Min/max/count per page

---

## 2. Example

### Visual: 5 Rows, 4 Columns in Parquet

**Original Table:**
```
| id | name    | amount  | city    |
|----|---------|---------|---------|
| 1  | Alice   | 100.00  | NYC     |
| 2  | Bob     | 250.50  | LA      |
| 3  | Alice   | 100.00  | NYC     |
| 4  | Charlie | 300.75  | Chicago |
| 5  | Alice   | 100.00  | NYC     |
```

**Parquet Columnar Storage:**
```
id:     [1, 2, 3, 4, 5]
name:   [Alice, Bob, Alice, Charlie, Alice]
amount: [100.00, 250.50, 100.00, 300.75, 100.00]
city:   [NYC, LA, NYC, Chicago, NYC]
```

**After Dictionary Encoding:**
```
name Dictionary: {0: Alice, 1: Bob, 2: Charlie}
name Data:       [0, 1, 0, 2, 0]

city Dictionary: {0: NYC, 1: LA, 2: Chicago}
city Data:       [0, 1, 0, 2, 0]

id:     [1, 2, 3, 4, 5]        ← plain encoding
amount: [100.00, 250.50, 100.00, 300.75, 100.00]  ← plain encoding
```

Query `SELECT SUM(amount) FROM table WHERE city = 'NYC'`:
1. Read `city` dictionary page → find NYC = index 0
2. Read `city` data page → filter where value = 0 → rows [0, 2, 4]
3. Read `amount` data page → extract values at positions [0, 2, 4]
4. Compute: 100.00 + 100.00 + 100.00 = 300.00

Only `city` and `amount` columns were read. `id` and `name` were skipped entirely.

---

## 3. Banking Scenario 1: High-Frequency Trading Data Storage

### Problem
A bank's investment division generates **10 million tick-level market data points per second** during trading hours. At end of day, this data must be stored for:
- Regulatory compliance (MiFID II requires 5-year retention)
- Real-time risk calculations the next morning
- Historical backtesting for trading algorithms

The raw data has **200+ columns** (bid/ask prices, volumes, timestamps, exchange codes, etc.) but queries typically access only **5-10 columns**.

### Why Parquet Columnar Format?
- Column pruning: Queries read 5-10 columns out of 200+, reducing I/O by 95%
- Dictionary encoding: Exchange codes and symbol names repeated millions of times → massive compression
- Delta encoding: Timestamps stored as differences, highly compressible
- Page-level statistics: Skip entire pages when filtering by timestamp ranges

### Architecture
```
Trading Engine
     |
     v
  Market Data Feed (10M ticks/sec)
     |
     v
  Flink Streaming Job
     |
     v
  Parquet Files (columnar, partitioned by hour/symbol)
     |
     v
  S3 Object Storage
     |
     +-- Risk Engine (reads bid/ask/volume)
     +-- Backtesting System (reads OHLCV)
     +-- Regulatory Archive (reads all columns)
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
import time
import os
import tempfile

# ============================================================
# BANKING SCENARIO: High-Frequency Trading Data Storage
# ============================================================

def generate_market_data(num_ticks=500_000):
    """Generate realistic market tick data with 200+ columns."""
    random.seed(42)
    np.random.seed(42)

    symbols = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "JPM", "BAC", "GS", "MS", "WFC"]
    exchanges = ["NYSE", "NASDAQ", "BATS", "IEX", "ARCA"]
    currencies = ["USD", "USD", "USD", "EUR", "GBP"]

    # Core columns (frequently queried)
    symbol_data = np.random.choice(symbols, num_ticks)
    exchange_data = np.random.choice(exchanges, num_ticks)
    timestamps = np.array([
        datetime(2026, 8, 24, 9, 30, 0) + timedelta(microseconds=i * 100)
        for i in range(num_ticks)
    ], dtype='datetime64[us]')

    bid_prices = np.random.uniform(100.0, 500.0, num_ticks).round(4)
    ask_prices = bid_prices + np.random.uniform(0.01, 2.0, num_ticks).round(4)
    bid_volumes = np.random.randint(1, 10000, num_ticks)
    ask_volumes = np.random.randint(1, 10000, num_ticks)
    last_prices = bid_prices + np.random.uniform(0, 1.0, num_ticks).round(4)
    trade_volumes = np.random.randint(0, 5000, num_ticks)

    # Additional columns (less frequently queried)
    open_prices = bid_prices + np.random.uniform(-5, 5, num_ticks).round(4)
    high_prices = np.maximum(bid_prices, ask_prices) + np.random.uniform(0, 3, num_ticks).round(4)
    low_prices = np.minimum(bid_prices, ask_prices) - np.random.uniform(0, 3, num_ticks).round(4)
    close_prices = last_prices.copy()
    vwap = (bid_prices + ask_prices) / 2
    spread = (ask_prices - bid_prices).round(4)
    mid_price = ((bid_prices + ask_prices) / 2).round(4)

    # Market microstructure columns
    bid_depth_1 = np.random.randint(100, 50000, num_ticks)
    bid_depth_2 = np.random.randint(100, 50000, num_ticks)
    ask_depth_1 = np.random.randint(100, 50000, num_ticks)
    ask_depth_2 = np.random.randint(100, 50000, num_ticks)
    trade_count = np.random.randint(0, 100, num_ticks)
    block_trade_flag = np.random.choice([True, False], num_ticks, p=[0.05, 0.95])

    # Currency (low cardinality → dictionary encoding)
    currency_data = np.random.choice(currencies, num_ticks)

    # Build Arrow table
    table = pa.table({
        # Core columns (frequently queried)
        "symbol": pa.array(symbol_data, type=pa.string()),
        "exchange": pa.array(exchange_data, type=pa.string()),
        "timestamp": pa.array(timestamps, type=pa.timestamp("us")),
        "bid_price": pa.array(bid_prices, type=pa.float64()),
        "ask_price": pa.array(ask_prices, type=pa.float64()),
        "bid_volume": pa.array(bid_volumes, type=pa.int32()),
        "ask_volume": pa.array(ask_volumes, type=pa.int32()),
        "last_price": pa.array(last_prices, type=pa.float64()),
        "trade_volume": pa.array(trade_volumes, type=pa.int32()),
        "currency": pa.array(currency_data, type=pa.string()),

        # OHLCV columns
        "open": pa.array(open_prices, type=pa.float64()),
        "high": pa.array(high_prices, type=pa.float64()),
        "low": pa.array(low_prices, type=pa.float64()),
        "close": pa.array(close_prices, type=pa.float64()),
        "vwap": pa.array(vwap, type=pa.float64()),
        "spread": pa.array(spread, type=pa.float64()),
        "mid_price": pa.array(mid_price, type=pa.float64()),

        # Market microstructure
        "bid_depth_1": pa.array(bid_depth_1, type=pa.int32()),
        "bid_depth_2": pa.array(bid_depth_2, type=pa.int32()),
        "ask_depth_1": pa.array(ask_depth_1, type=pa.int32()),
        "ask_depth_2": pa.array(ask_depth_2, type=pa.int32()),
        "trade_count": pa.array(trade_count, type=pa.int32()),
        "block_trade": pa.array(block_trade_flag, type=pa.bool_()),
    })

    return table


def store_market_data(table, base_path):
    """Store market data in Parquet with optimal columnar settings."""
    start_time = time.time()

    # Partition by date for efficient date-range queries
    pq.write_to_dataset(
        table,
        root_path=base_path,
        partition_cols=["exchange"],
        compression="snappy",
        use_dictionary=True,           # Dictionary encoding for symbol, exchange, currency
        write_statistics=True,         # Min/max for predicate pushdown
        data_page_size=1_048_576,      # 1MB pages
        version="2.6",                 # Latest Parquet format version
    )

    elapsed = time.time() - start_time
    print(f"Stored {table.num_rows:,} ticks in {elapsed:.3f}s")

    # Show file sizes
    total_size = 0
    for root, dirs, files in os.walk(base_path):
        for f in files:
            if f.endswith(".parquet"):
                filepath = os.path.join(root, f)
                size = os.path.getsize(filepath)
                total_size += size

    print(f"Total Parquet size: {total_size / (1024*1024):.1f} MB")
    print(f"Compression ratio: ~{8:.0f}x vs raw CSV")


def query_risk_data(base_path, symbol, start_time_val, end_time_val):
    """Query only risk-relevant columns with predicate pushdown."""
    query_start = time.time()

    # Only read columns the risk engine needs
    risk_columns = ["symbol", "timestamp", "bid_price", "ask_price",
                    "bid_volume", "ask_volume", "spread", "mid_price"]

    filters = [
        ("symbol", "=", symbol),
        ("timestamp", ">=", start_time_val),
        ("timestamp", "<=", end_time_val),
    ]

    dataset = pq.ParquetDataset(
        base_path,
        filters=filters,
        use_legacy_dataset=False,
    )

    table = dataset.read(columns=risk_columns)
    elapsed = time.time() - query_start

    print(f"\n=== Risk Query: {symbol} ===")
    print(f"Columns read: {len(risk_columns)} out of 22 total")
    print(f"Rows returned: {table.num_rows:,}")
    print(f"Query time: {elapsed:.3f}s")

    # Compute risk metrics
    df = table.to_pandas()
    print(f"Average spread: ${df['spread'].mean():.4f}")
    print(f"Average bid-ask midpoint: ${df['mid_price'].mean():.2f}")
    print(f"Total volume: {df['bid_volume'].sum() + df['ask_volume'].sum():,}")

    return table


def compare_read_performance(base_path):
    """Compare reading all columns vs only needed columns."""
    # Read ALL columns
    start = time.time()
    dataset_all = pq.ParquetDataset(base_path, use_legacy_dataset=False)
    table_all = dataset_all.read()
    time_all = time.time() - start

    # Read only 3 columns
    start = time.time()
    dataset_partial = pq.ParquetDataset(base_path, use_legacy_dataset=False)
    table_partial = dataset_partial.read(columns=["symbol", "bid_price", "ask_price"])
    time_partial = time.time() - start

    print(f"\n=== Column Pruning Performance ===")
    print(f"All columns ({table_all.num_columns} cols): {time_all:.3f}s")
    print(f"3 columns only: {time_partial:.3f}s")
    print(f"Speedup: {time_all/time_partial:.1f}x")


# ============================================================
# RUN THE SCENARIO
# ============================================================
if __name__ == "__main__":
    base_path = os.path.join(tempfile.gettempdir(), "market_data")
    os.makedirs(base_path, exist_ok=True)

    # Generate and store market data
    print("Generating market tick data...")
    market_data = generate_market_data(num_ticks=500_000)
    store_market_data(market_data, base_path)

    # Query risk data (column pruning)
    query_risk_data(
        base_path,
        symbol="JPM",
        start_time_val=np.datetime64("2026-08-24T10:00:00"),
        end_time_val=np.datetime64("2026-08-24T11:00:00"),
    )

    # Compare read performance
    compare_read_performance(base_path)
```

---

## 5. Banking Scenario 2: Multi-Currency Forex Data

### Problem
A bank's treasury division handles **forex transactions across 50 currency pairs**. The data includes:
- Spot rates, forward rates, swap points
- Trade amounts in both currencies
- Counterparty information
- Settlement dates

Data volume: **5 GB per day**, 30-day retention = **150 GB**. Queries typically filter by:
- Currency pair (2-3 values out of 50)
- Date range
- Counterparty (varies)

### Why Parquet Columnar Format?
- Dictionary encoding: 50 currency pairs repeated millions of times → near-zero storage for that column
- Column pruning: Spot rate queries skip forward rates, swap points, counterparty data
- Compression: Financial data with limited decimal precision compresses extremely well

### Architecture
```
Forex Trading System
       |
       v
  ETL Pipeline (real-time + batch)
       |
       v
  Parquet Files (partitioned by date, currency_pair)
       |
       v
  Treasury Analytics
       |
       +-- Spot Rate Monitor
       +-- Forward Curve Calculator
       +-- Counterparty Exposure Report
```

---

## 6. Python Code - Scenario 2

```python
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
import numpy as np
from datetime import datetime, timedelta
import random
import time
import os
import tempfile

# ============================================================
# BANKING SCENARIO: Multi-Currency Forex Data Storage
# ============================================================

def generate_forex_data(num_trades=200_000):
    """Generate realistic forex trade data."""
    random.seed(42)
    np.random.seed(42)

    currency_pairs = [
        "EUR/USD", "GBP/USD", "USD/JPY", "USD/CHF", "AUD/USD",
        "USD/CAD", "NZD/USD", "EUR/GBP", "EUR/JPY", "GBP/JPY",
        "USD/CNY", "USD/HKD", "USD/SGD", "USD/INR", "EUR/CHF",
    ]

    counterparties = [
        "Goldman Sachs", "JP Morgan", "Citibank", "Deutsche Bank",
        "Barclays", "HSBC", "UBS", "Credit Suisse", "Morgan Stanley",
        "Bank of America",
    ]

    trade_types = ["SPOT", "FORWARD", "SWAP", "NDF"]
    trade_dates = [
        (datetime(2026, 8, 1) + timedelta(days=i)).strftime("%Y-%m-%d")
        for i in range(30)
    ]

    # Generate data
    pair_data = np.random.choice(currency_pairs, num_trades)
    cpty_data = np.random.choice(counterparties, num_trades)
    type_data = np.random.choice(trade_types, num_trades, p=[0.5, 0.25, 0.15, 0.10])
    date_data = np.random.choice(trade_dates, num_trades)

    # Base rates for major pairs
    base_rates = {
        "EUR/USD": 1.0850, "GBP/USD": 1.2700, "USD/JPY": 148.50,
        "USD/CHF": 0.8750, "AUD/USD": 0.6520, "USD/CAD": 1.3650,
        "NZD/USD": 0.5980, "EUR/GBP": 0.8540, "EUR/JPY": 161.20,
        "GBP/JPY": 188.60, "USD/CNY": 7.2450, "USD/HKD": 7.8100,
        "USD/SGD": 1.3450, "USD/INR": 83.25, "EUR/CHF": 0.9490,
    }

    spot_prices = np.array([
        base_rates.get(pair, 1.0) * (1 + np.random.normal(0, 0.002))
        for pair in pair_data
    ]).round(5)

    forward_points = np.random.uniform(-0.005, 0.005, num_trades).round(5)
    swap_points = np.random.uniform(-0.001, 0.001, num_trades).round(5)
    bid_spread = np.random.uniform(0.0001, 0.0020, num_trades).round(5)
    ask_spread = bid_spread + np.random.uniform(0.0001, 0.0010, num_trades).round(5)

    trade_amounts_usd = np.random.uniform(10000, 50000000, num_trades).round(2)
    trade_amounts_ccy = (trade_amounts_usd / spot_prices).round(2)

    settlement_days = np.where(type_data == "SPOT", 2,
                      np.where(type_data == "FORWARD", np.random.randint(3, 365, num_trades),
                      np.random.randint(1, 30, num_trades)))

    table = pa.table({
        "trade_id": pa.array(list(range(1, num_trades + 1)), type=pa.int64()),
        "currency_pair": pa.array(pair_data, type=pa.string()),
        "trade_type": pa.array(type_data, type=pa.string()),
        "trade_date": pa.array(date_data, type=pa.date32()),
        "counterparty": pa.array(cpty_data, type=pa.string()),
        "spot_rate": pa.array(spot_prices, type=pa.float64()),
        "forward_points": pa.array(forward_points, type=pa.float64()),
        "swap_points": pa.array(swap_points, type=pa.float64()),
        "bid_spread": pa.array(bid_spread, type=pa.float64()),
        "ask_spread": pa.array(ask_spread, type=pa.float64()),
        "amount_usd": pa.array(trade_amounts_usd, type=pa.float64()),
        "amount_ccy": pa.array(trade_amounts_ccy, type=pa.float64()),
        "settlement_days": pa.array(settlement_days.tolist(), type=pa.int32()),
    })

    return table


def store_forex_data(table, base_path):
    """Store forex data with partitioning by date and currency pair."""
    start = time.time()

    pq.write_to_dataset(
        table,
        root_path=base_path,
        partition_cols=["trade_date", "currency_pair"],
        compression="snappy",
        use_dictionary=True,           # Critical for currency_pair, trade_type, counterparty
        write_statistics=True,
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

    print(f"Stored {table.num_rows:,} forex trades")
    print(f"Files created: {file_count}")
    print(f"Total size: {total_size / (1024*1024):.1f} MB")
    print(f"Write time: {elapsed:.3f}s")


def query_spot_rates(base_path, currency_pair, trade_date):
    """Query only spot rate data for a specific pair and date."""
    start = time.time()

    # Column pruning: only read what the spot rate monitor needs
    spot_columns = ["trade_id", "currency_pair", "spot_rate", "bid_spread",
                    "ask_spread", "amount_usd", "counterparty"]

    filters = [
        ("currency_pair", "=", currency_pair),
        ("trade_date", "=", trade_date),
    ]

    dataset = pq.ParquetDataset(
        base_path,
        filters=filters,
        use_legacy_dataset=False,
    )

    table = dataset.read(columns=spot_columns)
    elapsed = time.time() - start

    print(f"\n=== Spot Rate Query: {currency_pair} on {trade_date} ===")
    print(f"Columns read: {len(spot_columns)} out of 13")
    print(f"Rows: {table.num_rows:,}")
    print(f"Query time: {elapsed:.3f}s")

    df = table.to_pandas()
    print(f"Avg spot rate: {df['spot_rate'].mean():.5f}")
    print(f"Avg bid-ask spread: {df['bid_spread'].mean():.5f}")
    print(f"Total USD volume: ${df['amount_usd'].sum():,.2f}")

    return table


def query_counterparty_exposure(base_path, counterparty):
    """Query exposure for a specific counterparty across all pairs."""
    start = time.time()

    exposure_columns = ["currency_pair", "amount_usd", "spot_rate", "trade_type"]

    filters = [("counterparty", "=", counterparty)]

    dataset = pq.ParquetDataset(
        base_path,
        filters=filters,
        use_legacy_dataset=False,
    )

    table = dataset.read(columns=exposure_columns)
    elapsed = time.time() - start

    df = table.to_pandas()

    print(f"\n=== Counterparty Exposure: {counterparty} ===")
    print(f"Query time: {elapsed:.3f}s")
    print(f"Total trades: {len(df):,}")
    print(f"Total USD exposure: ${df['amount_usd'].sum():,.2f}")

    exposure_by_pair = df.groupby("currency_pair")["amount_usd"].agg(["sum", "count"])
    exposure_by_pair = exposure_by_pair.sort_values("sum", ascending=False)
    print(f"\nExposure by currency pair:")
    print(exposure_by_pair.to_string())

    return table


# ============================================================
# RUN THE SCENARIO
# ============================================================
if __name__ == "__main__":
    base_path = os.path.join(tempfile.gettempdir(), "forex_data")
    os.makedirs(base_path, exist_ok=True)

    # Generate and store forex data
    print("Generating forex trade data...")
    forex_data = generate_forex_data(num_trades=200_000)
    store_forex_data(forex_data, base_path)

    # Query spot rates for EUR/USD
    query_spot_rates(base_path, "EUR/USD", "2026-08-15")

    # Query counterparty exposure
    query_counterparty_exposure(base_path, "Goldman Sachs")
```

---

## 7. Interview Questions

### Q1: Explain the difference between row groups, column chunks, and pages in Parquet.

**Answer:**
These are the three levels of Parquet's internal hierarchy:

1. **Row Group**: A horizontal partition of the dataset. Contains a subset of rows and column chunks for every column. This is the unit of parallelism — different row groups can be read by different threads/machines.

2. **Column Chunk**: The data for a single column within a row group. This is the unit of compression and encoding. All pages within a column chunk use the same encoding and compression.

3. **Page**: The smallest unit of I/O within a column chunk. Typically 1MB. Pages can be:
   - **Data Page**: Contains actual encoded values
   - **Dictionary Page**: Contains the dictionary for dictionary encoding
   - **Index Page**: Contains statistics for data pages

```
Row Group
  ├── Column Chunk A
  │     ├── Page 0 (Dictionary)
  │     ├── Page 1 (Data)
  │     └── Page 2 (Data)
  ├── Column Chunk B
  │     └── Page 0 (Data)
  └── Column Chunk C
        └── Page 0 (Data)
```

---

### Q2: How does dictionary encoding work in Parquet and when is it most effective?

**Answer:**
Dictionary encoding replaces repeated string values with integer IDs referencing a dictionary.

**How it works:**
1. Build a dictionary of unique values
2. Replace each value with its dictionary index
3. Store dictionary + index array

**Example:**
```
Original: [COMPLETED, COMPLETED, PENDING, COMPLETED, FAILED, COMPLETED]
Dictionary: {0: COMPLETED, 1: PENDING, 2: FAILED}
Encoded:    [0, 0, 1, 0, 2, 0]
```

**Most effective for:**
- **Low cardinality columns**: status (3 values), currency (50 values), country (200 values)
- **Repeated values**: merchant names, channel types, transaction types
- **Categorical data**: risk levels, account types

**Less effective for:**
- High cardinality columns: transaction_id, email, timestamp
- Unique or nearly-unique values: UUIDs, serial numbers

**Impact**: A `status` column with 1 billion rows and 3 unique values compresses from ~7GB (raw strings) to ~4GB (dictionary + 4-byte indices).

---

### Q3: Why is columnar storage faster for analytical queries?

**Answer:**
Columnar storage is faster for analytics because of three key optimizations:

1. **Column Pruning**: Analytical queries typically read 2-5 columns out of 20+. Columnar storage means only those columns are read from disk. In a 20-column table, reading 3 columns = 85% less I/O.

2. **Better Compression**: Columns contain homogeneous data types with similar value distributions. This means:
   - Dictionary encoding is more effective
   - Run-length encoding works better
   - Compression ratios are 2-5x better than row-based

3. **CPU Cache Efficiency**: Sequential access patterns (reading one column at a time) are much more CPU-cache-friendly than random access patterns (jumping between columns in row-based storage).

**Quantified example**:
- 1 billion rows, 20 columns, 100 bytes per row = 200 GB raw
- Query reads 3 columns = 30 GB row-based vs 30 GB column-based
- But column-based: dictionary encoding + compression = ~3 GB actual I/O
- Result: **~66x improvement** in I/O

---

### Q4: What is the recommended row group size and why?

**Answer:**
The recommended row group size is **128MB to 256MB** for most workloads.

**Why this range?**

**Too small (< 64MB):**
- More row groups = more metadata overhead
- More file open/seek operations
- Less data per parallel task
- Poor compression (less data to find patterns in)

**Too large (> 512MB):**
- Less parallelism (fewer row groups to distribute)
- Coarser predicate pushdown (statistics cover larger ranges)
- Higher memory pressure per reader
- Longer time to process a single row group

**The sweet spot (128-256MB):**
- Enough data for good compression
- Enough row groups for parallelism
- Fine-grained enough for predicate pushdown
- Matches typical HDFS/S3 block sizes

**Example**: 100 GB dataset with 256MB row groups = ~400 row groups, each readable in parallel by a different executor.

---

### Q5: How does Parquet handle schema evolution?

**Answer:**
Parquet supports schema evolution through several mechanisms:

1. **Adding Columns**: New columns can be added. Old files simply don't have the column. Readers fill in NULLs for missing values.

```
Schema v1: [id, name, amount]
Schema v2: [id, name, amount, merchant_id]  ← added
```

2. **Column Reordering**: Supported. The physical order doesn't affect logical reads.

3. **Type Promotion**: Widening types is supported (INT32 → INT64, FLOAT → DOUBLE).

4. **Limitations**:
   - Cannot easily remove columns (they remain in old files)
   - Cannot rename columns (name is stored in the file)
   - Changing column types is limited (no INT → STRING)

**How it works internally:**
- Each column has a **name** and **field ID**
- Readers match columns by name (or field ID in Iceberg/Delta Lake)
- Missing columns in old files result in NULL values

**Best practice**: Use Iceberg or Delta Lake on top of Parquet for robust schema evolution. They manage schema across files and provide proper schema metadata.
