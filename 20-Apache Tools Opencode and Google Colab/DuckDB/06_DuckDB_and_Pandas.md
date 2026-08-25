# DuckDB and Pandas

## Table of Contents
1. [Detailed Explanation](#1-detailed-explanation)
2. [Example](#2-example)
3. [Real-World Banking Scenario 1](#3-banking-scenario-1-data-transformation)
4. [Python Code - Scenario 1](#4-python-code---scenario-1)
5. [Real-World Banking Scenario 2](#5-banking-scenario-2-ml-feature-engineering)
6. [Python Code - Scenario 2](#6-python-code---scenario-2)
7. [Interview Questions](#7-interview-questions)

---

## 1. Detailed Explanation

### DuckDB + Pandas Integration

> **DuckDB can query Pandas DataFrames directly using SQL — no conversion, no data loading, no copies. This combines Pandas' data manipulation with DuckDB's SQL power.**

### How It Works

```
Pandas DataFrame (in-memory)
       |
       v
  DuckDB (registers DataFrame)
       |
       v
  SQL Query (SELECT, JOIN, GROUP BY)
       |
       v
  Result (DataFrame / Arrow Table)
```

### Key Benefits

1. **SQL for DataFrames**: Write SQL instead of method chains
2. **Automatic optimization**: DuckDB optimizes DataFrame queries
3. **Zero-copy**: No data duplication
4. **Large DataFrames**: DuckDB handles memory efficiently
5. **Complex operations**: JOINs, window functions, CTEs

### Integration Methods

#### Method 1: Register DataFrame
```python
import duckdb
import pandas as pd

df = pd.DataFrame({"id": [1, 2], "amount": [100, 200]})
con = duckdb.connect()
con.register("my_table", df)

result = con.execute("SELECT * FROM my_table WHERE amount > 100").fetchdf()
```

#### Method 2: Query DataFrame directly
```python
result = con.execute("SELECT * FROM df WHERE amount > 100").fetchdf()
```

#### Method 3: Convert result to Pandas
```python
result = con.execute("SELECT SUM(amount) FROM df").fetchdf()  # Already DataFrame
```

### DuckDB SQL vs Pandas Methods

| Operation | Pandas | DuckDB SQL |
|-----------|--------|------------|
| Filter | `df[df.amount > 100]` | `SELECT * FROM df WHERE amount > 100` |
| Group By | `df.groupby('status').sum()` | `SELECT status, SUM(*) FROM df GROUP BY status` |
| Join | `pd.merge(df1, df2, on='id')` | `SELECT * FROM df1 JOIN df2 ON df1.id = df2.id` |
| Window | `df.amount.rank()` | `RANK() OVER (ORDER BY amount)` |
| Sort | `df.sort_values('amount')` | `ORDER BY amount` |
| Aggregate | `df.amount.sum()` | `SELECT SUM(amount) FROM df` |

### When to Use Which?

**Use DuckDB SQL when:**
- Complex queries (multiple JOINs, window functions)
- Large DataFrames (DuckDB handles memory better)
- SQL is more readable than method chains
- Need CTEs for readability

**Use Pandas when:**
- Simple transformations
- Data manipulation (melt, pivot, explode)
- Integration with ML libraries
- Quick prototyping

---

## 2. Example

### DuckDB + Pandas Demo

```python
import duckdb
import pandas as pd
import numpy as np

# Create sample data
np.random.seed(42)
num_rows = 10000

df = pd.DataFrame({
    "id": range(1, num_rows + 1),
    "customer_id": [f"CUST{np.random.randint(1, 1000):04d}" for _ in range(num_rows)],
    "amount": np.random.uniform(1.0, 100000.0, num_rows).round(2),
    "status": np.random.choice(["COMPLETED", "PENDING", "FAILED"], num_rows),
    "channel": np.random.choice(["ONLINE", "MOBILE", "BRANCH", "ATM"], num_rows),
})

# Create DuckDB connection
con = duckdb.connect()

# Query DataFrame directly
print("=== Querying Pandas DataFrame with DuckDB ===")

# 1. Simple aggregation
result = con.execute("""
    SELECT status, COUNT(*), AVG(amount)
    FROM df
    GROUP BY status
""").fetchdf()
print("\nStatus Summary:")
print(result)

# 2. Window function
result = con.execute("""
    SELECT 
        id,
        customer_id,
        amount,
        RANK() OVER (PARTITION BY status ORDER BY amount DESC) as rank_in_status
    FROM df
    LIMIT 10
""").fetchdf()
print("\nWindow Function:")
print(result)

# 3. CTE
result = con.execute("""
    WITH 
    customer_stats AS (
        SELECT 
            customer_id,
            COUNT(*) as tx_count,
            SUM(amount) as total_amount
        FROM df
        GROUP BY customer_id
    )
    SELECT *
    FROM customer_stats
    WHERE total_amount > 10000
    ORDER BY total_amount DESC
    LIMIT 10
""").fetchdf()
print("\nHigh-Value Customers:")
print(result)

con.close()
```

---

## 3. Banking Scenario 1: Data Transformation

### Problem
A bank needs to transform raw transaction data:
- Clean data (remove nulls, fix types)
- Enrich with calculations
- Aggregate for reporting
- Join with reference data

### Why DuckDB + Pandas?
- SQL transforms are more readable
- Complex joins easier in SQL
- Window functions for rankings
- Direct DataFrame output

### Architecture
```
Raw Pandas DataFrame
       |
       v
  DuckDB (SQL transforms)
       |
       +-- Clean data
       +-- Enrich calculations
       +-- Join reference data
       +-- Aggregate for reporting
       |
       v
  Transformed DataFrame
```

---

## 4. Python Code - Scenario 1

```python
import duckdb
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# ============================================================
# BANKING SCENARIO: Data Transformation
# ============================================================

def generate_raw_data():
    """Generate raw transaction data."""
    random.seed(42)
    np.random.seed(42)

    num_transactions = 50_000

    transactions = pd.DataFrame({
        "transaction_id": range(1, num_transactions + 1),
        "account_id": [f"ACC{random.randint(100000, 999999)}" for _ in range(num_transactions)],
        "amount": np.random.lognormal(6, 2, num_transactions).round(2),
        "currency": np.random.choice(["USD", "EUR", "GBP", None], num_transactions, p=[0.7, 0.15, 0.1, 0.05]),
        "status": np.random.choice(["COMPLETED", "PENDING", "FAILED", ""], num_transactions, p=[0.85, 0.08, 0.05, 0.02]),
        "channel": np.random.choice(["ONLINE", "MOBILE", "BRANCH", "ATM"], num_transactions),
        "date": pd.date_range("2026-01-01", periods=num_transactions, freq="30s"),
    })

    # Reference data
    accounts = pd.DataFrame({
        "account_id": [f"ACC{i:06d}" for i in range(1, 1001)],
        "customer_id": [f"CUST{random.randint(1, 500):04d}" for _ in range(1000)],
        "account_type": np.random.choice(["CHECKING", "SAVINGS", "CREDIT_CARD"], 1000),
    })

    customers = pd.DataFrame({
        "customer_id": [f"CUST{i:04d}" for i in range(1, 501)],
        "name": [f"Customer {i}" for i in range(1, 501)],
        "segment": np.random.choice(["PREMIUM", "STANDARD", "BASIC"], 500, p=[0.2, 0.5, 0.3]),
    })

    return transactions, accounts, customers


def transform_data(transactions_df, accounts_df, customers_df):
    """Transform data using DuckDB SQL."""
    con = duckdb.connect()

    # Register DataFrames
    con.register("transactions", transactions_df)
    con.register("accounts", accounts_df)
    con.register("customers", customers_df)

    # 1. Clean data
    print("=== Step 1: Clean Data ===")
    cleaned = con.execute("""
        SELECT 
            transaction_id,
            account_id,
            amount,
            COALESCE(currency, 'USD') as currency,
            CASE 
                WHEN status = '' THEN 'UNKNOWN'
                ELSE status
            END as status,
            channel,
            date
        FROM transactions
        WHERE amount > 0
    """).fetchdf()
    print(f"Cleaned rows: {len(cleaned)}")

    # 2. Enrich with account and customer info
    print("\n=== Step 2: Enrich Data ===")
    con.register("cleaned", cleaned)
    
    enriched = con.execute("""
        SELECT 
            c.transaction_id,
            c.account_id,
            a.customer_id,
            a.account_type,
            cust.segment as customer_segment,
            c.amount,
            c.currency,
            c.status,
            c.channel,
            c.date
        FROM cleaned c
        JOIN accounts a ON c.account_id = a.account_id
        JOIN customers cust ON a.customer_id = cust.customer_id
    """).fetchdf()
    print(f"Enriched rows: {len(enriched)}")

    # 3. Add calculated fields
    print("\n=== Step 3: Add Calculations ===")
    con.register("enriched", enriched)
    
    final = con.execute("""
        SELECT 
            *,
            CASE 
                WHEN customer_segment = 'PREMIUM' THEN amount * 1.1
                WHEN customer_segment = 'STANDARD' THEN amount * 1.05
                ELSE amount
            END as adjusted_amount,
            EXTRACT(HOUR FROM date) as transaction_hour,
            CASE 
                WHEN EXTRACT(HOUR FROM date) BETWEEN 9 AND 17 THEN 'BUSINESS_HOURS'
                ELSE 'AFTER_HOURS'
            END as time_category
        FROM enriched
    """).fetchdf()
    print(f"Final rows: {len(final)}")

    # 4. Aggregate for reporting
    print("\n=== Step 4: Aggregate for Reporting ===")
    con.register("final", final)
    
    summary = con.execute("""
        SELECT 
            customer_segment,
            account_type,
            status,
            COUNT(*) as tx_count,
            SUM(amount) as total_amount,
            AVG(amount) as avg_amount
        FROM final
        GROUP BY 1, 2, 3
        ORDER BY total_amount DESC
    """).fetchdf()
    print(summary.to_string(index=False))

    con.close()

    return final, summary


# ============================================================
# RUN THE SCENARIO
# ============================================================
if __name__ == "__main__":
    # Generate data
    print("Generating raw data...")
    transactions, accounts, customers = generate_raw_data()

    # Transform
    final, summary = transform_data(transactions, accounts, customers)
```

---

## 5. Banking Scenario 2: ML Feature Engineering

### Problem
A bank needs to engineer features for ML models:
- Transaction velocity (count per hour/day)
- Amount statistics (mean, std, z-score)
- Time-based features (hour, day of week)
- Customer behavior patterns

### Why DuckDB + Pandas?
- SQL window functions for rolling calculations
- CTEs for complex feature logic
- Direct DataFrame output for ML

### Architecture
```
Raw Transaction DataFrame
       |
       v
  DuckDB (feature engineering SQL)
       |
       +-- Velocity features
       +-- Amount statistics
       +-- Time features
       +-- Behavior patterns
       |
       v
  Feature DataFrame (for ML)
```

---

## 6. Python Code - Scenario 2

```python
import duckdb
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# ============================================================
# BANKING SCENARIO: ML Feature Engineering
# ============================================================

def generate_transaction_data(num_rows=100_000):
    """Generate transaction data for feature engineering."""
    random.seed(42)
    np.random.seed(42)

    timestamps = pd.date_range("2026-01-01", periods=num_rows, freq="2min")

    df = pd.DataFrame({
        "transaction_id": range(1, num_rows + 1),
        "card_id": [f"CARD{random.randint(10000, 99999)}" for _ in range(num_rows)],
        "amount": np.random.lognormal(3, 2, num_rows).round(2),
        "merchant_category": np.random.choice(
            ["GROCERY", "RESTAURANT", "GAS", "ONLINE", "ATM", "HOTEL"], num_rows
        ),
        "country": np.random.choice(["US", "GB", "DE", "FR", "JP"], num_rows),
        "channel": np.random.choice(["POS", "ONLINE", "MOBILE", "ATM"], num_rows),
        "is_fraud": np.random.choice([0, 1], num_rows, p=[0.97, 0.03]),
        "timestamp": timestamps,
    })

    return df


def engineer_features(df):
    """Engineer ML features using DuckDB SQL."""
    con = duckdb.connect()
    con.register("transactions", df)

    print("=== Feature Engineering with DuckDB ===")

    # 1. Velocity features
    print("\n1. Velocity Features...")
    features = con.execute("""
        SELECT 
            *,
            -- Transaction count in last 1 hour
            COUNT(*) OVER (
                PARTITION BY card_id 
                ORDER BY timestamp 
                RANGE BETWEEN INTERVAL '1 hour' PRECEDING AND CURRENT ROW
            ) as tx_count_1h,
            
            -- Transaction count in last 24 hours
            COUNT(*) OVER (
                PARTITION BY card_id 
                ORDER BY timestamp 
                RANGE BETWEEN INTERVAL '24 hours' PRECEDING AND CURRENT ROW
            ) as tx_count_24h,
            
            -- Amount sum in last 1 hour
            SUM(amount) OVER (
                PARTITION BY card_id 
                ORDER BY timestamp 
                RANGE BETWEEN INTERVAL '1 hour' PRECEDING AND CURRENT ROW
            ) as amount_sum_1h
        FROM transactions
    """).fetchdf()

    # 2. Amount statistics
    print("2. Amount Statistics...")
    con.register("features", features)
    
    features = con.execute("""
        SELECT 
            *,
            -- Amount mean and std in last 24 hours
            AVG(amount) OVER (
                PARTITION BY card_id 
                ORDER BY timestamp 
                RANGE BETWEEN INTERVAL '24 hours' PRECEDING AND CURRENT ROW
            ) as amount_mean_24h,
            
            STDDEV(amount) OVER (
                PARTITION BY card_id 
                ORDER BY timestamp 
                RANGE BETWEEN INTERVAL '24 hours' PRECEDING AND CURRENT ROW
            ) as amount_std_24h,
            
            -- Z-score
            (amount - AVG(amount) OVER (
                PARTITION BY card_id 
                ORDER BY timestamp 
                RANGE BETWEEN INTERVAL '24 hours' PRECEDING AND CURRENT ROW
            )) / NULLIF(STDDEV(amount) OVER (
                PARTITION BY card_id 
                ORDER BY timestamp 
                RANGE BETWEEN INTERVAL '24 hours' PRECEDING AND CURRENT ROW
            ), 0) as amount_zscore
        FROM features
    """).fetchdf()

    # 3. Time features
    print("3. Time Features...")
    con.register("features", features)
    
    features = con.execute("""
        SELECT 
            *,
            EXTRACT(HOUR FROM timestamp) as hour,
            EXTRACT(DOW FROM timestamp) as day_of_week,
            CASE WHEN EXTRACT(DOW FROM timestamp) IN (0, 6) THEN 1 ELSE 0 END as is_weekend,
            CASE WHEN EXTRACT(HOUR FROM timestamp) < 6 OR EXTRACT(HOUR FROM timestamp) > 22 THEN 1 ELSE 0 END as is_night
        FROM features
    """).fetchdf()

    # 4. Merchant risk features
    print("4. Merchant Risk Features...")
    con.register("features", features)
    
    features = con.execute("""
        SELECT 
            *,
            -- Merchant category fraud rate
            AVG(is_fraud) OVER (PARTITION BY merchant_category) as merchant_fraud_rate,
            
            -- Country fraud rate
            AVG(is_fraud) OVER (PARTITION BY country) as country_fraud_rate
        FROM features
    """).fetchdf()

    con.close()

    print(f"\nFeatures engineered: {len(features.columns)} columns")
    print(f"Rows: {len(features):,}")

    return features


def show_feature_importance(features_df):
    """Show feature correlations with fraud."""
    print("\n=== Feature Correlations with Fraud ===")
    
    numeric_cols = features_df.select_dtypes(include=[np.number]).columns
    correlations = features_df[numeric_cols].corr()["is_fraud"].drop("is_fraud")
    print(correlations.sort_values(ascending=False).to_string())


# ============================================================
# RUN THE SCENARIO
# ============================================================
if __name__ == "__main__":
    # Generate data
    print("Generating transaction data...")
    df = generate_transaction_data(num_rows=50_000)

    # Engineer features
    features = engineer_features(df)

    # Show feature importance
    show_feature_importance(features)

    # Preview features
    print("\n=== Feature Preview ===")
    print(features.head(10).to_string())
```

---

## 7. Interview Questions

### Q1: How do you query a Pandas DataFrame with DuckDB?

**Answer:**

**Method 1: Register DataFrame**
```python
con = duckdb.connect()
con.register("my_table", df)
result = con.execute("SELECT * FROM my_table WHERE amount > 100").fetchdf()
```

**Method 2: Query directly**
```python
result = con.execute("SELECT * FROM df WHERE amount > 100").fetchdf()
```

**Benefits:**
- SQL syntax for complex queries
- Automatic optimization
- No data copying
- Works with large DataFrames

---

### Q2: When would you use DuckDB SQL over Pandas methods?

**Answer:**

**Use DuckDB SQL when:**
- Complex JOINs (multiple tables)
- Window functions (ranking, running totals)
- CTEs for readability
- Large DataFrames (memory efficiency)
- SQL is more readable

**Use Pandas when:**
- Simple transformations
- Data manipulation (melt, pivot, explode)
- Integration with ML libraries
- Quick prototyping

**Example:**
```python
# Complex query - DuckDB SQL is clearer
result = con.execute("""
    WITH customer_stats AS (
        SELECT customer_id, SUM(amount) as total
        FROM transactions
        GROUP BY customer_id
    )
    SELECT * FROM customer_stats WHERE total > 10000
""").fetchdf()

# Simple operation - Pandas is simpler
df_filtered = df[df.amount > 10000]
```

---

### Q3: How does DuckDB handle large Pandas DataFrames?

**Answer:**

**DuckDB optimizations:**
1. **Streaming**: Processes data in chunks
2. **Vectorized execution**: Batch processing
3. **Memory efficient**: Doesn't load entire DataFrame
4. **Spilling**: Results to disk if needed

**Best practices:**
```python
# For large DataFrames, use DuckDB
con = duckdb.connect()
con.execute("SET memory_limit='4GB'")

result = con.execute("""
    SELECT status, SUM(amount)
    FROM large_df
    GROUP BY status
""").fetchdf()
```

**Comparison:**
| Operation | Pandas (10M rows) | DuckDB (10M rows) |
|-----------|-------------------|-------------------|
| Group By | 2.5s | 0.3s |
| Join | 5.0s | 0.8s |
| Window | 8.0s | 1.2s |

---

### Q4: How do you convert between DuckDB and Pandas?

**Answer:**

**DuckDB to Pandas:**
```python
result = con.execute("SELECT * FROM table").fetchdf()  # Returns DataFrame
```

**Pandas to DuckDB:**
```python
con.register("my_table", df)  # Register DataFrame
# OR
con.execute("SELECT * FROM df")  # Query directly
```

**Arrow to DuckDB:**
```python
con.register("my_table", arrow_table)  # Register Arrow Table
```

**DuckDB to Arrow:**
```python
result = con.execute("SELECT * FROM table").arrow()  # Returns Arrow Table
```

---

### Q5: What are the performance benefits of DuckDB over Pandas?

**Answer:**

| Feature | Pandas | DuckDB |
|---------|--------|--------|
| **Execution** | Row-based | Vectorized, columnar |
| **Memory** | Loads entire DataFrame | Streaming |
| **JOINs** | Hash merge (slow) | Optimized joins |
| **Window functions** | Limited | Full SQL support |
| **Large data** | Limited by RAM | Handles > RAM |

**Performance examples:**
```python
# 10M rows DataFrame

# Pandas: 2.5 seconds
df.groupby("status")["amount"].sum()

# DuckDB: 0.3 seconds (8x faster)
con.execute("SELECT status, SUM(amount) FROM df GROUP BY status").fetchdf()
```

**When DuckDB is faster:**
- Complex queries (multiple operations)
- Large DataFrames (> 1 GB)
- Window functions
- Multiple JOINs
