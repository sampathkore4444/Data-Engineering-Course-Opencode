# DuckDB Aggregate Functions

## Table of Contents
1. [Detailed Explanation](#1-detailed-explanation)
2. [Example](#2-example)
3. [Real-World Banking Scenario 1](#3-banking-scenario-1-statistical-analysis)
4. [Python Code - Scenario 1](#4-python-code---scenario-1)
5. [Real-World Banking Scenario 2](#5-banking-scenario-2-business-reporting)
6. [Python Code - Scenario 2](#6-python-code---scenario-2)
7. [Interview Questions](#7-interview-questions)

---

## 1. Detailed Explanation

### Aggregate Functions in DuckDB

> **Aggregate functions combine multiple rows into a single result — used for summarizing, analyzing, and reporting data.**

### Standard Aggregate Functions

#### 1. COUNT
```sql
-- Count all rows
SELECT COUNT(*) FROM transactions

-- Count non-null values
SELECT COUNT(amount) FROM transactions

-- Count distinct values
SELECT COUNT(DISTINCT account_id) FROM transactions
```

#### 2. SUM
```sql
-- Total sum
SELECT SUM(amount) FROM transactions

-- Sum with filter
SELECT SUM(amount) FROM transactions WHERE status = 'COMPLETED'

-- Sum per group
SELECT status, SUM(amount) FROM transactions GROUP BY status
```

#### 3. AVG
```sql
-- Average
SELECT AVG(amount) FROM transactions

-- Average per group
SELECT status, AVG(amount) FROM transactions GROUP BY status
```

#### 4. MIN / MAX
```sql
SELECT 
    MIN(amount) as min_amount,
    MAX(amount) as max_amount
FROM transactions
```

#### 5. STDDEV / VAR
```sql
-- Standard deviation
SELECT STDDEV(amount) FROM transactions

-- Variance
SELECT VAR_SAMP(amount) FROM transactions
```

#### 6. FIRST / LAST
```sql
-- First value (ordered)
SELECT FIRST(amount ORDER BY date) FROM transactions

-- Last value (ordered)
SELECT LAST(amount ORDER BY date) FROM transactions
```

#### 7. LIST / ARRAY_AGG
```sql
-- Collect values into array
SELECT LIST(amount) FROM transactions

-- Collect distinct values
SELECT LIST(DISTINCT status) FROM transactions
```

### Statistical Aggregate Functions

```sql
-- Percentile
SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount) as median
FROM transactions

-- Mode
SELECT MODE() WITHIN GROUP (ORDER BY status) as most_common_status
FROM transactions
```

### GROUPING SETS

```sql
-- Multiple grouping levels
SELECT 
    COALESCE(channel, 'ALL') as channel,
    COALESCE(status, 'ALL') as status,
    COUNT(*) as count
FROM transactions
GROUP BY GROUPING SETS (
    (channel, status),
    (channel),
    (status),
    ()
)
```

### FILTER Clause

```sql
-- Aggregate with filter
SELECT 
    COUNT(*) as total,
    COUNT(*) FILTER (WHERE status = 'COMPLETED') as completed,
    COUNT(*) FILTER (WHERE status = 'FAILED') as failed
FROM transactions
```

### LIST Aggregation

```sql
-- Collect values
SELECT 
    account_id,
    LIST(amount) as all_amounts,
    LIST(DISTINCT status) as all_statuses
FROM transactions
GROUP BY account_id
```

### Histogram

```sql
-- Create histogram buckets
SELECT 
    WIDTH_BUCKET(amount, 0, 100000, 10) as bucket,
    COUNT(*) as count
FROM transactions
GROUP BY 1
ORDER BY 1
```

---

## 2. Example

### Aggregate Functions Demo

```python
import duckdb
import pandas as pd
import numpy as np

# Create sample data
np.random.seed(42)
num_rows = 10000

df = pd.DataFrame({
    "id": range(1, num_rows + 1),
    "status": np.random.choice(["COMPLETED", "PENDING", "FAILED"], num_rows),
    "channel": np.random.choice(["ONLINE", "MOBILE", "BRANCH", "ATM"], num_rows),
    "amount": np.random.uniform(100, 100000, num_rows).round(2),
})

con = duckdb.connect()
con.register("transactions", df)

# 1. Basic aggregates
print("=== Basic Aggregates ===")
result = con.execute("""
    SELECT 
        COUNT(*) as total_count,
        COUNT(DISTINCT status) as unique_statuses,
        SUM(amount) as total_amount,
        AVG(amount) as avg_amount,
        MIN(amount) as min_amount,
        MAX(amount) as max_amount,
        STDDEV(amount) as std_dev
    FROM transactions
""").fetchdf()
print(result.to_string(index=False))

# 2. GROUP BY aggregates
print("\n=== GROUP BY Aggregates ===")
result = con.execute("""
    SELECT 
        status,
        COUNT(*) as count,
        SUM(amount) as total,
        AVG(amount) as average
    FROM transactions
    GROUP BY status
    ORDER BY total DESC
""").fetchdf()
print(result.to_string(index=False))

# 3. FILTER clause
print("\n=== FILTER Clause ===")
result = con.execute("""
    SELECT 
        channel,
        COUNT(*) as total,
        COUNT(*) FILTER (WHERE status = 'COMPLETED') as completed,
        COUNT(*) FILTER (WHERE status = 'FAILED') as failed
    FROM transactions
    GROUP BY channel
""").fetchdf()
print(result.to_string(index=False))

# 4. LIST aggregation
print("\n=== LIST Aggregation ===")
result = con.execute("""
    SELECT 
        status,
        LIST(amount)[:5] as sample_amounts
    FROM transactions
    GROUP BY status
""").fetchdf()
print(result.to_string(index=False))

# 5. Histogram
print("\n=== Histogram ===")
result = con.execute("""
    SELECT 
        WIDTH_BUCKET(amount, 0, 100000, 5) as bucket,
        COUNT(*) as count
    FROM transactions
    GROUP BY 1
    ORDER BY 1
""").fetchdf()
print(result.to_string(index=False))

con.close()
```

---

## 3. Banking Scenario 1: Statistical Analysis

### Problem
A bank needs to perform statistical analysis:
- Calculate risk metrics (VaR, standard deviation)
- Analyze transaction patterns
- Compute percentiles for reporting
- Generate distribution analysis

### Why Aggregate Functions?
- Statistical calculations (STDDEV, VAR)
- Percentile calculations
- Distribution analysis (histograms)
- Pattern analysis (LIST, MODE)

---

## 4. Python Code - Scenario 1

```python
import duckdb
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# ============================================================
# BANKING SCENARIO: Statistical Analysis
# ============================================================

def generate_transaction_data(num_rows=50_000):
    """Generate transaction data for statistical analysis."""
    random.seed(42)
    np.random.seed(42)

    df = pd.DataFrame({
        "transaction_id": range(1, num_rows + 1),
        "account_id": [f"ACC{random.randint(1, 500):03d}" for _ in range(num_rows)],
        "amount": np.random.lognormal(6, 2, num_rows).round(2),
        "status": np.random.choice(["COMPLETED", "PENDING", "FAILED"], num_rows),
        "channel": np.random.choice(["ONLINE", "MOBILE", "BRANCH", "ATM"], num_rows),
        "date": pd.date_range("2026-01-01", periods=num_rows, freq="30s"),
    })

    return df


def perform_statistical_analysis(df):
    """Perform statistical analysis using DuckDB."""
    con = duckdb.connect()
    con.register("transactions", df)

    # 1. Descriptive statistics
    print("=== Descriptive Statistics ===")
    result = con.execute("""
        SELECT 
            COUNT(*) as count,
            AVG(amount) as mean,
            STDDEV(amount) as std_dev,
            VAR_SAMP(amount) as variance,
            MIN(amount) as min,
            MAX(amount) as max,
            SUM(amount) as total
        FROM transactions
        WHERE status = 'COMPLETED'
    """).fetchdf()
    print(result.to_string(index=False))

    # 2. Percentiles
    print("\n=== Percentiles ===")
    result = con.execute("""
        SELECT 
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY amount) as p25,
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY amount) as median,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY amount) as p75,
            PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY amount) as p90,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY amount) as p95,
            PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY amount) as p99
        FROM transactions
        WHERE status = 'COMPLETED'
    """).fetchdf()
    print(result.to_string(index=False))

    # 3. Distribution analysis
    print("\n=== Distribution Analysis ===")
    result = con.execute("""
        SELECT 
            WIDTH_BUCKET(amount, 0, 200000, 10) as bucket,
            COUNT(*) as count,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as pct
        FROM transactions
        WHERE status = 'COMPLETED'
        GROUP BY 1
        ORDER BY 1
    """).fetchdf()
    print(result.to_string(index=False))

    # 4. Risk metrics (Value at Risk)
    print("\n=== Risk Metrics ===")
    result = con.execute("""
        WITH 
        daily_stats AS (
            SELECT 
                DATE_TRUNC('day', date) as day,
                SUM(amount) as daily_total
            FROM transactions
            WHERE status = 'COMPLETED'
            GROUP BY 1
        )
        SELECT 
            AVG(daily_total) as avg_daily,
            STDDEV(daily_total) as std_daily,
            MIN(daily_total) as min_daily,
            MAX(daily_total) as max_daily,
            PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY daily_total) as var_95
        FROM daily_stats
    """).fetchdf()
    print(result.to_string(index=False))

    # 5. Channel statistics
    print("\n=== Channel Statistics ===")
    result = con.execute("""
        SELECT 
            channel,
            COUNT(*) as count,
            AVG(amount) as mean,
            STDDEV(amount) as std_dev,
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY amount) as median
        FROM transactions
        WHERE status = 'COMPLETED'
        GROUP BY channel
    """).fetchdf()
    print(result.to_string(index=False))

    con.close()


# ============================================================
# RUN THE SCENARIO
# ============================================================
if __name__ == "__main__":
    # Generate data
    print("Generating transaction data...")
    df = generate_transaction_data(num_rows=50_000)

    # Analyze
    perform_statistical_analysis(df)
```

---

## 5. Banking Scenario 2: Business Reporting

### Problem**
A bank needs to generate business reports:
- Daily/weekly/monthly summaries
- Product performance reports
- Customer segment analysis
- Revenue by region

### Why Aggregate Functions?
- Summarize large datasets
- Calculate KPIs
- Generate comparative reports
- Support decision making

---

## 6. Python Code - Scenario 2

```python
import duckdb
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# ============================================================
# BANKING SCENARIO: Business Reporting
# ============================================================

def generate_business_data(num_days=90):
    """Generate business data for reporting."""
    random.seed(42)
    np.random.seed(42)

    dates = pd.date_range("2026-01-01", periods=num_days, freq="D")
    products = ["CHECKING", "SAVINGS", "CREDIT_CARD", "LOAN"]
    regions = ["NORTH", "SOUTH", "EAST", "WEST"]

    data = []
    for date in dates:
        for product in products:
            for region in regions:
                revenue = random.uniform(10000, 100000)
                cost = revenue * random.uniform(0.3, 0.7)
                transactions = random.randint(100, 1000)
                data.append({
                    "date": date,
                    "product": product,
                    "region": region,
                    "revenue": round(revenue, 2),
                    "cost": round(cost, 2),
                    "profit": round(revenue - cost, 2),
                    "transactions": transactions,
                })

    return pd.DataFrame(data)


def generate_reports(df):
    """Generate business reports using aggregate functions."""
    con = duckdb.connect()
    con.register("business_data", df)

    # 1. Daily summary
    print("=== Daily Summary ===")
    result = con.execute("""
        SELECT 
            date,
            SUM(revenue) as total_revenue,
            SUM(cost) as total_cost,
            SUM(profit) as total_profit,
            SUM(transactions) as total_transactions,
            ROUND(SUM(profit) / SUM(revenue) * 100, 2) as profit_margin
        FROM business_data
        GROUP BY date
        ORDER BY date
        LIMIT 10
    """).fetchdf()
    print(result.to_string(index=False))

    # 2. Product performance
    print("\n=== Product Performance ===")
    result = con.execute("""
        SELECT 
            product,
            SUM(revenue) as total_revenue,
            SUM(profit) as total_profit,
            SUM(transactions) as total_transactions,
            ROUND(SUM(profit) / SUM(revenue) * 100, 2) as profit_margin,
            RANK() OVER (ORDER BY SUM(profit) DESC) as profit_rank
        FROM business_data
        GROUP BY product
        ORDER BY profit_rank
    """).fetchdf()
    print(result.to_string(index=False))

    # 3. Regional analysis
    print("\n=== Regional Analysis ===")
    result = con.execute("""
        SELECT 
            region,
            SUM(revenue) as total_revenue,
            SUM(profit) as total_profit,
            SUM(transactions) as total_transactions,
            ROUND(SUM(revenue) / SUM(SUM(revenue)) OVER () * 100, 2) as revenue_share
        FROM business_data
        GROUP BY region
        ORDER BY total_revenue DESC
    """).fetchdf()
    print(result.to_string(index=False))

    # 4. Monthly trend
    print("\n=== Monthly Trend ===")
    result = con.execute("""
        SELECT 
            DATE_TRUNC('month', date) as month,
            SUM(revenue) as monthly_revenue,
            SUM(profit) as monthly_profit,
            SUM(transactions) as monthly_transactions
        FROM business_data
        GROUP BY 1
        ORDER BY 1
    """).fetchdf()
    print(result.to_string(index=False))

    # 5. Top products by region
    print("\n=== Top Products by Region ===")
    result = con.execute("""
        SELECT *
        FROM (
            SELECT 
                region,
                product,
                SUM(profit) as total_profit,
                RANK() OVER (PARTITION BY region ORDER BY SUM(profit) DESC) as rank
            FROM business_data
            GROUP BY 1, 2
        )
        WHERE rank <= 2
        ORDER BY region, rank
    """).fetchdf()
    print(result.to_string(index=False))

    con.close()


# ============================================================
# RUN THE SCENARIO
# ============================================================
if __name__ == "__main__":
    # Generate data
    print("Generating business data...")
    df = generate_business_data(num_days=90)

    # Generate reports
    generate_reports(df)
```

---

## 7. Interview Questions

### Q1: What is the difference between COUNT(*) and COUNT(column)?

**Answer:**

| Function | Counts | Nulls |
|----------|--------|-------|
| `COUNT(*)` | All rows | Includes nulls |
| `COUNT(column)` | Non-null values | Excludes nulls |

**Example:**
```sql
-- Data: [1, 2, NULL, 4]
SELECT 
    COUNT(*) as count_all,      -- 4
    COUNT(value) as count_value -- 3
FROM table
```

---

### Q2: How do you calculate percentiles in DuckDB?

**Answer:**

**Continuous percentile:**
```sql
SELECT 
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY amount) as median
FROM transactions
```

**Discrete percentile:**
```sql
SELECT 
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY amount) as median
FROM transactions
```

**Multiple percentiles:**
```sql
SELECT 
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY amount) as p25,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY amount) as p50,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY amount) as p75
FROM transactions
```

---

### Q3: What are GROUPING SETS?

**Answer:**

GROUPING SETS allow multiple grouping levels in one query:

```sql
SELECT 
    channel,
    status,
    COUNT(*) as count
FROM transactions
GROUP BY GROUPING SETS (
    (channel, status),
    (channel),
    (status),
    ()
)
```

**Equivalent to:**
```sql
SELECT channel, status, COUNT(*) FROM transactions GROUP BY channel, status
UNION ALL
SELECT channel, NULL, COUNT(*) FROM transactions GROUP BY channel
UNION ALL
SELECT NULL, status, COUNT(*) FROM transactions GROUP BY status
UNION ALL
SELECT NULL, NULL, COUNT(*) FROM transactions
```

---

### Q4: How do you create a histogram in DuckDB?

**Answer:**

**Using WIDTH_BUCKET:**
```sql
SELECT 
    WIDTH_BUCKET(amount, 0, 100000, 10) as bucket,
    COUNT(*) as count
FROM transactions
GROUP BY 1
ORDER BY 1
```

**Using CASE:**
```sql
SELECT 
    CASE 
        WHEN amount < 1000 THEN 'Low'
        WHEN amount < 10000 THEN 'Medium'
        ELSE 'High'
    END as category,
    COUNT(*) as count
FROM transactions
GROUP BY 1
```

---

### Q5: What is the FILTER clause?

**Answer:**

FILTER clause applies aggregate to a subset of rows:

```sql
SELECT 
    COUNT(*) as total,
    COUNT(*) FILTER (WHERE status = 'COMPLETED') as completed,
    COUNT(*) FILTER (WHERE status = 'FAILED') as failed,
    SUM(amount) FILTER (WHERE amount > 10000) as high_value_sum
FROM transactions
```

**Equivalent to:**
```sql
SELECT 
    COUNT(*) as total,
    SUM(CASE WHEN status = 'COMPLETED' THEN 1 ELSE 0 END) as completed,
    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed,
    SUM(CASE WHEN amount > 10000 THEN amount ELSE 0 END) as high_value_sum
FROM transactions
```

**Benefits:**
- More readable
- Often more efficient
- Clearer intent
