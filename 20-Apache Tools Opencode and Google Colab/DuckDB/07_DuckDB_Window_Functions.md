# DuckDB Window Functions

## Table of Contents
1. [Detailed Explanation](#1-detailed-explanation)
2. [Example](#2-example)
3. [Real-World Banking Scenario 1](#3-banking-scenario-1-customer-analytics)
4. [Python Code - Scenario 1](#4-python-code---scenario-1)
5. [Real-World Banking Scenario 2](#5-banking-scenario-2-financial-reporting)
6. [Python Code - Scenario 2](#6-python-code---scenario-2)
7. [Interview Questions](#7-interview-questions)

---

## 1. Detailed Explanation

### Window Functions in DuckDB

> **Window functions perform calculations across a set of rows related to the current row — without collapsing the result set like GROUP BY.**

### Window Function Syntax

```sql
function_name() OVER (
    [PARTITION BY partition_expression]
    [ORDER BY sort_expression [ASC|DESC]]
    [frame_clause]
)
```

### Types of Window Functions

#### 1. Ranking Functions

```sql
-- Rank with gaps
RANK() OVER (ORDER BY amount DESC)

-- Rank without gaps
DENSE_RANK() OVER (ORDER BY amount DESC)

-- Unique row numbers
ROW_NUMBER() OVER (ORDER BY amount DESC)

-- Divide into n buckets
NTILE(4) OVER (ORDER BY amount DESC)

-- Percentile rank
PERCENT_RANK() OVER (ORDER BY amount DESC)

-- Cumulative distribution
CUME_DIST() OVER (ORDER BY amount DESC)
```

#### 2. Aggregate Window Functions

```sql
-- Running sum
SUM(amount) OVER (ORDER BY date)

-- Running average
AVG(amount) OVER (ORDER BY date)

-- Running count
COUNT(*) OVER (ORDER BY date)

-- Running min/max
MIN(amount) OVER (ORDER BY date)
MAX(amount) OVER (ORDER BY date)
```

#### 3. Value Functions

```sql
-- Previous row
LAG(amount, 1) OVER (ORDER BY date)

-- Next row
LEAD(amount, 1) OVER (ORDER BY date)

-- First value in window
FIRST_VALUE(amount) OVER (ORDER BY date)

-- Last value in window
LAST_VALUE(amount) OVER (
    ORDER BY date
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
)

-- Nth value
NTH_VALUE(amount, 3) OVER (ORDER BY date)
```

### Frame Clauses

```sql
-- Default frame
ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW

-- Sliding window (7-day moving average)
ROWS BETWEEN 6 PRECEDING AND CURRENT ROW

-- Forward window
ROWS BETWEEN CURRENT ROW AND 6 FOLLOWING

-- Full window
ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING

-- Range-based (for dates)
RANGE BETWEEN INTERVAL '7 days' PRECEDING AND CURRENT ROW
```

### PARTITION BY

```sql
-- Without PARTITION BY: entire result set
RANK() OVER (ORDER BY amount DESC)

-- With PARTITION BY: per group
RANK() OVER (PARTITION BY status ORDER BY amount DESC)
```

### Common Use Cases

#### Running Total
```sql
SELECT 
    date,
    amount,
    SUM(amount) OVER (ORDER BY date) as running_total
FROM transactions
```

#### Moving Average
```sql
SELECT 
    date,
    amount,
    AVG(amount) OVER (
        ORDER BY date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as moving_avg_7d
FROM transactions
```

#### Ranking
```sql
SELECT 
    customer_id,
    amount,
    RANK() OVER (ORDER BY amount DESC) as overall_rank,
    RANK() OVER (PARTITION BY status ORDER BY amount DESC) as rank_in_status
FROM transactions
```

#### Lag/Lead
```sql
SELECT 
    date,
    amount,
    LAG(amount, 1) OVER (ORDER BY date) as prev_amount,
    LEAD(amount, 1) OVER (ORDER BY date) as next_amount,
    amount - LAG(amount, 1) OVER (ORDER BY date) as change
FROM transactions
```

#### Percent of Total
```sql
SELECT 
    status,
    amount,
    SUM(amount) OVER () as total_amount,
    amount / SUM(amount) OVER () * 100 as pct_of_total
FROM transactions
```

---

## 2. Example

### Window Functions Demo

```python
import duckdb
import pandas as pd
import numpy as np

# Create sample data
np.random.seed(42)
num_rows = 1000

df = pd.DataFrame({
    "id": range(1, num_rows + 1),
    "customer_id": [f"CUST{np.random.randint(1, 100):03d}" for _ in range(num_rows)],
    "amount": np.random.uniform(100, 10000, num_rows).round(2),
    "status": np.random.choice(["COMPLETED", "PENDING", "FAILED"], num_rows),
    "date": pd.date_range("2026-01-01", periods=num_rows, freq="1h"),
})

con = duckdb.connect()
con.register("transactions", df)

# 1. Ranking
print("=== Ranking Functions ===")
result = con.execute("""
    SELECT 
        id,
        customer_id,
        amount,
        RANK() OVER (ORDER BY amount DESC) as rank,
        DENSE_RANK() OVER (ORDER BY amount DESC) as dense_rank,
        ROW_NUMBER() OVER (ORDER BY amount DESC) as row_num
    FROM transactions
    LIMIT 10
""").fetchdf()
print(result.to_string(index=False))

# 2. Running total
print("\n=== Running Total ===")
result = con.execute("""
    SELECT 
        id,
        date,
        amount,
        SUM(amount) OVER (ORDER BY date) as running_total
    FROM transactions
    LIMIT 10
""").fetchdf()
print(result.to_string(index=False))

# 3. Moving average
print("\n=== 7-Period Moving Average ===")
result = con.execute("""
    SELECT 
        id,
        date,
        amount,
        AVG(amount) OVER (
            ORDER BY date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as moving_avg_7d
    FROM transactions
    LIMIT 15
""").fetchdf()
print(result.to_string(index=False))

# 4. Lag/Lead
print("\n=== Lag/Lead ===")
result = con.execute("""
    SELECT 
        id,
        date,
        amount,
        LAG(amount, 1) OVER (ORDER BY date) as prev_amount,
        LEAD(amount, 1) OVER (ORDER BY date) as next_amount
    FROM transactions
    LIMIT 10
""").fetchdf()
print(result.to_string(index=False))

# 5. Partitioned ranking
print("\n=== Partitioned Ranking ===")
result = con.execute("""
    SELECT 
        id,
        status,
        amount,
        RANK() OVER (PARTITION BY status ORDER BY amount DESC) as rank_in_status
    FROM transactions
    LIMIT 15
""").fetchdf()
print(result.to_string(index=False))

con.close()
```

---

## 3. Banking Scenario 1: Customer Analytics

### Problem
A bank needs to analyze customer behavior:
- Rank customers by transaction volume
- Calculate running balances
- Identify trends (increasing/decreasing spending)
- Compute customer lifetime value

### Why Window Functions?
- Ranking without collapsing data
- Running totals for balance tracking
- Lag/Lead for trend analysis
- Partitioned calculations per customer

---

## 4. Python Code - Scenario 1

```python
import duckdb
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# ============================================================
# BANKING SCENARIO: Customer Analytics with Window Functions
# ============================================================

def generate_customer_transactions(num_rows=50_000):
    """Generate customer transaction data."""
    random.seed(42)
    np.random.seed(42)

    num_customers = 1000

    df = pd.DataFrame({
        "transaction_id": range(1, num_rows + 1),
        "customer_id": [f"CUST{random.randint(1, num_customers):05d}" for _ in range(num_rows)],
        "amount": np.random.lognormal(6, 2, num_rows).round(2),
        "type": np.random.choice(["CREDIT", "DEBIT"], num_rows),
        "date": pd.date_range("2026-01-01", periods=num_rows, freq="30s"),
    })

    return df


def analyze_customers(df):
    """Analyze customers using window functions."""
    con = duckdb.connect()
    con.register("transactions", df)

    # 1. Customer ranking by total volume
    print("=== Customer Ranking by Volume ===")
    result = con.execute("""
        WITH customer_totals AS (
            SELECT 
                customer_id,
                SUM(amount) as total_volume,
                COUNT(*) as tx_count
            FROM transactions
            GROUP BY customer_id
        )
        SELECT 
            customer_id,
            total_volume,
            tx_count,
            RANK() OVER (ORDER BY total_volume DESC) as volume_rank,
            NTILE(10) OVER (ORDER BY total_volume DESC) as decile
        FROM customer_totals
        ORDER BY volume_rank
        LIMIT 20
    """).fetchdf()
    print(result.to_string(index=False))

    # 2. Running balance per customer
    print("\n=== Running Balance per Customer ===")
    result = con.execute("""
        SELECT 
            customer_id,
            date,
            amount,
            type,
            CASE 
                WHEN type = 'CREDIT' THEN amount
                ELSE -amount
            END as signed_amount,
            SUM(CASE WHEN type = 'CREDIT' THEN amount ELSE -amount END) OVER (
                PARTITION BY customer_id 
                ORDER BY date
            ) as running_balance
        FROM transactions
        WHERE customer_id = 'CUST00001'
        ORDER BY date
        LIMIT 20
    """).fetchdf()
    print(result.to_string(index=False))

    # 3. Month-over-month change
    print("\n=== Month-over-Month Change ===")
    result = con.execute("""
        WITH monthly AS (
            SELECT 
                customer_id,
                DATE_TRUNC('month', date) as month,
                SUM(amount) as monthly_amount
            FROM transactions
            GROUP BY 1, 2
        )
        SELECT 
            customer_id,
            month,
            monthly_amount,
            LAG(monthly_amount, 1) OVER (
                PARTITION BY customer_id 
                ORDER BY month
            ) as prev_month,
            monthly_amount - LAG(monthly_amount, 1) OVER (
                PARTITION BY customer_id 
                ORDER BY month
            ) as change,
            ROUND(
                (monthly_amount - LAG(monthly_amount, 1) OVER (
                    PARTITION BY customer_id 
                    ORDER BY month
                )) / NULLIF(LAG(monthly_amount, 1) OVER (
                    PARTITION BY customer_id 
                    ORDER BY month
                ), 0) * 100, 2
            ) as pct_change
        FROM monthly
        WHERE customer_id = 'CUST00001'
        ORDER BY month
    """).fetchdf()
    print(result.to_string(index=False))

    # 4. Customer percentile
    print("\n=== Customer Percentiles ===")
    result = con.execute("""
        WITH customer_stats AS (
            SELECT 
                customer_id,
                SUM(amount) as total_amount,
                COUNT(*) as tx_count
            FROM transactions
            GROUP BY customer_id
        )
        SELECT 
            customer_id,
            total_amount,
            tx_count,
            PERCENT_RANK() OVER (ORDER BY total_amount) as percentile,
            CUME_DIST() OVER (ORDER BY total_amount) as cumulative_dist
        FROM customer_stats
        ORDER BY total_amount DESC
        LIMIT 10
    """).fetchdf()
    print(result.to_string(index=False))

    con.close()


# ============================================================
# RUN THE SCENARIO
# ============================================================
if __name__ == "__main__":
    # Generate data
    print("Generating customer transactions...")
    df = generate_customer_transactions(num_rows=50_000)

    # Analyze
    analyze_customers(df)
```

---

## 5. Banking Scenario 2: Financial Reporting

### Problem
A bank needs to generate financial reports:
- Daily P&L with running totals
- Monthly comparisons (MoM, YoY)
- Rank products by profitability
- Calculate moving averages for trend analysis

### Why Window Functions?
- Running totals for P&L
- LAG/LEAD for comparisons
- RANK for product ranking
- Moving averages for trends

---

## 6. Python Code - Scenario 2

```python
import duckdb
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# ============================================================
# BANKING SCENARIO: Financial Reporting with Window Functions
# ============================================================

def generate_financial_data(num_days=90):
    """Generate daily financial data."""
    random.seed(42)
    np.random.seed(42)

    dates = pd.date_range("2026-01-01", periods=num_days, freq="D")
    products = ["CHECKING", "SAVINGS", "CREDIT_CARD", "LOAN"]

    data = []
    for date in dates:
        for product in products:
            revenue = random.uniform(100000, 500000)
            cost = revenue * random.uniform(0.3, 0.7)
            data.append({
                "date": date,
                "product": product,
                "revenue": round(revenue, 2),
                "cost": round(cost, 2),
                "profit": round(revenue - cost, 2),
            })

    return pd.DataFrame(data)


def generate_reports(df):
    """Generate financial reports using window functions."""
    con = duckdb.connect()
    con.register("daily_pnl", df)

    # 1. Running P&L by product
    print("=== Running P&L by Product ===")
    result = con.execute("""
        SELECT 
            date,
            product,
            profit,
            SUM(profit) OVER (
                PARTITION BY product 
                ORDER BY date
            ) as running_profit,
            AVG(profit) OVER (
                PARTITION BY product 
                ORDER BY date 
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) as moving_avg_7d
        FROM daily_pnl
        WHERE product = 'CHECKING'
        ORDER BY date
        LIMIT 15
    """).fetchdf()
    print(result.to_string(index=False))

    # 2. Month-over-month comparison
    print("\n=== Month-over-Month Comparison ===")
    result = con.execute("""
        WITH monthly AS (
            SELECT 
                product,
                DATE_TRUNC('month', date) as month,
                SUM(profit) as monthly_profit
            FROM daily_pnl
            GROUP BY 1, 2
        )
        SELECT 
            product,
            month,
            monthly_profit,
            LAG(monthly_profit, 1) OVER (
                PARTITION BY product 
                ORDER BY month
            ) as prev_month,
            ROUND(
                (monthly_profit - LAG(monthly_profit, 1) OVER (
                    PARTITION BY product 
                    ORDER BY month
                )) / NULLIF(LAG(monthly_profit, 1) OVER (
                    PARTITION BY product 
                    ORDER BY month
                ), 0) * 100, 2
            ) as mom_pct_change
        FROM monthly
        ORDER BY product, month
    """).fetchdf()
    print(result.to_string(index=False))

    # 3. Product ranking
    print("\n=== Product Ranking by Profit ===")
    result = con.execute("""
        WITH product_totals AS (
            SELECT 
                product,
                SUM(profit) as total_profit,
                AVG(profit) as avg_daily_profit
            FROM daily_pnl
            GROUP BY product
        )
        SELECT 
            product,
            total_profit,
            avg_daily_profit,
            RANK() OVER (ORDER BY total_profit DESC) as profit_rank,
            ROUND(total_profit / SUM(total_profit) OVER () * 100, 2) as pct_of_total
        FROM product_totals
        ORDER BY profit_rank
    """).fetchdf()
    print(result.to_string(index=False))

    # 4. Cumulative profit
    print("\n=== Cumulative Profit (All Products) ===")
    result = con.execute("""
        SELECT 
            date,
            SUM(profit) as daily_profit,
            SUM(SUM(profit)) OVER (ORDER BY date) as cumulative_profit
        FROM daily_pnl
        GROUP BY date
        ORDER BY date
        LIMIT 15
    """).fetchdf()
    print(result.to_string(index=False))

    con.close()


# ============================================================
# RUN THE SCENARIO
# ============================================================
if __name__ == "__main__":
    # Generate data
    print("Generating financial data...")
    df = generate_financial_data(num_days=90)

    # Generate reports
    generate_reports(df)
```

---

## 7. Interview Questions

### Q1: What is the difference between WHERE and HAVING?

**Answer:**

| Feature | WHERE | HAVING |
|---------|-------|--------|
| **Filters** | Individual rows | Groups (after GROUP BY) |
| **Timing** | Before aggregation | After aggregation |
| **Aggregate functions** | Cannot use | Can use |

**Example:**
```sql
-- WHERE: Filter before aggregation
SELECT status, COUNT(*)
FROM transactions
WHERE amount > 1000  -- Filters individual rows
GROUP BY status

-- HAVING: Filter after aggregation
SELECT status, COUNT(*)
FROM transactions
GROUP BY status
HAVING COUNT(*) > 100  -- Filters groups
```

---

### Q2: Explain the difference between RANK, DENSE_RANK, and ROW_NUMBER.

**Answer:**

| Function | Gaps | Unique |
|----------|------|--------|
| `RANK()` | Yes | No |
| `DENSE_RANK()` | No | No |
| `ROW_NUMBER()` | No | Yes |

**Example:**
```sql
-- Amounts: 1000, 1000, 900, 800
SELECT 
    amount,
    RANK() OVER (ORDER BY amount DESC) as rank,       -- 1, 1, 3, 4
    DENSE_RANK() OVER (ORDER BY amount DESC) as dense, -- 1, 1, 2, 3
    ROW_NUMBER() OVER (ORDER BY amount DESC) as row_num -- 1, 2, 3, 4
FROM transactions
```

---

### Q3: How do you calculate a 7-day moving average?

**Answer:**

```sql
SELECT 
    date,
    amount,
    AVG(amount) OVER (
        ORDER BY date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as moving_avg_7d
FROM transactions
```

**Frame clause explained:**
- `ROWS BETWEEN 6 PRECEDING AND CURRENT ROW`
- Includes current row + 6 previous rows = 7 rows total

**For date-based windows:**
```sql
AVG(amount) OVER (
    ORDER BY date 
    RANGE BETWEEN INTERVAL '6 days' PRECEDING AND CURRENT ROW
)
```

---

### Q4: What are window functions and how do they differ from GROUP BY?

**Answer:**

| Feature | Window Functions | GROUP BY |
|---------|-----------------|----------|
| **Result rows** | Same as input | Collapsed |
| **Detail** | Preserves individual rows | Aggregates only |
| **Output** | Original rows + calculation | One row per group |

**Example:**
```sql
-- GROUP BY: Collapses rows
SELECT status, SUM(amount)
FROM transactions
GROUP BY status
-- Result: 3 rows (one per status)

-- Window function: Preserves rows
SELECT 
    id,
    status,
    amount,
    SUM(amount) OVER (PARTITION BY status) as total_by_status
FROM transactions
-- Result: All original rows + total column
```

---

### Q5: How do you use LAG and LEAD for trend analysis?

**Answer:**

**LAG: Access previous row**
```sql
SELECT 
    date,
    amount,
    LAG(amount, 1) OVER (ORDER BY date) as prev_amount,
    amount - LAG(amount, 1) OVER (ORDER BY date) as change
FROM transactions
```

**LEAD: Access next row**
```sql
SELECT 
    date,
    amount,
    LEAD(amount, 1) OVER (ORDER BY date) as next_amount,
    LEAD(amount, 1) OVER (ORDER BY date) - amount as future_change
FROM transactions
```

**Trend detection:**
```sql
SELECT 
    date,
    amount,
    CASE 
        WHEN amount > LAG(amount, 1) OVER (ORDER BY date) THEN 'INCREASING'
        WHEN amount < LAG(amount, 1) OVER (ORDER BY date) THEN 'DECREASING'
        ELSE 'STABLE'
    END as trend
FROM transactions
```

**Example:**
```sql
-- Month-over-month growth
SELECT 
    month,
    revenue,
    LAG(revenue, 1) OVER (ORDER BY month) as prev_month,
    ROUND(
        (revenue - LAG(revenue, 1) OVER (ORDER BY month)) / 
        LAG(revenue, 1) OVER (ORDER BY month) * 100, 2
    ) as growth_pct
FROM monthly_sales
```
