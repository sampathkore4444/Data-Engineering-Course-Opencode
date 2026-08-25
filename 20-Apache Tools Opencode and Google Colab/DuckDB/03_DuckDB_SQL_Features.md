# DuckDB SQL Features

## Table of Contents
1. [Detailed Explanation](#1-detailed-explanation)
2. [Example](#2-example)
3. [Real-World Banking Scenario 1](#3-banking-scenario-1-financial-reporting)
4. [Python Code - Scenario 1](#4-python-code---scenario-1)
5. [Real-World Banking Scenario 2](#5-banking-scenario-2-data-analysis)
6. [Python Code - Scenario 2](#6-python-code---scenario-2)
7. [Interview Questions](#7-interview-questions)

---

## 1. Detailed Explanation

### DuckDB SQL Capabilities

DuckDB provides **full ANSI SQL support** with powerful analytical extensions:

> **DuckDB supports standard SQL plus advanced analytical features like window functions, CTEs, lambda functions, and array operations — making it ideal for complex data analysis.**

### Core SQL Features

#### 1. SELECT with Filtering

```sql
SELECT column1, column2
FROM table
WHERE condition
GROUP BY column1
HAVING aggregate_condition
ORDER BY column1
LIMIT 100
```

#### 2. JOINs

```sql
-- Inner Join
SELECT a.id, b.name
FROM table_a a
JOIN table_b b ON a.id = b.id

-- Left Join
SELECT a.id, b.name
FROM table_a a
LEFT JOIN table_b b ON a.id = b.id

-- Self Join
SELECT a.id, b.id
FROM table_a a
JOIN table_b b ON a.parent_id = b.id
```

#### 3. Window Functions

```sql
-- Ranking
SELECT 
    id,
    amount,
    RANK() OVER (ORDER BY amount DESC) as rank,
    DENSE_RANK() OVER (ORDER BY amount DESC) as dense_rank,
    ROW_NUMBER() OVER (ORDER BY amount DESC) as row_num

-- Partitioned ranking
SELECT 
    id,
    status,
    amount,
    RANK() OVER (PARTITION BY status ORDER BY amount DESC) as rank_in_status

-- Running total
SELECT 
    id,
    date,
    amount,
    SUM(amount) OVER (ORDER BY date) as running_total

-- Moving average
SELECT 
    id,
    date,
    amount,
    AVG(amount) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as moving_avg_7d

-- Lag/Lead
SELECT 
    id,
    date,
    amount,
    LAG(amount, 1) OVER (ORDER BY date) as prev_amount,
    LEAD(amount, 1) OVER (ORDER BY date) as next_amount
```

#### 4. Common Table Expressions (CTEs)

```sql
WITH 
monthly_summary AS (
    SELECT 
        DATE_TRUNC('month', date) as month,
        SUM(amount) as total_amount
    FROM transactions
    GROUP BY 1
),
yearly_summary AS (
    SELECT 
        EXTRACT(YEAR FROM month) as year,
        AVG(total_amount) as avg_monthly
    FROM monthly_summary
    GROUP BY 1
)
SELECT * FROM yearly_summary
```

#### 5. Aggregate Functions

```sql
SELECT 
    status,
    COUNT(*) as count,
    SUM(amount) as total,
    AVG(amount) as average,
    MIN(amount) as min_amount,
    MAX(amount) as max_amount,
    STDDEV(amount) as std_dev,
    VAR_SAMP(amount) as variance,
    FIRST(amount) as first_amount,
    LAST(amount) as last_amount,
    LIST(amount) as all_amounts
FROM transactions
GROUP BY status
```

#### 6. String Functions

```sql
SELECT 
    UPPER(name),
    LOWER(email),
    CONCAT(first_name, ' ', last_name),
    SUBSTRING(phone, 1, 3),
    REPLACE(description, 'old', 'new'),
    LENGTH(name),
    TRIM(whitespace),
    REGEXP_REPLACE(phone, '[^0-9]', ''),
    SPLIT_PART(address, ',', 1)
FROM customers
```

#### 7. Date/Time Functions

```sql
SELECT 
    CURRENT_DATE,
    CURRENT_TIMESTAMP,
    DATE_TRUNC('month', date),
    DATE_PART('day', date),
    DATE_DIFF('day', start_date, end_date),
    DATE_ADD(date, INTERVAL 30 DAY),
    EXTRACT(YEAR FROM date),
    STRFTIME(date, '%Y-%m-%d'),
    PARSE_DATE('%Y-%m-%d', date_string)
FROM transactions
```

#### 8. Array/Struct Operations

```sql
-- Array operations
SELECT 
    tags,
    ARRAY_LENGTH(tags),
    ARRAY_CONTAINS(tags, 'important'),
    ARRAY_CAT(tags, ARRAY['new_tag']),
    UNNEST(tags)
FROM documents

-- Struct operations
SELECT 
    address.city,
    address.state,
    STRUCT_PACK(city := 'NYC', state := 'NY')
FROM customers
```

#### 9. PIVOT/UNPIVOT

```sql
-- Pivot
SELECT *
FROM monthly_sales
PIVOT (
    SUM(amount)
    FOR month IN ('2026-01', '2026-02', '2026-03')
)

-- Unpivot
SELECT *
FROM pivoted_data
UNPIVOT (
    amount
    FOR month IN (jan, feb, mar)
)
```

#### 10. Lambda Functions

```sql
-- Filter arrays
SELECT 
    amount,
    LIST_FILTER(amounts, x -> x > 1000) as large_amounts
FROM transactions

-- Transform arrays
SELECT 
    amounts,
    LIST_TRANSFORM(amounts, x -> x * 1.1) as increased_amounts
FROM transactions

-- Aggregate arrays
SELECT 
    amounts,
    LIST_REDUCE(amounts, (a, b) -> a + b) as total
FROM transactions
```

---

## 2. Example

### DuckDB SQL Feature Demo

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
    "date": pd.date_range("2026-01-01", periods=num_rows, freq="30min"),
})

con = duckdb.connect()
con.register("transactions", df)

# 1. Window functions
print("\n=== Window Functions ===")
result = con.execute("""
    SELECT 
        id,
        customer_id,
        amount,
        RANK() OVER (PARTITION BY status ORDER BY amount DESC) as rank_in_status,
        SUM(amount) OVER (PARTITION BY customer_id ORDER BY date) as running_total,
        LAG(amount, 1) OVER (PARTITION BY customer_id ORDER BY date) as prev_amount
    FROM transactions
    LIMIT 10
""").fetchdf()
print(result.to_string(index=False))

# 2. CTEs
print("\n=== Common Table Expressions ===")
result = con.execute("""
    WITH 
    customer_stats AS (
        SELECT 
            customer_id,
            COUNT(*) as tx_count,
            SUM(amount) as total_amount
        FROM transactions
        WHERE status = 'COMPLETED'
        GROUP BY customer_id
    ),
    high_value_customers AS (
        SELECT *
        FROM customer_stats
        WHERE total_amount > 50000
    )
    SELECT 
        hvc.*,
        cs.tx_count as all_tx_count
    FROM high_value_customers hvc
    JOIN customer_stats cs ON hvc.customer_id = cs.customer_id
""").fetchdf()
print(result.to_string(index=False))

# 3. Aggregate functions
print("\n=== Aggregate Functions ===")
result = con.execute("""
    SELECT 
        status,
        COUNT(*) as count,
        SUM(amount) as total,
        AVG(amount) as average,
        STDDEV(amount) as std_dev,
        LIST(amount)[:5] as sample_amounts
    FROM transactions
    GROUP BY status
""").fetchdf()
print(result.to_string(index=False))

con.close()
```

---

## 3. Banking Scenario 1: Financial Reporting

### Problem
A bank needs to generate complex financial reports:
- Monthly P&L statements
- Customer profitability analysis
- Risk-adjusted returns
- Regulatory capital calculations

Requirements:
- Complex SQL with CTEs, window functions
- Aggregations and pivots
- Date calculations

### Why DuckDB SQL?
- Full SQL support for complex queries
- Window functions for ranking and running totals
- CTEs for readable, modular queries
- Fast execution on Parquet files

---

## 4. Python Code - Scenario 1

```python
import duckdb
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os
import tempfile

# ============================================================
# BANKING SCENARIO: Financial Reporting
# ============================================================

def generate_financial_data():
    """Generate financial transaction data."""
    random.seed(42)
    np.random.seed(42)

    num_transactions = 50_000
    products = ["CHECKING", "SAVINGS", "CREDIT_CARD", "LOAN", "MORTGAGE"]
    departments = ["RETAIL", "CORPORATE", "INVESTMENT", "WEALTH_MGMT"]

    transactions = pd.DataFrame({
        "transaction_id": range(1, num_transactions + 1),
        "account_id": [f"ACC{random.randint(100000, 999999)}" for _ in range(num_transactions)],
        "product": np.random.choice(products, num_transactions),
        "department": np.random.choice(departments, num_transactions),
        "revenue": np.random.uniform(0.01, 50000.0, num_transactions).round(2),
        "cost": np.random.uniform(0.01, 10000.0, num_transactions).round(2),
        "fee_income": np.random.uniform(0.0, 500.0, num_transactions).round(2),
        "date": pd.date_range("2026-01-01", periods=num_transactions, freq="30min"),
    })

    return transactions


def generate_reports(transactions_df):
    """Generate financial reports using DuckDB SQL."""
    con = duckdb.connect()
    con.register("transactions", transactions_df)

    # 1. Monthly P&L by Department
    print("\n=== Monthly P&L by Department ===")
    result = con.execute("""
        WITH monthly_pnl AS (
            SELECT 
                DATE_TRUNC('month', date) as month,
                department,
                SUM(revenue) as total_revenue,
                SUM(cost) as total_cost,
                SUM(fee_income) as total_fees,
                SUM(revenue) - SUM(cost) + SUM(fee_income) as net_profit
            FROM transactions
            GROUP BY 1, 2
        )
        SELECT 
            month,
            department,
            total_revenue,
            total_cost,
            total_fees,
            net_profit,
            ROUND(net_profit / total_revenue * 100, 2) as profit_margin_pct
        FROM monthly_pnl
        ORDER BY month, department
    """).fetchdf()
    print(result.to_string(index=False))

    # 2. Product Profitability Ranking
    print("\n=== Product Profitability Ranking ===")
    result = con.execute("""
        WITH product_stats AS (
            SELECT 
                product,
                SUM(revenue) as total_revenue,
                SUM(cost) as total_cost,
                SUM(revenue) - SUM(cost) as gross_profit
            FROM transactions
            GROUP BY product
        )
        SELECT 
            product,
            total_revenue,
            total_cost,
            gross_profit,
            RANK() OVER (ORDER BY gross_profit DESC) as profit_rank,
            ROUND(gross_profit / total_revenue * 100, 2) as margin_pct
        FROM product_stats
        ORDER BY profit_rank
    """).fetchdf()
    print(result.to_string(index=False))

    # 3. Department Performance with Running Total
    print("\n=== Department Monthly Running Total ===")
    result = con.execute("""
        SELECT 
            DATE_TRUNC('month', date) as month,
            department,
            SUM(revenue) as monthly_revenue,
            SUM(SUM(revenue)) OVER (
                PARTITION BY department 
                ORDER BY DATE_TRUNC('month', date)
            ) as running_total
        FROM transactions
        GROUP BY 1, 2
        ORDER BY department, month
    """).fetchdf()
    print(result.head(15).to_string(index=False))

    # 4. Pivot: Revenue by Department per Month
    print("\n=== Revenue Pivot by Department ===")
    result = con.execute("""
        SELECT *
        FROM (
            SELECT 
                DATE_TRUNC('month', date) as month,
                department,
                SUM(revenue) as revenue
            FROM transactions
            GROUP BY 1, 2
        )
        PIVOT (
            SUM(revenue)
            FOR department IN ('RETAIL', 'CORPORATE', 'INVESTMENT', 'WEALTH_MGMT')
        )
        ORDER BY month
    """).fetchdf()
    print(result.to_string(index=False))

    con.close()


# ============================================================
# RUN THE SCENARIO
# ============================================================
if __name__ == "__main__":
    # Generate data
    print("Generating financial data...")
    transactions = generate_financial_data()

    # Generate reports
    generate_reports(transactions)
```

---

## 5. Banking Scenario 2: Data Analysis

### Problem
A bank's data analyst needs to:
- Analyze customer behavior patterns
- Identify high-value customers
- Compute cohort retention
- Generate churn predictions

Requirements:
- Complex SQL with CTEs
- Window functions for cohort analysis
- Array operations for customer tags

---

## 6. Python Code - Scenario 2

```python
import duckdb
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# ============================================================
# BANKING SCENARIO: Customer Data Analysis
# ============================================================

def generate_customer_data():
    """Generate customer and transaction data."""
    random.seed(42)
    np.random.seed(42)

    num_customers = 5000
    num_transactions = 100_000

    # Customers
    customers = pd.DataFrame({
        "customer_id": [f"CUST{i:05d}" for i in range(1, num_customers + 1)],
        "name": [f"Customer {i}" for i in range(1, num_customers + 1)],
        "segment": np.random.choice(["PREMIUM", "STANDARD", "BASIC"], num_customers, p=[0.2, 0.5, 0.3]),
        "join_date": pd.date_range("2020-01-01", periods=num_customers, freq="2h"),
        "churned": np.random.choice([True, False], num_customers, p=[0.1, 0.9]),
    })

    # Transactions
    transactions = pd.DataFrame({
        "transaction_id": range(1, num_transactions + 1),
        "customer_id": [f"CUST{random.randint(1, num_customers):05d}" for _ in range(num_transactions)],
        "amount": np.random.lognormal(6, 2, num_transactions).round(2),
        "date": pd.date_range("2026-01-01", periods=num_transactions, freq="10min"),
    })

    return customers, transactions


def analyze_customers(customers_df, transactions_df):
    """Analyze customer data using DuckDB SQL."""
    con = duckdb.connect()
    con.register("customers", customers_df)
    con.register("transactions", transactions_df)

    # 1. Customer Lifetime Value
    print("\n=== Customer Lifetime Value ===")
    result = con.execute("""
        WITH customer_stats AS (
            SELECT 
                c.customer_id,
                c.segment,
                c.join_date,
                COUNT(t.transaction_id) as tx_count,
                SUM(t.amount) as total_spent,
                AVG(t.amount) as avg_transaction,
                MIN(t.date) as first_transaction,
                MAX(t.date) as last_transaction,
                DATE_DIFF('day', MIN(t.date), MAX(t.date)) as tenure_days
            FROM customers c
            LEFT JOIN transactions t ON c.customer_id = t.customer_id
            GROUP BY 1, 2, 3
        )
        SELECT 
            customer_id,
            segment,
            tx_count,
            total_spent,
            avg_transaction,
            tenure_days,
            CASE 
                WHEN tenure_days > 0 THEN ROUND(total_spent / tenure_days * 365, 2)
                ELSE 0
            END as annual_value
        FROM customer_stats
        WHERE tx_count > 0
        ORDER BY annual_value DESC
        LIMIT 20
    """).fetchdf()
    print(result.to_string(index=False))

    # 2. Cohort Retention Analysis
    print("\n=== Cohort Retention (Monthly) ===")
    result = con.execute("""
        WITH customer_cohorts AS (
            SELECT 
                customer_id,
                DATE_TRUNC('month', join_date) as cohort_month
            FROM customers
        ),
        activity AS (
            SELECT 
                c.customer_id,
                c.cohort_month,
                DATE_TRUNC('month', t.date) as activity_month
            FROM customer_cohorts c
            JOIN transactions t ON c.customer_id = t.customer_id
        ),
        cohort_size AS (
            SELECT cohort_month, COUNT(DISTINCT customer_id) as cohort_size
            FROM customer_cohorts
            GROUP BY 1
        ),
        retention AS (
            SELECT 
                a.cohort_month,
                DATE_DIFF('month', a.cohort_month, a.activity_month) as months_since_join,
                COUNT(DISTINCT a.customer_id) as active_customers
            FROM activity a
            GROUP BY 1, 2
        )
        SELECT 
            r.cohort_month,
            cs.cohort_size,
            r.months_since_join,
            r.active_customers,
            ROUND(r.active_customers * 100.0 / cs.cohort_size, 2) as retention_pct
        FROM retention r
        JOIN cohort_size cs ON r.cohort_month = cs.cohort_month
        WHERE r.months_since_join <= 6
        ORDER BY r.cohort_month, r.months_since_join
    """).fetchdf()
    print(result.head(20).to_string(index=False))

    # 3. RFM Analysis (Recency, Frequency, Monetary)
    print("\n=== RFM Analysis ===")
    result = con.execute("""
        WITH rfm AS (
            SELECT 
                customer_id,
                DATE_DIFF('day', MAX(date), CURRENT_DATE) as recency,
                COUNT(*) as frequency,
                SUM(amount) as monetary
            FROM transactions
            GROUP BY customer_id
        ),
        rfm_scores AS (
            SELECT 
                *,
                NTILE(5) OVER (ORDER BY recency ASC) as r_score,
                NTILE(5) OVER (ORDER BY frequency DESC) as f_score,
                NTILE(5) OVER (ORDER BY monetary DESC) as m_score
            FROM rfm
        )
        SELECT 
            customer_id,
            recency,
            frequency,
            monetary,
            r_score,
            f_score,
            m_score,
            r_score + f_score + m_score as total_rfm
        FROM rfm_scores
        ORDER BY total_rfm DESC
        LIMIT 20
    """).fetchdf()
    print(result.to_string(index=False))

    con.close()


# ============================================================
# RUN THE SCENARIO
# ============================================================
if __name__ == "__main__":
    # Generate data
    print("Generating customer data...")
    customers, transactions = generate_customer_data()

    # Analyze
    analyze_customers(customers, transactions)
```

---

## 7. Interview Questions

### Q1: What window functions does DuckDB support?

**Answer:**

DuckDB supports all standard window functions:

**Ranking:**
```sql
RANK() OVER (...)           -- Gap ranking
DENSE_RANK() OVER (...)     -- No gap ranking
ROW_NUMBER() OVER (...)     -- Unique row numbers
NTILE(n) OVER (...)         -- Divide into n buckets
PERCENT_RANK() OVER (...)   -- Percentile rank
CUME_DIST() OVER (...)      -- Cumulative distribution
```

**Aggregate:**
```sql
SUM() OVER (...)            -- Running sum
AVG() OVER (...)            -- Running average
COUNT() OVER (...)          -- Running count
MIN() OVER (...)            -- Running min
MAX() OVER (...)            -- Running max
```

**Value:**
```sql
LAG(col, n) OVER (...)      -- Previous row value
LEAD(col, n) OVER (...)     -- Next row value
FIRST_VALUE(col) OVER (...) -- First value in window
LAST_VALUE(col) OVER (...)  -- Last value in window
NTH_VALUE(col, n) OVER (...)-- Nth value in window
```

**Example:**
```sql
SELECT 
    date,
    amount,
    SUM(amount) OVER (ORDER BY date) as running_total,
    AVG(amount) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as moving_avg_7d,
    RANK() OVER (ORDER BY amount DESC) as rank
FROM transactions
```

---

### Q2: Explain CTEs and their benefits.

**Answer:**

**CTE (Common Table Expression)** is a temporary named result set:

```sql
WITH 
monthly_stats AS (
    SELECT 
        DATE_TRUNC('month', date) as month,
        SUM(amount) as total
    FROM transactions
    GROUP BY 1
),
yearly_stats AS (
    SELECT 
        EXTRACT(YEAR FROM month) as year,
        AVG(total) as avg_monthly
    FROM monthly_stats
    GROUP BY 1
)
SELECT * FROM yearly_stats
```

**Benefits:**
1. **Readability**: Break complex queries into logical steps
2. **Reusability**: Reference CTE multiple times
3. **Recursion**: Support recursive queries (hierarchical data)
4. **Optimization**: DuckDB optimizes CTEs automatically

---

### Q3: How do you pivot data in DuckDB?

**Answer:**

**PIVOT syntax:**
```sql
SELECT *
FROM monthly_sales
PIVOT (
    SUM(amount)
    FOR month IN ('2026-01', '2026-02', '2026-03')
)
```

**UNPIVOT (reverse):**
```sql
SELECT *
FROM pivoted_data
UNPIVOT (
    amount
    FOR month IN (jan, feb, mar)
)
```

**Manual pivot (without PIVOT keyword):**
```sql
SELECT 
    customer_id,
    SUM(CASE WHEN EXTRACT(MONTH FROM date) = 1 THEN amount ELSE 0 END) as jan,
    SUM(CASE WHEN EXTRACT(MONTH FROM date) = 2 THEN amount ELSE 0 END) as feb,
    SUM(CASE WHEN EXTRACT(MONTH FROM date) = 3 THEN amount ELSE 0 END) as mar
FROM transactions
GROUP BY customer_id
```

---

### Q4: What array functions does DuckDB support?

**Answer:**

**Array creation:**
```sql
ARRAY[1, 2, 3]
ARRAY_AGG(column)
```

**Array operations:**
```sql
ARRAY_LENGTH(arr)              -- Length
ARRAY_CONTAINS(arr, val)       -- Contains value
ARRAY_APPEND(arr, val)         -- Add element
ARRAY_CAT(arr1, arr2)          -- Concatenate
ARRAY_SLICE(arr, start, end)   -- Slice
ARRAY_REVERSE(arr)             -- Reverse
ARRAY_SORT(arr)                -- Sort
ARRAY_DISTINCT(arr)            -- Remove duplicates
```

**Array transforms:**
```sql
LIST_FILTER(arr, x -> x > 100)      -- Filter
LIST_TRANSFORM(arr, x -> x * 2)     -- Transform
LIST_REDUCE(arr, (a, b) -> a + b)   -- Reduce
LIST_AGGR(arr, 'sum')               -- Aggregate
```

**Example:**
```sql
SELECT 
    customer_id,
    LIST(amount) as all_amounts,
    LIST_FILTER(amount, x -> x > 1000) as large_amounts,
    LIST_TRANSFORM(amount, x -> x * 1.1) as with_tip
FROM transactions
GROUP BY customer_id
```

---

### Q5: How do you handle date/time operations in DuckDB?

**Answer:**

**Current date/time:**
```sql
CURRENT_DATE
CURRENT_TIMESTAMP
NOW()
```

**Date truncation:**
```sql
DATE_TRUNC('day', date)
DATE_TRUNC('month', date)
DATE_TRUNC('year', date)
```

**Date differences:**
```sql
DATE_DIFF('day', date1, date2)
DATE_DIFF('month', date1, date2)
DATE_DIFF('year', date1, date2)
```

**Date arithmetic:**
```sql
date + INTERVAL 30 DAY
date - INTERVAL 7 DAY
DATE_ADD(date, INTERVAL 1 MONTH)
DATE_SUB(date, INTERVAL 1 WEEK)
```

**Extraction:**
```sql
EXTRACT(YEAR FROM date)
EXTRACT(MONTH FROM date)
EXTRACT(DAY FROM date)
EXTRACT(DOW FROM date)  -- Day of week
```

**Formatting:**
```sql
STRFTIME(date, '%Y-%m-%d')
STRFTIME(date, '%B %d, %Y')
```

**Example:**
```sql
SELECT 
    date,
    DATE_TRUNC('month', date) as month,
    EXTRACT(DAY FROM date) as day,
    DATE_DIFF('day', date, CURRENT_DATE) as days_ago,
    DATE_ADD(date, INTERVAL 30 DAY) as due_date
FROM transactions
```
