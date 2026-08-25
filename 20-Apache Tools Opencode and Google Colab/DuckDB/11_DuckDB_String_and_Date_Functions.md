# DuckDB String and Date Functions

## Table of Contents
1. [Detailed Explanation](#1-detailed-explanation)
2. [Example](#2-example)
3. [Real-World Banking Scenario 1](#3-banking-scenario-1-data-cleansing)
4. [Python Code - Scenario 1](#4-python-code---scenario-1)
5. [Real-World Banking Scenario 2](#5-banking-scenario-2-date-analysis)
6. [Python Code - Scenario 2](#6-python-code---scenario-2)
7. [Interview Questions](#7-interview-questions)

---

## 1. Detailed Explanation

### String Functions in DuckDB

#### 1. Case Functions
```sql
UPPER('hello')           -- 'HELLO'
LOWER('HELLO')           -- 'hello'
INITCAP('hello world')   -- 'Hello World'
```

#### 2. Length and Trim
```sql
LENGTH('hello')          -- 5
TRIM('  hello  ')        -- 'hello'
LTRIM('  hello')         -- 'hello'
RTRIM('hello  ')         -- 'hello'
```

#### 3. Substring and Position
```sql
SUBSTRING('hello' FROM 1 FOR 3)  -- 'hel'
SUBSTRING('hello', 2, 3)         -- 'ell'
POSITION('l' IN 'hello')         -- 3
STRPOS('hello', 'l')             -- 3
```

#### 4. Concatenation
```sql
CONCAT('hello', ' ', 'world')    -- 'hello world'
'hello' || ' ' || 'world'        -- 'hello world'
CONCAT_WS(',', 'a', 'b', 'c')   -- 'a,b,c'
```

#### 5. Replace and Translate
```sql
REPLACE('hello world', 'world', 'sql')  -- 'hello sql'
TRANSLATE('hello', 'elo', 'ELO')        -- 'hELO'
```

#### 6. Split and Extract
```sql
SPLIT_PART('a,b,c', ',', 1)     -- 'a'
SPLIT('a,b,c', ',')             -- ['a', 'b', 'c']
```

#### 7. Regular Expressions
```sql
REGEXP_REPLACE('hello123', '[0-9]', '')    -- 'hello'
REGEXP_MATCHES('hello123', '[0-9]+')       -- ['123']
REGEXP_CONTAINS('hello123', '[0-9]')       -- true
```

#### 8. Padding
```sql
LPAD('42', 5, '0')            -- '00042'
RPAD('hi', 5, '.')            -- 'hi...'
```

### Date/Time Functions in DuckDB

#### 1. Current Date/Time
```sql
CURRENT_DATE                 -- 2026-08-25
CURRENT_TIMESTAMP            -- 2026-08-25 12:00:00
NOW()                        -- 2026-08-25 12:00:00
```

#### 2. Date Truncation
```sql
DATE_TRUNC('day', timestamp)       -- 2026-08-25 00:00:00
DATE_TRUNC('month', timestamp)     -- 2026-08-01 00:00:00
DATE_TRUNC('year', timestamp)      -- 2026-01-01 00:00:00
DATE_TRUNC('week', timestamp)      -- 2026-08-24 00:00:00
```

#### 3. Date Extraction
```sql
EXTRACT(YEAR FROM date)            -- 2026
EXTRACT(MONTH FROM date)           -- 8
EXTRACT(DAY FROM date)             -- 25
EXTRACT(DOW FROM date)             -- 1 (day of week)
EXTRACT(DOY FROM date)             -- 237 (day of year)
```

#### 4. Date Differences
```sql
DATE_DIFF('day', date1, date2)     -- days between
DATE_DIFF('month', date1, date2)   -- months between
DATE_DIFF('year', date1, date2)    -- years between
```

#### 5. Date Arithmetic
```sql
date + INTERVAL 30 DAY             -- add 30 days
date - INTERVAL 7 DAY              -- subtract 7 days
DATE_ADD(date, INTERVAL 1 MONTH)   -- add 1 month
DATE_SUB(date, INTERVAL 1 WEEK)    -- subtract 1 week
```

#### 6. Date Formatting
```sql
STRFTIME(date, '%Y-%m-%d')         -- '2026-08-25'
STRFTIME(date, '%B %d, %Y')        -- 'August 25, 2026'
STRFTIME(date, '%H:%M:%S')         -- '12:00:00'
```

#### 7. Date Parsing
```sql
STRPTIME('2026-08-25', '%Y-%m-%d')  -- 2026-08-25
PARSE_DATE('%Y-%m-%d', '2026-08-25') -- 2026-08-25
```

#### 8. Age and Intervals
```sql
AGE(timestamp1, timestamp2)         -- interval between
INTERVAL '1' YEAR + INTERVAL '2' MONTH  -- combined interval
```

---

## 2. Example

### String and Date Functions Demo

```python
import duckdb
import pandas as pd

con = duckdb.connect()

# String functions
print("=== String Functions ===")
result = con.execute("""
    SELECT 
        UPPER('hello') as upper,
        LOWER('HELLO') as lower,
        LENGTH('hello') as length,
        TRIM('  hello  ') as trimmed,
        CONCAT('hello', ' ', 'world') as concatenated,
        REPLACE('hello world', 'world', 'sql') as replaced,
        SUBSTRING('hello world' FROM 1 FOR 5) as substring,
        SPLIT_PART('a,b,c', ',', 2) as split_part
""").fetchdf()
print(result.to_string(index=False))

# Date functions
print("\n=== Date Functions ===")
result = con.execute("""
    SELECT 
        CURRENT_DATE as today,
        CURRENT_TIMESTAMP as now,
        DATE_TRUNC('month', CURRENT_DATE) as month_start,
        EXTRACT(YEAR FROM CURRENT_DATE) as year,
        EXTRACT(MONTH FROM CURRENT_DATE) as month,
        EXTRACT(DAY FROM CURRENT_DATE) as day,
        DATE_DIFF('day', DATE '2026-01-01', CURRENT_DATE) as days_elapsed,
        DATE_ADD(CURRENT_DATE, INTERVAL 30 DAY) as future_date,
        STRFTIME(CURRENT_DATE, '%Y-%m-%d') as formatted
""").fetchdf()
print(result.to_string(index=False))

con.close()
```

---

## 3. Banking Scenario 1: Data Cleansing

### Problem
A bank needs to cleanse customer data:
- Standardize names (uppercase, trim)
- Validate phone numbers
- Format addresses
- Parse dates from various formats

### Why String/Date Functions?
- Data standardization
- Validation rules
- Format conversions
- Pattern matching

---

## 4. Python Code - Scenario 1

```python
import duckdb
import pandas as pd
import numpy as np
import random

# ============================================================
# BANKING SCENARIO: Data Cleansing
# ============================================================

def generate_dirty_data(num_rows=1000):
    """Generate dirty customer data."""
    random.seed(42)

    first_names = ["alice", "BOB", " charlie", "Diana ", " Eve ", "frank", "GRACE"]
    last_names = ["SMITH", "smith", " johnson ", "WILSON", "brown", " DAVIS "]
    emails = ["alice@email.com", "BOB@EMAIL.COM", " charlie@email.com", 
              "invalid-email", "diana@email.com", "", "frank@email.com"]
    phones = ["555-1234", "(555) 567-8901", "555.123.4567", "12345", 
              "+1-555-789-0123", "abc-defg", "555-9876"]
    dates = ["2026-01-15", "01/15/2026", "Jan 15, 2026", "2026/01/15",
             "15-01-2026", "invalid-date", "2026-02-20"]

    data = []
    for i in range(num_rows):
        data.append({
            "customer_id": f"CUST{i+1:05d}",
            "first_name": random.choice(first_names),
            "last_name": random.choice(last_names),
            "email": random.choice(emails),
            "phone": random.choice(phones),
            "join_date": random.choice(dates),
            "address": f"  {random.randint(100, 999)} Main St, {random.choice(['NYC', 'LA', 'Chicago'])}  ",
        })

    return pd.DataFrame(data)


def cleanse_data(df):
    """Cleanse data using DuckDB string functions."""
    con = duckdb.connect()
    con.register("customers", df)

    # 1. Standardize names
    print("=== Standardize Names ===")
    result = con.execute("""
        SELECT 
            customer_id,
            first_name,
            UPPER(TRIM(first_name)) as first_name_clean,
            INITCAP(TRIM(last_name)) as last_name_clean
        FROM customers
        LIMIT 10
    """).fetchdf()
    print(result.to_string(index=False))

    # 2. Validate emails
    print("\n=== Email Validation ===")
    result = con.execute("""
        SELECT 
            customer_id,
            email,
            CASE 
                WHEN email LIKE '%@%.%' THEN 'VALID'
                ELSE 'INVALID'
            END as email_status
        FROM customers
        WHERE email NOT LIKE '%@%.%'
        LIMIT 10
    """).fetchdf()
    print(result.to_string(index=False))

    # 3. Clean phone numbers
    print("\n=== Phone Number Cleaning ===")
    result = con.execute("""
        SELECT 
            customer_id,
            phone,
            REGEXP_REPLACE(phone, '[^0-9]', '', 'g') as phone_digits,
            LENGTH(REGEXP_REPLACE(phone, '[^0-9]', '', 'g')) as digit_count
        FROM customers
        LIMIT 10
    """).fetchdf()
    print(result.to_string(index=False))

    # 4. Clean addresses
    print("\n=== Address Cleaning ===")
    result = con.execute("""
        SELECT 
            customer_id,
            address,
            TRIM(address) as address_clean,
            SPLIT_PART(TRIM(address), ',', 1) as street,
            SPLIT_PART(TRIM(address), ',', 2) as city
        FROM customers
        LIMIT 10
    """).fetchdf()
    print(result.to_string(index=False))

    # 5. Full cleanse pipeline
    print("\n=== Full Cleanse Pipeline ===")
    result = con.execute("""
        SELECT 
            customer_id,
            UPPER(TRIM(first_name)) as first_name,
            INITCAP(TRIM(last_name)) as last_name,
            LOWER(TRIM(email)) as email,
            REGEXP_REPLACE(phone, '[^0-9+]', '', 'g') as phone_clean,
            TRIM(address) as address
        FROM customers
        WHERE email LIKE '%@%.%'
        LIMIT 15
    """).fetchdf()
    print(result.to_string(index=False))

    con.close()


# ============================================================
# RUN THE SCENARIO
# ============================================================
if __name__ == "__main__":
    # Generate dirty data
    print("Generating dirty data...")
    df = generate_dirty_data(num_rows=1000)

    # Cleanse
    cleanse_data(df)
```

---

## 5. Banking Scenario 2: Date Analysis

### Problem
A bank needs to analyze transactions by time:
- Daily/weekly/monthly trends
- Hourly patterns
- Day-of-week analysis
- Seasonal patterns

### Why Date Functions?
- Time-based aggregations
- Trend analysis
- Pattern detection
- Reporting periods

---

## 6. Python Code - Scenario 2

```python
import duckdb
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# ============================================================
# BANKING SCENARIO: Date Analysis
# ============================================================

def generate_transaction_data(num_rows=50_000):
    """Generate transaction data for date analysis."""
    random.seed(42)
    np.random.seed(42)

    df = pd.DataFrame({
        "transaction_id": range(1, num_rows + 1),
        "amount": np.random.lognormal(6, 2, num_rows).round(2),
        "status": np.random.choice(["COMPLETED", "PENDING", "FAILED"], num_rows),
        "channel": np.random.choice(["ONLINE", "MOBILE", "BRANCH", "ATM"], num_rows),
        "timestamp": pd.date_range("2025-01-01", periods=num_rows, freq="30s"),
    })

    return df


def analyze_by_date(df):
    """Analyze transactions by date using DuckDB."""
    con = duckdb.connect()
    con.register("transactions", df)

    # 1. Daily trend
    print("=== Daily Transaction Trend ===")
    result = con.execute("""
        SELECT 
            DATE_TRUNC('day', timestamp) as day,
            COUNT(*) as tx_count,
            SUM(amount) as total_amount
        FROM transactions
        WHERE status = 'COMPLETED'
        GROUP BY 1
        ORDER BY 1
        LIMIT 15
    """).fetchdf()
    print(result.to_string(index=False))

    # 2. Hourly pattern
    print("\n=== Hourly Pattern ===")
    result = con.execute("""
        SELECT 
            EXTRACT(HOUR FROM timestamp) as hour,
            COUNT(*) as tx_count,
            AVG(amount) as avg_amount
        FROM transactions
        WHERE status = 'COMPLETED'
        GROUP BY 1
        ORDER BY 1
    """).fetchdf()
    print(result.to_string(index=False))

    # 3. Day of week analysis
    print("\n=== Day of Week Analysis ===")
    result = con.execute("""
        SELECT 
            CASE EXTRACT(DOW FROM timestamp)
                WHEN 0 THEN 'Sunday'
                WHEN 1 THEN 'Monday'
                WHEN 2 THEN 'Tuesday'
                WHEN 3 THEN 'Wednesday'
                WHEN 4 THEN 'Thursday'
                WHEN 5 THEN 'Friday'
                WHEN 6 THEN 'Saturday'
            END as day_name,
            COUNT(*) as tx_count,
            SUM(amount) as total_amount
        FROM transactions
        WHERE status = 'COMPLETED'
        GROUP BY 1, EXTRACT(DOW FROM timestamp)
        ORDER BY EXTRACT(DOW FROM timestamp)
    """).fetchdf()
    print(result.to_string(index=False))

    # 4. Monthly trend
    print("\n=== Monthly Trend ===")
    result = con.execute("""
        SELECT 
            DATE_TRUNC('month', timestamp) as month,
            COUNT(*) as tx_count,
            SUM(amount) as total_amount
        FROM transactions
        WHERE status = 'COMPLETED'
        GROUP BY 1
        ORDER BY 1
    """).fetchdf()
    print(result.to_string(index=False))

    # 5. Year-over-year comparison
    print("\n=== Year-over-Year Comparison ===")
    result = con.execute("""
        WITH 
        yearly AS (
            SELECT 
                EXTRACT(YEAR FROM timestamp) as year,
                EXTRACT(MONTH FROM timestamp) as month,
                SUM(amount) as total_amount
            FROM transactions
            WHERE status = 'COMPLETED'
            GROUP BY 1, 2
        )
        SELECT 
            year,
            month,
            total_amount,
            LAG(total_amount, 12) OVER (ORDER BY year, month) as prev_year_amount,
            ROUND(
                (total_amount - LAG(total_amount, 12) OVER (ORDER BY year, month)) / 
                NULLIF(LAG(total_amount, 12) OVER (ORDER BY year, month), 0) * 100, 2
            ) as yoy_growth_pct
        FROM yearly
        ORDER BY year, month
    """).fetchdf()
    print(result.to_string(index=False))

    # 6. Time between transactions
    print("\n=== Time Between Transactions ===")
    result = con.execute("""
        SELECT 
            channel,
            AVG(time_diff) as avg_minutes_between,
            MIN(time_diff) as min_minutes,
            MAX(time_diff) as max_minutes
        FROM (
            SELECT 
                channel,
                EXTRACT(EPOCH FROM (
                    timestamp - LAG(timestamp) OVER (PARTITION BY channel ORDER BY timestamp)
                )) / 60 as time_diff
            FROM transactions
            WHERE status = 'COMPLETED'
        ) subq
        WHERE time_diff IS NOT NULL
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
    analyze_by_date(df)
```

---

## 7. Interview Questions

### Q1: What string functions does DuckDB support?

**Answer:**

**Case:** `UPPER()`, `LOWER()`, `INITCAP()`
**Length:** `LENGTH()`, `TRIM()`, `LTRIM()`, `RTRIM()`
**Substring:** `SUBSTRING()`, `POSITION()`, `STRPOS()`
**Concatenation:** `CONCAT()`, `||`, `CONCAT_WS()`
**Replace:** `REPLACE()`, `TRANSLATE()`
**Split:** `SPLIT_PART()`, `SPLIT()`
**Regex:** `REGEXP_REPLACE()`, `REGEXP_MATCHES()`, `REGEXP_CONTAINS()`
**Padding:** `LPAD()`, `RPAD()`

---

### Q2: How do you parse dates from different formats?

**Answer:**

```sql
-- ISO format
STRPTIME('2026-08-25', '%Y-%m-%d')

-- US format
STRPTIME('08/25/2026', '%m/%d/%Y')

-- Text format
STRPTIME('Aug 25, 2026', '%b %d, %Y')

-- With time
STRPTIME('2026-08-25 12:30:00', '%Y-%m-%d %H:%M:%S')
```

---

### Q3: What date extraction functions are available?

**Answer:**

```sql
EXTRACT(YEAR FROM date)      -- 2026
EXTRACT(MONTH FROM date)     -- 8
EXTRACT(DAY FROM date)       -- 25
EXTRACT(DOW FROM date)       -- Day of week (0=Sunday)
EXTRACT(DOY FROM date)       -- Day of year
EXTRACT(QUARTER FROM date)   -- Quarter
EXTRACT(WEEK FROM date)      -- Week number
```

---

### Q4: How do you calculate date differences?

**Answer:**

```sql
-- Days between
DATE_DIFF('day', date1, date2)

-- Months between
DATE_DIFF('month', date1, date2)

-- Years between
DATE_DIFF('year', date1, date2)

-- Example: Customer tenure
DATE_DIFF('day', join_date, CURRENT_DATE) as tenure_days
```

---

### Q5: How do you format dates for display?

**Answer:**

```sql
-- Standard formats
STRFTIME(date, '%Y-%m-%d')           -- 2026-08-25
STRFTIME(date, '%m/%d/%Y')           -- 08/25/2026
STRFTIME(date, '%B %d, %Y')          -- August 25, 2026

-- With time
STRFTIME(timestamp, '%Y-%m-%d %H:%M:%S')  -- 2026-08-25 12:30:00

-- Custom formats
STRFTIME(date, '%d-%b-%Y')           -- 25-Aug-2026
STRFTIME(date, '%Y年第%W周')          -- 2026年第34周
```
