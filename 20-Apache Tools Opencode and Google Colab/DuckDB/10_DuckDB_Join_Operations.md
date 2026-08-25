# DuckDB Join Operations

## Table of Contents
1. [Detailed Explanation](#1-detailed-explanation)
2. [Example](#2-example)
3. [Real-World Banking Scenario 1](#3-banking-scenario-1-customer-360)
4. [Python Code - Scenario 1](#4-python-code---scenario-1)
5. [Real-World Banking Scenario 2](#5-banking-scenario-2-fraud-detection)
6. [Python Code - Scenario 2](#6-python-code---scenario-2)
7. [Interview Questions](#7-interview-questions)

---

## 1. Detailed Explanation

### Join Operations in DuckDB

> **Joins combine rows from two or more tables based on a related column — essential for relational data analysis.**

### Types of Joins

#### 1. INNER JOIN
Returns only matching rows from both tables:

```sql
SELECT a.id, b.name
FROM table_a a
INNER JOIN table_b b ON a.id = b.id
```

```
Table A:    Table B:    Result:
1, A        1, X        1, A, X
2, B        3, Y        3, C, Y
3, C
```

#### 2. LEFT JOIN (LEFT OUTER JOIN)
Returns all rows from left table, matching rows from right:

```sql
SELECT a.id, b.name
FROM table_a a
LEFT JOIN table_b b ON a.id = b.id
```

```
Table A:    Table B:    Result:
1, A        1, X        1, A, X
2, B        3, Y        2, B, NULL
3, C                    3, C, NULL
```

#### 3. RIGHT JOIN (RIGHT OUTER JOIN)
Returns all rows from right table, matching rows from left:

```sql
SELECT a.id, b.name
FROM table_a a
RIGHT JOIN table_b b ON a.id = b.id
```

#### 4. FULL OUTER JOIN
Returns all rows from both tables:

```sql
SELECT a.id, b.name
FROM table_a a
FULL OUTER JOIN table_b b ON a.id = b.id
```

#### 5. CROSS JOIN
Returns Cartesian product of both tables:

```sql
SELECT a.id, b.id
FROM table_a a
CROSS JOIN table_b b
```

#### 6. SELF JOIN
Joins a table with itself:

```sql
SELECT a.id, b.id as parent_id
FROM table_a a
LEFT JOIN table_a b ON a.parent_id = b.id
```

### Join Syntax Variations

```sql
-- Explicit JOIN
SELECT * FROM a INNER JOIN b ON a.id = b.id

-- Implicit JOIN (same result)
SELECT * FROM a, b WHERE a.id = b.id

-- USING clause
SELECT * FROM a INNER JOIN b USING (id)

-- NATURAL JOIN
SELECT * FROM a NATURAL JOIN b
```

### Join Performance Tips

```
1. Join on indexed columns
2. Filter before joining (reduce dataset)
3. Use appropriate join type
4. Consider join order (small table first)
5. Use CTEs for complex joins
```

### Join Algorithms

| Algorithm | Best For | Memory |
|-----------|----------|--------|
| Hash Join | Equi-joins | High |
| Merge Join | Pre-sorted data | Low |
| Nested Loop | Small datasets | Low |

---

## 2. Example

### Join Operations Demo

```python
import duckdb
import pandas as pd
import numpy as np

# Create sample data
customers = pd.DataFrame({
    "customer_id": [1, 2, 3, 4, 5],
    "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
    "segment": ["PREMIUM", "STANDARD", "BASIC", "PREMIUM", "STANDARD"],
})

accounts = pd.DataFrame({
    "account_id": [101, 102, 103, 104, 105],
    "customer_id": [1, 1, 2, 3, 6],  # Note: customer 6 doesn't exist
    "balance": [10000, 5000, 25000, 1500, 8000],
})

transactions = pd.DataFrame({
    "transaction_id": range(1, 11),
    "account_id": [101, 101, 102, 103, 103, 104, 105, 101, 102, 103],
    "amount": [100, 200, 50, 300, 150, 75, 400, 250, 100, 500],
})

con = duckdb.connect()
con.register("customers", customers)
con.register("accounts", accounts)
con.register("transactions", transactions)

# 1. INNER JOIN
print("=== INNER JOIN ===")
result = con.execute("""
    SELECT c.name, a.account_id, a.balance
    FROM customers c
    INNER JOIN accounts a ON c.customer_id = a.customer_id
""").fetchdf()
print(result.to_string(index=False))

# 2. LEFT JOIN
print("\n=== LEFT JOIN ===")
result = con.execute("""
    SELECT c.name, a.account_id, a.balance
    FROM customers c
    LEFT JOIN accounts a ON c.customer_id = a.customer_id
""").fetchdf()
print(result.to_string(index=False))

# 3. Multiple JOINs
print("\n=== Multiple JOINs ===")
result = con.execute("""
    SELECT 
        c.name,
        a.account_id,
        SUM(t.amount) as total_amount
    FROM customers c
    INNER JOIN accounts a ON c.customer_id = a.customer_id
    INNER JOIN transactions t ON a.account_id = t.account_id
    GROUP BY c.name, a.account_id
    ORDER BY total_amount DESC
""").fetchdf()
print(result.to_string(index=False))

# 4. Self JOIN
print("\n=== Self JOIN (Account pairs) ===")
result = con.execute("""
    SELECT 
        a1.account_id as account_1,
        a2.account_id as account_2,
        a1.customer_id
    FROM accounts a1
    INNER JOIN accounts a2 ON a1.customer_id = a2.customer_id
    AND a1.account_id < a2.account_id
""").fetchdf()
print(result.to_string(index=False))

con.close()
```

---

## 3. Banking Scenario 1: Customer 360

### Problem
A bank needs a complete customer view:
- Customer info (name, segment, contact)
- Account info (balance, type)
- Transaction summary (total, count, last transaction)
- Product holdings

### Why Joins?
- Combine data from multiple tables
- Create comprehensive customer profile
- Enable 360-degree view

### Architecture
```
Customers Table ─┐
                  ├──► Customer 360 View
Accounts Table ──┤
                  │
Transactions ────┘
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
# BANKING SCENARIO: Customer 360 View
# ============================================================

def generate_banking_data():
    """Generate banking data for Customer 360."""
    random.seed(42)
    np.random.seed(42)

    num_customers = 1000
    num_accounts = 2000
    num_transactions = 50_000

    # Customers
    customers = pd.DataFrame({
        "customer_id": [f"CUST{i:05d}" for i in range(1, num_customers + 1)],
        "name": [f"Customer {i}" for i in range(1, num_customers + 1)],
        "segment": np.random.choice(["PREMIUM", "STANDARD", "BASIC"], num_customers, p=[0.2, 0.5, 0.3]),
        "join_date": pd.date_range("2020-01-01", periods=num_customers, freq="1h"),
        "email": [f"customer{i}@bank.com" for i in range(1, num_customers + 1)],
    })

    # Accounts
    accounts = pd.DataFrame({
        "account_id": [f"ACC{i:06d}" for i in range(1, num_accounts + 1)],
        "customer_id": [f"CUST{random.randint(1, num_customers):05d}" for _ in range(num_accounts)],
        "account_type": np.random.choice(["CHECKING", "SAVINGS", "CREDIT_CARD"], num_accounts),
        "balance": np.random.lognormal(10, 2, num_accounts).round(2),
        "opened_date": pd.date_range("2020-01-01", periods=num_accounts, freq="30min"),
    })

    # Transactions
    transactions = pd.DataFrame({
        "transaction_id": range(1, num_transactions + 1),
        "account_id": [f"ACC{random.randint(1, num_accounts):06d}" for _ in range(num_transactions)],
        "amount": np.random.lognormal(6, 2, num_transactions).round(2),
        "status": np.random.choice(["COMPLETED", "PENDING", "FAILED"], num_transactions),
        "date": pd.date_range("2026-01-01", periods=num_transactions, freq="30s"),
    })

    return customers, accounts, transactions


def build_customer_360(customers_df, accounts_df, transactions_df):
    """Build Customer 360 view using JOINs."""
    con = duckdb.connect()
    con.register("customers", customers_df)
    con.register("accounts", accounts_df)
    con.register("transactions", transactions_df)

    # 1. Customer with accounts
    print("=== Customer 360: Customer + Accounts ===")
    result = con.execute("""
        SELECT 
            c.customer_id,
            c.name,
            c.segment,
            a.account_id,
            a.account_type,
            a.balance
        FROM customers c
        LEFT JOIN accounts a ON c.customer_id = a.customer_id
        LIMIT 15
    """).fetchdf()
    print(result.to_string(index=False))

    # 2. Full Customer 360
    print("\n=== Full Customer 360 ===")
    result = con.execute("""
        WITH 
        customer_accounts AS (
            SELECT 
                c.customer_id,
                c.name,
                c.segment,
                c.join_date,
                COUNT(DISTINCT a.account_id) as account_count,
                SUM(a.balance) as total_balance
            FROM customers c
            LEFT JOIN accounts a ON c.customer_id = a.customer_id
            GROUP BY 1, 2, 3, 4
        ),
        account_transactions AS (
            SELECT 
                a.customer_id,
                COUNT(t.transaction_id) as tx_count,
                SUM(CASE WHEN t.status = 'COMPLETED' THEN t.amount ELSE 0 END) as total_volume,
                MAX(t.date) as last_transaction_date
            FROM accounts a
            LEFT JOIN transactions t ON a.account_id = t.account_id
            GROUP BY a.customer_id
        )
        SELECT 
            ca.customer_id,
            ca.name,
            ca.segment,
            ca.account_count,
            ca.total_balance,
            COALESCE(at.tx_count, 0) as tx_count,
            COALESCE(at.total_volume, 0) as total_volume,
            at.last_transaction_date
        FROM customer_accounts ca
        LEFT JOIN account_transactions at ON ca.customer_id = at.customer_id
        ORDER BY ca.total_balance DESC
        LIMIT 20
    """).fetchdf()
    print(result.to_string(index=False))

    # 3. Customer lifetime value
    print("\n=== Customer Lifetime Value ===")
    result = con.execute("""
        SELECT 
            c.customer_id,
            c.name,
            c.segment,
            SUM(t.amount) as lifetime_value,
            COUNT(t.transaction_id) as tx_count,
            DATE_DIFF('day', MIN(t.date), MAX(t.date)) as tenure_days
        FROM customers c
        INNER JOIN accounts a ON c.customer_id = a.customer_id
        INNER JOIN transactions t ON a.account_id = t.account_id
        WHERE t.status = 'COMPLETED'
        GROUP BY 1, 2, 3
        ORDER BY lifetime_value DESC
        LIMIT 15
    """).fetchdf()
    print(result.to_string(index=False))

    con.close()


# ============================================================
# RUN THE SCENARIO
# ============================================================
if __name__ == "__main__":
    # Generate data
    print("Generating banking data...")
    customers, accounts, transactions = generate_banking_data()

    # Build Customer 360
    build_customer_360(customers, accounts, transactions)
```

---

## 5. Banking Scenario 2: Fraud Detection

### Problem
A bank needs to detect fraud patterns:
- Unusual transaction amounts per customer
- Velocity checks (too many transactions)
- Cross-account patterns
- Geographic anomalies

### Why Joins?
- Combine transaction data with customer info
- Compare against historical patterns
- Identify suspicious relationships

---

## 6. Python Code - Scenario 2

```python
import duckdb
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# ============================================================
# BANKING SCENARIO: Fraud Detection with JOINs
# ============================================================

def generate_fraud_data():
    """Generate data for fraud detection."""
    random.seed(42)
    np.random.seed(42)

    num_transactions = 20_000
    num_customers = 500

    transactions = pd.DataFrame({
        "transaction_id": range(1, num_transactions + 1),
        "card_id": [f"CARD{random.randint(1, num_customers):05d}" for _ in range(num_transactions)],
        "amount": np.random.lognormal(6, 2, num_transactions).round(2),
        "merchant_category": np.random.choice(
            ["GROCERY", "RESTAURANT", "GAS", "ONLINE", "ATM", "HOTEL"], num_transactions
        ),
        "country": np.random.choice(["US", "GB", "DE", "FR", "JP", "CN"], num_transactions),
        "timestamp": pd.date_range("2026-01-01", periods=num_transactions, freq="30s"),
    })

    cards = pd.DataFrame({
        "card_id": [f"CARD{i:05d}" for i in range(1, num_customers + 1)],
        "customer_id": [f"CUST{random.randint(1, 100):05d}" for _ in range(num_customers)],
        "card_type": np.random.choice(["VISA", "MASTERCARD", "AMEX"], num_customers),
        "credit_limit": np.random.choice([5000, 10000, 25000, 50000], num_customers),
    })

    customers = pd.DataFrame({
        "customer_id": [f"CUST{i:05d}" for i in range(1, 101)],
        "name": [f"Customer {i}" for i in range(1, 101)],
        "risk_score": np.random.uniform(0, 100, 100).round(2),
    })

    return transactions, cards, customers


def detect_fraud_patterns(transactions_df, cards_df, customers_df):
    """Detect fraud patterns using JOINs."""
    con = duckdb.connect()
    con.register("transactions", transactions_df)
    con.register("cards", cards_df)
    con.register("customers", customers_df)

    # 1. High-value transactions with customer info
    print("=== High-Value Transactions ===")
    result = con.execute("""
        SELECT 
            t.transaction_id,
            t.amount,
            t.merchant_category,
            t.country,
            c.customer_id,
            c.name,
            c.risk_score
        FROM transactions t
        INNER JOIN cards ca ON t.card_id = ca.card_id
        INNER JOIN customers c ON ca.customer_id = c.customer_id
        WHERE t.amount > 10000
        ORDER BY t.amount DESC
        LIMIT 20
    """).fetchdf()
    print(result.to_string(index=False))

    # 2. Velocity check (transactions per hour per customer)
    print("\n=== Velocity Check ===")
    result = con.execute("""
        WITH 
        customer_transactions AS (
            SELECT 
                ca.customer_id,
                DATE_TRUNC('hour', t.timestamp) as hour,
                COUNT(*) as tx_count,
                SUM(t.amount) as total_amount
            FROM transactions t
            INNER JOIN cards ca ON t.card_id = ca.card_id
            GROUP BY 1, 2
        )
        SELECT 
            ct.customer_id,
            c.name,
            ct.hour,
            ct.tx_count,
            ct.total_amount,
            CASE 
                WHEN ct.tx_count > 10 THEN 'VELOCITY_ALERT'
                WHEN ct.total_amount > 50000 THEN 'AMOUNT_ALERT'
                ELSE 'NORMAL'
            END as alert_type
        FROM customer_transactions ct
        INNER JOIN customers c ON ct.customer_id = c.customer_id
        WHERE ct.tx_count > 10 OR ct.total_amount > 50000
        ORDER BY ct.total_amount DESC
        LIMIT 20
    """).fetchdf()
    print(result.to_string(index=False))

    # 3. Cross-border transactions
    print("\n=== Cross-Border Transactions ===")
    result = con.execute("""
        SELECT 
            t.transaction_id,
            t.amount,
            t.country,
            ca.customer_id,
            c.name,
            c.risk_score
        FROM transactions t
        INNER JOIN cards ca ON t.card_id = ca.card_id
        INNER JOIN customers c ON ca.customer_id = c.customer_id
        WHERE t.country != 'US' AND t.amount > 5000
        ORDER BY c.risk_score DESC, t.amount DESC
        LIMIT 15
    """).fetchdf()
    print(result.to_string(index=False))

    # 4. Customer spending patterns
    print("\n=== Customer Spending Patterns ===")
    result = con.execute("""
        SELECT 
            c.customer_id,
            c.name,
            c.risk_score,
            COUNT(t.transaction_id) as tx_count,
            SUM(t.amount) as total_spent,
            AVG(t.amount) as avg_amount,
            COUNT(DISTINCT t.country) as countries_visited
        FROM customers c
        INNER JOIN cards ca ON c.customer_id = ca.customer_id
        INNER JOIN transactions t ON ca.card_id = t.card_id
        GROUP BY 1, 2, 3
        HAVING total_spent > 50000 OR countries_visited > 3
        ORDER BY total_spent DESC
        LIMIT 15
    """).fetchdf()
    print(result.to_string(index=False))

    con.close()


# ============================================================
# RUN THE SCENARIO
# ============================================================
if __name__ == "__main__":
    # Generate data
    print("Generating fraud detection data...")
    transactions, cards, customers = generate_fraud_data()

    # Detect fraud
    detect_fraud_patterns(transactions, cards, customers)
```

---

## 7. Interview Questions

### Q1: What is the difference between INNER JOIN and LEFT JOIN?

**Answer:**

| Join Type | Returns |
|-----------|---------|
| INNER JOIN | Only matching rows from both tables |
| LEFT JOIN | All rows from left table + matching from right |

**Example:**
```sql
-- Customers: [1, Alice], [2, Bob], [3, Charlie]
-- Accounts: [1, 1000], [3, 2000]

-- INNER JOIN
SELECT c.name, a.balance
FROM customers c INNER JOIN accounts a ON c.id = a.customer_id
-- Result: Alice 1000, Charlie 2000

-- LEFT JOIN
SELECT c.name, a.balance
FROM customers c LEFT JOIN accounts a ON c.id = a.customer_id
-- Result: Alice 1000, Bob NULL, Charlie 2000
```

---

### Q2: When would you use a CROSS JOIN?

**Answer:**

**Use CROSS JOIN for:**
- Generating combinations (e.g., all products × all regions)
- Calendar tables
- Matrix operations
- Testing all scenarios

**Example:**
```sql
-- Generate all date × product combinations
SELECT d.date, p.product
FROM dates d
CROSS JOIN products p
```

**Warning:** CROSS JOIN produces `m × n` rows. Use carefully!

---

### Q3: How do you optimize JOIN performance?

**Answer:**

1. **Join on indexed columns**
```sql
CREATE INDEX idx_account_id ON transactions(account_id)
```

2. **Filter before joining**
```sql
WITH filtered AS (
    SELECT * FROM transactions WHERE date >= '2026-01-01'
)
SELECT * FROM filtered f
JOIN accounts a ON f.account_id = a.account_id
```

3. **Use appropriate join type**
```sql
-- Use INNER JOIN when you only need matches
-- Use LEFT JOIN when you need all rows from one table
```

4. **Consider join order**
```sql
-- Small table first
SELECT * FROM small_table s
JOIN large_table l ON s.id = l.id
```

---

### Q4: What is a self JOIN and when to use it?

**Answer:**

A **self JOIN** joins a table with itself:

```sql
-- Find employees and their managers
SELECT 
    e.name as employee,
    m.name as manager
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.id
```

**Use cases:**
- Hierarchical data (org charts)
- Comparing rows (duplicate detection)
- Running totals within same table
- Finding pairs (account relationships)

---

### Q5: How do you handle NULL values in JOINs?

**Answer:**

**NULL handling in JOINs:**

```sql
-- NULL != NULL (they don't match)
SELECT * FROM a LEFT JOIN b ON a.id = b.id
-- If b.id is NULL, it won't match a.id

-- Use IS NULL to check
SELECT * FROM a LEFT JOIN b ON a.id = b.id
WHERE b.id IS NULL  -- Find unmatched rows
```

**COALESCE for default values:**
```sql
SELECT 
    c.name,
    COALESCE(SUM(t.amount), 0) as total_amount
FROM customers c
LEFT JOIN transactions t ON c.id = t.customer_id
GROUP BY c.name
```
