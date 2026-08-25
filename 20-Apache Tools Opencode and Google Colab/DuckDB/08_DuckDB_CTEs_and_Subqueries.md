# DuckDB CTEs and Subqueries

## Table of Contents
1. [Detailed Explanation](#1-detailed-explanation)
2. [Example](#2-example)
3. [Real-World Banking Scenario 1](#3-banking-scenario-1-complex-analytics)
4. [Python Code - Scenario 1](#4-python-code---scenario-1)
5. [Real-World Banking Scenario 2](#5-banking-scenario-2-data-validation)
6. [Python Code - Scenario 2](#6-python-code---scenario-2)
7. [Interview Questions](#7-interview-questions)

---

## 1. Detailed Explanation

### Common Table Expressions (CTEs)

> **CTEs are temporary named result sets that exist within a single query — making complex SQL readable and modular.**

### CTE Syntax

```sql
WITH 
cte_name1 AS (
    SELECT ...
),
cte_name2 AS (
    SELECT ...
)
SELECT * FROM cte_name1
JOIN cte_name2 ON ...
```

### Types of CTEs

#### 1. Non-Recursive CTE
```sql
WITH 
monthly_stats AS (
    SELECT 
        DATE_TRUNC('month', date) as month,
        SUM(amount) as total
    FROM transactions
    GROUP BY 1
)
SELECT * FROM monthly_stats
```

#### 2. Recursive CTE
```sql
WITH RECURSIVE 
hierarchy AS (
    -- Base case
    SELECT id, name, manager_id, 1 as level
    FROM employees
    WHERE manager_id IS NULL
    
    UNION ALL
    
    -- Recursive case
    SELECT e.id, e.name, e.manager_id, h.level + 1
    FROM employees e
    JOIN hierarchy h ON e.manager_id = h.id
)
SELECT * FROM hierarchy
```

### Subqueries

#### 1. Scalar Subquery (returns one value)
```sql
SELECT *
FROM transactions
WHERE amount > (SELECT AVG(amount) FROM transactions)
```

#### 2. Row Subquery (returns one row)
```sql
SELECT *
FROM transactions
WHERE (status, amount) = (
    SELECT status, MAX(amount) 
    FROM transactions 
    GROUP BY status 
    LIMIT 1
)
```

#### 3. Table Subquery (returns multiple rows)
```sql
SELECT *
FROM transactions
WHERE account_id IN (
    SELECT account_id 
    FROM accounts 
    WHERE balance > 100000
)
```

#### 4. Correlated Subquery
```sql
SELECT 
    t.*,
    (SELECT COUNT(*) FROM transactions t2 
     WHERE t2.account_id = t.account_id 
     AND t2.date < t.date) as tx_before
FROM transactions t
```

### CTE vs Subquery

| Feature | CTE | Subquery |
|---------|-----|----------|
| **Readability** | High | Low |
| **Reusability** | Can reference multiple times | Inline only |
| **Recursion** | Supported | Not supported |
| **Performance** | Optimized by engine | May be inlined |
| **Scope** | Entire query | Single statement |

### When to Use CTEs

```
1. Complex queries (multiple steps)
2. Recursive queries (hierarchies)
3. Reusable calculations
4. Readability improvement
5. Modular query design
```

### When to Use Subqueries

```
1. Simple filtering (IN, EXISTS)
2. One-time calculations
3. Inline comparisons
4. Simple scalar values
```

---

## 2. Example

### CTEs and Subqueries Demo

```python
import duckdb
import pandas as pd
import numpy as np

# Create sample data
np.random.seed(42)
num_rows = 10000

transactions = pd.DataFrame({
    "id": range(1, num_rows + 1),
    "account_id": [f"ACC{np.random.randint(1, 500):03d}" for _ in range(num_rows)],
    "amount": np.random.uniform(100, 100000, num_rows).round(2),
    "status": np.random.choice(["COMPLETED", "PENDING", "FAILED"], num_rows),
    "date": pd.date_range("2026-01-01", periods=num_rows, freq="30min"),
})

accounts = pd.DataFrame({
    "account_id": [f"ACC{i:03d}" for i in range(1, 501)],
    "customer_id": [f"CUST{np.random.randint(1, 100):03d}" for _ in range(500)],
    "balance": np.random.uniform(1000, 500000, 500).round(2),
})

con = duckdb.connect()
con.register("transactions", transactions)
con.register("accounts", accounts)

# 1. CTE for customer summary
print("=== CTE: Customer Summary ===")
result = con.execute("""
    WITH 
    customer_stats AS (
        SELECT 
            a.customer_id,
            COUNT(t.id) as tx_count,
            SUM(t.amount) as total_amount
        FROM transactions t
        JOIN accounts a ON t.account_id = a.account_id
        GROUP BY a.customer_id
    )
    SELECT *
    FROM customer_stats
    WHERE total_amount > 50000
    ORDER BY total_amount DESC
    LIMIT 10
""").fetchdf()
print(result.to_string(index=False))

# 2. Multiple CTEs
print("\n=== Multiple CTEs ===")
result = con.execute("""
    WITH 
    high_value_txns AS (
        SELECT * FROM transactions WHERE amount > 50000
    ),
    active_accounts AS (
        SELECT * FROM accounts WHERE balance > 100000
    )
    SELECT 
        hv.id,
        hv.amount,
        hv.status,
        aa.customer_id,
        aa.balance
    FROM high_value_txns hv
    JOIN active_accounts aa ON hv.account_id = aa.account_id
    ORDER BY hv.amount DESC
    LIMIT 10
""").fetchdf()
print(result.to_string(index=False))

# 3. Subquery for filtering
print("\n=== Subquery: Filter ===")
result = con.execute("""
    SELECT *
    FROM transactions
    WHERE account_id IN (
        SELECT account_id 
        FROM accounts 
        WHERE balance > 200000
    )
    LIMIT 10
""").fetchdf()
print(result.to_string(index=False))

# 4. Correlated subquery
print("\n=== Correlated Subquery ===")
result = con.execute("""
    SELECT 
        t.id,
        t.account_id,
        t.amount,
        (SELECT COUNT(*) FROM transactions t2 
         WHERE t2.account_id = t.account_id 
         AND t2.date <= t.date) as cumulative_count
    FROM transactions t
    WHERE t.account_id = 'ACC001'
    ORDER BY t.date
    LIMIT 10
""").fetchdf()
print(result.to_string(index=False))

con.close()
```

---

## 3. Banking Scenario 1: Complex Analytics

### Problem
A bank needs to perform complex analytics:
- Identify high-value customers with specific patterns
- Find accounts with unusual activity
- Generate cohort analysis
- Detect fraud patterns

### Why CTEs?
- Break complex queries into logical steps
- Reuse calculations across the query
- Improve readability for analysts
- Support recursive hierarchies

---

## 4. Python Code - Scenario 1

```python
import duckdb
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# ============================================================
# BANKING SCENARIO: Complex Analytics with CTEs
# ============================================================

def generate_banking_data():
    """Generate banking data for analytics."""
    random.seed(42)
    np.random.seed(42)

    num_transactions = 50_000
    num_customers = 1000

    transactions = pd.DataFrame({
        "transaction_id": range(1, num_transactions + 1),
        "account_id": [f"ACC{random.randint(1, 500):03d}" for _ in range(num_transactions)],
        "amount": np.random.lognormal(6, 2, num_transactions).round(2),
        "status": np.random.choice(["COMPLETED", "PENDING", "FAILED"], num_transactions),
        "channel": np.random.choice(["ONLINE", "MOBILE", "BRANCH", "ATM"], num_transactions),
        "date": pd.date_range("2026-01-01", periods=num_transactions, freq="30s"),
    })

    accounts = pd.DataFrame({
        "account_id": [f"ACC{i:03d}" for i in range(1, 501)],
        "customer_id": [f"CUST{random.randint(1, num_customers):05d}" for _ in range(500)],
        "balance": np.random.lognormal(10, 2, 500).round(2),
        "account_type": np.random.choice(["CHECKING", "SAVINGS", "CREDIT_CARD"], 500),
    })

    customers = pd.DataFrame({
        "customer_id": [f"CUST{i:05d}" for i in range(1, num_customers + 1)],
        "name": [f"Customer {i}" for i in range(1, num_customers + 1)],
        "segment": np.random.choice(["PREMIUM", "STANDARD", "BASIC"], num_customers),
    })

    return transactions, accounts, customers


def run_complex_analytics(transactions_df, accounts_df, customers_df):
    """Run complex analytics using CTEs."""
    con = duckdb.connect()
    con.register("transactions", transactions_df)
    con.register("accounts", accounts_df)
    con.register("customers", customers_df)

    # 1. High-value customer identification
    print("=== High-Value Customer Identification ===")
    result = con.execute("""
        WITH 
        customer_transactions AS (
            SELECT 
                a.customer_id,
                COUNT(t.transaction_id) as tx_count,
                SUM(t.amount) as total_amount,
                AVG(t.amount) as avg_amount,
                MAX(t.amount) as max_amount
            FROM transactions t
            JOIN accounts a ON t.account_id = a.account_id
            WHERE t.status = 'COMPLETED'
            GROUP BY a.customer_id
        ),
        customer_segments AS (
            SELECT 
                ct.*,
                c.segment,
                c.name,
                CASE 
                    WHEN ct.total_amount > 100000 THEN 'PLATINUM'
                    WHEN ct.total_amount > 50000 THEN 'GOLD'
                    WHEN ct.total_amount > 10000 THEN 'SILVER'
                    ELSE 'BRONZE'
                END as value_tier
            FROM customer_transactions ct
            JOIN customers c ON ct.customer_id = c.customer_id
        )
        SELECT *
        FROM customer_segments
        WHERE value_tier IN ('PLATINUM', 'GOLD')
        ORDER BY total_amount DESC
        LIMIT 20
    """).fetchdf()
    print(result.to_string(index=False))

    # 2. Unusual activity detection
    print("\n=== Unusual Activity Detection ===")
    result = con.execute("""
        WITH 
        account_stats AS (
            SELECT 
                account_id,
                AVG(amount) as avg_amount,
                STDDEV(amount) as std_amount,
                COUNT(*) as tx_count
            FROM transactions
            WHERE status = 'COMPLETED'
            GROUP BY account_id
        ),
        unusual_transactions AS (
            SELECT 
                t.*,
                s.avg_amount,
                s.std_amount,
                (t.amount - s.avg_amount) / NULLIF(s.std_amount, 0) as z_score
            FROM transactions t
            JOIN account_stats s ON t.account_id = s.account_id
            WHERE t.status = 'COMPLETED'
        )
        SELECT 
            transaction_id,
            account_id,
            amount,
            avg_amount,
            ROUND(z_score, 2) as z_score,
            CASE 
                WHEN z_score > 3 THEN 'EXTREMELY_HIGH'
                WHEN z_score > 2 THEN 'HIGH'
                WHEN z_score < -2 THEN 'LOW'
                ELSE 'NORMAL'
            END as anomaly_level
        FROM unusual_transactions
        WHERE ABS(z_score) > 2
        ORDER BY z_score DESC
        LIMIT 20
    """).fetchdf()
    print(result.to_string(index=False))

    # 3. Channel performance analysis
    print("\n=== Channel Performance Analysis ===")
    result = con.execute("""
        WITH 
        channel_stats AS (
            SELECT 
                channel,
                COUNT(*) as total_tx,
                SUM(CASE WHEN status = 'COMPLETED' THEN 1 ELSE 0 END) as completed,
                SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed,
                SUM(amount) as total_amount
            FROM transactions
            GROUP BY channel
        ),
        channel_metrics AS (
            SELECT 
                *,
                ROUND(completed * 100.0 / total_tx, 2) as success_rate,
                ROUND(failed * 100.0 / total_tx, 2) as failure_rate,
                ROUND(total_amount / total_tx, 2) as avg_amount
            FROM channel_stats
        )
        SELECT 
            channel,
            total_tx,
            success_rate,
            failure_rate,
            avg_amount,
            RANK() OVER (ORDER BY success_rate DESC) as reliability_rank
        FROM channel_metrics
        ORDER BY reliability_rank
    """).fetchdf()
    print(result.to_string(index=False))

    con.close()


# ============================================================
# RUN THE SCENARIO
# ============================================================
if __name__ == "__main__":
    # Generate data
    print("Generating banking data...")
    transactions, accounts, customers = generate_banking_data()

    # Run analytics
    run_complex_analytics(transactions, accounts, customers)
```

---

## 5. Banking Scenario 2: Data Validation

### Problem
A bank needs to validate data quality:
- Check for duplicate transactions
- Identify missing data
- Validate referential integrity
- Detect anomalies

### Why CTEs?
- Step-by-step validation logic
- Reusable validation queries
- Clear audit trail

---

## 6. Python Code - Scenario 2

```python
import duckdb
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# ============================================================
# BANKING SCENARIO: Data Validation with CTEs
# ============================================================

def generate_data_with_issues():
    """Generate data with quality issues."""
    random.seed(42)
    np.random.seed(42)

    num_transactions = 10_000

    # Create some duplicates
    base_ids = list(range(1, num_transactions - 100 + 1))
    duplicate_ids = random.sample(base_ids, 100)
    all_ids = base_ids + duplicate_ids

    transactions = pd.DataFrame({
        "transaction_id": all_ids,
        "account_id": [f"ACC{random.randint(1, 100):03d}" for _ in range(len(all_ids))],
        "amount": np.random.uniform(100, 10000, len(all_ids)).round(2),
        "status": np.random.choice(["COMPLETED", "PENDING", "FAILED", ""], len(all_ids)),
        "date": pd.date_range("2026-01-01", periods=len(all_ids), freq="30s"),
    })

    accounts = pd.DataFrame({
        "account_id": [f"ACC{i:03d}" for i in range(1, 101)],
        "customer_id": [f"CUST{random.randint(1, 50):03d}" for _ in range(100)],
    })

    return transactions, accounts


def validate_data(transactions_df, accounts_df):
    """Validate data quality using CTEs."""
    con = duckdb.connect()
    con.register("transactions", transactions_df)
    con.register("accounts", accounts_df)

    # 1. Check for duplicates
    print("=== Duplicate Check ===")
    result = con.execute("""
        WITH 
        duplicate_check AS (
            SELECT 
                transaction_id,
                COUNT(*) as occurrence_count
            FROM transactions
            GROUP BY transaction_id
            HAVING COUNT(*) > 1
        )
        SELECT 
            transaction_id,
            occurrence_count,
            'DUPLICATE' as issue_type
        FROM duplicate_check
        ORDER BY occurrence_count DESC
        LIMIT 10
    """).fetchdf()
    print(result.to_string(index=False))

    # 2. Check for missing values
    print("\n=== Missing Value Check ===")
    result = con.execute("""
        WITH 
        null_check AS (
            SELECT 
                COUNT(*) as total_rows,
                COUNT(transaction_id) - COUNT(transaction_id) as null_transaction_id,
                COUNT(account_id) - COUNT(account_id) as null_account_id,
                COUNT(amount) - COUNT(amount) as null_amount,
                SUM(CASE WHEN status = '' THEN 1 ELSE 0 END) as empty_status
            FROM transactions
        )
        SELECT *
        FROM null_check
    """).fetchdf()
    print(result.to_string(index=False))

    # 3. Check referential integrity
    print("\n=== Referential Integrity Check ===")
    result = con.execute("""
        WITH 
        orphan_accounts AS (
            SELECT DISTINCT 
                t.account_id,
                'ORPHAN_ACCOUNT' as issue_type
            FROM transactions t
            LEFT JOIN accounts a ON t.account_id = a.account_id
            WHERE a.account_id IS NULL
        )
        SELECT *
        FROM orphan_accounts
        LIMIT 10
    """).fetchdf()
    print(result.to_string(index=False))

    # 4. Check for anomalies
    print("\n=== Anomaly Check ===")
    result = con.execute("""
        WITH 
        amount_stats AS (
            SELECT 
                AVG(amount) as avg_amount,
                STDDEV(amount) as std_amount
            FROM transactions
            WHERE status = 'COMPLETED'
        ),
        anomalies AS (
            SELECT 
                t.*,
                (t.amount - s.avg_amount) / NULLIF(s.std_amount, 0) as z_score
            FROM transactions t
            CROSS JOIN amount_stats s
            WHERE t.status = 'COMPLETED'
        )
        SELECT 
            transaction_id,
            amount,
            ROUND(z_score, 2) as z_score,
            CASE 
                WHEN z_score > 3 THEN 'EXTREME_HIGH'
                WHEN z_score < -3 THEN 'EXTREME_LOW'
                ELSE 'NORMAL'
            END as anomaly_level
        FROM anomalies
        WHERE ABS(z_score) > 3
        ORDER BY z_score DESC
        LIMIT 10
    """).fetchdf()
    print(result.to_string(index=False))

    # 5. Summary report
    print("\n=== Validation Summary ===")
    result = con.execute("""
        WITH 
        summary AS (
            SELECT 
                COUNT(DISTINCT transaction_id) as unique_transactions,
                COUNT(*) as total_rows,
                COUNT(*) - COUNT(DISTINCT transaction_id) as duplicate_count,
                SUM(CASE WHEN status = '' THEN 1 ELSE 0 END) as empty_status_count
            FROM transactions
        )
        SELECT 
            unique_transactions,
            total_rows,
            duplicate_count,
            empty_status_count,
            ROUND(duplicate_count * 100.0 / total_rows, 2) as duplicate_pct
        FROM summary
    """).fetchdf()
    print(result.to_string(index=False))

    con.close()


# ============================================================
# RUN THE SCENARIO
# ============================================================
if __name__ == "__main__":
    # Generate data with issues
    print("Generating data with quality issues...")
    transactions, accounts = generate_data_with_issues()

    # Validate
    validate_data(transactions, accounts)
```

---

## 7. Interview Questions

### Q1: What is a CTE and why use it?

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
)
SELECT * FROM monthly_stats
```

**Benefits:**
1. **Readability**: Break complex queries into logical steps
2. **Reusability**: Reference CTE multiple times
3. **Recursion**: Support recursive queries
4. **Optimization**: DuckDB optimizes CTEs automatically

---

### Q2: When would you use a subquery vs a CTE?

**Answer:**

**Use subquery when:**
- Simple filtering (IN, EXISTS)
- One-time calculation
- Inline comparison

**Use CTE when:**
- Complex queries (multiple steps)
- Reusable calculations
- Recursive queries
- Readability is important

**Example:**
```sql
-- Subquery: Simple filter
SELECT * FROM transactions
WHERE account_id IN (SELECT account_id FROM accounts WHERE balance > 100000)

-- CTE: Complex analysis
WITH 
customer_stats AS (
    SELECT customer_id, SUM(amount) as total
    FROM transactions
    GROUP BY customer_id
)
SELECT * FROM customer_stats WHERE total > 100000
```

---

### Q3: How do recursive CTEs work?

**Answer:**

**Recursive CTE structure:**
```sql
WITH RECURSIVE 
cte_name AS (
    -- Base case (non-recursive)
    SELECT ...
    
    UNION ALL
    
    -- Recursive case
    SELECT ...
    FROM cte_name
    JOIN ...
)
SELECT * FROM cte_name
```

**Example: Organizational hierarchy**
```sql
WITH RECURSIVE 
hierarchy AS (
    SELECT id, name, manager_id, 1 as level
    FROM employees
    WHERE manager_id IS NULL
    
    UNION ALL
    
    SELECT e.id, e.name, e.manager_id, h.level + 1
    FROM employees e
    JOIN hierarchy h ON e.manager_id = h.id
)
SELECT * FROM hierarchy
```

**Banking example: Transaction chain**
```sql
WITH RECURSIVE 
tx_chain AS (
    SELECT transaction_id, parent_id, amount, 1 as depth
    FROM transactions
    WHERE parent_id IS NULL
    
    UNION ALL
    
    SELECT t.transaction_id, t.parent_id, t.amount, tc.depth + 1
    FROM transactions t
    JOIN tx_chain tc ON t.parent_id = tc.transaction_id
)
SELECT * FROM tx_chain
```

---

### Q4: What are correlated subqueries?

**Answer:**

A **correlated subquery** references columns from the outer query:

```sql
SELECT 
    t.*,
    (SELECT COUNT(*) FROM transactions t2 
     WHERE t2.account_id = t.account_id 
     AND t2.date < t.date) as tx_before
FROM transactions t
```

**How it works:**
1. Outer query executes
2. For each row, subquery executes
3. Subquery uses outer row's values

**Performance:** Can be slow (executes per row). Consider CTEs or window functions instead.

---

### Q5: How do you optimize CTE performance?

**Answer:**

1. **Filter early**: Push WHERE clauses into CTEs
```sql
WITH filtered AS (
    SELECT * FROM transactions WHERE date >= '2026-01-01'
)
SELECT * FROM filtered
```

2. **Aggregate early**: Reduce data before joins
```sql
WITH aggregated AS (
    SELECT customer_id, SUM(amount) as total
    FROM transactions
    GROUP BY customer_id
)
SELECT * FROM aggregated WHERE total > 10000
```

3. **Avoid SELECT ***: Only select needed columns
```sql
-- Bad
WITH cte AS (SELECT * FROM transactions)

-- Good
WITH cte AS (SELECT id, amount, status FROM transactions)
```

4. **Use indexes**: Ensure proper indexes on join/filter columns
