# What is DuckDB?

## Table of Contents
1. [Detailed Explanation](#1-detailed-explanation)
2. [Example](#2-example)
3. [Real-World Banking Scenario 1](#3-banking-scenario-1-ad-hoc-analytics)
4. [Python Code - Scenario 1](#4-python-code---scenario-1)
5. [Real-World Banking Scenario 2](#5-banking-scenario-2-embedded-analytics)
6. [Python Code - Scenario 2](#6-python-code---scenario-2)
7. [Interview Questions](#7-interview-questions)

---

## 1. Detailed Explanation

### What is DuckDB?

DuckDB is an **in-process, embedded analytical SQL database** designed for fast query performance on data stored in files:

> **DuckDB is to analytical SQL what SQLite is to transactional SQL — a lightweight, zero-dependency database that runs in your application process, but optimized for analytics instead of transactions.**

The single most important thing to understand about DuckDB:

> **DuckDB reads Parquet, CSV, JSON, and Arrow files directly — without loading them into memory or importing them into a database. You query files as if they were database tables.**

### The One-Line Definition

> **DuckDB is an embedded analytical SQL database that queries files directly with zero configuration.**

### Core Characteristics

| Characteristic | Description |
|---------------|-------------|
| **Embedded** | Runs in your application process (like SQLite) |
| **Analytical** | Optimized for OLAP queries (aggregations, joins, window functions) |
| **Zero-config** | No server to install or configure |
| **File-based** | Queries Parquet, CSV, JSON, Arrow files directly |
| **Vectorized** | Uses columnar execution for high performance |
| **Extension-rich** | Supports spatial, JSON, parquet, and more |
| **Cross-platform** | Runs on Windows, macOS, Linux, Python, R, Java |

### Where DuckDB Fits in the Data Ecosystem

```
Your Python Script
       |
       v
  DuckDB (embedded, in-process)
       |
       +-- SQL Query Engine
       +-- Vectorized Execution
       +-- Parquet/CSV/JSON Reader
       |
       v
  Files on Disk / S3 / GCS
       |
       +-- Parquet files
       +-- CSV files
       +-- JSON files
       +-- Arrow datasets
```

### DuckDB vs Traditional Databases

| Feature | DuckDB | PostgreSQL | SQLite |
|---------|--------|------------|--------|
| **Type** | Analytical (OLAP) | Transactional (OLTP) | Transactional (OLTP) |
| **Deployment** | Embedded | Server | Embedded |
| **Query focus** | Aggregations, analytics | CRUD, transactions | CRUD, transactions |
| **Storage** | Files (Parquet, CSV) | Internal storage | Internal storage |
| **Setup** | pip install | Server install | pip install |
| **Concurrency** | Single-writer, multi-reader | Full ACID | Single-writer |
| **Best for** | Data analysis | Web apps, APIs | Mobile, embedded apps |

### DuckDB Architecture

```
SQL Query
    ↓
Parser → Optimizer → Planner
    ↓
Vectorized Execution Engine
    ↓
Columnar Memory Format (Arrow-based)
    ↓
Result (Arrow Table / Pandas DataFrame)
```

### Key Architectural Decisions

1. **Vectorized execution**: Processes data in batches (vectors), not row-by-row
2. **Columnar execution**: Works on columns, not rows
3. **Push-based execution**: Data flows from producers to consumers
4. **Morsel-driven parallelism**: Work split into small units (morsels)
5. **Cache-conscious algorithms**: Optimized for CPU cache

### DuckDB Extensions

DuckDB has a rich extension ecosystem:

| Extension | Purpose |
|-----------|---------|
| **parquet** | Read/write Parquet files |
| **json** | Read/write JSON files |
| **httpfs** | Read files from S3, GCS, HTTP |
| **spatial** | Geospatial operations |
| **fts** | Full-text search |
| **excel** | Read/write Excel files |
| **iceberg** | Read Iceberg tables |
| **delta** | Read Delta Lake tables |
| **motherduck** | Cloud DuckDB |

### DuckDB Use Cases

```
1. Ad-hoc Analysis
   Analyst → DuckDB → Parquet files → Insights

2. Data Pipeline
   Python ETL → DuckDB (SQL transforms) → Parquet output

3. Notebooks
   Jupyter → DuckDB → Query data → Visualize

4. Microservices
   FastAPI → DuckDB → Analytics API

5. Embedded Analytics
   Desktop App → DuckDB → Local data analysis

6. Data Validation
   ETL Job → DuckDB → Validate data quality

7. Reporting
   Scheduler → DuckDB → Generate reports
```

---

## 2. Example

### DuckDB Basic Operations

```python
import duckdb
import pandas as pd
import numpy as np

# Create in-memory database
con = duckdb.connect()

# Create table from Python data
data = pd.DataFrame({
    "customer_id": range(1, 1001),
    "name": [f"Customer {i}" for i in range(1, 1001)],
    "balance": np.random.uniform(1000, 100000, 1000).round(2),
    "segment": np.random.choice(["PREMIUM", "STANDARD", "BASIC"], 1000),
})

# Register as view (virtual table)
con.register("customers", data)

# Run SQL queries
result = con.execute("""
    SELECT 
        segment,
        COUNT(*) as customer_count,
        AVG(balance) as avg_balance,
        SUM(balance) as total_balance
    FROM customers
    GROUP BY segment
    ORDER BY total_balance DESC
""").fetchdf()

print(result)

# Query Parquet files directly
# con.execute("SELECT * FROM read_parquet('transactions.parquet')")

# Query CSV files directly
# con.execute("SELECT * FROM read_csv('data.csv')")

# Close connection
con.close()
```

---

## 3. Banking Scenario 1: Ad-Hoc Analytics

### Problem
A bank's analytics team needs to answer urgent business questions:
- "What's the total loan portfolio by branch?"
- "Which customers have the highest transaction volume?"
- "What's the monthly revenue trend?"

Data is stored in Parquet files. Analysts need fast SQL access without setting up a database server.

### Why DuckDB?
- No server setup required
- SQL interface familiar to analysts
- Direct Parquet file access
- Results in seconds

### Architecture
```
Analyst (Jupyter / Python)
       |
       v
  DuckDB (embedded, SQL)
       |
       v
  Parquet Files (S3 / Local)
       |
       v
  Business Insights
```

---

## 4. Python Code - Scenario 1

```python
import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
from datetime import datetime, timedelta
import random
import os
import tempfile
import time

# ============================================================
# BANKING SCENARIO: Ad-Hoc Analytics with DuckDB
# ============================================================

def generate_banking_data():
    """Generate realistic banking data for analytics."""
    random.seed(42)
    np.random.seed(42)

    # Transactions
    num_transactions = 100_000
    transactions = pd.DataFrame({
        "transaction_id": range(1, num_transactions + 1),
        "account_id": [f"ACC{random.randint(100000, 999999)}" for _ in range(num_transactions)],
        "customer_id": [f"CUST{random.randint(1, 10000):05d}" for _ in range(num_transactions)],
        "branch_id": [f"BR{random.randint(1, 100):03d}" for _ in range(num_transactions)],
        "amount": np.random.lognormal(6, 2, num_transactions).round(2),
        "currency": np.random.choice(["USD", "EUR", "GBP"], num_transactions),
        "status": np.random.choice(["COMPLETED", "PENDING", "FAILED"], num_transactions, p=[0.90, 0.07, 0.03]),
        "channel": np.random.choice(["ONLINE", "MOBILE", "BRANCH", "ATM", "POS"], num_transactions),
        "date": pd.date_range("2026-01-01", periods=num_transactions, freq="1min"),
    })

    # Customers
    num_customers = 10_000
    customers = pd.DataFrame({
        "customer_id": [f"CUST{i:05d}" for i in range(1, num_customers + 1)],
        "name": [f"Customer {i}" for i in range(1, num_customers + 1)],
        "segment": np.random.choice(["PREMIUM", "STANDARD", "BASIC"], num_customers, p=[0.2, 0.5, 0.3]),
        "age": np.random.randint(18, 80, num_customers),
        "balance": np.random.lognormal(10, 2, num_customers).round(2),
        "join_date": pd.date_range("2020-01-01", periods=num_customers, freq="1h"),
    })

    # Branches
    branches = pd.DataFrame({
        "branch_id": [f"BR{i:03d}" for i in range(1, 101)],
        "branch_name": [f"Branch {i}" for i in range(1, 101)],
        "region": np.random.choice(["NORTH", "SOUTH", "EAST", "WEST"], 100),
        "manager": [f"Manager {i}" for i in range(1, 101)],
    })

    return transactions, customers, branches


def store_as_parquet(transactions, customers, branches, base_path):
    """Store data as Parquet files."""
    pq.write_table(pq.Table.from_pandas(transactions), os.path.join(base_path, "transactions.parquet"), compression="zstd")
    pq.write_table(pq.Table.from_pandas(customers), os.path.join(base_path, "customers.parquet"), compression="zstd")
    pq.write_table(pq.Table.from_pandas(branches), os.path.join(base_path, "branches.parquet"), compression="zstd")


def run_analytics_queries(base_path):
    """Run ad-hoc analytics queries with DuckDB."""
    con = duckdb.connect()

    # 1. Transaction summary by status
    print("\n=== Transaction Status Summary ===")
    result = con.execute(f"""
        SELECT 
            status,
            COUNT(*) as count,
            SUM(amount) as total_amount,
            AVG(amount) as avg_amount
        FROM read_parquet('{base_path}/transactions.parquet')
        GROUP BY status
        ORDER BY total_amount DESC
    """).fetchdf()
    print(result.to_string(index=False))

    # 2. Top customers by transaction volume
    print("\n=== Top 10 Customers by Volume ===")
    result = con.execute(f"""
        SELECT 
            customer_id,
            COUNT(*) as tx_count,
            SUM(amount) as total_volume,
            AVG(amount) as avg_amount
        FROM read_parquet('{base_path}/transactions.parquet')
        WHERE status = 'COMPLETED'
        GROUP BY customer_id
        ORDER BY total_volume DESC
        LIMIT 10
    """).fetchdf()
    print(result.to_string(index=False))

    # 3. Regional performance (JOIN)
    print("\n=== Regional Performance ===")
    result = con.execute(f"""
        SELECT 
            b.region,
            COUNT(*) as tx_count,
            SUM(t.amount) as total_volume,
            COUNT(DISTINCT t.customer_id) as unique_customers
        FROM read_parquet('{base_path}/transactions.parquet') t
        JOIN read_parquet('{base_path}/branches.parquet') b
            ON t.branch_id = b.branch_id
        WHERE t.status = 'COMPLETED'
        GROUP BY b.region
        ORDER BY total_volume DESC
    """).fetchdf()
    print(result.to_string(index=False))

    # 4. Customer segment analysis (JOIN)
    print("\n=== Customer Segment Analysis ===")
    result = con.execute(f"""
        SELECT 
            c.segment,
            COUNT(DISTINCT c.customer_id) as customer_count,
            SUM(t.amount) as total_volume,
            AVG(c.balance) as avg_balance
        FROM read_parquet('{base_path}/customers.parquet') c
        JOIN read_parquet('{base_path}/transactions.parquet') t
            ON c.customer_id = t.customer_id
        WHERE t.status = 'COMPLETED'
        GROUP BY c.segment
        ORDER BY total_volume DESC
    """).fetchdf()
    print(result.to_string(index=False))

    # 5. Monthly trend
    print("\n=== Monthly Transaction Trend ===")
    result = con.execute(f"""
        SELECT 
            DATE_TRUNC('month', date) as month,
            COUNT(*) as tx_count,
            SUM(amount) as total_volume
        FROM read_parquet('{base_path}/transactions.parquet')
        WHERE status = 'COMPLETED'
        GROUP BY DATE_TRUNC('month', date)
        ORDER BY month
    """).fetchdf()
    print(result.to_string(index=False))

    con.close()


# ============================================================
# RUN THE SCENARIO
# ============================================================
if __name__ == "__main__":
    base_path = os.path.join(tempfile.gettempdir(), "duckdb_analytics")
    os.makedirs(base_path, exist_ok=True)

    # Generate and store data
    print("Generating banking data...")
    transactions, customers, branches = generate_banking_data()
    store_as_parquet(transactions, customers, branches, base_path)

    # Run analytics
    run_analytics_queries(base_path)
```

---

## 5. Banking Scenario 2: Embedded Analytics

### Problem
A bank wants to embed analytics into a Python application:
- Customer-facing dashboard (shows transaction history)
- Risk scoring (real-time credit assessment)
- Fraud detection (pattern matching on transactions)

Requirements:
- No external database server
- Fast query response (< 100ms)
- Easy integration with Python

### Why DuckDB?
- Embedded (no server)
- SQL interface for complex queries
- Vectorized execution for speed
- Works with Parquet files directly

### Architecture
```
Python Application
       |
       v
  DuckDB (embedded, in-memory)
       |
       +-- Query customer data
       +-- Compute risk scores
       +-- Detect fraud patterns
       |
       v
  Results (DataFrame)
       |
       v
  API Response / Dashboard
```

---

## 6. Python Code - Scenario 2

```python
import duckdb
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os
import tempfile
import time

# ============================================================
# BANKING SCENARIO: Embedded Analytics
# ============================================================

class BankingAnalytics:
    """Embedded analytics engine using DuckDB."""

    def __init__(self, data_path=None):
        self.con = duckdb.connect()
        self.data_path = data_path

    def load_data(self, transactions_df, customers_df):
        """Load data into DuckDB."""
        self.con.register("transactions", transactions_df)
        self.con.register("customers", customers_df)

    def get_customer_summary(self, customer_id):
        """Get transaction summary for a customer."""
        result = self.con.execute(f"""
            SELECT 
                customer_id,
                COUNT(*) as total_transactions,
                SUM(CASE WHEN status = 'COMPLETED' THEN amount ELSE 0 END) as total_amount,
                AVG(CASE WHEN status = 'COMPLETED' THEN amount END) as avg_amount,
                MIN(date) as first_transaction,
                MAX(date) as last_transaction
            FROM transactions
            WHERE customer_id = '{customer_id}'
            GROUP BY customer_id
        """).fetchdf()
        return result

    def get_monthly_trend(self, customer_id=None):
        """Get monthly transaction trend."""
        where_clause = f"WHERE customer_id = '{customer_id}'" if customer_id else ""
        result = self.con.execute(f"""
            SELECT 
                DATE_TRUNC('month', date) as month,
                COUNT(*) as tx_count,
                SUM(amount) as total_amount
            FROM transactions
            {where_clause}
            GROUP BY DATE_TRUNC('month', date)
            ORDER BY month
        """).fetchdf()
        return result

    def compute_risk_score(self, customer_id):
        """Compute risk score for a customer."""
        result = self.con.execute(f"""
            WITH stats AS (
                SELECT 
                    COUNT(*) as tx_count,
                    AVG(amount) as avg_amount,
                    STDDEV(amount) as std_amount,
                    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed_count
                FROM transactions
                WHERE customer_id = '{customer_id}'
            )
            SELECT 
                customer_id,
                tx_count,
                avg_amount,
                std_amount,
                failed_count,
                CASE 
                    WHEN tx_count = 0 THEN 0
                    ELSE ROUND((failed_count::FLOAT / tx_count) * 100, 2)
                END as failure_rate_pct,
                CASE 
                    WHEN avg_amount > 10000 THEN 'HIGH'
                    WHEN avg_amount > 1000 THEN 'MEDIUM'
                    ELSE 'LOW'
                END as risk_level
            FROM stats
        """).fetchdf()
        return result

    def detect_fraud_patterns(self, lookback_hours=24):
        """Detect potential fraud patterns."""
        result = self.con.execute(f"""
            WITH hourly_stats AS (
                SELECT 
                    customer_id,
                    DATE_TRUNC('hour', date) as hour,
                    COUNT(*) as tx_count,
                    SUM(amount) as total_amount
                FROM transactions
                WHERE date >= CURRENT_TIMESTAMP - INTERVAL '{lookback_hours} hours'
                GROUP BY customer_id, DATE_TRUNC('hour', date)
            )
            SELECT 
                customer_id,
                hour,
                tx_count,
                total_amount,
                CASE 
                    WHEN tx_count > 50 THEN 'VELOCITY_ALERT'
                    WHEN total_amount > 100000 THEN 'AMOUNT_ALERT'
                    ELSE 'NORMAL'
                END as alert_type
            FROM hourly_stats
            WHERE tx_count > 50 OR total_amount > 100000
            ORDER BY total_amount DESC
        """).fetchdf()
        return result

    def get_branch_performance(self):
        """Get branch performance metrics."""
        result = self.con.execute("""
            SELECT 
                branch_id,
                COUNT(*) as tx_count,
                SUM(amount) as total_volume,
                AVG(amount) as avg_amount,
                SUM(CASE WHEN status = 'COMPLETED' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as success_rate
            FROM transactions
            GROUP BY branch_id
            ORDER BY total_volume DESC
        """).fetchdf()
        return result

    def close(self):
        """Close DuckDB connection."""
        self.con.close()


def generate_sample_data():
    """Generate sample transaction and customer data."""
    random.seed(42)
    np.random.seed(42)

    num_transactions = 50_000
    num_customers = 1_000

    transactions = pd.DataFrame({
        "transaction_id": range(1, num_transactions + 1),
        "account_id": [f"ACC{random.randint(100000, 999999)}" for _ in range(num_transactions)],
        "customer_id": [f"CUST{random.randint(1, num_customers):05d}" for _ in range(num_transactions)],
        "branch_id": [f"BR{random.randint(1, 50):03d}" for _ in range(num_transactions)],
        "amount": np.random.lognormal(6, 2, num_transactions).round(2),
        "status": np.random.choice(["COMPLETED", "PENDING", "FAILED"], num_transactions, p=[0.90, 0.07, 0.03]),
        "channel": np.random.choice(["ONLINE", "MOBILE", "BRANCH", "ATM"], num_transactions),
        "date": pd.date_range("2026-01-01", periods=num_transactions, freq="30s"),
    })

    customers = pd.DataFrame({
        "customer_id": [f"CUST{i:05d}" for i in range(1, num_customers + 1)],
        "name": [f"Customer {i}" for i in range(1, num_customers + 1)],
        "segment": np.random.choice(["PREMIUM", "STANDARD", "BASIC"], num_customers),
        "balance": np.random.lognormal(10, 2, num_customers).round(2),
    })

    return transactions, customers


def run_embedded_analytics():
    """Run embedded analytics demo."""
    # Generate data
    print("Generating sample data...")
    transactions, customers = generate_sample_data()

    # Initialize analytics engine
    analytics = BankingAnalytics()
    analytics.load_data(transactions, customers)

    # 1. Customer summary
    print("\n=== Customer Summary ===")
    summary = analytics.get_customer_summary("CUST00001")
    print(summary.to_string(index=False))

    # 2. Risk score
    print("\n=== Risk Score ===")
    risk = analytics.compute_risk_score("CUST00001")
    print(risk.to_string(index=False))

    # 3. Fraud patterns
    print("\n=== Fraud Patterns ===")
    fraud = analytics.detect_fraud_patterns(lookback_hours=24)
    print(fraud.head(10).to_string(index=False))

    # 4. Branch performance
    print("\n=== Branch Performance (Top 10) ===")
    branches = analytics.get_branch_performance()
    print(branches.head(10).to_string(index=False))

    analytics.close()


# ============================================================
# RUN THE SCENARIO
# ============================================================
if __name__ == "__main__":
    run_embedded_analytics()
```

---

## 7. Interview Questions

### Q1: What is DuckDB and how is it different from SQLite?

**Answer:**

| Feature | DuckDB | SQLite |
|---------|--------|--------|
| **Type** | Analytical (OLAP) | Transactional (OLTP) |
| **Query focus** | Aggregations, joins, analytics | CRUD, transactions |
| **Execution** | Vectorized, columnar | Row-based |
| **Storage** | Queries external files (Parquet, CSV) | Internal database file |
| **Best for** | Data analysis, reporting | Web apps, mobile apps |
| **Concurrency** | Single-writer, multi-reader | Single-writer |

**Key difference**: DuckDB is optimized for **analytical queries** (SUM, AVG, GROUP BY, window functions), while SQLite is optimized for **transactional queries** (INSERT, UPDATE, DELETE).

**Example**:
```python
# DuckDB: Query Parquet files directly
con.execute("SELECT SUM(amount) FROM read_parquet('data.parquet')")

# SQLite: Must import data first
con.execute("SELECT SUM(amount) FROM transactions")
```

---

### Q2: When would you use DuckDB vs Pandas?

**Answer:**

| Scenario | DuckDB | Pandas |
|----------|--------|--------|
| **SQL queries** | ✅ Full SQL | ❌ DataFrame API |
| **Large data (> RAM)** | ✅ Streaming | ❌ Loads into memory |
| **Complex joins** | ✅ Optimized | ⚠️ Possible but slower |
| **Window functions** | ✅ Full SQL | ⚠️ Limited |
| **Data manipulation** | ⚠️ SQL only | ✅ Full API |
| **Quick prototyping** | ✅ SQL | ✅ DataFrame |

**Use DuckDB when:**
- Running SQL queries on Parquet/CSV files
- Data is larger than memory
- Complex joins and aggregations
- Need window functions

**Use Pandas when:**
- Data manipulation and transformation
- Data fits in memory
- Quick exploration
- Integration with ML libraries

---

### Q3: How does DuckDB achieve high performance?

**Answer:**

1. **Vectorized execution**: Processes data in batches (vectors), not row-by-row
2. **Columnar execution**: Works on columns, enabling SIMD operations
3. **Push-based execution**: Data flows from producers to consumers
4. **Morsel-driven parallelism**: Work split into small units
5. **Cache-conscious algorithms**: Optimized for CPU cache
6. **Late materialization**: Delays type conversions

**Example**:
```python
# DuckDB processes this efficiently:
SELECT status, SUM(amount)
FROM transactions
GROUP BY status

# Because:
# 1. Reads only 'status' and 'amount' columns
# 2. Processes in vectorized batches
# 3. Uses SIMD for aggregations
```

---

### Q4: What are DuckDB's limitations?

**Answer:**

1. **Single-node**: Runs on one machine (not distributed)
2. **Single-writer**: Only one process can write at a time
3. **No ACID on external files**: Can't do transactions on Parquet files
4. **Memory constraints**: Large datasets may require streaming
5. **No built-in server**: Must embed in application

**When to use alternatives:**
- **Spark**: Distributed processing (> 1 TB)
- **PostgreSQL**: Transactional workloads
- **Trino**: Multi-user concurrent queries
- **Motherduck**: Cloud-based DuckDB

---

### Q5: How do you install and use DuckDB in Python?

**Answer:**

**Installation:**
```bash
pip install duckdb
```

**Basic usage:**
```python
import duckdb

# In-memory database
con = duckdb.connect()

# Query Parquet files
result = con.execute("""
    SELECT status, SUM(amount)
    FROM read_parquet('transactions.parquet')
    GROUP BY status
""").fetchdf()

# Query CSV files
result = con.execute("""
    SELECT * FROM read_csv('data.csv')
    WHERE amount > 1000
""").fetchdf()

# Create table from DataFrame
import pandas as pd
df = pd.DataFrame({"id": [1, 2], "amount": [100, 200]})
con.register("my_table", df)
result = con.execute("SELECT * FROM my_table").fetchdf()

# Close connection
con.close()
```
