# DuckDB Python API

## Table of Contents
1. [Detailed Explanation](#1-detailed-explanation)
2. [Example](#2-example)
3. [Real-World Banking Scenario 1](#3-banking-scenario-1-data-pipeline)
4. [Python Code - Scenario 1](#4-python-code---scenario-1)
5. [Real-World Banking Scenario 2](#5-banking-scenario-2-api-service)
6. [Python Code - Scenario 2](#6-python-code---scenario-2)
7. [Interview Questions](#7-interview-questions)

---

## 1. Detailed Explanation

### DuckDB Python API Overview

The DuckDB Python API provides seamless integration with Python data science libraries:

> **DuckDB's Python API lets you query Pandas DataFrames, Polars DataFrames, and files (Parquet, CSV, JSON) using SQL — all without leaving Python.**

### Core API Components

#### 1. Connection

```python
import duckdb

# In-memory database (default)
con = duckdb.connect()

# File-based database
con = duckdb.connect('database.duckdb')

# Read-only connection
con = duckdb.connect('database.duckdb', read_only=True)
```

#### 2. Query Execution

```python
# Execute and fetch all results
result = con.execute("SELECT * FROM table").fetchdf()

# Execute and fetch one row
result = con.execute("SELECT * FROM table").fetchone()

# Execute and fetch many rows
result = con.execute("SELECT * FROM table").fetchmany(100)

# Execute without fetching (for DDL/DML)
con.execute("CREATE TABLE test (id INT, name VARCHAR)")
```

#### 3. Registering DataFrames

```python
import pandas as pd

df = pd.DataFrame({"id": [1, 2], "name": ["A", "B"]})

# Register as view (virtual table)
con.register("my_view", df)

# Now query with SQL
result = con.execute("SELECT * FROM my_view WHERE id > 1").fetchdf()
```

#### 4. Querying Files Directly

```python
# Parquet
result = con.execute("SELECT * FROM read_parquet('file.parquet')").fetchdf()

# CSV
result = con.execute("SELECT * FROM read_csv('file.csv')").fetchdf()

# JSON
result = con.execute("SELECT * FROM read_json('file.json')").fetchdf()

# Multiple files
result = con.execute("SELECT * FROM read_parquet('data/*.parquet')").fetchdf()
```

#### 5. Parameterized Queries

```python
# Using parameters (safe from SQL injection)
result = con.execute(
    "SELECT * FROM transactions WHERE amount > ? AND status = ?",
    [1000, "COMPLETED"]
).fetchdf()

# Named parameters
result = con.execute(
    "SELECT * FROM transactions WHERE amount > :amount AND status = :status",
    {"amount": 1000, "status": "COMPLETED"}
).fetchdf()
```

#### 6. Transaction Management

```python
# Manual transaction
con.execute("BEGIN")
try:
    con.execute("INSERT INTO table VALUES (1, 'A')")
    con.execute("UPDATE table SET name = 'B' WHERE id = 1")
    con.execute("COMMIT")
except:
    con.execute("ROLLBACK")
```

#### 7. Configuration

```python
# Set memory limit
con.execute("SET memory_limit='4GB'")

# Set thread count
con.execute("SET threads=4")

# Enable progress bar
con.execute("SET enable_progress_bar=true")

# Enable file system access
con.execute("SET enable_external_access=true")
```

### Integration with Data Science Libraries

#### Pandas Integration

```python
import pandas as pd
import duckdb

df = pd.read_csv("data.csv")
con = duckdb.connect()

# Query Pandas DataFrame
result = con.execute("""
    SELECT status, SUM(amount)
    FROM df
    GROUP BY status
""").fetchdf()

# Convert back to Pandas
result_df = result  # Already a DataFrame!
```

#### Polars Integration

```python
import polars as pl
import duckdb

df = pl.read_parquet("data.parquet")
con = duckdb.connect()

# Query Polars DataFrame
result = con.execute("""
    SELECT status, SUM(amount)
    FROM df
    GROUP BY status
""").pl()  # Returns Polars DataFrame
```

#### PyArrow Integration

```python
import pyarrow as pa
import duckdb

table = pa.read_parquet("data.parquet")
con = duckdb.connect()

# Query Arrow Table
result = con.execute("""
    SELECT status, SUM(amount)
    FROM table
    GROUP BY status
""").arrow()  # Returns Arrow Table
```

### Best Practices

```python
# 1. Use parameterized queries (prevent SQL injection)
con.execute("SELECT * FROM users WHERE id = ?", [user_id])

# 2. Close connections when done
con.close()

# 3. Use context manager
with duckdb.connect() as con:
    result = con.execute("SELECT 1").fetchdf()

# 4. Set appropriate memory limits
con.execute("SET memory_limit='2GB'")

# 5. Use read_only for concurrent reads
con = duckdb.connect('db.duckdb', read_only=True)
```

---

## 2. Example

### DuckDB Python API Demo

```python
import duckdb
import pandas as pd
import numpy as np
import os
import tempfile

# 1. Create connection
con = duckdb.connect()

# 2. Create table from Python data
data = pd.DataFrame({
    "id": range(1, 1001),
    "name": [f"Customer {i}" for i in range(1, 1001)],
    "amount": np.random.uniform(100, 10000, 1000).round(2),
    "status": np.random.choice(["ACTIVE", "INACTIVE"], 1000),
})
con.register("customers", data)

# 3. Query with SQL
result = con.execute("""
    SELECT status, COUNT(*), AVG(amount)
    FROM customers
    GROUP BY status
""").fetchdf()
print(result)

# 4. Parameterized query
result = con.execute(
    "SELECT * FROM customers WHERE amount > ? AND status = ?",
    [5000, "ACTIVE"]
).fetchdf()
print(f"\nHigh-value active customers: {len(result)}")

# 5. Create table with DDL
con.execute("""
    CREATE TABLE transactions (
        id INTEGER PRIMARY KEY,
        customer_id INTEGER,
        amount DECIMAL(18,2),
        date DATE
    )
""")

# 6. Insert data
con.execute("""
    INSERT INTO transactions VALUES 
    (1, 1, 100.00, '2026-08-24'),
    (2, 1, 250.00, '2026-08-24'),
    (3, 2, 500.00, '2026-08-24')
""")

# 7. Query with JOIN
result = con.execute("""
    SELECT c.name, SUM(t.amount) as total
    FROM customers c
    JOIN transactions t ON c.id = t.customer_id
    GROUP BY c.name
""").fetchdf()
print(result)

con.close()
```

---

## 3. Banking Scenario 1: Data Pipeline

### Problem
A bank needs a Python data pipeline that:
- Reads data from multiple sources (CSV, Parquet, APIs)
- Transforms data using SQL
- Loads into data lake (Parquet)
- Validates data quality

### Why DuckDB Python API?
- Seamless Pandas integration
- SQL transforms in Python
- Direct Parquet file access
- No external database required

### Architecture
```
Data Sources (CSV, API, Database)
       |
       v
  Python (Pandas)
       |
       v
  DuckDB (SQL transforms)
       |
       v
  Parquet Files (Data Lake)
```

---

## 4. Python Code - Scenario 1

```python
import duckdb
import pandas as pd
import numpy as np
from datetime import datetime
import os
import tempfile
import pyarrow.parquet as pq

# ============================================================
# BANKING SCENARIO: Data Pipeline with DuckDB Python API
# ============================================================

class BankingDataPipeline:
    """Data pipeline using DuckDB Python API."""

    def __init__(self, output_path):
        self.output_path = output_path
        self.con = duckdb.connect()
        self.con.execute("SET memory_limit='2GB'")

    def extract_from_csv(self, csv_path, table_name):
        """Extract data from CSV file."""
        self.con.execute(f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT * FROM read_csv('{csv_path}')
        """)
        print(f"Extracted {self.con.execute(f'SELECT COUNT(*) FROM {table_name}').fetchone()[0]} rows from {csv_path}")

    def extract_from_dataframe(self, df, table_name):
        """Extract data from Pandas DataFrame."""
        self.con.register(table_name, df)
        print(f"Registered DataFrame as {table_name}")

    def transform_customers(self):
        """Transform customer data."""
        self.con.execute("""
            CREATE OR REPLACE TABLE customers_transformed AS
            WITH 
            customer_stats AS (
                SELECT 
                    customer_id,
                    COUNT(*) as tx_count,
                    SUM(amount) as total_amount,
                    AVG(amount) as avg_amount
                FROM transactions
                GROUP BY customer_id
            )
            SELECT 
                c.*,
                cs.tx_count,
                cs.total_amount,
                cs.avg_amount,
                CASE 
                    WHEN cs.total_amount > 100000 THEN 'PLATINUM'
                    WHEN cs.total_amount > 50000 THEN 'GOLD'
                    WHEN cs.total_amount > 10000 THEN 'SILVER'
                    ELSE 'BRONZE'
                END as tier
            FROM customers c
            LEFT JOIN customer_stats cs ON c.customer_id = cs.customer_id
        """)
        print("Transformed customers with tier assignment")

    def transform_transactions(self):
        """Transform transaction data."""
        self.con.execute("""
            CREATE OR REPLACE TABLE transactions_transformed AS
            SELECT 
                *,
                DATE_TRUNC('day', date) as transaction_date,
                EXTRACT(HOUR FROM date) as transaction_hour,
                CASE 
                    WHEN EXTRACT(HOUR FROM date) BETWEEN 9 AND 17 THEN 'BUSINESS_HOURS'
                    ELSE 'AFTER_HOURS'
                END as time_category
            FROM transactions
        """)
        print("Transformed transactions with time categories")

    def validate_data(self, table_name):
        """Validate data quality."""
        # Check for nulls
        null_counts = self.con.execute(f"""
            SELECT 
                column_name,
                null_count
            FROM (
                SELECT 
                    COUNT(*) - COUNT(customer_id) as customer_id_nulls,
                    COUNT(*) - COUNT(amount) as amount_nulls
                FROM {table_name}
            )
            UNPIVOT (
                null_count
                FOR column_name IN (customer_id_nulls, amount_nulls)
            )
        """).fetchdf()

        print(f"\nData Quality Check for {table_name}:")
        print(null_counts.to_string(index=False))

        # Check row count
        count = self.con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"Total rows: {count}")

        return count > 0

    def load_to_parquet(self, table_name, filename):
        """Load data to Parquet file."""
        output_file = os.path.join(self.output_path, filename)
        self.con.execute(f"""
            COPY (SELECT * FROM {table_name}) TO '{output_file}' 
            (FORMAT PARQUET, COMPRESSION 'ZSTD')
        """)
        size = os.path.getsize(output_file)
        print(f"Loaded {table_name} to {output_file} ({size / 1024:.1f} KB)")

    def run_pipeline(self, customers_df, transactions_df):
        """Run the complete pipeline."""
        print("=== Starting Data Pipeline ===\n")

        # Extract
        print("--- Extract Phase ---")
        self.extract_from_dataframe(customers_df, "customers")
        self.extract_from_dataframe(transactions_df, "transactions")

        # Transform
        print("\n--- Transform Phase ---")
        self.transform_customers()
        self.transform_transactions()

        # Validate
        print("\n--- Validate Phase ---")
        self.validate_data("customers_transformed")
        self.validate_data("transactions_transformed")

        # Load
        print("\n--- Load Phase ---")
        self.load_to_parquet("customers_transformed", "customers.parquet")
        self.load_to_parquet("transactions_transformed", "transactions.parquet")

        print("\n=== Pipeline Complete ===")

    def close(self):
        """Close DuckDB connection."""
        self.con.close()


def generate_sample_data():
    """Generate sample banking data."""
    random.seed(42)
    np.random.seed(42)
    import random

    num_customers = 1000
    num_transactions = 50_000

    customers = pd.DataFrame({
        "customer_id": [f"CUST{i:05d}" for i in range(1, num_customers + 1)],
        "name": [f"Customer {i}" for i in range(1, num_customers + 1)],
        "email": [f"customer{i}@bank.com" for i in range(1, num_customers + 1)],
        "join_date": pd.date_range("2020-01-01", periods=num_customers, freq="1h"),
    })

    transactions = pd.DataFrame({
        "transaction_id": range(1, num_transactions + 1),
        "customer_id": [f"CUST{random.randint(1, num_customers):05d}" for _ in range(num_transactions)],
        "amount": np.random.lognormal(6, 2, num_transactions).round(2),
        "status": np.random.choice(["COMPLETED", "PENDING", "FAILED"], num_transactions),
        "date": pd.date_range("2026-01-01", periods=num_transactions, freq="30s"),
    })

    return customers, transactions


# ============================================================
# RUN THE SCENARIO
# ============================================================
if __name__ == "__main__":
    # Generate data
    print("Generating sample data...")
    customers, transactions = generate_sample_data()

    # Create pipeline
    output_path = os.path.join(tempfile.gettempdir(), "banking_pipeline")
    os.makedirs(output_path, exist_ok=True)

    pipeline = BankingDataPipeline(output_path)
    pipeline.run_pipeline(customers, transactions)
    pipeline.close()
```

---

## 5. Banking Scenario 2: API Service

### Problem
A bank wants to build a Python API service that:
- Serves analytics queries via REST API
- Reads from Parquet files
- Returns JSON responses
- Handles concurrent requests

### Why DuckDB Python API?
- Embedded (no separate database server)
- Fast query execution
- Works with Parquet files
- Easy integration with FastAPI

### Architecture
```
Client Request (REST API)
       |
       v
  FastAPI (Python)
       |
       v
  DuckDB (embedded, SQL)
       |
       v
  Parquet Files (S3 / Local)
       |
       v
  JSON Response
```

---

## 6. Python Code - Scenario 2

```python
import duckdb
import pandas as pd
import numpy as np
from datetime import datetime
import os
import tempfile
import pyarrow.parquet as pq
import time
from concurrent.futures import ThreadPoolExecutor

# ============================================================
# BANKING SCENARIO: Analytics API Service
# ============================================================

class AnalyticsAPI:
    """Analytics API service using DuckDB."""

    def __init__(self, data_path):
        self.data_path = data_path
        self._setup_data()

    def _setup_data(self):
        """Setup sample data for API."""
        # Generate sample data
        np.random.seed(42)
        num_transactions = 100_000

        transactions = pd.DataFrame({
            "transaction_id": range(1, num_transactions + 1),
            "account_id": [f"ACC{np.random.randint(100000, 999999)}" for _ in range(num_transactions)],
            "amount": np.random.lognormal(6, 2, num_transactions).round(2),
            "status": np.random.choice(["COMPLETED", "PENDING", "FAILED"], num_transactions),
            "channel": np.random.choice(["ONLINE", "MOBILE", "BRANCH", "ATM"], num_transactions),
            "date": pd.date_range("2026-01-01", periods=num_transactions, freq="30s"),
        })

        # Save as Parquet
        pq.write_table(
            pq.Table.from_pandas(transactions),
            os.path.join(self.data_path, "transactions.parquet"),
            compression="zstd"
        )

    def _get_connection(self):
        """Get DuckDB connection (thread-safe)."""
        return duckdb.connect()

    def get_transaction_summary(self, start_date=None, end_date=None):
        """Get transaction summary."""
        con = self._get_connection()
        try:
            # Build query with optional filters
            where_clause = ""
            params = []
            
            if start_date:
                where_clause += " AND date >= ?"
                params.append(start_date)
            if end_date:
                where_clause += " AND date <= ?"
                params.append(end_date)

            query = f"""
                SELECT 
                    status,
                    COUNT(*) as tx_count,
                    SUM(amount) as total_amount,
                    AVG(amount) as avg_amount
                FROM read_parquet('{self.data_path}/transactions.parquet')
                WHERE 1=1 {where_clause}
                GROUP BY status
                ORDER BY total_amount DESC
            """

            result = con.execute(query, params).fetchdf()
            return result.to_dict(orient='records')
        finally:
            con.close()

    def get_channel_analysis(self):
        """Get channel analysis."""
        con = self._get_connection()
        try:
            result = con.execute(f"""
                SELECT 
                    channel,
                    COUNT(*) as tx_count,
                    SUM(amount) as total_amount,
                    AVG(amount) as avg_amount,
                    SUM(CASE WHEN status = 'COMPLETED' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as success_rate
                FROM read_parquet('{self.data_path}/transactions.parquet')
                GROUP BY channel
                ORDER BY total_amount DESC
            """).fetchdf()
            return result.to_dict(orient='records')
        finally:
            con.close()

    def get_hourly_trend(self, days=7):
        """Get hourly transaction trend."""
        con = self._get_connection()
        try:
            result = con.execute(f"""
                SELECT 
                    DATE_TRUNC('hour', date) as hour,
                    COUNT(*) as tx_count,
                    SUM(amount) as total_amount
                FROM read_parquet('{self.data_path}/transactions.parquet')
                WHERE date >= CURRENT_DATE - INTERVAL '{days} days'
                GROUP BY 1
                ORDER BY 1
            """).fetchdf()
            return result.to_dict(orient='records')
        finally:
            con.close()

    def get_top_accounts(self, limit=10):
        """Get top accounts by volume."""
        con = self._get_connection()
        try:
            result = con.execute(f"""
                SELECT 
                    account_id,
                    COUNT(*) as tx_count,
                    SUM(amount) as total_volume,
                    AVG(amount) as avg_amount
                FROM read_parquet('{self.data_path}/transactions.parquet')
                WHERE status = 'COMPLETED'
                GROUP BY account_id
                ORDER BY total_volume DESC
                LIMIT {limit}
            """).fetchdf()
            return result.to_dict(orient='records')
        finally:
            con.close()


def simulate_api_calls(api, num_calls=10):
    """Simulate concurrent API calls."""
    print(f"\n=== Simulating {num_calls} API Calls ===")

    def call_api(args):
        endpoint, func = args
        start = time.time()
        result = func()
        elapsed = time.time() - start
        return endpoint, elapsed, len(result)

    endpoints = [
        ("Transaction Summary", api.get_transaction_summary),
        ("Channel Analysis", api.get_channel_analysis),
        ("Hourly Trend", lambda: api.get_hourly_trend(7)),
        ("Top Accounts", lambda: api.get_top_accounts(10)),
    ]

    # Run concurrently
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(call_api, endpoints[i % len(endpoints)]) 
                  for i in range(num_calls)]
        
        results = []
        for future in futures:
            endpoint, elapsed, rows = future.result()
            results.append((endpoint, elapsed, rows))

    # Print results
    print(f"{'Endpoint':<25} {'Time (s)':<12} {'Rows':<10}")
    print("-" * 47)
    for endpoint, elapsed, rows in results:
        print(f"{endpoint:<25} {elapsed:<12.3f} {rows:<10}")

    avg_time = sum(r[1] for r in results) / len(results)
    print(f"\nAverage response time: {avg_time:.3f}s")


# ============================================================
# RUN THE SCENARIO
# ============================================================
if __name__ == "__main__":
    # Setup
    data_path = os.path.join(tempfile.gettempdir(), "analytics_api")
    os.makedirs(data_path, exist_ok=True)

    # Create API
    api = AnalyticsAPI(data_path)

    # Test endpoints
    print("=== Testing API Endpoints ===")
    
    print("\nTransaction Summary:")
    summary = api.get_transaction_summary()
    print(pd.DataFrame(summary).to_string(index=False))

    print("\nChannel Analysis:")
    channels = api.get_channel_analysis()
    print(pd.DataFrame(channels).to_string(index=False))

    # Simulate concurrent calls
    simulate_api_calls(api, num_calls=8)
```

---

## 7. Interview Questions

### Q1: How do you integrate DuckDB with Pandas?

**Answer:**

**Method 1: Register DataFrame**
```python
import duckdb
import pandas as pd

df = pd.DataFrame({"id": [1, 2], "amount": [100, 200]})
con = duckdb.connect()
con.register("my_table", df)

result = con.execute("SELECT * FROM my_table WHERE amount > 100").fetchdf()
```

**Method 2: Query directly**
```python
result = con.execute("SELECT * FROM df WHERE amount > 100").fetchdf()
```

**Method 3: Convert result to Pandas**
```python
result = con.execute("SELECT SUM(amount) FROM df").fetchdf()  # Already DataFrame
```

**Benefits:**
- SQL interface for complex queries
- Automatic optimization
- No data copying (zero-copy)
- Works with large DataFrames

---

### Q2: How do you handle parameterized queries in DuckDB?

**Answer:**

**Positional parameters:**
```python
con.execute(
    "SELECT * FROM users WHERE age > ? AND city = ?",
    [25, 'New York']
)
```

**Named parameters:**
```python
con.execute(
    "SELECT * FROM users WHERE age > :age AND city = :city",
    {"age": 25, "city": "New York"}
)
```

**Why use parameterized queries:**
1. **SQL injection prevention**: User input sanitized
2. **Performance**: Query plan cached
3. **Readability**: Clear parameter separation

**Example:**
```python
# Bad (SQL injection risk)
user_input = "1; DROP TABLE users;"
con.execute(f"SELECT * FROM users WHERE id = {user_input}")

# Good (safe)
con.execute("SELECT * FROM users WHERE id = ?", [user_input])
```

---

### Q3: What are the different ways to query data in DuckDB?

**Answer:**

1. **Query Pandas DataFrame:**
```python
con.execute("SELECT * FROM df WHERE amount > 1000")
```

2. **Query Parquet file:**
```python
con.execute("SELECT * FROM read_parquet('file.parquet')")
```

3. **Query CSV file:**
```python
con.execute("SELECT * FROM read_csv('file.csv')")
```

4. **Query JSON file:**
```python
con.execute("SELECT * FROM read_json('file.json')")
```

5. **Query multiple files:**
```python
con.execute("SELECT * FROM read_parquet('data/*.parquet')")
```

6. **Query registered views:**
```python
con.register("my_view", df)
con.execute("SELECT * FROM my_view")
```

---

### Q4: How do you manage DuckDB connections in a multi-threaded application?

**Answer:**

**Each thread should have its own connection:**
```python
import duckdb
from concurrent.futures import ThreadPoolExecutor

def query_data(thread_id):
    con = duckdb.connect()  # Each thread gets its own connection
    result = con.execute("SELECT * FROM data").fetchdf()
    con.close()
    return result

# Run concurrently
with ThreadPoolExecutor(max_workers=4) as executor:
    futures = [executor.submit(query_data, i) for i in range(4)]
    results = [f.result() for f in futures]
```

**Best practices:**
1. One connection per thread
2. Close connections when done
3. Use connection pooling for web applications
4. Set appropriate memory limits

---

### Q5: How do you export DuckDB results to different formats?

**Answer:**

**To Pandas:**
```python
result = con.execute("SELECT * FROM table").fetchdf()
```

**To Polars:**
```python
result = con.execute("SELECT * FROM table").pl()
```

**To Arrow:**
```python
result = con.execute("SELECT * FROM table").arrow()
```

**To Parquet:**
```python
con.execute("COPY (SELECT * FROM table) TO 'output.parquet' (FORMAT PARQUET)")
```

**To CSV:**
```python
con.execute("COPY (SELECT * FROM table) TO 'output.csv' (FORMAT CSV, HEADER true)")
```

**To JSON:**
```python
con.execute("COPY (SELECT * FROM table) TO 'output.json' (FORMAT JSON)")
```
