# DuckDB Extensions

## Table of Contents
1. [Detailed Explanation](#1-detailed-explanation)
2. [Example](#2-example)
3. [Real-World Banking Scenario 1](#3-banking-scenario-1-spatial-analytics)
4. [Python Code - Scenario 1](#4-python-code---scenario-1)
5. [Real-World Banking Scenario 2](#5-banking-scenario-2-json-processing)
6. [Python Code - Scenario 2](#6-python-code---scenario-2)
7. [Interview Questions](#7-interview-questions)

---

## 1. Detailed Explanation

### DuckDB Extensions

> **DuckDB has a rich extension ecosystem that adds functionality like spatial analysis, JSON processing, HTTP access, and more.**

### Core Extensions

#### 1. parquet (Built-in)
```sql
-- Read Parquet files
SELECT * FROM read_parquet('file.parquet')

-- Write Parquet files
COPY (SELECT * FROM table) TO 'output.parquet' (FORMAT PARQUET)
```

#### 2. json (Built-in)
```sql
-- Read JSON files
SELECT * FROM read_json('file.json')

-- Read JSON arrays
SELECT * FROM read_json('file.json', format='array')

-- Extract JSON fields
SELECT json_extract_string(data, '$.name') FROM table
```

#### 3. httpfs
```sql
-- Query S3 files
SELECT * FROM read_parquet('s3://bucket/data.parquet')

-- Query GCS files
SELECT * FROM read_parquet('gs://bucket/data.parquet')

-- Query HTTP files
SELECT * FROM read_csv('https://example.com/data.csv')
```

#### 4. spatial
```sql
-- Load spatial extension
INSTALL spatial;
LOAD spatial;

-- Create geometry column
SELECT ST_Point(longitude, latitude) as geom FROM table

-- Distance calculation
SELECT ST_Distance(geom1, geom2) FROM table

-- Spatial filtering
SELECT * FROM table WHERE ST_Contains(polygon, point)
```

#### 5. fts (Full-Text Search)
```sql
-- Load FTS extension
INSTALL fts;
LOAD fts;

-- Create full-text index
PRAGMA create_fts_index('table', 'id', 'text_column')

-- Search
SELECT * FROM fts_main_table.match('search term')
```

#### 6. excel
```sql
-- Load Excel extension
INSTALL excel;
LOAD excel;

-- Read Excel files
SELECT * FROM read_xlsx('file.xlsx')

-- Read specific sheet
SELECT * FROM read_xlsx('file.xlsx', sheet='Sheet1')
```

### Extension Management

```sql
-- Install extension
INSTALL extension_name

-- Load extension
LOAD extension_name

-- List installed extensions
SELECT * FROM duckdb_extensions()

-- List loaded extensions
SELECT * FROM duckdb_loaded_extensions()
```

### Community Extensions

| Extension | Purpose |
|-----------|---------|
| **iceberg** | Read Iceberg tables |
| **delta** | Read Delta Lake tables |
| **postgres** | Connect to PostgreSQL |
| **mysql** | Connect to MySQL |
| **sqlite** | Connect to SQLite |
| **spatial** | Geospatial operations |
| **fts** | Full-text search |
| **excel** | Read/write Excel |
| **httpfs** | HTTP/S3/GCS access |
| **json** | JSON processing |

---

## 2. Example

### Extensions Demo

```python
import duckdb

con = duckdb.connect()

# 1. JSON extension
print("=== JSON Extension ===")
result = con.execute("""
    SELECT * FROM read_json_auto('[{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]')
""").fetchdf()
print(result.to_string(index=False))

# 2. List extensions
print("\n=== Installed Extensions ===")
result = con.execute("SELECT extension_name, loaded, installed FROM duckdb_extensions() WHERE installed=true").fetchdf()
print(result.to_string(index=False))

# 3. String functions (built-in)
print("\n=== String Functions ===")
result = con.execute("""
    SELECT 
        UPPER('hello') as upper,
        LOWER('HELLO') as lower,
        LENGTH('hello') as length,
        CONCAT('hello', ' ', 'world') as concatenated
""").fetchdf()
print(result.to_string(index=False))

con.close()
```

---

## 3. Banking Scenario 1: Spatial Analytics

### Problem
A bank needs to analyze branch locations:
- Find nearest branch to customer
- Calculate distance between branches
- Identify coverage areas
- Optimize branch placement

### Why Spatial Extension?
- Geospatial calculations
- Distance computations
- Area calculations
- Spatial filtering

---

## 4. Python Code - Scenario 1

```python
import duckdb
import pandas as pd
import numpy as np

# ============================================================
# BANKING SCENARIO: Spatial Analytics
# ============================================================

def generate_branch_data():
    """Generate branch location data."""
    random = np.random.RandomState(42)

    branches = pd.DataFrame({
        "branch_id": [f"BR{i:03d}" for i in range(1, 51)],
        "branch_name": [f"Branch {i}" for i in range(1, 51)],
        "latitude": random.uniform(40.5, 41.0, 50),
        "longitude": random.uniform(-74.5, -73.5, 50),
        "region": random.choice(["MANHATTAN", "BROOKLYN", "QUEENS", "BRONX"], 50),
    })

    customers = pd.DataFrame({
        "customer_id": [f"CUST{i:05d}" for i in range(1, 1001)],
        "name": [f"Customer {i}" for i in range(1, 1001)],
        "latitude": random.uniform(40.5, 41.0, 1000),
        "longitude": random.uniform(-74.5, -73.5, 1000),
    })

    return branches, customers


def analyze_spatial_data(branches_df, customers_df):
    """Analyze spatial data using DuckDB."""
    con = duckdb.connect()
    con.register("branches", branches_df)
    con.register("customers", customers_df)

    # 1. Create geometry columns
    print("=== Branch Locations ===")
    result = con.execute("""
        SELECT 
            branch_id,
            branch_name,
            latitude,
            longitude,
            region
        FROM branches
        LIMIT 10
    """).fetchdf()
    print(result.to_string(index=False))

    # 2. Calculate distances (simplified)
    print("\n=== Distance Analysis ===")
    result = con.execute("""
        WITH 
        branch_center AS (
            SELECT AVG(latitude) as center_lat, AVG(longitude) as center_lon
            FROM branches
        )
        SELECT 
            b.branch_id,
            b.branch_name,
            b.latitude,
            b.longitude,
            SQRT(
                POWER(b.latitude - bc.center_lat, 2) + 
                POWER(b.longitude - bc.center_lon, 2)
            ) * 111 as approx_km_from_center
        FROM branches b
        CROSS JOIN branch_center bc
        ORDER BY approx_km_from_center
        LIMIT 10
    """).fetchdf()
    print(result.to_string(index=False))

    # 3. Regional analysis
    print("\n=== Regional Branch Count ===")
    result = con.execute("""
        SELECT 
            region,
            COUNT(*) as branch_count,
            AVG(latitude) as avg_lat,
            AVG(longitude) as avg_lon
        FROM branches
        GROUP BY region
    """).fetchdf()
    print(result.to_string(index=False))

    # 4. Customer distribution
    print("\n=== Customer Distribution by Region ===")
    result = con.execute("""
        SELECT 
            CASE 
                WHEN c.latitude > 40.75 THEN 'NORTH'
                ELSE 'SOUTH'
            END as area,
            COUNT(*) as customer_count
        FROM customers c
        GROUP BY 1
    """).fetchdf()
    print(result.to_string(index=False))

    con.close()


# ============================================================
# RUN THE SCENARIO
# ============================================================
if __name__ == "__main__":
    # Generate data
    print("Generating spatial data...")
    branches, customers = generate_branch_data()

    # Analyze
    analyze_spatial_data(branches, customers)
```

---

## 5. Banking Scenario 2: JSON Processing

### Problem
A bank needs to process JSON data:
- Parse API responses
- Extract nested fields
- Transform JSON to tables
- Validate JSON structure

### Why JSON Extension?
- Native JSON processing
- Extract nested fields
- Transform to relational data
- Handle semi-structured data

---

## 6. Python Code - Scenario 2

```python
import duckdb
import pandas as pd
import json

# ============================================================
# BANKING SCENARIO: JSON Processing
# ============================================================

def generate_json_data():
    """Generate sample JSON data."""
    data = [
        {
            "transaction_id": "TX001",
            "account_id": "ACC001",
            "amount": 1000.00,
            "status": "COMPLETED",
            "details": {
                "channel": "ONLINE",
                "merchant": "Amazon",
                "category": "Shopping"
            },
            "tags": ["electronics", "sale"]
        },
        {
            "transaction_id": "TX002",
            "account_id": "ACC002",
            "amount": 2500.00,
            "status": "PENDING",
            "details": {
                "channel": "MOBILE",
                "merchant": "Starbucks",
                "category": "Food"
            },
            "tags": ["coffee", "daily"]
        },
        {
            "transaction_id": "TX003",
            "account_id": "ACC001",
            "amount": 500.00,
            "status": "FAILED",
            "details": {
                "channel": "BRANCH",
                "merchant": "Wire Transfer",
                "category": "Transfer"
            },
            "tags": ["international", "urgent"]
        }
    ]

    return json.dumps(data)


def process_json_data(json_str):
    """Process JSON data using DuckDB."""
    con = duckdb.connect()

    # 1. Read JSON
    print("=== Read JSON Data ===")
    result = con.execute(f"""
        SELECT * FROM read_json_auto('{json_str}')
    """).fetchdf()
    print(result.to_string(index=False))

    # 2. Extract nested fields
    print("\n=== Extract Nested Fields ===")
    result = con.execute(f"""
        SELECT 
            transaction_id,
            amount,
            details->>'channel' as channel,
            details->>'merchant' as merchant,
            details->>'category' as category
        FROM read_json_auto('{json_str}')
    """).fetchdf()
    print(result.to_string(index=False))

    # 3. Process arrays
    print("\n=== Process Arrays ===")
    result = con.execute(f"""
        SELECT 
            transaction_id,
            tags,
            array_length(tags) as tag_count
        FROM read_json_auto('{json_str}')
    """).fetchdf()
    print(result.to_string(index=False))

    # 4. Aggregate JSON data
    print("\n=== Aggregate JSON Data ===")
    result = con.execute(f"""
        SELECT 
            details->>'channel' as channel,
            COUNT(*) as tx_count,
            SUM(amount) as total_amount
        FROM read_json_auto('{json_str}')
        GROUP BY 1
    """).fetchdf()
    print(result.to_string(index=False))

    con.close()


# ============================================================
# RUN THE SCENARIO
# ============================================================
if __name__ == "__main__":
    # Generate JSON data
    print("Generating JSON data...")
    json_str = generate_json_data()

    # Process
    process_json_data(json_str)
```

---

## 7. Interview Questions

### Q1: What extensions does DuckDB support?

**Answer:**

**Core extensions:**
- `parquet`: Read/write Parquet files
- `json`: Read/write JSON files
- `httpfs`: HTTP/S3/GCS access
- `excel`: Read/write Excel files
- `fts`: Full-text search
- `spatial`: Geospatial operations

**Community extensions:**
- `iceberg`: Read Iceberg tables
- `delta`: Read Delta Lake tables
- `postgres`: Connect to PostgreSQL
- `mysql`: Connect to MySQL
- `sqlite`: Connect to SQLite

---

### Q2: How do you install and load extensions?

**Answer:**

```sql
-- Install extension
INSTALL spatial

-- Load extension
LOAD spatial

-- List installed extensions
SELECT * FROM duckdb_extensions()

-- List loaded extensions
SELECT * FROM duckdb_loaded_extensions()
```

**Python:**
```python
con.execute("INSTALL spatial")
con.execute("LOAD spatial")
```

---

### Q3: How do you query S3 files with DuckDB?

**Answer:**

**Using httpfs extension:**
```sql
INSTALL httpfs;
LOAD httpfs;

-- Configure S3 credentials
SET s3_region='us-east-1';
SET s3_access_key_id='your_key';
SET s3_secret_access_key='your_secret';

-- Query S3 Parquet
SELECT * FROM read_parquet('s3://bucket/data.parquet')

-- Query S3 CSV
SELECT * FROM read_csv('s3://bucket/data.csv')
```

**Python:**
```python
con.execute("INSTALL httpfs")
con.execute("LOAD httpfs")
con.execute("SET s3_region='us-east-1'")
result = con.execute("SELECT * FROM read_parquet('s3://bucket/data.parquet')").fetchdf()
```

---

### Q4: How do you process JSON with DuckDB?

**Answer:**

```sql
-- Read JSON file
SELECT * FROM read_json('data.json')

-- Read JSON array
SELECT * FROM read_json('data.json', format='array')

-- Extract nested fields
SELECT json_extract_string(data, '$.name') FROM table

-- Extract with ->> operator
SELECT data->>'name' as name FROM table
```

**Python:**
```python
result = con.execute("""
    SELECT 
        data->>'name' as name,
        data->>'age' as age
    FROM read_json('data.json')
""").fetchdf()
```

---

### Q5: When would you use DuckDB extensions?

**Answer:**

| Extension | Use Case |
|-----------|----------|
| **parquet** | Data lake queries |
| **json** | API data processing |
| **httpfs** | Cloud storage access |
| **spatial** | Location analytics |
| **fts** | Text search |
| **excel** | Report processing |
| **iceberg** | Lakehouse queries |
| **delta** | Delta Lake access |

**Example:**
```sql
-- Spatial analytics
INSTALL spatial;
LOAD spatial;
SELECT ST_Distance(point1, point2) FROM table

-- JSON processing
SELECT data->>'field' FROM read_json('api_response.json')

-- S3 access
INSTALL httpfs;
LOAD httpfs;
SELECT * FROM read_parquet('s3://bucket/data.parquet')
```
