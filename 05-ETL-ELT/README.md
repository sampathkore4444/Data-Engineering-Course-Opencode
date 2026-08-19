# 05 - ETL/ELT Processes

## Table of Contents
1. [ETL Fundamentals](#1-etl-fundamentals)
2. [Change Data Capture (CDC)](#2-change-data-capture-cdc)
3. [Transformation Patterns](#3-transformation-patterns)
4. [Data Quality Checks](#4-data-quality-checks)
5. [Error Handling](#5-error-handling)
6. [Testing ETL Pipelines](#6-testing-etl-pipelines)
7. [Real-World Scenarios](#7-real-world-scenarios)
8. [Banking Examples](#8-banking-examples)
9. [E-Commerce Examples](#9-e-commerce-examples)
10. [Hands-On Exercises](#10-hands-on-exercises)
11. [Interview Questions](#11-interview-questions)

---

## 1. ETL Fundamentals

### ETL vs ELT

```
ETL (Traditional):
Source -> Extract -> Transform -> Load -> Target
              |        |           |
         Raw data   Cleanse     Load clean
         pulled     Standardize  data to DW
                    Enrich
                    Aggregate

ELT (Modern):
Source -> Extract -> Load -> Transform
              |        |        |
         Raw data   Load raw  Transform
         pulled     to data   in target
                    lake/     using SQL
                    warehouse compute
```

| Aspect | ETL | ELT |
|--------|-----|-----|
| Transform Location | Separate server | Target system |
| Data Volume | Limited by transform server | Scales with target |
| Flexibility | Fixed transformations | Ad-hoc transformations |
| Latency | Higher (two hops) | Lower (one hop) |
| Cost | Transform server + target | Target only |
| Modern Tools | Informatica, Talend | dbt, Snowflake, BigQuery |

### Modern ETL/ELT Tools

| Category | Tools | Description |
|----------|-------|-------------|
| **Orchestration** | Apache Airflow, Dagster, Prefect, Mage | Pipeline scheduling and monitoring |
| **Transformation** | dbt, SQLMesh, Dataform | SQL-based transformations with testing |
| **CDC Tools** | Debezium, AWS DMS, Fivetran, Airbyte | Real-time change data capture |
| **Stream Processing** | Apache Kafka, Apache Flink, Spark Streaming | Real-time data processing |
| **Data Quality** | Great Expectations, Soda, Monte Carlo | Validation and monitoring |
| **ELT Platforms** | Fivetran, Stitch, Airbyte | Managed data ingestion |

### Incremental Load Patterns

#### Timestamp-Based
```sql
-- Extract only new/changed records
SELECT *
FROM source_orders
WHERE updated_at > :last_extract_timestamp;
```

#### Watermark-Based
```sql
-- Use a watermark column (auto-increment ID)
SELECT *
FROM source_orders
WHERE order_id > :last_watermark_id
ORDER BY order_id
LIMIT 10000;
```

#### Hash-Based Change Detection
```sql
-- Compare hash of all columns to detect changes
SELECT 
    source.*,
    MD5(CONCAT(source.col1, source.col2, source.col3)) as source_hash,
    target.target_hash
FROM source_table source
LEFT JOIN target_table target ON source.id = target.id
WHERE target.id IS NULL  -- New records
   OR source_hash <> target_hash;  -- Changed records
```

---

## 2. Change Data Capture (CDC)

CDC identifies and captures changes made to data in a database.

### Log-Based CDC (Best)
Reads the database transaction log (WAL, binlog) to capture changes.

```
Database --> Transaction Log --> CDC Tool --> Target
                                  |
                            Reads log stream
                            Captures INSERT/UPDATE/DELETE
                            No impact on source performance
```

**Tools:** Debezium, AWS DMS, Oracle GoldenGate, Maxwell, Singer

### Trigger-Based CDC
Database triggers fire on INSERT/UPDATE/DELETE to capture changes.

```sql
CREATE OR REPLACE FUNCTION capture_order_changes()
RETURNS TRIGGER AS 
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO order_cdc (operation, order_id, data, timestamp)
        VALUES ('I', NEW.order_id, row_to_json(NEW), NOW());
    ELSIF TG_OP = 'UPDATE' THEN
        INSERT INTO order_cdc (operation, order_id, data, timestamp)
        VALUES ('U', NEW.order_id, row_to_json(NEW), NOW());
    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO order_cdc (operation, order_id, data, timestamp)
        VALUES ('D', OLD.order_id, row_to_json(OLD), NOW());
    END IF;
    RETURN NEW;
END;
 LANGUAGE plpgsql;

CREATE TRIGGER trg_order_changes
AFTER INSERT OR UPDATE OR DELETE ON orders
FOR EACH ROW EXECUTE FUNCTION capture_order_changes();
```

### Timestamp-Based CDC
Simple but misses deletes.

```sql
-- Source: Add timestamp tracking
ALTER TABLE orders ADD COLUMN last_modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
CREATE INDEX idx_orders_modified ON orders(last_modified);

-- Extract query
SELECT * FROM orders WHERE last_modified > :last_sync_timestamp;
```

### Comparison

| Method | Pros | Cons | Best For |
|--------|------|------|----------|
| Log-based | Real-time, no source impact | Complex setup | High-volume OLTP |
| Trigger-based | Captures all operations | Source performance impact | Compliance needs |
| Timestamp-based | Simple | Misses deletes, clock sync issues | Low-volume, append-only |
| Full comparison | Complete picture | Expensive, slow | Small tables |

---

## 3. Transformation Patterns

### Data Cleansing

```python
import pandas as pd

def cleanse_customer_data(df):
    # Remove duplicates
    df = df.drop_duplicates(subset=['customer_id'], keep='last')
    
    # Handle nulls
    df['email'] = df['email'].fillna('unknown@unknown.com')
    df['phone'] = df['phone'].fillna('')
    
    # Standardize formats
    df['email'] = df['email'].str.lower().str.strip()
    df['phone'] = df['phone'].str.replace(r'[^0-9+]', '', regex=True)
    
    # Validate email format
    df['is_valid_email'] = df['email'].str.match(r'^[\w\.-]+@[\w\.-]+\.\w+$')
    
    # Remove invalid records
    df = df[df['is_valid_email'] == True]
    
    return df
```

### Data Enrichment

```python
def enrich_order_data(orders_df, products_df, customers_df):
    # Join with product details
    enriched = orders_df.merge(
        products_df[['product_id', 'category', 'brand', 'cost']],
        on='product_id',
        how='left'
    )
    
    # Join with customer details
    enriched = enriched.merge(
        customers_df[['customer_id', 'segment', 'city', 'country']],
        on='customer_id',
        how='left'
    )
    
    # Calculate derived fields
    enriched['profit'] = enriched['amount'] - enriched['cost']
    enriched['profit_margin'] = enriched['profit'] / enriched['amount']
    enriched['order_age_days'] = (pd.Timestamp.now() - enriched['order_date']).dt.days
    
    return enriched
```

### Data Deduplication

```sql
-- Method 1: ROW_NUMBER
WITH ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id, order_date, amount 
            ORDER BY load_timestamp DESC
        ) as rn
    FROM staging_orders
)
DELETE FROM staging_orders WHERE rn > 1;

-- Method 2: DISTINCT with hash
SELECT DISTINCT ON (customer_id, order_date, amount)
    *
FROM staging_orders
ORDER BY customer_id, order_date, amount, load_timestamp DESC;

-- Method 3: GROUP BY (for exact duplicates)
DELETE FROM staging_orders
WHERE ctid NOT IN (
    SELECT MIN(ctid)
    FROM staging_orders
    GROUP BY customer_id, order_date, amount
);
```

### Aggregation

```sql
-- Daily aggregated sales
INSERT INTO agg_daily_sales (date_key, store_key, product_key, 
                             units_sold, revenue, discount, orders)
SELECT 
    o.order_date as date_key,
    oi.store_key,
    oi.product_key,
    SUM(oi.quantity) as units_sold,
    SUM(oi.quantity * oi.unit_price) as revenue,
    SUM(oi.discount) as discount,
    COUNT(DISTINCT o.order_id) as orders
FROM order_items oi
JOIN orders o ON oi.order_id = o.order_id
WHERE o.order_date = :process_date
GROUP BY o.order_date, oi.store_key, oi.product_key;
```

---

## 4. Data Quality Checks

### Great Expectations Example

```python
import great_expectations as gx

context = gx.get_context()

# Connect to data source
datasource = context.sources.add_pandas("orders_source")
data_asset = datasource.add_dataframe_asset(name="orders")
batch_request = data_asset.build_batch_request()

# Define expectations
validator = context.get_validator(batch_request=batch_request)

# Schema validation
validator.expect_column_to_exist("order_id")
validator.expect_column_values_to_not_be_null("order_id")
validator.expect_column_values_to_be_unique("order_id")

# Data quality rules
validator.expect_column_values_to_be_between("amount", min_value=0, max_value=1000000)
validator.expect_column_values_to_match_regex("email", r"^[\w\.-]+@[\w\.-]+\.\w+$")
validator.expect_column_values_to_be_in_set("status", ["pending", "completed", "cancelled"])

# Freshness
validator.expect_compound_columns_to_be_unique(["order_id", "order_date"])

# Run validation
results = validator.validate()
print(results)
```

### SQL-Based Quality Checks

```sql
-- Completeness check
SELECT 
    COUNT(*) as total_rows,
    COUNT(customer_id) as non_null_customers,
    ROUND(COUNT(customer_id) * 100.0 / COUNT(*), 2) as completeness_pct
FROM staging_orders;

-- Uniqueness check
SELECT order_id, COUNT(*) as cnt
FROM staging_orders
GROUP BY order_id
HAVING COUNT(*) > 1;

-- Range check
SELECT 
    MIN(amount) as min_amount,
    MAX(amount) as max_amount,
    AVG(amount) as avg_amount,
    COUNT(CASE WHEN amount < 0 THEN 1 END) as negative_amounts
FROM staging_orders;

-- Referential integrity check
SELECT o.*
FROM staging_orders o
LEFT JOIN dim_customer c ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

-- Freshness check
SELECT 
    MAX(updated_at) as last_update,
    EXTRACT(EPOCH FROM (NOW() - MAX(updated_at))) / 3600 as hours_since_update
FROM staging_orders;
```

---

## 5. Error Handling

### Dead Letter Queue Pattern

```python
from kafka import KafkaProducer, KafkaConsumer
import json

producer = KafkaProducer(bootstrap_servers='localhost:9092')

def process_message(message):
    try:
        # Process valid message
        result = transform(message.value)
        producer.send('processed-topic', value=result)
        return True
    except Exception as e:
        # Send to dead letter queue
        producer.send('dead-letter-topic', value={
            'original': message.value,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        })
        return False
```

### Retry Mechanism

```python
import time
from functools import wraps

def retry(max_retries=3, delay=1):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries - 1:
                        raise e
                    time.sleep(delay * (2 ** attempt))  # Exponential backoff
            return None
        return wrapper
    return decorator

@retry(max_retries=3, delay=1)
def extract_data(source):
    # Extraction logic with automatic retry
    pass
```

### Circuit Breaker Pattern

```python
import time

class CircuitBreaker:
    def __init__(self, failure_threshold=5, reset_timeout=60):
        self.failure_count = 0
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self.last_failure_time = None
        self.state = 'CLOSED'
    
    def call(self, func, *args, **kwargs):
        if self.state == 'OPEN':
            if time.time() - self.last_failure_time > self.reset_timeout:
                self.state = 'HALF_OPEN'
            else:
                raise Exception("Circuit breaker is OPEN")
        
        try:
            result = func(*args, **kwargs)
            self.failure_count = 0
            self.state = 'CLOSED'
            return result
        except Exception as e:
            self.failure_count += 1
            self.last_failure_time = time.time()
            if self.failure_count >= self.failure_threshold:
                self.state = 'OPEN'
            raise e
```

### Transaction Management

```sql
BEGIN TRANSACTION;

-- Stage data
INSERT INTO staging_orders SELECT * FROM source_orders WHERE load_date = CURRENT_DATE;

-- Validate
DO 
DECLARE
    v_count INT;
BEGIN
    SELECT COUNT(*) INTO v_count FROM staging_orders WHERE amount < 0;
    IF v_count > 0 THEN
        RAISE EXCEPTION 'Invalid records found: %', v_count;
    END IF;
END ;

-- Load
INSERT INTO fact_orders SELECT * FROM staging_orders;

-- Verify row counts
DO 
DECLARE
    v_source INT;
    v_target INT;
BEGIN
    SELECT COUNT(*) INTO v_source FROM staging_orders;
    SELECT COUNT(*) INTO v_target FROM fact_orders WHERE load_date = CURRENT_DATE;
    IF v_source <> v_target THEN
        RAISE EXCEPTION 'Row count mismatch: source=%, target=%', v_source, v_target;
    END IF;
END ;

COMMIT;
-- If any error, ROLLBACK is automatic
```

---

## 6. Testing ETL Pipelines

### Unit Testing Transformations

```python
import pytest

def test_transform_amount():
    input_data = pd.DataFrame({
        'amount': [100, -50, 0, 1000],
        'tax_rate': [0.1, 0.1, 0.1, 0.1]
    })
    
    result = transform_amount(input_data)
    
    assert result['tax_amount'].tolist() == [10, 0, 0, 100]
    assert result['total'].tolist() == [110, -50, 0, 1100]

def test_deduplication():
    input_data = pd.DataFrame({
        'id': [1, 1, 2, 3],
        'name': ['A', 'A', 'B', 'C']
    })
    
    result = deduplicate(input_data, key='id')
    
    assert len(result) == 3
    assert result['id'].tolist() == [1, 2, 3]

def test_date_parsing():
    input_data = pd.DataFrame({
        'date_str': ['2024-01-15', '01/15/2024', '15-Jan-2024']
    })
    
    result = parse_dates(input_data, 'date_str')
    
    assert all(result['date_parsed'] == pd.Timestamp('2024-01-15'))
```

### Data Reconciliation

```python
def reconcile_data(source_count, target_count, source_sum, target_sum):
    """Verify source and target are consistent"""
    errors = []
    
    if source_count != target_count:
        errors.append(f"Row count mismatch: source={source_count}, target={target_count}")
    
    if abs(source_sum - target_sum) > 0.01:
        errors.append(f"Sum mismatch: source={source_sum}, target={target_sum}")
    
    if errors:
        raise ValueError("Reconciliation failed:\n" + "\n".join(errors))
    
    return True
```

---

## 7. Real-World Scenarios

### Scenario 1: Financial ETL Pipeline

```
Source Systems          ETL Process              Target
+------------+         +--------------+        +------------+
| Core Banking|--CDC-->|              |        | Data       |
| (Oracle)   |        | Kafka        |        | Warehouse  |
+------------+        |              |        | (Redshift) |
| Credit Card|---CDC->| +----------+ |        |            |
| (Mainframe)|        | | Spark    | |------->| Fact:      |
+------------+        | | Transform| |        | Daily      |
| Loan System|--CDC-->| |          | |        | Balances   |
+------------+        | +----------+ |        |            |
                      +--------------+        | Dim:       |
                                              | Customer   |
                                              | (SCD Type 2)|
                                              +------------+
```

### Scenario 3: Modern Data Stack with dbt

```
Source --> Airbyte/ELT --> Snowflake --> dbt --> BI Tools
  |           |              |          |        |
  |      Managed ELT    Raw data    Transforms  Reports
  |      (Fivetran,     (Schema     (Tests,     (Looker,
  |       Airbyte)       on-read)    Docs)       Tableau)
```

**Key Benefits:**
- Managed ingestion (no custom ETL code)
- Version-controlled transformations (Git)
- Built-in testing and documentation
- Instant data availability in raw layer

### Scenario 2: E-Commerce Real-Time Inventory

```
Real-Time Pipeline:
Web App --> Kafka --> Flink --> Inventory Updates --> Dashboard
                              |
                              +--> Alert System (Low Stock)
                              |
                              +--> Data Lake (Analytics)

Batch Pipeline:
Web App --> S3 (Parquet) --> dbt --> Data Warehouse --> Reports
```

---

## 8. Banking Examples

### Example 1: Daily Regulatory Report Pipeline

```sql
-- Step 1: Extract daily transactions
CREATE TABLE stg_daily_transactions AS
SELECT * FROM source_core_banking.transactions
WHERE transaction_date = CURRENT_DATE - 1;

-- Step 2: Validate
DO 
BEGIN
    -- Check for negative amounts (except refunds)
    IF EXISTS (
        SELECT 1 FROM stg_daily_transactions 
        WHERE amount < 0 AND transaction_type <> 'REFUND'
    ) THEN
        RAISE EXCEPTION 'Invalid negative amounts found';
    END IF;
    
    -- Check for orphaned transactions
    IF EXISTS (
        SELECT 1 FROM stg_daily_transactions t
        LEFT JOIN dim_customer c ON t.customer_id = c.customer_id
        WHERE c.customer_id IS NULL
    ) THEN
        RAISE EXCEPTION 'Orphaned transactions found';
    END IF;
END ;

-- Step 3: Transform and load
INSERT INTO fact_daily_transactions (
    date_key, customer_key, account_key, transaction_type,
    amount, balance_after, channel, status
)
SELECT 
    t.transaction_date as date_key,
    c.customer_key,
    a.account_key,
    t.transaction_type,
    t.amount,
    t.balance_after,
    t.channel,
    t.status
FROM stg_daily_transactions t
JOIN dim_customer c ON t.customer_id = c.customer_id
JOIN dim_account a ON t.account_id = a.account_id;

-- Step 4: Generate regulatory report
SELECT 
    'NPA_REPORT' as report_name,
    c.customer_name,
    l.loan_amount,
    l.days_past_due,
    l.classification
FROM fact_loan l
JOIN dim_customer c ON l.customer_key = c.customer_key
WHERE l.days_past_due > 90;
```

---

## 9. E-Commerce Examples

### Example 1: Product Catalog Sync

```python
import pandas as pd
from sqlalchemy import create_engine

def sync_product_catalog():
    # Extract from source
    source_products = pd.read_sql("""
        SELECT * FROM source_erp.products 
        WHERE last_modified > :last_sync
    """, source_engine, params={'last_sync': last_sync_timestamp})
    
    # Transform
    source_products['category'] = source_products['category'].str.upper()
    source_products['price'] = source_products['price'].round(2)
    source_products['is_active'] = source_products['end_date'].isna()
    
    # Load using MERGE
    target_engine.execute("""
        MERGE INTO dim_product tgt
        USING stg_products src
        ON tgt.product_id = src.product_id
        WHEN MATCHED THEN UPDATE SET
            product_name = src.product_name,
            category = src.category,
            price = src.price,
            is_active = src.is_active,
            updated_at = CURRENT_TIMESTAMP
        WHEN NOT MATCHED THEN INSERT VALUES (
            src.product_id, src.product_name, src.category,
            src.price, src.is_active, CURRENT_TIMESTAMP
        )
    """)
```

### Example 2: Order Processing Pipeline

```python
def process_orders_batch(batch_date):
    # Extract orders for the day
    orders = spark.sql(f"""
        SELECT * FROM raw.orders 
        WHERE order_date = '{batch_date}'
    """)
    
    # Validate
    assert orders.count() > 0, "No orders found for batch date"
    assert orders.filter(orders.amount < 0).count() == 0, "Negative amounts found"
    
    # Transform
    enriched = (orders
        .join(customers, 'customer_id', 'left')
        .join(products, 'product_id', 'left')
        .withColumn('profit', col('amount') - col('cost'))
        .withColumn('profit_margin', col('profit') / col('amount'))
    )
    
    # Load
    enriched.write.mode('append').saveAsTable('fact_orders')
    
    # Verify
    count = spark.sql(f"""
        SELECT COUNT(*) FROM fact_orders 
        WHERE order_date = '{batch_date}'
    """).collect()[0][0]
    
    assert count == orders.count(), f"Count mismatch: expected {orders.count()}, got {count}"
    
    return count
```

---

## 10. Hands-On Exercises

### Exercise 1: Implement Incremental Load (Python)
```python
import pandas as pd
from datetime import datetime, timedelta

# Task: Implement timestamp-based incremental load

def incremental_extract(source_table, last_sync_col, last_sync_value, batch_size=10000):
    """
    Extract only new/changed records since last sync.
    
    Args:
        source_table: SQLAlchemy table object
        last_sync_col: Column to track changes (e.g., 'updated_at')
        last_sync_value: Last sync timestamp
        batch_size: Number of rows per batch
    """
    query = f"""
        SELECT * FROM {source_table}
        WHERE {last_sync_col} > :last_sync
        ORDER BY {last_sync_col}
        LIMIT :batch_size
    """
    
    df = pd.read_sql(query, engine, params={
        'last_sync': last_sync_value,
        'batch_size': batch_size
    })
    
    return df

# Test with sample data
def test_incremental_extract():
    # Create sample source table
    source_data = pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'name': ['A', 'B', 'C', 'D', 'E'],
        'updated_at': pd.to_datetime([
            '2024-01-01 10:00:00',
            '2024-01-02 11:00:00',  
            '2024-01-03 12:00:00',
            '2024-01-04 13:00:00',
            '2024-01-05 14:00:00'
        ])
    })
    
    # Simulate last sync was on Jan 3rd
    last_sync = '2024-01-03 12:00:00'
    
    # Should return only rows 4 and 5
    result = source_data[source_data['updated_at'] > last_sync]
    assert len(result) == 2
    assert result['id'].tolist() == [4, 5]
    print("Test passed!")

test_incremental_extract()
```

### Exercise 2: Data Deduplication (SQL)
```sql
-- Task: Remove duplicates while keeping the latest record

-- Create sample duplicate data
CREATE TEMPORARY TABLE orders_raw (
    order_id INT,
    customer_id INT,
    amount DECIMAL(10,2),
    load_timestamp TIMESTAMP
);

INSERT INTO orders_raw VALUES
(1, 101, 100.00, '2024-01-01 10:00:00'),
(1, 101, 150.00, '2024-01-02 11:00:00'),  -- Duplicate
(2, 102, 200.00, '2024-01-01 10:00:00'),
(3, 103, 300.00, '2024-01-01 10:00:00'),
(3, 103, 350.00, '2024-01-03 12:00:00'),  -- Duplicate
(3, 103, 400.00, '2024-01-04 13:00:00');  -- Duplicate

-- Solution: Keep only the latest record per order_id
WITH deduplicated AS (
    SELECT 
        order_id,
        customer_id,
        amount,
        load_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY order_id 
            ORDER BY load_timestamp DESC
        ) as rn
    FROM orders_raw
)
SELECT order_id, customer_id, amount, load_timestamp
FROM deduplicated
WHERE rn = 1
ORDER BY order_id;

-- Expected result: 3 rows (order_id 1, 2, 3 with latest timestamps)
```

### Exercise 3: Data Quality Validation (Python)
```python
import pandas as pd
import numpy as np

# Task: Implement comprehensive data quality checks

def validate_orders(df):
    """
    Validate orders data quality.
    Returns dict with check results.
    """
    results = {
        'total_rows': len(df),
        'checks': [],
        'passed': True
    }
    
    # Check 1: No null order_ids
    null_orders = df['order_id'].isnull().sum()
    results['checks'].append({
        'name': 'order_id not null',
        'passed': null_orders == 0,
        'details': f'{null_orders} null values found'
    })
    
    # Check 2: Unique order_ids
    dup_orders = df['order_id'].duplicated().sum()
    results['checks'].append({
        'name': 'order_id unique',
        'passed': dup_orders == 0,
        'details': f'{dup_orders} duplicates found'
    })
    
    # Check 3: Positive amounts
    negative_amounts = (df['amount'] < 0).sum()
    results['checks'].append({
        'name': 'amount positive',
        'passed': negative_amounts == 0,
        'details': f'{negative_amounts} negative amounts found'
    })
    
    # Check 4: Valid status values
    valid_statuses = ['pending', 'completed', 'cancelled']
    invalid_status = ~df['status'].isin(valid_statuses)
    results['checks'].append({
        'name': 'valid status',
        'passed': invalid_status.sum() == 0,
        'details': f'{invalid_status.sum()} invalid statuses found'
    })
    
    # Check 5: Reasonable date range
    df['order_date'] = pd.to_datetime(df['order_date'])
    too_old = (df['order_date'] < '2020-01-01').sum()
    results['checks'].append({
        'name': 'date range valid',
        'passed': too_old == 0,
        'details': f'{too_old} orders before 2020'
    })
    
    # Overall result
    results['passed'] = all(c['passed'] for c in results['checks'])
    return results

# Test with sample data
def test_validation():
    # Good data
    good_data = pd.DataFrame({
        'order_id': [1, 2, 3],
        'amount': [100, 200, 300],
        'status': ['pending', 'completed', 'cancelled'],
        'order_date': ['2024-01-01', '2024-01-02', '2024-01-03']
    })
    
    result = validate_orders(good_data)
    assert result['passed'] == True
    print("Good data test passed!")
    
    # Bad data
    bad_data = pd.DataFrame({
        'order_id': [1, 1, 3],  # Duplicate
        'amount': [100, -50, 300],  # Negative
        'status': ['pending', 'invalid', 'cancelled'],  # Invalid
        'order_date': ['2019-01-01', '2024-01-02', '2024-01-03']  # Too old
    })
    
    result = validate_orders(bad_data)
    assert result['passed'] == False
    print("Bad data test passed!")
    
    # Print results
    for check in result['checks']:
        status = "PASS" if check['passed'] else "FAIL"
        print(f"  [{status}] {check['name']}: {check['details']}")

test_validation()
```

### Exercise 4: Implement CDC with Triggers (SQL)
```sql
-- Task: Create CDC mechanism for an orders table

-- Create source table
CREATE TABLE orders_source (
    order_id SERIAL PRIMARY KEY,
    customer_id INT,
    amount DECIMAL(10,2),
    status VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create CDC table
CREATE TABLE orders_cdc (
    cdc_id SERIAL PRIMARY KEY,
    operation CHAR(1),  -- I=Insert, U=Update, D=Delete
    order_id INT,
    old_data JSONB,
    new_data JSONB,
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create CDC function
CREATE OR REPLACE FUNCTION capture_orders_changes()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO orders_cdc (operation, order_id, new_data)
        VALUES ('I', NEW.order_id, to_jsonb(NEW));
    ELSIF TG_OP = 'UPDATE' THEN
        INSERT INTO orders_cdc (operation, order_id, old_data, new_data)
        VALUES ('U', NEW.order_id, to_jsonb(OLD), to_jsonb(NEW));
    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO orders_cdc (operation, order_id, old_data)
        VALUES ('D', OLD.order_id, to_jsonb(OLD));
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger
CREATE TRIGGER trg_orders_cdc
AFTER INSERT OR UPDATE OR DELETE ON orders_source
FOR EACH ROW EXECUTE FUNCTION capture_orders_changes();

-- Test the CDC
INSERT INTO orders_source (customer_id, amount, status) VALUES (101, 150.00, 'pending');
UPDATE orders_source SET amount = 200.00, status = 'completed' WHERE order_id = 1;
DELETE FROM orders_source WHERE order_id = 1;

-- Check CDC log
SELECT * FROM orders_cdc ORDER BY cdc_id;
```

### Exercise 5: ETL Pipeline with Error Handling (Python)
```python
import pandas as pd
from datetime import datetime
import logging

# Task: Build a robust ETL pipeline with error handling

class ETLPipeline:
    def __init__(self, name):
        self.name = name
        self.errors = []
        self.stats = {'extracted': 0, 'transformed': 0, 'loaded': 0}
        
    def extract(self, source_func):
        """Extract data from source."""
        try:
            data = source_func()
            self.stats['extracted'] = len(data)
            logging.info(f"Extracted {len(data)} records")
            return data
        except Exception as e:
            self.errors.append(f"Extract error: {str(e)}")
            logging.error(f"Extract failed: {e}")
            raise
    
    def transform(self, data, transform_func):
        """Transform data."""
        try:
            transformed = transform_func(data)
            self.stats['transformed'] = len(transformed)
            logging.info(f"Transformed {len(transformed)} records")
            return transformed
        except Exception as e:
            self.errors.append(f"Transform error: {str(e)}")
            logging.error(f"Transform failed: {e}")
            raise
    
    def load(self, data, target_func):
        """Load data to target."""
        try:
            target_func(data)
            self.stats['loaded'] = len(data)
            logging.info(f"Loaded {len(data)} records")
        except Exception as e:
            self.errors.append(f"Load error: {str(e)}")
            logging.error(f"Load failed: {e}")
            raise
    
    def run(self, extract_func, transform_func, load_func):
        """Run the ETL pipeline."""
        logging.info(f"Starting ETL pipeline: {self.name}")
        start_time = datetime.now()
        
        try:
            # Extract
            raw_data = self.extract(extract_func)
            
            # Validate
            if raw_data is None or len(raw_data) == 0:
                raise ValueError("No data extracted")
            
            # Transform
            transformed_data = self.transform(raw_data, transform_func)
            
            # Load
            self.load(transformed_data, load_func)
            
            duration = (datetime.now() - start_time).total_seconds()
            logging.info(f"Pipeline completed in {duration:.2f}s")
            return True
            
        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            logging.error(f"Pipeline failed after {duration:.2f}s: {e}")
            return False

# Test the pipeline
def test_etl_pipeline():
    # Setup logging
    logging.basicConfig(level=logging.INFO)
    
    # Create pipeline
    pipeline = ETLPipeline("test_pipeline")
    
    # Define extract function (simulated)
    def extract():
        return pd.DataFrame({
            'id': [1, 2, 3],
            'amount': [100, 200, 300],
            'status': ['pending', 'completed', 'pending']
        })
    
    # Define transform function
    def transform(data):
        data['total'] = data['amount'] * 1.1  # Add 10% tax
        data['status'] = data['status'].str.upper()
        return data
    
    # Define load function (simulated)
    def load(data):
        print(f"Loaded {len(data)} records")
        print(data)
    
    # Run pipeline
    success = pipeline.run(extract, transform, load)
    
    assert success == True
    assert pipeline.stats['extracted'] == 3
    assert pipeline.stats['transformed'] == 3
    assert pipeline.stats['loaded'] == 3
    print("\nPipeline test passed!")
    print(f"Stats: {pipeline.stats}")

test_etl_pipeline()
```

---

## 11. Interview Questions

### Q1: What is the difference between ETL and ELT? When would you use each?

**Answer:** **ETL** transforms data before loading into the target - better when transformation logic is complex, data volume is manageable, or target system has limited compute (e.g., traditional data warehouses). 

**ELT** loads raw data first, then transforms using target system's compute - better for modern cloud warehouses (Snowflake, BigQuery) where compute is cheap and scalable, data volumes are huge, or you need to preserve raw data. ELT is the modern standard; ETL is still used for compliance or when source data needs heavy cleansing before storage.

### Q2: Explain the different CDC methods. Which is best for high-volume OLTP?

**Answer:** 
**Log-based CDC** reads the database transaction log (binlog, WAL) - best for high-volume OLTP because it has zero impact on source performance, captures all changes in real-time, and doesn't require schema changes. 

**Trigger-based** captures changes via DB triggers but adds overhead. 

**Timestamp-based** is simple but misses deletes and depends on clock sync. 

**Full load comparison** is expensive. For high-volume OLTP, log-based CDC (Debezium, AWS DMS) is the clear winner.

### Q3: How do you handle late-arriving data in a batch ETL pipeline?

**Answer:** Several strategies: 

1) **Late-arriving window:** Accept data within X hours of scheduled run, then cut off. 

2) **Backfill mechanism:** When late data arrives, reprocess affected partitions. 

3) **Idempotent loads:** Design transforms to handle duplicates gracefully (MERGE instead of INSERT). 

4) **Watermarking:** Track maximum timestamps and compare against expected ranges. 

5) **Partition awareness:** Repartition historical data when late records arrive for past dates.

### Q4: How would you test an ETL pipeline?

**Answer:** Multi-layered testing: 
1) **Unit tests:** Test individual transformations (Python pytest, dbt tests). 
2) **Integration tests:** Test end-to-end data flow. 
3) **Schema tests:** Validate column names, types, nullability. 
4) **Data quality tests:** Check ranges, uniqueness, referential integrity. 
5) **Reconciliation tests:** Compare source vs target counts and aggregates. 
6) **Regression tests:** Verify changes don't break existing logic. 
7) **Performance tests:** Measure pipeline execution time. Tools: Great Expectations, dbt tests, custom pytest suites.

### Q5: Describe a real-time data pipeline architecture.

**Answer:** 

Source systems -> CDC (Debezium) -> Kafka (message queue) -> Stream processor (Flink/Spark Streaming) -> Serving layer. 

The stream processor handles: event-time windowing, state management, exactly-once semantics, and enrichment via side-inputs. 

Serving layer includes: real-time dashboard (druid/pinot), alerting system (PagerDuty), and batch storage (S3/Parquet) for historical analysis. 

Critical considerations: backpressure handling, idempotent processing, dead letter queues, monitoring lag, and checkpointing for fault tolerance.

### Q6: What is the difference between batch and streaming ETL?

**Answer:**
**Batch ETL:**
- Processes data in scheduled intervals (hourly, daily)
- High throughput, lower latency
- Simpler error handling and recovery
- Good for historical analytics and reporting
- Tools: Apache Spark, dbt, Airflow

**Streaming ETL:**
- Processes data continuously in real-time
- Low latency (milliseconds to seconds)
- Complex state management and exactly-once semantics
- Good for real-time dashboards and alerts
- Tools: Apache Flink, Kafka Streams, Spark Streaming

### Q7: How do you ensure idempotency in ETL pipelines?

**Answer:**
Idempotency means running the same pipeline multiple times produces the same result. Strategies:

1. **MERGE/UPSERT:** Use INSERT ... ON CONFLICT or MERGE instead of INSERT
2. **Natural keys:** Use business keys for deduplication, not auto-increment IDs
3. **Timestamps:** Track when records were last processed
4. **Checksums:** Compare source and target checksums before processing
5. **Transaction boundaries:** Wrap loads in transactions with rollback on failure
6. **Watermarking:** Track processed offsets/watermarks

---

## Summary Checklist

### ETL Fundamentals
- [ ] Understand ETL vs ELT differences and use cases
- [ ] Know incremental load patterns (timestamp, watermark, hash)
- [ ] Can design batch vs streaming pipelines

### Change Data Capture
- [ ] Compare CDC methods (log-based, trigger-based, timestamp)
- [ ] Know when to use Debezium, AWS DMS, or custom solutions

### Transformation Patterns
- [ ] Implement data cleansing (nulls, formats, validation)
- [ ] Apply data enrichment (joins, derived fields)
- [ ] Deduplicate records using SQL or Python

### Data Quality
- [ ] Use Great Expectations for validation
- [ ] Write SQL quality checks (completeness, uniqueness, ranges)
- [ ] Implement data reconciliation between source and target

### Error Handling
- [ ] Implement retry mechanisms with exponential backoff
- [ ] Use circuit breaker pattern for external dependencies
- [ ] Send failed messages to dead letter queues
- [ ] Wrap loads in transactions with proper rollback

### Modern Tools
- [ ] Know orchestration tools (Airflow, Dagster, Prefect)
- [ ] Understand dbt for SQL transformations
- [ ] Familiar with CDC tools (Debezium, Fivetran, Airbyte)

### Practical Skills
- [ ] Build ETL pipelines with Python and SQL
- [ ] Implement incremental loading
- [ ] Test transformations with pytest
- [ ] Monitor pipeline execution and handle failures

---

*Next Section: [06 - Database Systems](../06-Database-Systems/README.md)*
