# Concept 08: Partitioning and Hidden Partitioning

## 📚 Detailed Explanation

**Partitioning** in Apache Iceberg is the process of organizing data into logical groups based on column values. **Hidden Partitioning** is Iceberg's ability to manage partitioning transparently, so users don't need to know the physical layout.

### What is Partitioning?

Partitioning divides a table into segments based on column values:

```
Without Partitioning:
transactions/
  ├── part-001.parquet (1M rows)
  ├── part-002.parquet (1M rows)
  └── part-003.parquet (1M rows)

With Partitioning by date:
transactions/
  ├── date=2026-08-24/
  │   ├── part-001.parquet
  │   └── part-002.parquet
  ├── date=2026-08-25/
  │   └── part-003.parquet
  └── date=2026-08-26/
      └── part-004.parquet
```

### Why Partitioning Matters

**Without Partitioning:**
```sql
SELECT * FROM transactions WHERE date = '2026-08-24';
-- Scans ALL files (millions of rows)
```

**With Partitioning:**
```sql
SELECT * FROM transactions WHERE date = '2026-08-24';
-- Scans ONLY partition for Aug 24 (thousands of rows)
```

**Performance Improvement**: 100-1000x faster queries

### Partition Transformations

Iceberg supports various transformations:

| Transform | Description | Example |
|-----------|-------------|---------|
| **Identity** | No transformation | `partitioned by (date)` |
| **Bucket** | Hash into N buckets | `partitioned by (bucket(16, user_id))` |
| **Truncate** | Truncate to N chars | `partitioned by (truncate(16, name))` |
| **Year** | Extract year | `partitioned by (years(date))` |
| **Month** | Extract month | `partitioned by (months(date))` |
| **Day** | Extract day | `partitioned by (days(date))` |
| **Hour** | Extract hour | `partitioned by (hours(timestamp))` |

### Hidden Partitioning

**Traditional Hive Partitioning:**
```sql
-- User must know partition structure
SELECT * FROM transactions 
WHERE year = 2026 AND month = 8 AND day = 24;
```

**Iceberg Hidden Partitioning:**
```sql
-- User queries business column
SELECT * FROM transactions 
WHERE transaction_date = '2026-08-24';

-- Iceberg handles partition pruning automatically
```

**Benefits:**
- Users don't need to know physical layout
- Partition strategy can change without query changes
- Simpler SQL queries

---

## 💡 Example: Partitioning in Banking

### Scenario: Daily Transaction Query

**Table: transactions (10 billion rows)**
```
partitioned by days(transaction_date)
```

**Query:**
```sql
SELECT SUM(amount) 
FROM transactions 
WHERE transaction_date = '2026-08-24';
```

**Without Partitioning:**
- Scan: 10 billion rows
- Time: 30 minutes
- Cost: High

**With Partitioning:**
- Scan: ~30 million rows (Aug 24 only)
- Time: 1 second
- Cost: Low

---

## 🏦 Real-World Banking Scenario 1: Monthly Reporting Partitioning

### Scenario
A bank generates **monthly financial reports**. The transaction table has **10 billion rows** spanning 5 years. Reports need to query specific months efficiently.

### Problem
- Queries scan too many files
- Report generation takes hours
- Need month-level partitioning

### Solution
Partition by month:
```
transactions/
  ├── month=2026-01/
  ├── month=2026-02/
  ├── ...
  └── month=2026-08/
```

### Python Code

```python
"""
Banking Scenario 1: Monthly Reporting Partitioning
Using Iceberg Partitioning for Efficient Queries
"""

from pyiceberg.catalog import load_catalog
from pyiceberg.table import Table
from datetime import datetime, timedelta
import pyarrow as pa
import random

# ============================================================
# STEP 1: Setup Catalog
# ============================================================

catalog = load_catalog(
    "banking_catalog",
    **{
        "uri": "http://localhost:8181",
        "warehouse": "s3a://banking-lakehouse/"
    }
)

# ============================================================
# STEP 2: Define Partitioned Schema
# ============================================================

print("=== MONTHLY REPORTING PARTITIONING ===\n")

# Schema with transaction_date for partitioning
schema = pa.schema([
    pa.field("transaction_id", pa.string(), nullable=False),
    pa.field("account_id", pa.string(), nullable=False),
    pa.field("amount", pa.decimal128(18, 2), nullable=False),
    pa.field("transaction_date", pa.date32(), nullable=False),
    pa.field("transaction_type", pa.string(), nullable=False),
    pa.field("status", pa.string(), nullable=False),
])

# Create table with monthly partitioning
try:
    txn_table = catalog.create_table(
        identifier="banking.transactions_monthly",
        schema=schema,
        partition_spec={"transform": "month", "source": "transaction_date"}
    )
    print("Created table with MONTH partitioning")
except Exception:
    txn_table = catalog.load_table("banking.transactions_monthly")
    print("Loaded existing table")

# ============================================================
# STEP 3: Load Data Across Multiple Months
# ============================================================

print("\n--- Loading Data Across Multiple Months ---")

def generate_monthly_transactions(year: int, month: int, count: int) -> pa.Table:
    """Generate transactions for a specific month."""
    
    # Generate dates within the month
    start_date = datetime(year, month, 1)
    if month == 12:
        end_date = datetime(year + 1, 1, 1)
    else:
        end_date = datetime(year, month + 1, 1)
    
    dates = [
        start_date + timedelta(days=random.randint(0, (end_date - start_date).days - 1))
        for _ in range(count)
    ]
    
    data = {
        "transaction_id": [f"TXN-{year}{month:02d}-{i:06d}" for i in range(1, count + 1)],
        "account_id": [f"ACC-{1000 + (i % 100):06d}" for i in range(count)],
        "amount": [round(random.uniform(100, 50000), 2) for _ in range(count)],
        "transaction_date": [d.date() for d in dates],
        "transaction_type": [random.choice(["CREDIT", "DEBIT", "TRANSFER"]) for _ in range(count)],
        "status": ["COMPLETED"] * count,
    }
    return pa.table(data)

# Load data for multiple months
months_to_load = [
    (2026, 1, 5000),   # January: 5K transactions
    (2026, 2, 6000),   # February: 6K
    (2026, 3, 5500),   # March: 5.5K
    (2026, 4, 7000),   # April: 7K
    (2026, 5, 8000),   # May: 8K
    (2026, 6, 9000),   # June: 9K
    (2026, 7, 10000),  # July: 10K
    (2026, 8, 12000),  # August: 12K
]

total_records = 0
for year, month, count in months_to_load:
    monthly_data = generate_monthly_transactions(year, month, count)
    txn_table.append(monthly_data)
    total_records += count
    print(f"  {year}-{month:02d}: {count:,} transactions")

print(f"\nTotal records loaded: {total_records:,}")

# ============================================================
# STEP 4: Demonstrate Partition Pruning
# ============================================================

print("\n--- Partition Pruning Demo ---")

# Query 1: Single month
print("\nQuery 1: SELECT SUM(amount) FROM transactions WHERE transaction_date >= '2026-08-01' AND transaction_date < '2026-09-01'")
print("  Without partitioning: Scan all 62,500 rows")
print("  With partitioning: Scan only August partition (12,000 rows)")
print("  Improvement: 5x faster")

# Query 2: Multiple months
print("\nQuery 2: SELECT SUM(amount) FROM transactions WHERE transaction_date >= '2026-06-01' AND transaction_date < '2026-09-01'")
print("  Without partitioning: Scan all 62,500 rows")
print("  With partitioning: Scan only Jun, Jul, Aug partitions (31,000 rows)")
print("  Improvement: 2x faster")

# ============================================================
# STEP 5: Query Performance Test
# ============================================================

print("\n--- Query Performance Test ---")

import time

# Query specific month
start = time.time()
result = txn_table.scan(
    row_filter="transaction_date >= '2026-08-01' AND transaction_date < '2026-09-01'"
).to_arrow()
query_time = time.time() - start

print(f"Query: August 2026 transactions")
print(f"Rows returned: {len(result)}")
print(f"Query time: {query_time:.3f} seconds")

# ============================================================
# STEP 6: Partition Statistics
# ============================================================

print("\n--- Partition Statistics ---")

# Get metadata
metadata = txn_table.metadata
current_snapshot = metadata.current_snapshot()
manifests = current_snapshot.manifest_list.manifests()

print(f"Total Manifests: {len(manifests)}")

# Analyze partition distribution
partition_counts = {}
for manifest in manifests:
    files = manifest.data_files()
    for file_entry in files:
        # Get partition value (transaction_date)
        partition_value = file_entry.partition.get(0)  # First partition column
        if partition_value not in partition_counts:
            partition_counts[partition_value] = 0
        partition_counts[partition_value] += file_entry.record_count

print(f"\nPartition Distribution:")
for partition, count in sorted(partition_counts.items()):
    print(f"  {partition}: {count:,} rows")

# ============================================================
# STEP 7: Hidden Partitioning Benefits
# ============================================================

print("\n--- Hidden Partitioning Benefits ---")

print("""
HIDDEN PARTITIONING IN ACTION:

1. USER QUERY (simple):
   SELECT * FROM transactions 
   WHERE transaction_date = '2026-08-24';

2. ICEBERG AUTOMATICALLY:
   a. Identifies partition column (transaction_date)
   b. Applies transformation (month)
   c. Maps '2026-08-24' to partition 'month=2026-08'
   d. Reads only that partition

3. USER DOESN'T NEED TO KNOW:
   - Physical partition structure
   - Directory layout
   - Transformation details

4. BENEFITS:
   - Simpler SQL queries
   - Partition strategy can change
   - No query rewrites needed
""")

# ============================================================
# STEP 8: Partition Maintenance
# ============================================================

print("--- Partition Maintenance ---")

print("""
PARTITION MAINTENANCE OPERATIONS:

1. MONITOR PARTITION SIZES
   - Identify small partitions (merge)
   - Identify large partitions (split)
   - Balance data distribution

2. COMPACTION
   - Merge small files within partitions
   - Optimize file sizes (128MB-1GB)
   - Improve query performance

3. EXPIRATION
   - Remove old partitions
   - Reduce storage costs
   - Maintain retention policies

4. EVOLUTION
   - Change partition strategy
   - Add new partition columns
   - Support changing query patterns
""")
```

---

## 🏦 Real-World Banking Scenario 2: Fraud Detection with Bucket Partitioning

### Scenario
A bank's **fraud detection system** needs to query transactions by `customer_id`. The table has **10 billion rows**. Queries filter by customer for:
- Customer 360 view
- Fraud pattern analysis
- Suspicious activity monitoring

### Problem
- Range partitioning by date doesn't help customer-specific queries
- Need to distribute data evenly across partitions
- Avoid data skew

### Solution
Bucket partitioning by customer_id:
```
partitioned by bucket(16, customer_id)
```

### Python Code

```python
"""
Banking Scenario 2: Fraud Detection with Bucket Partitioning
Using Iceberg Bucket Partitioning for Customer Queries
"""

from pyiceberg.catalog import load_catalog
from pyiceberg.table import Table
from datetime import datetime
import pyarrow as pa
import random

# ============================================================
# STEP 1: Setup Catalog
# ============================================================

catalog = load_catalog(
    "banking_catalog",
    **{
        "uri": "http://localhost:8181",
        "warehouse": "s3a://banking-lakehouse/"
    }
)

# ============================================================
# STEP 2: Define Schema with Bucket Partitioning
# ============================================================

print("=== FRAUD DETECTION WITH BUCKET PARTITIONING ===\n")

schema = pa.schema([
    pa.field("transaction_id", pa.string(), nullable=False),
    pa.field("customer_id", pa.string(), nullable=False),
    pa.field("amount", pa.decimal128(18, 2), nullable=False),
    pa.field("merchant_id", pa.string(), nullable=False),
    pa.field("transaction_timestamp", pa.timestamp("us"), nullable=False),
    pa.field("fraud_score", pa.float64(), nullable=True),
    pa.field("is_fraud", pa.boolean(), nullable=True),
])

# Create table with bucket partitioning
try:
    fraud_table = catalog.create_table(
        identifier="banking.fraud_transactions",
        schema=schema,
        partition_spec={"transform": "bucket(16, customer_id)"}
    )
    print("Created table with BUCKET(16, customer_id) partitioning")
except Exception:
    fraud_table = catalog.load_table("banking.fraud_transactions")
    print("Loaded existing table")

# ============================================================
# STEP 3: Load Transaction Data
# ============================================================

print("\n--- Loading Transaction Data ---")

def generate_fraud_transactions(count: int) -> pa.Table:
    """Generate transaction data for fraud detection."""
    
    # Create customer distribution (some customers have more transactions)
    customer_ids = []
    for i in range(count):
        if random.random() < 0.1:  # 10% are "frequent" customers
            customer_ids.append(f"CUST-{random.randint(1, 100):06d}")
        else:
            customer_ids.append(f"CUST-{random.randint(1, 10000):06d}")
    
    data = {
        "transaction_id": [f"TXN-{i:08d}" for i in range(1, count + 1)],
        "customer_id": customer_ids,
        "amount": [round(random.uniform(10, 100000), 2) for _ in range(count)],
        "merchant_id": [f"MERCHANT-{random.randint(1, 5000):06d}" for _ in range(count)],
        "transaction_timestamp": [
            datetime(2026, 8, 24, random.randint(0, 23), random.randint(0, 59))
            for _ in range(count)
        ],
        "fraud_score": [round(random.uniform(0, 1), 4) for _ in range(count)],
        "is_fraud": [random.choice([True, False, False, False, False]) for _ in range(count)],
    }
    return pa.table(data)

# Load 100,000 transactions
transactions = generate_fraud_transactions(100000)
fraud_table.append(transactions)
print(f"Loaded {len(transactions):,} transactions")

# ============================================================
# STEP 4: Demonstrate Bucket Partitioning
# ============================================================

print("\n--- Bucket Partitioning Demo ---")

# Get metadata
metadata = fraud_table.metadata
current_snapshot = metadata.current_snapshot()
manifests = current_snapshot.manifest_list.manifacts()

print(f"Total Manifests: {len(manifests)}")

# Analyze bucket distribution
bucket_counts = {}
for manifest in manifests:
    files = manifest.data_files()
    for file_entry in files:
        # Get bucket value (customer_id hash)
        bucket_value = file_entry.partition.get(0)  # First partition column
        if bucket_value not in bucket_counts:
            bucket_counts[bucket_value] = 0
        bucket_counts[bucket_value] += file_entry.record_count

print(f"\nBucket Distribution:")
for bucket, count in sorted(bucket_counts.items()):
    print(f"  Bucket {bucket}: {count:,} rows")

# ============================================================
# STEP 5: Customer-Specific Query
# ============================================================

print("\n--- Customer-Specific Query ---")

# Query for specific customer
customer_id = "CUST-000001"

print(f"Query: SELECT * FROM transactions WHERE customer_id = '{customer_id}'")
print(f"\nWith Bucket Partitioning:")
print(f"  1. Hash customer_id to find bucket")
print(f"  2. Read only files in that bucket")
print(f"  3. Filter by customer_id within bucket")

# Execute query
start = time.time()
result = fraud_table.scan(
    row_filter=f"customer_id = '{customer_id}'"
).to_arrow()
query_time = time.time() - start

print(f"\nResults:")
print(f"  Rows found: {len(result)}")
print(f"  Query time: {query_time:.3f} seconds")

# ============================================================
# STEP 6: Fraud Detection Query
# ============================================================

print("\n--- Fraud Detection Query ---")

# Query high-risk transactions
print("Query: High-risk transactions (fraud_score > 0.8)")

start = time.time()
high_risk = fraud_table.scan(
    row_filter="fraud_score > 0.8"
).to_arrow()
query_time = time.time() - start

print(f"\nResults:")
print(f"  High-risk transactions: {len(high_risk)}")
print(f"  Query time: {query_time:.3f} seconds")

# Count fraud cases
fraud_count = sum(1 for i in range(len(high_risk)) if high_risk.column("is_fraud")[i].as_py())
print(f"  Confirmed fraud: {fraud_count}")

# ============================================================
# STEP 7: Bucket Partitioning Benefits
# ============================================================

print("\n--- Bucket Partitioning Benefits ---")

print("""
BUCKET PARTITIONING ADVANTAGES:

1. EVEN DATA DISTRIBUTION
   - Hash function distributes evenly
   - No data skew
   - Balanced partition sizes

2. EFFICIENT POINT LOOKUPS
   - Customer-specific queries fast
   - Only read relevant bucket
   - No full table scan

3. SUPPORT FOR HIGH CARDINALITY
   - Works with millions of customers
   - No partition explosion
   - Efficient bucket pruning

4. FLEXIBLE QUERY PATTERNS
   - Works for customer queries
   - Works for amount queries
   - Works for time queries

5. COMBINABLE WITH OTHER PARTITIONS
   - bucket(customer_id) + days(timestamp)
   - Multi-dimensional partitioning
   - Optimized for multiple query patterns
""")

# ============================================================
# STEP 8: Compare Partitioning Strategies
# ============================================================

print("--- Compare Partitioning Strategies ---")

print("""
For Fraud Detection Table:

STRATEGY 1: Partition by Date
  partitioned by days(transaction_timestamp)
  ✓ Good for: Date range queries
  ✗ Bad for: Customer-specific queries
  
STRATEGY 2: Partition by Customer
  partitioned by bucket(16, customer_id)
  ✓ Good for: Customer-specific queries
  ✗ Bad for: Date range queries (scan all buckets)
  
STRATEGY 3: Composite Partitioning
  partitioned by days(transaction_timestamp), bucket(16, customer_id)
  ✓ Good for: Both query types
  ✗ Bad for: More complex, more files

RECOMMENDATION: Strategy 2 (Bucket) for fraud detection
  - Primary query pattern: Customer-specific
  - Secondary: Date range (acceptable performance)
""")
```

---

## 🎯 5 Real-World Interview Questions

### Question 1: What is hidden partitioning and why is it important?

**Answer:**

**Hidden Partitioning:**
- Partition transformation defined in table metadata
- Users query business columns, not partition columns
- Iceberg handles partition pruning automatically

**Example:**
```sql
-- Table defined with:
-- partitioned by days(transaction_date)

-- User query (simple):
SELECT * FROM transactions 
WHERE transaction_date = '2026-08-24';

-- Iceberg automatically:
-- 1. Maps transaction_date to partition
-- 2. Reads only relevant partition
-- 3. User doesn't know physical layout
```

**Importance:**
1. **Simpler SQL**: No need to know partition structure
2. **Flexibility**: Change partition strategy without query changes
3. **Safety**: Prevents partition-related errors
4. **Performance**: Automatic partition pruning

---

### Question 2: Compare different partitioning strategies for a banking transaction table.

**Answer:**

**Strategy 1: Partition by Date**
```sql
partitioned by days(transaction_date)
```
- ✓ Good for: Date range queries, monthly reports
- ✗ Bad for: Customer-specific queries
- ✓ Data distribution: Even (by time)
- ✓ Partition count: Predictable

**Strategy 2: Partition by Customer**
```sql
partitioned by bucket(16, customer_id)
```
- ✓ Good for: Customer 360, fraud detection
- ✗ Bad for: Date range queries
- ✓ Data distribution: Even (hash-based)
- ✓ Partition count: Fixed (16 buckets)

**Strategy 3: Composite Partitioning**
```sql
partitioned by days(transaction_date), bucket(16, customer_id)
```
- ✓ Good for: Both query types
- ✗ Bad for: More files, complex
- ✓ Data distribution: Even
- ✓ Partition count: High (days × buckets)

**Recommendation:**
- **Reporting table**: Partition by date
- **Fraud table**: Partition by customer
- **OLAP table**: Composite partitioning

---

### Question 3: How does bucket partitioning work and when should you use it?

**Answer:**

**Bucket Partitioning:**
- Hash column value into N buckets
- Each bucket contains ~equal rows
- Query uses same hash to find bucket

**Example:**
```sql
partitioned by bucket(16, customer_id)

-- Query for customer_id = 'CUST-001'
-- Hash('CUST-001') % 16 = 7
-- Read only bucket 7
```

**When to Use:**
1. **High Cardinality Columns**: Millions of customers
2. **Point Lookups**: Customer-specific queries
3. **Even Distribution**: Avoid data skew
4. **Fixed Partition Count**: Don't want partition explosion

**When NOT to Use:**
1. **Range Queries**: Bucketing doesn't help
2. **Low Cardinality**: Few distinct values
3. **Time-Series**: Date partitioning better

**Banking Example:**
```
Customer table: bucket(16, customer_id) ✓
Transaction table: days(transaction_date) ✓
Fraud table: bucket(16, customer_id) ✓
```

---

### Question 4: What are the performance implications of partitioning choices?

**Answer:**

**Good Partitioning:**
- Query scans minimal data (partition pruning)
- Even data distribution (no skew)
- Reasonable partition count (10s-1000s)

**Bad Partitioning:**
- Query scans many partitions (no pruning)
- Uneven distribution (hot partitions)
- Too many partitions (metadata overhead)

**Performance Metrics:**

| Scenario | Rows Scanned | Query Time |
|----------|--------------|------------|
| No partitioning | 10 billion | 30 minutes |
| Date partitioning (monthly) | 100 million | 30 seconds |
| Date partitioning (daily) | 30 million | 1 second |
| Bucket partitioning | 600 million | 10 seconds |

**Optimization Tips:**
1. **Match query patterns**: Partition by frequently filtered columns
2. **Avoid over-partitioning**: Don't create too many small partitions
3. **Monitor skew**: Ensure even data distribution
4. **Use composite**: When multiple query patterns exist

---

### Question 5: How do you handle partition evolution in Iceberg?

**Answer:**

**Partition Evolution:**
- Change partition strategy without rewriting data
- Old data uses old partition spec
- New data uses new partition spec
- Queries work seamlessly

**Example:**
```sql
-- Initial: Monthly partitioning
ALTER TABLE transactions ADD PARTITION SPEC month(transaction_date);

-- After 2 years, switch to daily
ALTER TABLE transactions ADD PARTITION SPEC day(transaction_date);

-- Old data: Monthly partitions
-- New data: Daily partitions
-- Queries: Automatic partition pruning
```

**Benefits:**
1. **No Data Rewrite**: Old files stay as-is
2. **Gradual Migration**: Switch over time
3. **Query Continuity**: No SQL changes needed
4. **Cost Savings**: Avoid expensive migrations

**Best Practices:**
1. **Plan Ahead**: Consider future query patterns
2. **Monitor Usage**: Track query patterns
3. **Test Changes**: Validate before production
4. **Document Specs**: Record partition history

---

## 📝 Summary

| Aspect | Key Point |
|--------|-----------|
| **Definition** | Organize data into logical groups |
| **Hidden Partitioning** | Transparent to users |
| **Transforms** | Identity, bucket, truncate, time-based |
| **Benefits** | Faster queries, lower costs |
| **Trade-offs** | Write overhead, partition management |
| **Best Practice** | Match query patterns |
| **Evolution** | Change strategy without rewrite |
| **Banking Use Cases** | Monthly reports, fraud detection |
