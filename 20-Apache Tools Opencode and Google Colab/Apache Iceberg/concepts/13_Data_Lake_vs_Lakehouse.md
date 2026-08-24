# Concept 13: Data Lake vs Lakehouse

## 📚 Detailed Explanation

**Data Lake** and **Lakehouse** are two paradigms for storing and managing large-scale analytical data. Understanding their differences is crucial for modern data architecture.

### Data Lake

A **Data Lake** is a centralized repository for storing structured and unstructured data at any scale.

**Characteristics:**
```
S3 / HDFS
  ├── raw/          (unstructured)
  ├── processed/    (structured)
  └── archive/      (historical)
```

**Key Features:**
- Store any data format (Parquet, JSON, CSV, Avro)
- Low-cost storage (object storage)
- Schema-on-read (flexible)
- No ACID transactions
- No schema evolution
- No time travel

### Lakehouse

A **Lakehouse** combines the best of data lakes and data warehouses.

**Characteristics:**
```
S3 / HDFS + Iceberg
  ├── transactions/  (managed table)
  ├── customers/     (managed table)
  └── accounts/      (managed table)
```

**Key Features:**
- ACID transactions
- Schema enforcement
- Schema evolution
- Time travel
- Data quality
- Governance
- Performance optimization

### Comparison Table

| Aspect | Data Lake | Lakehouse |
|--------|-----------|-----------|
| **Storage** | Object storage | Object storage |
| **Format** | Any (Parquet, JSON, CSV) | Managed (Parquet + Iceberg) |
| **Schema** | Schema-on-read | Schema-on-write |
| **ACID** | No | Yes |
| **Schema Evolution** | Manual | Automatic |
| **Time Travel** | No | Yes |
| **Performance** | Variable | Optimized |
| **Governance** | Basic | Advanced |
| **Cost** | Low | Medium |

### Evolution Path

```
Data Lake (2010s)
  ↓
Data Warehouse (traditional)
  ↓
Data Lake (with tools)
  ↓
Lakehouse (Iceberg/Delta/Hudi)
```

---

## 💡 Example: Data Lake vs Lakehouse

### Data Lake Scenario

```
/data/lake/
  ├── transactions_2024.parquet
  ├── transactions_2025.parquet
  ├── customers.json
  ├── accounts.csv
  └── README.txt

Problems:
  - No schema enforcement
  - No ACID transactions
  - No time travel
  - Difficult to manage
```

### Lakehouse Scenario

```
/data/lakehouse/
  ├── transactions/
  │   ├── metadata/
  │   ├── data/
  │   └── schema.json
  ├── customers/
  │   ├── metadata/
  │   ├── data/
  │   └── schema.json
  └── accounts/
      ├── metadata/
      ├── data/
      └── schema.json

Benefits:
  - Schema enforced
  - ACID transactions
  - Time travel available
  - Managed tables
```

---

## 🏦 Real-World Banking Scenario 1: Migrating from Data Lake to Lakehouse

### Scenario
A bank has a **data lake** with 5 years of transaction data stored as Parquet files. The analytics team faces challenges:
- Inconsistent schemas across files
- No ACID transactions
- Difficult to query historical data
- Performance issues

### Problem
- Cannot enforce data quality
- No time travel for auditing
- Complex ETL logic

### Solution
Migrate to Lakehouse using Iceberg:
- Add schema enforcement
- Enable ACID transactions
- Enable time travel
- Optimize performance

### Python Code

```python
"""
Banking Scenario 1: Migrating from Data Lake to Lakehouse
Using Iceberg for Modern Data Architecture
"""

from pyiceberg.catalog import load_catalog
from pyiceberg.table import Table
from datetime import datetime, timedelta
import pyarrow as pa
import pyarrow.parquet as pq
import random
import os

# ============================================================
# STEP 1: Setup
# ============================================================

print("=== MIGRATING FROM DATA LAKE TO LAKEHOUSE ===\n")

# Create a temporary directory to simulate data lake
data_lake_path = "/tmp/banking_data_lake"
os.makedirs(data_lake_path, exist_ok=True)

# ============================================================
# STEP 2: Simulate Data Lake (Legacy)
# ============================================================

print("--- Simulating Data Lake (Legacy) ---")

def generate_legacy_data_lake():
    """Generate legacy Parquet files in data lake format."""
    
    files = []
    
    # Generate files for 2024 (inconsistent schemas)
    for month in range(1, 13):
        # Inconsistent schema (some files have extra columns)
        if month <= 6:
            schema = pa.schema([
                pa.field("txn_id", pa.string()),
                pa.field("acct_id", pa.string()),
                pa.field("amt", pa.float64()),
                pa.field("dt", pa.string()),
            ])
        else:
            schema = pa.schema([
                pa.field("transaction_id", pa.string()),
                pa.field("account_id", pa.string()),
                pa.field("amount", pa.float64()),
                pa.field("date", pa.string()),
                pa.field("status", pa.string()),  # Added in July
            ])
        
        # Generate data
        if month <= 6:
            data = {
                "txn_id": [f"TXN-{month}-{i:04d}" for i in range(100)],
                "acct_id": [f"ACC-{random.randint(1, 100):04d}" for _ in range(100)],
                "amt": [round(random.uniform(100, 10000), 2) for _ in range(100)],
                "dt": [f"2024-{month:02d}-{random.randint(1, 28):02d}" for _ in range(100)],
            }
        else:
            data = {
                "transaction_id": [f"TXN-{month}-{i:04d}" for i in range(100)],
                "account_id": [f"ACC-{random.randint(1, 100):04d}" for _ in range(100)],
                "amount": [round(random.uniform(100, 10000), 2) for _ in range(100)],
                "date": [f"2024-{month:02d}-{random.randint(1, 28):02d}" for _ in range(100)],
                "status": ["COMPLETED"] * 100,
            }
        
        # Write to Parquet
        table = pa.table(data)
        file_path = os.path.join(data_lake_path, f"transactions_2024_{month:02d}.parquet")
        pq.write_table(table, file_path)
        files.append(file_path)
    
    return files

# Generate legacy data lake
legacy_files = generate_legacy_data_lake()
print(f"Generated {len(legacy_files)} legacy Parquet files")

# ============================================================
# STEP 3: Identify Data Lake Issues
# ============================================================

print("\n--- Identifying Data Lake Issues ---")

print("""
DATA LAKE ISSUES:

1. INCONSISTENT SCHEMAS
   - Jan-Jun: txn_id, acct_id, amt, dt
   - Jul-Dec: transaction_id, account_id, amount, date, status
   - Problem: Column names differ

2. NO SCHEMA ENFORCEMENT
   - Any data can be written
   - No validation
   - Data quality issues

3. NO ACID TRANSACTIONS
   - Partial writes possible
   - No atomic commits
   - Data corruption risk

4. NO TIME TRAVEL
   - Cannot query historical states
   - No audit trail
   - Difficult debugging

5. PERFORMANCE ISSUES
   - No file statistics
   - No partition pruning
   - Slow queries
""")

# ============================================================
# STEP 4: Create Lakehouse with Iceberg
# ============================================================

print("--- Creating Lakehouse with Iceberg ---")

catalog = load_catalog(
    "banking_catalog",
    **{
        "uri": "http://localhost:8181",
        "warehouse": "s3a://banking-lakehouse/"
    }
)

# Define consistent schema for lakehouse
lakehouse_schema = pa.schema([
    pa.field("transaction_id", pa.string(), nullable=False),
    pa.field("account_id", pa.string(), nullable=False),
    pa.field("amount", pa.decimal128(18, 2), nullable=False),
    pa.field("transaction_date", pa.date32(), nullable=False),
    pa.field("status", pa.string(), nullable=False),
])

# Create Iceberg table
try:
    lakehouse_table = catalog.create_table(
        identifier="banking.transactions_lakehouse",
        schema=lakehouse_schema,
        partition_spec={"transform": "month", "source": "transaction_date"}
    )
    print("Created Iceberg table: banking.transactions_lakehouse")
except Exception:
    lakehouse_table = catalog.load_table("banking.transactions_lakehouse")
    print("Loaded existing Iceberg table")

# ============================================================
# STEP 5: Migrate Data to Lakehouse
# ============================================================

print("\n--- Migrating Data to Lakehouse ---")

def migrate_legacy_to_lakehouse(legacy_files: list, lakehouse_table: Table) -> dict:
    """
    Migrate legacy Parquet files to Iceberg table.
    Handles schema inconsistencies.
    """
    
    migrated_records = 0
    errors = []
    
    for file_path in legacy_files:
        try:
            # Read legacy Parquet
            legacy_data = pq.read_table(file_path)
            
            # Transform to consistent schema
            if "txn_id" in legacy_data.column_names:
                # Jan-Jun schema
                transformed = pa.table({
                    "transaction_id": legacy_data.column("txn_id"),
                    "account_id": legacy_data.column("acct_id"),
                    "amount": legacy_data.column("amt"),
                    "transaction_date": [
                        datetime.strptime(d, "%Y-%m-%d").date()
                        for d in legacy_data.column("dt").to_pylist()
                    ],
                    "status": ["COMPLETED"] * len(legacy_data),
                })
            else:
                # Jul-Dec schema
                transformed = pa.table({
                    "transaction_id": legacy_data.column("transaction_id"),
                    "account_id": legacy_data.column("account_id"),
                    "amount": legacy_data.column("amount"),
                    "transaction_date": [
                        datetime.strptime(d, "%Y-%m-%d").date()
                        for d in legacy_data.column("date").to_pylist()
                    ],
                    "status": legacy_data.column("status"),
                })
            
            # Append to Iceberg table
            lakehouse_table.append(transformed)
            migrated_records += len(transformed)
            
        except Exception as e:
            errors.append(f"Error migrating {file_path}: {str(e)}")
    
    return {
        "migrated_records": migrated_records,
        "files_processed": len(legacy_files),
        "errors": errors
    }

# Migrate data
migration_result = migrate_legacy_to_lakehouse(legacy_files, lakehouse_table)

print(f"\nMigration Results:")
print(f"  Files processed: {migration_result['files_processed']}")
print(f"  Records migrated: {migration_result['migrated_records']}")
print(f"  Errors: {len(migration_result['errors'])}")

if migration_result['errors']:
    print("\nErrors:")
    for error in migration_result['errors'][:5]:
        print(f"  - {error}")

# ============================================================
# STEP 6: Demonstrate Lakehouse Benefits
# ============================================================

print("\n--- Demonstrating Lakehouse Benefits ---")

# Query 1: Consistent schema
print("\n1. CONSISTENT SCHEMA:")
result = lakehouse_table.scan().to_arrow()
print(f"   Total records: {len(result)}")
print(f"   Schema: {[field.name for field in result.schema]}")

# Query 2: ACID transactions
print("\n2. ACID TRANSACTIONS:")
new_data = pa.table({
    "transaction_id": ["TXN-NEW-001"],
    "account_id": ["ACC-NEW-001"],
    "amount": [50000.00],
    "transaction_date": [datetime(2026, 8, 24).date()],
    "status": ["COMPLETED"],
})
lakehouse_table.append(new_data)
print(f"   New record appended (atomic commit)")
print(f"   Snapshot ID: {lakehouse_table.metadata.current_snapshot_id}")

# Query 3: Time travel
print("\n3. TIME TRAVEL:")
snapshots = lakehouse_table.metadata.snapshots
print(f"   Total snapshots: {len(snapshots)}")
print(f"   Can query historical states: Yes")

# Query 4: Partition pruning
print("\n4. PARTITION PRUNING:")
partitioned_result = lakehouse_table.scan(
    row_filter="transaction_date >= '2024-01-01' AND transaction_date < '2024-07-01'"
).to_arrow()
print(f"   Records in Jan-Jun 2024: {len(partitioned_result)}")

# ============================================================
# STEP 7: Compare Performance
# ============================================================

print("\n--- Performance Comparison ---")

import time

# Query legacy data lake
start = time.time()
legacy_query_time = 0
for file_path in legacy_files[:3]:  # Sample 3 files
    data = pq.read_table(file_path)
legacy_query_time = time.time() - start

# Query lakehouse
start = time.time()
lakehouse_result = lakehouse_table.scan().to_arrow()
lakehouse_query_time = time.time() - start

print(f"\nQuery Performance:")
print(f"  Data Lake (3 files): {legacy_query_time:.3f} seconds")
print(f"  Lakehouse (full table): {lakehouse_query_time:.3f} seconds")
print(f"  Lakehouse with partition pruning: ~{lakehouse_query_time/10:.3f} seconds")

# ============================================================
# STEP 8: Lakehouse Architecture
# ============================================================

print("\n--- Lakehouse Architecture ---")

print("""
LAKEHOUSE ARCHITECTURE:

┌─────────────────────────────────────────────┐
│                BI / ML / Apps                │
└─────────────────┬───────────────────────────┘
                  │
          Query / Data Access
                  │
         Trino / Spark / DuckDB
                  │
         ┌────────▼────────┐
         │  Apache Iceberg │  ◄── Table Format
         │   Table Format  │
         └────────┬────────┘
                  │
         ┌────────▼────────┐
         │    Parquet      │  ◄── File Format
         │   File Format   │
         └────────┬────────┘
                  │
         Object Storage (S3/GCS/ADLS)

KEY COMPONENTS:
  - Iceberg: Table management, ACID, time travel
  - Parquet: Efficient columnar storage
  - Object Storage: Scalable, cost-effective

BENEFITS:
  ✓ ACID transactions
  ✓ Schema evolution
  ✓ Time travel
  ✓ Performance optimization
  ✓ Cost-effective storage
  ✓ Open format
""")
```

---

## 🏦 Real-World Banking Scenario 2: Building a Lakehouse for Analytics

### Scenario
A bank wants to build a **modern analytics platform** using Lakehouse architecture:
- Real-time dashboards
- Regulatory reporting
- Machine learning
- Ad-hoc analysis

### Problem
- Traditional data warehouse: Expensive, limited scale
- Data lake: No governance, poor performance
- Need balance of cost, performance, governance

### Solution
Lakehouse architecture with Iceberg:
- Cost-effective storage (object storage)
- Data warehouse features (ACID, schema)
- Performance optimization (partitioning, compaction)

### Python Code

```python
"""
Banking Scenario 2: Building a Lakehouse for Analytics
Using Iceberg for Modern Analytics Platform
"""

from pyiceberg.catalog import load_catalog
from pyiceberg.table import Table
from datetime import datetime, timedelta
import pyarrow as pa
import random
import time

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
# STEP 2: Define Analytics Tables
# ============================================================

print("=== BUILDING A LAKEHOUSE FOR ANALYTICS ===\n")

# Transaction Table
transaction_schema = pa.schema([
    pa.field("transaction_id", pa.string(), nullable=False),
    pa.field("account_id", pa.string(), nullable=False),
    pa.field("amount", pa.decimal128(18, 2), nullable=False),
    pa.field("transaction_date", pa.date32(), nullable=False),
    pa.field("transaction_type", pa.string(), nullable=False),
    pa.field("status", pa.string(), nullable=False),
])

# Customer Table
customer_schema = pa.schema([
    pa.field("customer_id", pa.string(), nullable=False),
    pa.field("customer_name", pa.string(), nullable=False),
    pa.field("segment", pa.string(), nullable=False),
    pa.field("risk_score", pa.float64(), nullable=True),
])

# Create tables
try:
    txn_table = catalog.create_table(
        identifier="banking.analytics_transactions",
        schema=transaction_schema,
        partition_spec={"transform": "month", "source": "transaction_date"}
    )
except Exception:
    txn_table = catalog.load_table("banking.analytics_transactions")

try:
    cust_table = catalog.create_table(
        identifier="banking.analytics_customers",
        schema=customer_schema
    )
except Exception:
    cust_table = catalog.load_table("banking.analytics_customers")

# ============================================================
# STEP 3: Load Data
# ============================================================

print("--- Loading Data ---")

# Generate transaction data
txn_data = pa.table({
    "transaction_id": [f"TXN-{i:08d}" for i in range(1, 10001)],
    "account_id": [f"ACC-{1000 + (i % 100):06d}" for i in range(10000)],
    "amount": [round(random.uniform(100, 100000), 2) for _ in range(10000)],
    "transaction_date": [
        datetime(2024, random.randint(1, 12), random.randint(1, 28)).date()
        for _ in range(10000)
    ],
    "transaction_type": [random.choice(["CREDIT", "DEBIT", "TRANSFER"]) for _ in range(10000)],
    "status": ["COMPLETED"] * 10000,
})

txn_table.append(txn_data)
print(f"Loaded {len(txn_data)} transactions")

# Generate customer data
cust_data = pa.table({
    "customer_id": [f"CUST-{i:06d}" for i in range(1, 1001)],
    "customer_name": [f"Customer_{i}" for i in range(1, 1001)],
    "segment": [random.choice(["PREMIUM", "STANDARD", "BASIC"]) for _ in range(1000)],
    "risk_score": [round(random.uniform(0, 1), 4) for _ in range(1000)],
})

cust_table.append(cust_data)
print(f"Loaded {len(cust_data)} customers")

# ============================================================
# STEP 4: Demonstrate Analytics Use Cases
# ============================================================

print("\n--- Analytics Use Cases ---")

# Use Case 1: Real-time Dashboard
print("\n1. REAL-TIME DASHBOARD:")
start = time.time()
dashboard_result = txn_table.scan(
    row_filter="transaction_date >= '2024-06-01'"
).to_arrow()
dashboard_time = time.time() - start
print(f"   Query time: {dashboard_time:.3f} seconds")
print(f"   Records: {len(dashboard_result)}")

# Use Case 2: Regulatory Reporting
print("\n2. REGULATORY REPORTING:")
start = time.time()
regulatory_result = txn_table.scan(
    row_filter="transaction_date >= '2024-01-01' AND transaction_date < '2025-01-01'"
).to_arrow()
regulatory_time = time.time() - start
print(f"   Query time: {regulatory_time:.3f} seconds")
print(f"   Records: {len(regulatory_result)}")

# Use Case 3: Machine Learning
print("\n3. MACHINE LEARNING:")
start = time.time()
ml_result = txn_table.scan().to_arrow()
ml_time = time.time() - start
print(f"   Data preparation time: {ml_time:.3f} seconds")
print(f"   Features: {len(ml_result.column_names)}")

# Use Case 4: Ad-hoc Analysis
print("\n4. AD-HOC ANALYSIS:")
start = time.time()
adhoc_result = txn_table.scan(
    row_filter="amount > 50000"
).to_arrow()
adhoc_time = time.time() - start
print(f"   Query time: {adhoc_time:.3f} seconds")
print(f"   High-value transactions: {len(adhoc_result)}")

# ============================================================
# STEP 5: Lakehouse Benefits Summary
# ============================================================

print("\n--- Lakehouse Benefits Summary ---")

print("""
LAKEHOUSE BENEFITS FOR BANKING:

1. COST-EFFECTIVE STORAGE
   - Object storage (S3/GCS): 90% cheaper than SAN
   - Parquet compression: 70-80% space savings
   - Pay-as-you-go pricing

2. DATA WAREHOUSE FEATURES
   - ACID transactions
   - Schema enforcement
   - Schema evolution
   - Time travel

3. PERFORMANCE OPTIMIZATION
   - Partition pruning
   - File statistics
   - Compaction
   - Materialized views

4. GOVERNANCE & COMPLIANCE
   - Audit trail
   - Data lineage
   - Access control
   - Encryption

5. FLEXIBILITY
   - Multiple engines (Spark, Trino, DuckDB)
   - Open format
   - Vendor-neutral
   - Future-proof

COST COMPARISON:
  Traditional Data Warehouse: $100K/year
  Data Lake: $10K/year (but no features)
  Lakehouse: $15K/year (with features)
  
  Savings: 85% vs traditional warehouse
""")
```

---

## 🎯 5 Real-World Interview Questions

### Question 1: What is the difference between a data lake and a lakehouse?

**Answer:**

**Data Lake:**
- Centralized repository for any data
- Schema-on-read (flexible)
- No ACID transactions
- No schema evolution
- No time travel
- Low cost, poor governance

**Lakehouse:**
- Combines data lake + data warehouse
- Schema-on-write (enforced)
- ACID transactions
- Schema evolution
- Time travel
- Cost-effective with governance

**Key Difference:**
- Data Lake = Storage only
- Lakehouse = Storage + Management + Governance

**Example:**
```
Data Lake: /data/transactions/*.parquet (no management)
Lakehouse: /data/transactions/ (Iceberg table with metadata)
```

---

### Question 2: Why choose Lakehouse over a traditional data warehouse?

**Answer:**

**Lakehouse Advantages:**

1. **Cost**: 80-90% cheaper storage
2. **Scalability**: Object storage scales infinitely
3. **Flexibility**: Multiple engines (Spark, Trino, DuckDB)
4. **Open Format**: No vendor lock-in
5. **Performance**: Optimized with Iceberg features

**Data Warehouse Advantages:**

1. **Mature**: Established technology
2. **Optimized**: Purpose-built for analytics
3. **Governance**: Built-in security
4. **Support**: Enterprise support

**When to Choose Lakehouse:**
- Cost is primary concern
- Need multiple engines
- Want open format
- Have data engineering expertise

**When to Choose Data Warehouse:**
- Need enterprise support
- Have budget
- Want simplicity
- Have compliance requirements

---

### Question 3: How does Iceberg enable the lakehouse architecture?

**Answer:**

**Iceberg's Role:**

1. **Table Format**: Manages metadata and transactions
2. **ACID Transactions**: Ensures data consistency
3. **Schema Evolution**: Supports changing requirements
4. **Time Travel**: Enables auditing and debugging
5. **Performance**: Partition pruning, file statistics

**Lakehouse Stack:**
```
BI/ML → Query Engine → Iceberg → Parquet → Object Storage
```

**Key Features:**
- Iceberg = Table management layer
- Parquet = Efficient storage format
- Object Storage = Cost-effective infrastructure

**Example:**
```sql
-- Iceberg enables this in a lakehouse:
CREATE TABLE transactions (
    id STRING,
    amount DECIMAL(18,2)
) USING iceberg;

-- With ACID, schema evolution, time travel
```

---

### Question 4: What are the challenges of implementing a lakehouse?

**Answer:**

**Challenges:**

1. **Complexity**
   - Multiple components to manage
   - Requires data engineering expertise
   - Debugging can be difficult

2. **Performance**
   - Not as fast as purpose-built warehouse
   - Requires optimization (partitioning, compaction)
   - May need tuning

3. **Governance**
   - Need to implement security
   - Access control requires setup
   - Compliance may need additional tools

4. **Skills**
   - Team needs new skills
   - Training required
   - Hiring challenges

**Solutions:**
- Start with managed services
- Use open-source tools
- Invest in training
- Implement gradually

---

### Question 5: How do you migrate from a data warehouse to a lakehouse?

**Answer:**

**Migration Steps:**

1. **Assess Current State**
   - Inventory tables and schemas
   - Identify critical workloads
   - Assess data volumes

2. **Design Target Architecture**
   - Choose Iceberg as table format
   - Design schema and partitioning
   - Plan governance

3. **Migrate Data**
   - Use CDC or batch migration
   - Validate data integrity
   - Test queries

4. **Migrate Workloads**
   - Update ETL jobs
   - Update BI tools
   - Test performance

5. **Validate and Optimize**
   - Compare results
   - Optimize performance
   - Monitor costs

**Best Practices:**
- Migrate incrementally
- Keep old system running
- Validate thoroughly
- Train team

---

## 📝 Summary

| Aspect | Data Lake | Lakehouse |
|--------|-----------|-----------|
| **Storage** | Object storage | Object storage |
| **Format** | Any | Managed (Iceberg) |
| **Schema** | On-read | On-write |
| **ACID** | No | Yes |
| **Time Travel** | No | Yes |
| **Governance** | Basic | Advanced |
| **Cost** | Low | Medium |
| **Use Case** | Raw storage | Analytics platform |
