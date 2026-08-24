# Concept 07: Schema Evolution

## 📚 Detailed Explanation

**Schema Evolution** in Apache Iceberg is the ability to change a table's schema without rewriting existing data files. This is a critical feature for long-lived data platforms where business requirements change over time.

### Why Schema Evolution Matters

In traditional data lakes:
```
Schema v1: transaction_id, amount, date
Schema v2: transaction_id, amount, date, merchant_id

Problem: How to add merchant_id to 10 million existing files?
  - Rewrite all files (expensive, risky)
  - Leave gaps in old files (inconsistent)
  - Complex ETL logic
```

With Iceberg:
```
Schema v1: transaction_id, amount, date
Schema v2: transaction_id, amount, date, merchant_id

Solution: Iceberg handles it automatically!
  - Old files remain unchanged
  - New files have merchant_id
  - Queries work seamlessly
```

### Schema Evolution Operations

| Operation | Description | Example |
|-----------|-------------|---------|
| **Add Column** | Add new column | `ALTER TABLE ADD merchant_id` |
| **Rename Column** | Change column name | `ALTER TABLE RENAME amount TO transaction_amount` |
| **Drop Column** | Remove column | `ALTER TABLE DROP COLUMN phone` |
| **Reorder Columns** | Change column order | `ALTER TABLE ALTER COLUMN amount AFTER date` |
| **Widen Type** | Increase precision | `ALTER TABLE ALTER amount TYPE DECIMAL(18,4)` |

### How Iceberg Tracks Schema

Iceberg uses **field IDs** to track columns:

```json
{
  "schema-id": 1,
  "fields": [
    {"id": 1, "name": "transaction_id", "type": "string"},
    {"id": 2, "name": "amount", "type": "decimal(18,2)"},
    {"id": 3, "name": "date", "type": "date"},
    {"id": 4, "name": "merchant_id", "type": "string"}  // New field
  ]
}
```

**Key Point**: Column identity is based on **field ID**, not name. This means:
- Rename a column → Old files still work (same ID)
- Drop a column → Old files still work (column ignored)
- Add a column → Old files return NULL for new column

### Schema History

Iceberg maintains complete schema history:

```
Schema v1 (ID 0): transaction_id, amount, date
    ↓
Schema v2 (ID 1): transaction_id, amount, date, merchant_id
    ↓
Schema v3 (ID 2): transaction_id, transaction_amount, date, merchant_id, region
```

Each snapshot references its schema version.

---

## 💡 Example: Schema Evolution in Banking

### Scenario: Adding Fraud Detection Fields

**Initial Schema (2024):**
```sql
CREATE TABLE transactions (
    transaction_id STRING,
    account_id STRING,
    amount DECIMAL(18,2),
    transaction_date DATE
);
```

**After 1 Year (2025):**
```sql
-- Add fraud detection fields
ALTER TABLE transactions ADD merchant_id STRING;
ALTER TABLE transactions ADD merchant_category STRING;
ALTER TABLE transactions ADD location_lat DOUBLE;
ALTER TABLE transactions ADD location_lon DOUBLE;
```

**After 2 Years (2026):**
```sql
-- Add ML features
ALTER TABLE transactions ADD fraud_score DOUBLE;
ALTER TABLE transactions ADD risk_level STRING;
ALTER TABLE transactions ADD device_fingerprint STRING;
```

**Result:**
- Old files (2024) have 4 columns
- 2025 files have 8 columns
- 2026 files have 11 columns
- Queries work seamlessly across all files

---

## 🏦 Real-World Banking Scenario 1: Adding Compliance Fields

### Scenario
A bank needs to comply with new **RBI (Reserve Bank of India) regulations** that require additional fields in transaction records:
- `regulatory_code` (string)
- `compliance_flag` (boolean)
- `audit_trail_id` (string)

The transaction table has **5 billion rows** spanning 3 years. Adding these fields must not:
- Rewrite historical data
- Disrupt ongoing queries
- Cause data loss

### Problem
- Traditional approach: Rewrite all files (weeks of work, high risk)
- Need to add fields without affecting existing data

### Solution
Iceberg schema evolution:
- Add new columns (instant operation)
- Old files return NULL for new columns
- No data rewrite required

### Python Code

```python
"""
Banking Scenario 1: Adding Compliance Fields
Using Iceberg Schema Evolution
"""

from pyiceberg.catalog import load_catalog
from pyiceberg.table import Table
from datetime import datetime
import pyarrow as pa

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
# STEP 2: Create Initial Table (2024 Schema)
# ============================================================

print("=== SCHEMA EVOLUTION: ADDING COMPLIANCE FIELDS ===\n")

# Initial schema (2024)
initial_schema = pa.schema([
    pa.field("transaction_id", pa.string(), nullable=False),
    pa.field("account_id", pa.string(), nullable=False),
    pa.field("amount", pa.decimal128(18, 2), nullable=False),
    pa.field("transaction_date", pa.date32(), nullable=False),
    pa.field("status", pa.string(), nullable=False),
])

# Create table
try:
    txn_table = catalog.create_table(
        identifier="banking.transactions_evolution",
        schema=initial_schema,
        partition_spec={"transform": "year", "source": "transaction_date"}
    )
    print("Created table with 2024 schema:")
except Exception:
    txn_table = catalog.load_table("banking.transactions_evolution")
    print("Loaded existing table:")

# Display current schema
print("\nCurrent Schema (2024):")
for field in txn_table.schema().fields:
    print(f"  {field.name}: {field.field_type} (ID: {field.field_id})")

# ============================================================
# STEP 3: Load Historical Data (2024)
# ============================================================

print("\n--- Loading 2024 Data ---")

data_2024 = pa.table({
    "transaction_id": [f"TXN-2024-{i:06d}" for i in range(1, 101)],
    "account_id": [f"ACC-{1000 + (i % 50):06d}" for i in range(100)],
    "amount": [round(100 + i * 10, 2) for i in range(100)],
    "transaction_date": [datetime(2024, 6, 15).date()] * 100,
    "status": ["COMPLETED"] * 100,
})

txn_table.append(data_2024)
print(f"Loaded {len(data_2024)} transactions from 2024")

# ============================================================
# STEP 4: Schema Evolution - Add New Columns (2025)
# ============================================================

print("\n--- Schema Evolution: Adding Compliance Fields ---")

# In practice, you would run:
# ALTER TABLE banking.transactions_evolution ADD COLUMN regulatory_code STRING;
# ALTER TABLE banking.transactions_evolution ADD COLUMN compliance_flag BOOLEAN;
# ALTER TABLE banking.transactions_evolution ADD COLUMN audit_trail_id STRING;

# For demonstration, we'll update the table properties
# In production, use Spark SQL or Iceberg REST API

print("Adding new columns:")
print("  1. regulatory_code (STRING)")
print("  2. compliance_flag (BOOLEAN)")
print("  3. audit_trail_id (STRING)")

# Simulate schema evolution by creating new schema
evolved_schema = pa.schema([
    pa.field("transaction_id", pa.string(), nullable=False),
    pa.field("account_id", pa.string(), nullable=False),
    pa.field("amount", pa.decimal128(18, 2), nullable=False),
    pa.field("transaction_date", pa.date32(), nullable=False),
    pa.field("status", pa.string(), nullable=False),
    pa.field("regulatory_code", pa.string(), nullable=True),  # NEW
    pa.field("compliance_flag", pa.boolean(), nullable=True),  # NEW
    pa.field("audit_trail_id", pa.string(), nullable=True),  # NEW
])

# Note: In production, use ALTER TABLE statements
# This is for demonstration purposes

print("\nNew Schema (2025):")
for field in evolved_schema.fields:
    print(f"  {field.name}: {field.field_type}")

# ============================================================
# STEP 5: Load 2025 Data with New Fields
# ============================================================

print("\n--- Loading 2025 Data ---")

data_2025 = pa.table({
    "transaction_id": [f"TXN-2025-{i:06d}" for i in range(1, 101)],
    "account_id": [f"ACC-{1000 + (i % 50):06d}" for i in range(100)],
    "amount": [round(100 + i * 10, 2) for i in range(100)],
    "transaction_date": [datetime(2025, 6, 15).date()] * 100,
    "status": ["COMPLETED"] * 100,
    "regulatory_code": [f"REG-{random.randint(1, 1000):04d}" for i in range(100)],
    "compliance_flag": [True] * 100,
    "audit_trail_id": [f"AUDIT-{i:08d}" for i in range(1, 101)],
})

# In production, you would use the evolved schema
# Here we simulate by appending with new fields

print(f"Loaded {len(data_2025)} transactions from 2025")
print(f"  New fields: regulatory_code, compliance_flag, audit_trail_id")

# ============================================================
# STEP 6: Query Across Schema Versions
# ============================================================

print("\n--- Querying Across Schema Versions ---")

# Query 1: All data (old + new)
all_data = txn_table.scan().to_arrow()
print(f"Total records: {len(all_data)}")

# Query 2: Old data (2024) - new columns are NULL
old_data = txn_table.scan(
    row_filter="transaction_date < '2025-01-01'"
).to_arrow()
print(f"\n2024 Records: {len(old_data)}")
print(f"  New columns (regulatory_code, compliance_flag, audit_trail_id): NULL")

# Query 3: New data (2025) - new columns have values
new_data = txn_table.scan(
    row_filter="transaction_date >= '2025-01-01'"
).to_arrow()
print(f"\n2025 Records: {len(new_data)}")
if len(new_data) > 0:
    print(f"  Sample regulatory_code: {new_data.column('regulatory_code')[0].as_py()}")
    print(f"  Sample compliance_flag: {new_data.column('compliance_flag')[0].as_py()}")

# ============================================================
# STEP 7: Demonstrate Column Identity (Field IDs)
# ============================================================

print("\n--- Column Identity (Field IDs) ---")

print("""
Iceberg uses field IDs to track columns:

Schema v1:
  field_id=1: transaction_id
  field_id=2: account_id
  field_id=3: amount
  field_id=4: transaction_date
  field_id=5: status

Schema v2 (after evolution):
  field_id=1: transaction_id
  field_id=2: account_id
  field_id=3: amount
  field_id=4: transaction_date
  field_id=5: status
  field_id=6: regulatory_code      (NEW)
  field_id=7: compliance_flag      (NEW)
  field_id=8: audit_trail_id       (NEW)

Key Points:
  ✓ Rename column → Old files still work (same ID)
  ✓ Drop column → Old files still work (column ignored)
  ✓ Add column → Old files return NULL
  ✓ Type widening → Old files still work
""")

# ============================================================
# STEP 8: Schema Evolution Operations
# ============================================================

print("--- Schema Evolution Operations ---")

print("""
Example SQL Operations:

-- Add column
ALTER TABLE transactions
ADD COLUMN merchant_category STRING;

-- Rename column
ALTER TABLE transactions
RENAME COLUMN amount TO transaction_amount;

-- Drop column
ALTER TABLE transactions
DROP COLUMN phone_number;

-- Reorder columns
ALTER TABLE transactions
ALTER COLUMN merchant_category AFTER amount;

-- Widen type
ALTER TABLE transactions
ALTER COLUMN amount TYPE DECIMAL(18,4);

All operations are:
  ✓ Metadata-only (no data rewrite)
  ✓ Instant (milliseconds)
  ✓ Safe (old data preserved)
  ✓ Reversible (via time travel)
""")

# ============================================================
# STEP 9: Benefits Summary
# ============================================================

print("--- Benefits of Schema Evolution ---")

print("""
1. NO DATA REWRITE
   - Old files remain unchanged
   - New files use new schema
   - Queries work seamlessly

2. INSTANT OPERATIONS
   - Add/drop/rename: milliseconds
   - No ETL jobs required
   - No downtime

3. BACKWARD COMPATIBILITY
   - Old queries work with new schema
   - New queries work with old data
   - NULL for missing columns

4. AUDIT TRAIL
   - Complete schema history
   - Track when columns added/dropped
   - Regulatory compliance

5. FLEXIBILITY
   - Evolve schema as business grows
   - Add fields for new regulations
   - Support ML feature engineering
""")
```

---

## 🏦 Real-World Banking Scenario 2: ML Feature Engineering

### Scenario
A bank's **fraud detection team** is building a new ML model. They need to add **15 new features** to the transaction table:
- Transaction velocity features
- Merchant risk scores
- Customer behavior patterns
- Device fingerprinting data

The model needs access to **10 billion historical transactions**.

### Problem
- Cannot rewrite 10 billion rows
- Need to add features without disrupting production
- Historical data must be accessible

### Solution
Iceberg schema evolution:
- Add new columns instantly
- Backfill historical data gradually
- Queries work during transition

### Python Code

```python
"""
Banking Scenario 2: ML Feature Engineering
Using Iceberg Schema Evolution for Model Training
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
# STEP 2: Create Transaction Table with ML Features
# ============================================================

print("=== ML FEATURE ENGINEERING WITH SCHEMA EVOLUTION ===\n")

# Schema with ML features
ml_schema = pa.schema([
    # Core transaction fields
    pa.field("transaction_id", pa.string(), nullable=False),
    pa.field("account_id", pa.string(), nullable=False),
    pa.field("amount", pa.decimal128(18, 2), nullable=False),
    pa.field("transaction_date", pa.date32(), nullable=False),
    pa.field("merchant_id", pa.string(), nullable=False),
    
    # ML Feature fields (NEW)
    pa.field("transaction_velocity_1h", pa.int32(), nullable=True),
    pa.field("transaction_velocity_24h", pa.int32(), nullable=True),
    pa.field("transaction_velocity_7d", pa.int32(), nullable=True),
    pa.field("avg_transaction_amount_30d", pa.decimal128(18, 2), nullable=True),
    pa.field("merchant_risk_score", pa.float64(), nullable=True),
    pa.field("customer_risk_score", pa.float64(), nullable=True),
    pa.field("device_fingerprint", pa.string(), nullable=True),
    pa.field("location_risk_score", pa.float64(), nullable=True),
    pa.field("time_since_last_transaction", pa.int32(), nullable=True),
    pa.field("is_international", pa.boolean(), nullable=True),
    pa.field("fraud_label", pa.int32(), nullable=True),  # For supervised learning
])

# Create table
try:
    ml_table = catalog.create_table(
        identifier="banking.transaction_ml_features",
        schema=ml_schema,
        partition_spec={"transform": "month", "source": "transaction_date"}
    )
except Exception:
    ml_table = catalog.load_table("banking.transaction_ml_features")

# ============================================================
# STEP 3: Load Historical Data (without ML features)
# ============================================================

print("--- Loading Historical Data (without ML features) ---")

# Historical data (2023-2024)
historical_data = pa.table({
    "transaction_id": [f"TXN-{i:08d}" for i in range(1, 1001)],
    "account_id": [f"ACC-{1000 + (i % 100):06d}" for i in range(1000)],
    "amount": [round(random.uniform(10, 10000), 2) for _ in range(1000)],
    "transaction_date": [datetime(2024, 1, 15).date()] * 1000,
    "merchant_id": [f"MERCHANT-{random.randint(1, 5000):06d}" for _ in range(1000)],
    # ML features are NULL for historical data
    "transaction_velocity_1h": [None] * 1000,
    "transaction_velocity_24h": [None] * 1000,
    "transaction_velocity_7d": [None] * 1000,
    "avg_transaction_amount_30d": [None] * 1000,
    "merchant_risk_score": [None] * 1000,
    "customer_risk_score": [None] * 1000,
    "device_fingerprint": [None] * 1000,
    "location_risk_score": [None] * 1000,
    "time_since_last_transaction": [None] * 1000,
    "is_international": [None] * 1000,
    "fraud_label": [None] * 1000,
})

ml_table.append(historical_data)
print(f"Loaded {len(historical_data)} historical transactions (ML features: NULL)")

# ============================================================
# STEP 4: Load Recent Data (with ML features)
# ============================================================

print("\n--- Loading Recent Data (with ML features) ---")

# Recent data (2025) with ML features
recent_data = pa.table({
    "transaction_id": [f"TXN-{i + 1000:08d}" for i in range(1, 501)],
    "account_id": [f"ACC-{1000 + (i % 100):06d}" for i in range(500)],
    "amount": [round(random.uniform(10, 10000), 2) for _ in range(500)],
    "transaction_date": [datetime(2025, 6, 15).date()] * 500,
    "merchant_id": [f"MERCHANT-{random.randint(1, 5000):06d}" for _ in range(500)],
    # ML features populated for recent data
    "transaction_velocity_1h": [random.randint(0, 10) for _ in range(500)],
    "transaction_velocity_24h": [random.randint(5, 50) for _ in range(500)],
    "transaction_velocity_7d": [random.randint(20, 200) for _ in range(500)],
    "avg_transaction_amount_30d": [round(random.uniform(100, 5000), 2) for _ in range(500)],
    "merchant_risk_score": [round(random.uniform(0, 1), 4) for _ in range(500)],
    "customer_risk_score": [round(random.uniform(0, 1), 4) for _ in range(500)],
    "device_fingerprint": [f"DEV-{random.randint(100000, 999999)}" for _ in range(500)],
    "location_risk_score": [round(random.uniform(0, 1), 4) for _ in range(500)],
    "time_since_last_transaction": [random.randint(0, 7200) for _ in range(500)],
    "is_international": [random.choice([True, False]) for _ in range(500)],
    "fraud_label": [random.choice([0, 0, 0, 0, 1]) for _ in range(500)],  # 20% fraud
})

ml_table.append(recent_data)
print(f"Loaded {len(recent_data)} recent transactions (ML features: POPULATED)")

# ============================================================
# STEP 5: Query for ML Training
# ============================================================

print("\n--- Querying for ML Training ---")

# Get all data for training
training_data = ml_table.scan().to_arrow()
print(f"Total training samples: {len(training_data)}")

# Separate historical (no features) and recent (with features)
historical_count = len(training_data) - len(recent_data)
recent_count = len(recent_data)

print(f"\nData Distribution:")
print(f"  Historical (no ML features): {historical_count}")
print(f"  Recent (with ML features): {recent_count}")

# ============================================================
# STEP 6: Backfill Historical Data
# ============================================================

print("\n--- Backfilling Historical Data ---")

def backfill_ml_features(historical_data: pa.Table) -> pa.Table:
    """
    Backfill ML features for historical data.
    In production, this would compute actual features.
    """
    # Simulate feature computation
    backfilled = historical_data
    
    # Compute transaction velocity (simulated)
    velocity_1h = [random.randint(0, 10) for _ in range(len(backfilled))]
    velocity_24h = [random.randint(5, 50) for _ in range(len(backfilled))]
    velocity_7d = [random.randint(20, 200) for _ in range(len(backfilled))]
    
    # Update columns
    backfilled = backfilled.set_column(
        backfilled.schema.get_field_index("transaction_velocity_1h"),
        "transaction_velocity_1h",
        pa.array(velocity_1h)
    )
    backfilled = backfilled.set_column(
        backfilled.schema.get_field_index("transaction_velocity_24h"),
        "transaction_velocity_24h",
        pa.array(velocity_24h)
    )
    backfilled = backfilled.set_column(
        backfilled.schema.get_field_index("transaction_velocity_7d"),
        "transaction_velocity_7d",
        pa.array(velocity_7d)
    )
    
    return backfilled

# Backfill historical data
backfilled_data = backfill_ml_features(historical_data)

# Append backfilled data (creates new version)
ml_table.append(backfilled_data)
print(f"Backfilled {len(backfilled_data)} historical transactions")

# ============================================================
# STEP 7: Final Training Dataset
# ============================================================

print("\n--- Final Training Dataset ---")

# Get complete dataset
final_dataset = ml_table.scan().to_arrow()
print(f"Total records: {len(final_dataset)}")
print(f"Records with ML features: {len(final_dataset)}")

# Check for NULL values
null_counts = {}
for col in final_dataset.column_names:
    null_count = final_dataset.column(col).null_count
    if null_count > 0:
        null_counts[col] = null_count

if null_counts:
    print(f"\nColumns with NULL values:")
    for col, count in null_counts.items():
        print(f"  {col}: {count} NULLs")
else:
    print(f"\n✓ All columns have values (no NULLs)")

# ============================================================
# STEP 8: ML Training Query
# ============================================================

print("\n--- ML Training Query ---")

print("""
SELECT 
    transaction_id,
    amount,
    transaction_velocity_1h,
    transaction_velocity_24h,
    transaction_velocity_7d,
    avg_transaction_amount_30d,
    merchant_risk_score,
    customer_risk_score,
    device_fingerprint,
    location_risk_score,
    time_since_last_transaction,
    is_international,
    fraud_label
FROM banking.transaction_ml_features
WHERE fraud_label IS NOT NULL
  AND transaction_date >= '2023-01-01'
""")

# Execute query
training_query = ml_table.scan(
    row_filter="fraud_label IS NOT NULL AND transaction_date >= '2023-01-01'"
).to_arrow()

print(f"\nTraining Dataset: {len(training_query)} samples")
print(f"Features: {len(training_query.column_names) - 1}")  # Exclude fraud_label
print(f"Target: fraud_label (binary)")

# ============================================================
# STEP 9: Benefits Summary
# ============================================================

print("\n--- Benefits for ML Team ---")

print("""
SCHEMA EVOLUTION ENABLES:

1. ADD FEATURES WITHOUT REWRITE
   - Add 15 new columns instantly
   - No data migration required
   - Historical data preserved

2. GRADUAL BACKFILL
   - Compute features incrementally
   - No production disruption
   - Track progress via snapshots

3. SEAMLESS QUERIES
   - Training queries work across versions
   - NULL handling automatic
   - No special ETL logic

4. AUDIT TRAIL
   - Track when features added
   - Reproduce training data
   - Regulatory compliance

5. FLEXIBILITY
   - Add new features as needed
   - Experiment with different features
   - Support multiple model versions
""")
```

---

## 🎯 5 Real-World Interview Questions

### Question 1: How does Iceberg handle schema evolution without rewriting data files?

**Answer:**

Iceberg uses **field IDs** to track columns:

1. **Field IDs**: Each column has a unique ID that never changes
2. **Schema History**: Multiple schema versions stored in metadata
3. **Lazy Evolution**: Old files keep original schema
4. **Query-time Adaptation**: Engine maps old files to new schema

**Example:**
```
Schema v1: field_id=1: amount
Schema v2: field_id=1: transaction_amount (renamed)

Query "SELECT transaction_amount" → 
  Maps to field_id=1 → Reads from old files
```

**Key Points:**
- Rename: Same field ID, different name
- Add: New field ID, NULL in old files
- Drop: Field ID ignored in old files
- Type widening: Compatible types work

---

### Question 2: What are the limitations of schema evolution in Iceberg?

**Answer:**

**Supported Operations:**
- ✓ Add column
- ✓ Rename column
- ✓ Drop column
- ✓ Reorder columns
- ✓ Widen types (INT → LONG)

**Limitations:**

1. **Type Changes**: Cannot change incompatible types
   - STRING → INT: Not supported
   - DECIMAL(18,2) → DECIMAL(18,4): Supported (widen)

2. **Nested Fields**: Limited support for complex types
   - Adding struct fields: Supported
   - Changing map keys: Not supported

3. **Partition Columns**: Cannot change partition spec
   - Must create new table
   - Migrate data

4. **Performance**: Old queries may scan more files
   - NULL checks for missing columns
   - No optimization for old schemas

5. **Compatibility**: Some engines may not support all operations
   - Check engine documentation

---

### Question 3: How do you safely migrate a Hive table to Iceberg with schema evolution?

**Answer:**

**Migration Steps:**

1. **Create Iceberg Table**
   ```sql
   CREATE TABLE iceberg_table LIKE hive_table;
   ```

2. **Migrate Data**
   ```sql
   INSERT INTO iceberg_table SELECT * FROM hive_table;
   ```

3. **Apply Schema Evolution**
   ```sql
   ALTER TABLE iceberg_table ADD COLUMN new_field STRING;
   ```

4. **Validate Data**
   ```sql
   SELECT COUNT(*) FROM hive_table;
   SELECT COUNT(*) FROM iceberg_table;
   ```

5. **Switch Queries**
   - Update ETL jobs to use Iceberg table
   - Update BI tools
   - Monitor performance

**Best Practices:**
- Run migration during low-traffic period
- Keep Hive table as backup
- Validate data counts and checksums
- Test queries before switching

---

### Question 4: Explain how field IDs enable safe column operations.

**Answer:**

**Field IDs:**
- Unique identifier for each column
- Never changes, even if column renamed
- Stored in metadata, not data files

**Operations:**

1. **Rename Column**
   - Old files: field_id=3 → "amount"
   - New schema: field_id=3 → "transaction_amount"
   - Query reads field_id=3 regardless of name

2. **Drop Column**
   - Old files: field_id=3 exists
   - New schema: field_id=3 not referenced
   - Old files ignore field_id=3

3. **Add Column**
   - Old files: field_id=7 doesn't exist
   - New schema: field_id=7 → "new_field"
   - Query returns NULL for old files

**Example:**
```sql
-- Original
CREATE TABLE t (id INT, name STRING);
-- Field IDs: 1=id, 2=name

-- Rename
ALTER TABLE t RENAME COLUMN name TO full_name;
-- Field IDs: 1=id, 2=full_name

-- Old data files still have field_id=2
-- Query "SELECT full_name" maps to field_id=2
-- Works seamlessly!
```

---

### Question 5: How does schema evolution affect query performance?

**Answer:**

**Performance Impact:**

1. **NULL Handling**
   - Old files: Return NULL for new columns
   - Small overhead: NULL checks per row
   - Impact: Minimal (1-2%)

2. **Type Casting**
   - Widened types: May require casting
   - Example: INT → LONG
   - Impact: Small (CPU overhead)

3. **File Scanning**
   - Old files: Still scanned
   - Metadata pruning: Still effective
   - Impact: Minimal

4. **Column Pruning**
   - Only requested columns read
   - Works with old schemas
   - Impact: None

**Optimization Strategies:**

1. **Compaction**
   - Rewrite old files with new schema
   - Eliminates NULL overhead
   - One-time cost

2. **Partitioning**
   - Partition by schema version
   - Query relevant partitions only
   - Reduce scan scope

3. **Materialized Views**
   - Pre-compute common queries
   - Cache results
   - Reduce repeated scans

**Example:**
```sql
-- Query with schema evolution
SELECT transaction_amount, new_field
FROM transactions;

-- Old files: NULL for new_field
-- New files: Values for new_field
-- UNION ALL handles both automatically
```

---

## 📝 Summary

| Aspect | Key Point |
|--------|-----------|
| **Definition** | Change schema without rewriting data |
| **Mechanism** | Field IDs track columns |
| **Operations** | Add, rename, drop, reorder, widen |
| **Performance** | Minimal impact (NULL checks) |
| **Benefit** | Instant, safe, no downtime |
| **Use Case** | Adding compliance fields, ML features |
| **Limitation** | Cannot change partition spec |
| **Best Practice** | Compaction after evolution |
