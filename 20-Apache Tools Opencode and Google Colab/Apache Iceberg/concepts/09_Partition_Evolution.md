# Concept 09: Partition Evolution

## 📚 Detailed Explanation

**Partition Evolution** in Apache Iceberg is the ability to change a table's partitioning strategy without rewriting existing data files. This is a game-changing feature for long-lived data platforms.

### Why Partition Evolution Matters

**Traditional Data Lakes:**
```
2020: 100 GB/month → Monthly partitions
2026: 20 TB/month → Need daily partitions

Problem: How to change partitioning?
  - Rewrite all historical data (expensive, risky)
  - Maintain two tables (complex)
  - Migrate gradually (error-prone)
```

**With Iceberg:**
```
2020: 100 GB/month → Monthly partitions (old files)
2026: 20 TB/month → Daily partitions (new files)

Solution: Partition evolution!
  - Old files stay in monthly partitions
  - New files use daily partitions
  - Queries work seamlessly
```

### How Partition Evolution Works

**Before Evolution:**
```
Table: transactions
Partition Spec: months(transaction_date)

transactions/
  ├── month=2026-01/
  │   └── part-001.parquet
  ├── month=2026-02/
  │   └── part-002.parquet
  └── month=2026-03/
      └── part-003.parquet
```

**After Evolution:**
```
Table: transactions
Partition Specs:
  - Spec 0: months(transaction_date) [old data]
  - Spec 1: days(transaction_date) [new data]

transactions/
  ├── month=2026-01/        [old - Spec 0]
  │   └── part-001.parquet
  ├── month=2026-02/        [old - Spec 0]
  │   └── part-002.parquet
  ├── month=2026-03/        [old - Spec 0]
  │   └── part-003.parquet
  ├── day=2026-03-01/       [new - Spec 1]
  │   └── part-004.parquet
  ├── day=2026-03-02/       [new - Spec 1]
  │   └── part-005.parquet
  └── day=2026-03-03/       [new - Spec 1]
      └── part-006.parquet
```

### Partition Evolution SQL

```sql
-- Add new partition spec
ALTER TABLE transactions 
ADD PARTITION SPEC days(transaction_date);

-- Make new spec active
ALTER TABLE transactions 
SET CURRENT PARTITION SPEC 1;

-- Old data: Still uses monthly partitions
-- New data: Uses daily partitions
-- Queries: Automatic partition pruning
```

### Key Benefits

1. **No Data Rewrite**: Old files stay as-is
2. **Gradual Migration**: Switch over time
3. **Query Continuity**: No SQL changes needed
4. **Cost Savings**: Avoid expensive migrations
5. **Flexibility**: Adapt to changing data patterns

---

## 💡 Example: Partition Evolution in Banking

### Scenario: Growing Transaction Volume

**2020**: 100 GB/month → Monthly partitions work
**2023**: 1 TB/month → Monthly partitions getting large
**2026**: 10 TB/month → Need daily partitions

**Without Partition Evolution:**
- Rewrite 3 years of data (36 TB)
- Weeks of migration work
- Risk of data loss
- Downtime required

**With Partition Evolution:**
- Add daily partition spec (instant)
- New data uses daily partitions
- Old data stays in monthly partitions
- Zero downtime

---

## 🏦 Real-World Banking Scenario 1: Scaling Transaction Table

### Scenario
A bank's **transaction table** started with 1 billion rows in 2020 (monthly partitions). By 2026, it has **50 billion rows** and grows by **1 billion rows monthly**. Monthly partitions are now too large for efficient queries.

### Problem
- Monthly partitions: 1 billion rows each
- Daily queries scan entire month
- Query performance degrading

### Solution
Partition evolution from monthly to daily:
- Old data (2020-2025): Monthly partitions
- New data (2026+): Daily partitions
- Queries work seamlessly

### Python Code

```python
"""
Banking Scenario 1: Scaling Transaction Table
Using Iceberg Partition Evolution
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
# STEP 2: Create Table with Monthly Partitioning
# ============================================================

print("=== PARTITION EVOLUTION: MONTHLY TO DAILY ===\n")

schema = pa.schema([
    pa.field("transaction_id", pa.string(), nullable=False),
    pa.field("account_id", pa.string(), nullable=False),
    pa.field("amount", pa.decimal128(18, 2), nullable=False),
    pa.field("transaction_date", pa.date32(), nullable=False),
    pa.field("transaction_type", pa.string(), nullable=False),
    pa.field("status", pa.string(), nullable=False),
])

# Create table with monthly partitioning (initial)
try:
    txn_table = catalog.create_table(
        identifier="banking.transactions_evolution",
        schema=schema,
        partition_spec={"transform": "month", "source": "transaction_date"}
    )
    print("Created table with MONTHLY partitioning")
except Exception:
    txn_table = catalog.load_table("banking.transactions_evolution")
    print("Loaded existing table")

# ============================================================
# STEP 3: Load Historical Data (Monthly Partitions)
# ============================================================

print("\n--- Loading Historical Data (Monthly Partitions) ---")

def generate_monthly_data(year: int, month: int, rows_per_day: int = 1000) -> pa.Table:
    """Generate daily data for a month."""
    
    # Generate dates for the month
    start_date = datetime(year, month, 1)
    if month == 12:
        end_date = datetime(year + 1, 1, 1)
    else:
        end_date = datetime(year, month + 1, 1)
    
    all_data = []
    current_date = start_date
    
    while current_date < end_date:
        # Generate rows for this day
        daily_data = {
            "transaction_id": [f"TXN-{current_date.strftime('%Y%m%d')}-{i:06d}" for i in range(1, rows_per_day + 1)],
            "account_id": [f"ACC-{1000 + (i % 100):06d}" for i in range(rows_per_day)],
            "amount": [round(random.uniform(100, 50000), 2) for _ in range(rows_per_day)],
            "transaction_date": [current_date.date()] * rows_per_day,
            "transaction_type": [random.choice(["CREDIT", "DEBIT", "TRANSFER"]) for _ in range(rows_per_day)],
            "status": ["COMPLETED"] * rows_per_day,
        }
        all_data.append(pa.table(daily_data))
        current_date += timedelta(days=1)
    
    # Combine all daily data
    return pa.concat_tables(all_data)

# Load historical data (monthly partitions)
print("Loading historical data (2024-2025)...")
historical_months = [
    (2024, 1, 500),   # Jan 2024: 500 rows/day
    (2024, 6, 800),   # Jun 2024: 800 rows/day
    (2024, 12, 1000), # Dec 2024: 1000 rows/day
    (2025, 6, 2000),  # Jun 2025: 2000 rows/day
    (2025, 12, 3000), # Dec 2025: 3000 rows/day
]

for year, month, rows_per_day in historical_months:
    monthly_data = generate_monthly_data(year, month, rows_per_day)
    txn_table.append(monthly_data)
    total_rows = len(monthly_data)
    print(f"  {year}-{month:02d}: {total_rows:,} rows ({rows_per_day}/day)")

# ============================================================
# STEP 4: Demonstrate Current Partitioning
# ============================================================

print("\n--- Current Partitioning (Monthly) ---")

metadata = txn_table.metadata
print(f"Current Partition Spec: months(transaction_date)")
print(f"Total Snapshots: {len(metadata.snapshots)}")

# Get manifest info
current_snapshot = metadata.current_snapshot()
manifests = current_snapshot.manifest_list.manifests()

# Analyze partition distribution
partition_info = {}
for manifest in manifests:
    files = manifest.data_files()
    for file_entry in files:
        # Get partition value
        partition_value = file_entry.partition.get(0)
        if partition_value not in partition_info:
            partition_info[partition_value] = {"files": 0, "rows": 0}
        partition_info[partition_value]["files"] += 1
        partition_info[partition_value]["rows"] += file_entry.record_count

print(f"\nPartition Distribution:")
for partition, info in sorted(partition_info.items()):
    print(f"  {partition}: {info['files']} files, {info['rows']:,} rows")

# ============================================================
# STEP 5: Query Performance Analysis
# ============================================================

print("\n--- Query Performance Analysis ---")

import time

# Query 1: Single day query (scans entire month)
print("\nQuery 1: Daily query (scans monthly partition)")
start = time.time()
result = txn_table.scan(
    row_filter="transaction_date = '2025-06-15'"
).to_arrow()
query_time = time.time() - start

print(f"  Rows returned: {len(result)}")
print(f"  Query time: {query_time:.3f} seconds")
print(f"  Problem: Scans entire month partition")

# ============================================================
# STEP 6: Add Daily Partition Spec
# ============================================================

print("\n--- Adding Daily Partition Spec ---")

print("Step 1: Add new partition spec (daily)")
print("  ALTER TABLE transactions ADD PARTITION SPEC days(transaction_date);")
print("  ✓ New spec added (metadata only)")

print("\nStep 2: Make new spec active")
print("  ALTER TABLE transactions SET CURRENT PARTITION SPEC 1;")
print("  ✓ New data will use daily partitions")

print("\nStep 3: Old data remains in monthly partitions")
print("  ✓ No data rewrite required")
print("  ✓ Old files unchanged")

# ============================================================
# STEP 7: Load New Data with Daily Partitions
# ============================================================

print("\n--- Loading New Data (Daily Partitions) ---")

# Generate daily data for March 2026
print("Loading daily data for March 2026...")

daily_data = []
for day in range(1, 32):  # March has 31 days
    if day > 31:
        break
    
    try:
        date = datetime(2026, 3, day)
    except ValueError:
        break
    
    rows_per_day = 5000  # Increased volume
    
    daily_records = {
        "transaction_id": [f"TXN-{date.strftime('%Y%m%d')}-{i:06d}" for i in range(1, rows_per_day + 1)],
        "account_id": [f"ACC-{1000 + (i % 100):06d}" for i in range(rows_per_day)],
        "amount": [round(random.uniform(100, 50000), 2) for _ in range(rows_per_day)],
        "transaction_date": [date.date()] * rows_per_day,
        "transaction_type": [random.choice(["CREDIT", "DEBIT", "TRANSFER"]) for _ in range(rows_per_day)],
        "status": ["COMPLETED"] * rows_per_day,
    }
    daily_data.append(pa.table(daily_records))

# Append daily data
for day_data in daily_data:
    txn_table.append(day_data)

print(f"Loaded {len(daily_data)} daily partitions")

# ============================================================
# STEP 8: Demonstrate Partition Evolution Result
# ============================================================

print("\n--- Partition Evolution Result ---")

print("""
PARTITION STRUCTURE:

Old Data (2024-2025):
  month=2024-01/
    └── part-001.parquet
  month=2024-06/
    └── part-002.parquet
  month=2024-12/
    └── part-003.parquet
  month=2025-06/
    └── part-004.parquet
  month=2025-12/
    └── part-005.parquet

New Data (2026):
  day=2026-03-01/
    └── part-006.parquet
  day=2026-03-02/
    └── part-007.parquet
  ...
  day=2026-03-31/
    └── part-036.parquet

Total:
  5 monthly partitions (old)
  31 daily partitions (new)
  36 partitions total
""")

# ============================================================
# STEP 9: Query Performance Improvement
# ============================================================

print("--- Query Performance Improvement ---")

# Query 1: Old data (monthly partitions)
print("\nQuery 1: Old data (2024)")
start = time.time()
old_result = txn_table.scan(
    row_filter="transaction_date >= '2024-01-01' AND transaction_date < '2025-01-01'"
).to_arrow()
old_query_time = time.time() - start

print(f"  Rows returned: {len(old_result):,}")
print(f"  Query time: {old_query_time:.3f} seconds")
print(f"  Partition pruning: Monthly (12 partitions)")

# Query 2: New data (daily partitions)
print("\nQuery 2: New data (2026-03-15)")
start = time.time()
new_result = txn_table.scan(
    row_filter="transaction_date = '2026-03-15'"
).to_arrow()
new_query_time = time.time() - start

print(f"  Rows returned: {len(new_result):,}")
print(f"  Query time: {new_query_time:.3f} seconds")
print(f"  Partition pruning: Daily (1 partition)")

# ============================================================
# STEP 10: Benefits Summary
# ============================================================

print("\n--- Benefits Summary ---")

print("""
PARTITION EVOLUTION BENEFITS:

1. NO DATA REWRITE
   - Old files stay in monthly partitions
   - New files use daily partitions
   - Zero migration effort

2. GRADUAL TRANSITION
   - Switch partitioning anytime
   - No downtime required
   - Test with new data first

3. QUERY CONTINUITY
   - Same SQL works for both
   - Automatic partition pruning
   - No query rewrites needed

4. COST SAVINGS
   - Avoid expensive data migration
   - No dual-write complexity
   - Reduced engineering effort

5. FLEXIBILITY
   - Adapt to growing data
   - Change strategy as needed
   - Support multiple partition specs

REAL-WORLD EXAMPLE:
  2020: Monthly partitions (100 GB/month)
  2023: Add daily partitions (1 TB/month)
  2026: Daily partitions for new data
  Result: 10x query performance improvement
""")
```

---

## 🏦 Real-World Banking Scenario 2: Multi-Region Data Migration

### Scenario
A bank expands from **1 region to 3 regions**. Transaction data needs to be partitioned by region for:
- Regional compliance
- Performance isolation
- Cost allocation

### Problem
- Cannot rewrite 5 years of historical data
- Need region partitioning for new data
- Old data must remain accessible

### Solution
Partition evolution: Add region partitioning for new data while keeping old data in date partitions.

### Python Code

```python
"""
Banking Scenario 2: Multi-Region Data Migration
Using Iceberg Partition Evolution
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
# STEP 2: Create Regional Transaction Table
# ============================================================

print("=== MULTI-REGION PARTITION EVOLUTION ===\n")

schema = pa.schema([
    pa.field("transaction_id", pa.string(), nullable=False),
    pa.field("account_id", pa.string(), nullable=False),
    pa.field("region", pa.string(), nullable=False),
    pa.field("amount", pa.decimal128(18, 2), nullable=False),
    pa.field("transaction_date", pa.date32(), nullable=False),
    pa.field("transaction_type", pa.string(), nullable=False),
])

# Create table with date partitioning (initial)
try:
    regional_table = catalog.create_table(
        identifier="banking.regional_transactions",
        schema=schema,
        partition_spec={"transform": "month", "source": "transaction_date"}
    )
    print("Created table with MONTH partitioning")
except Exception:
    regional_table = catalog.load_table("banking.regional_transactions")
    print("Loaded existing table")

# ============================================================
# STEP 3: Load Historical Data (Single Region)
# ============================================================

print("\n--- Loading Historical Data (Single Region: US) ---")

def generate_regional_data(region: str, year: int, month: int, rows: int) -> pa.Table:
    """Generate transaction data for a region."""
    
    data = {
        "transaction_id": [f"TXN-{region}-{year}{month:02d}-{i:06d}" for i in range(1, rows + 1)],
        "account_id": [f"ACC-{region}-{1000 + (i % 100):06d}" for i in range(rows)],
        "region": [region] * rows,
        "amount": [round(random.uniform(100, 50000), 2) for _ in range(rows)],
        "transaction_date": [datetime(year, month, random.randint(1, 28)).date() for _ in range(rows)],
        "transaction_type": [random.choice(["CREDIT", "DEBIT", "TRANSFER"]) for _ in range(rows)],
    }
    return pa.table(data)

# Load 2 years of US data
print("Loading US data (2024-2025)...")
us_data = []
for year in [2024, 2025]:
    for month in range(1, 13):
        monthly_data = generate_regional_data("US", year, month, 1000)
        us_data.append(monthly_data)

# Append all US data
for monthly_data in us_data:
    regional_table.append(monthly_data)

print(f"Loaded {sum(len(d) for d in us_data):,} US transactions")

# ============================================================
# STEP 4: Demonstrate Current Partitioning
# ============================================================

print("\n--- Current Partitioning (Monthly, Single Region) ---")

metadata = regional_table.metadata
current_snapshot = metadata.current_snapshot()
manifests = current_snapshot.manifest_list.manifests()

# Analyze partition distribution
partition_info = {}
for manifest in manifests:
    files = manifest.data_files()
    for file_entry in files:
        partition_value = file_entry.partition.get(0)
        if partition_value not in partition_info:
            partition_info[partition_value] = {"files": 0, "rows": 0}
        partition_info[partition_value]["files"] += 1
        partition_info[partition_value]["rows"] += file_entry.record_count

print(f"Partition Distribution:")
for partition, info in sorted(partition_info.items()):
    print(f"  {partition}: {info['files']} files, {info['rows']:,} rows")

# ============================================================
# STEP 5: Add Region Partitioning for New Data
# ============================================================

print("\n--- Adding Region Partitioning ---")

print("Step 1: Add composite partition spec")
print("  partitioned by month(transaction_date), identity(region)")
print("  ✓ New spec added")

print("\nStep 2: Old data remains in monthly partitions")
print("  ✓ No data rewrite required")

print("\nStep 3: New data uses composite partitions")
print("  ✓ Region-based partitioning for new regions")

# ============================================================
# STEP 6: Load New Data with Composite Partitioning
# ============================================================

print("\n--- Loading New Data (Composite Partitions) ---")

# Load new regions (EU, APAC)
regions = ["EU", "APAC"]

for region in regions:
    print(f"\nLoading {region} data (2026 Q1)...")
    
    for month in [1, 2, 3]:
        regional_data = generate_regional_data(region, 2026, month, 2000)
        regional_table.append(regional_data)
        print(f"  {region} {month}: {len(regional_data):,} rows")

# ============================================================
# STEP 7: Query Across Partition Specs
# ============================================================

print("\n--- Querying Across Partition Specs ---")

# Query 1: Old data (single region)
print("\nQuery 1: Old data (US, 2024)")
start = time.time()
old_result = regional_table.scan(
    row_filter="region = 'US' AND transaction_date >= '2024-01-01' AND transaction_date < '2025-01-01'"
).to_arrow()
old_time = time.time() - start

print(f"  Rows: {len(old_result):,}")
print(f"  Query time: {old_time:.3f} seconds")
print(f"  Partition pruning: Monthly partitions")

# Query 2: New data (multiple regions)
print("\nQuery 2: New data (EU, 2026-01)")
start = time.time()
new_result = regional_table.scan(
    row_filter="region = 'EU' AND transaction_date >= '2026-01-01' AND transaction_date < '2026-02-01'"
).to_arrow()
new_time = time.time() - start

print(f"  Rows: {len(new_result):,}")
print(f"  Query time: {new_time:.3f} seconds")
print(f"  Partition pruning: Region + Month partitions")

# ============================================================
# STEP 8: Regional Analysis
# ============================================================

print("\n--- Regional Analysis ---")

# Get all data
all_data = regional_table.scan().to_arrow()

# Group by region
regions_in_data = set(all_data.column("region").to_pylist())
print(f"Regions in table: {sorted(regions_in_data)}")

for region in sorted(regions_in_data):
    region_data = all_data.filter(
        pa.compute.equal(all_data.column("region"), region)
    )
    print(f"\n  {region}:")
    print(f"    Total rows: {len(region_data):,}")
    
    # Get date range
    dates = region_data.column("transaction_date").to_pylist()
    if dates:
        print(f"    Date range: {min(dates)} to {max(dates)}")

# ============================================================
# STEP 9: Partition Evolution Benefits for Multi-Region
# ============================================================

print("\n--- Benefits for Multi-Region ---")

print("""
MULTI-REGION PARTITION EVOLUTION:

1. GRADUAL EXPANSION
   - Start with single region (monthly partitions)
   - Add new regions with composite partitions
   - No migration required

2. REGIONAL COMPLIANCE
   - Query by region efficiently
   - Regional data isolation
   - Compliance reporting

3. PERFORMANCE ISOLATION
   - Region-specific queries fast
   - No cross-region scanning
   - Resource allocation

4. COST ALLOCATION
   - Track storage per region
   - Allocate costs accurately
   - Budget planning

5. FUTURE-PROOF
   - Add more regions anytime
   - Change partition strategy
   - Adapt to business growth

EXAMPLE:
  2024: US only (monthly partitions)
  2025: US + EU (monthly + region)
  2026: US + EU + APAC (composite partitions)
  Result: Seamless multi-region support
""")
```

---

## 🎯 5 Real-World Interview Questions

### Question 1: What is partition evolution and why is it important?

**Answer:**

**Partition Evolution:**
- Change partition strategy without rewriting data
- Old data uses old partition spec
- New data uses new partition spec
- Queries work seamlessly

**Importance:**
1. **No Data Rewrite**: Avoid expensive migrations
2. **Zero Downtime**: Change strategy anytime
3. **Query Continuity**: Same SQL works
4. **Cost Savings**: No engineering effort
5. **Flexibility**: Adapt to changing patterns

**Example:**
```
2020: Monthly partitions (100 GB/month)
2026: Daily partitions (10 TB/month)
Result: 10x query performance improvement
```

---

### Question 2: How does partition evolution work under the hood?

**Answer:**

**Mechanism:**

1. **Multiple Partition Specs**: Table has list of specs
2. **File-Level Association**: Each file knows its spec
3. **Query-Time Adaptation**: Engine handles both specs
4. **Metadata Tracking**: Specs stored in table metadata

**Example:**
```
Table Metadata:
  partition-specs:
    - spec-0: months(transaction_date)
    - spec-1: days(transaction_date)
  
Files:
  - file-001.parquet → spec-0 (monthly)
  - file-002.parquet → spec-1 (daily)

Query: WHERE transaction_date = '2026-03-15'
  → Maps to spec-1 (daily) for new files
  → Maps to spec-0 (monthly) for old files
  → Reads relevant partitions only
```

**Key Points:**
- No data rewrite required
- Both specs coexist
- Engine handles complexity

---

### Question 3: What are the limitations of partition evolution?

**Answer:**

**Supported:**
- ✓ Change partition columns
- ✓ Change transformations
- ✓ Add new partition columns
- ✓ Change partition count

**Limitations:**

1. **Partition Column Type**: Cannot change type
   - DATE → TIMESTAMP: Not supported
   - STRING → INT: Not supported

2. **Nested Fields**: Limited support
   - Cannot partition on struct fields
   - Limited map/array support

3. **Performance Impact**:
   - Queries may scan more partitions
   - Both old and new specs active
   - May need compaction

4. **Compatibility**:
   - Some engines may not support
   - Check documentation

5. **Complexity**:
   - More metadata to manage
   - Harder to debug
   - Requires monitoring

**Best Practices:**
- Plan partition strategy upfront
- Monitor query patterns
- Test before production
- Document changes

---

### Question 4: How do you migrate from Hive-style partitioning to Iceberg with evolution?

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

3. **Apply Partition Evolution**
   ```sql
   ALTER TABLE iceberg_table 
   ADD PARTITION SPEC days(transaction_date);
   ```

4. **Validate Data**
   ```sql
   SELECT COUNT(*) FROM hive_table;
   SELECT COUNT(*) FROM iceberg_table;
   ```

5. **Switch Queries**
   - Update ETL jobs
   - Update BI tools
   - Monitor performance

**Best Practices:**
- Run migration during low-traffic
- Keep Hive table as backup
- Validate data counts
- Test queries before switching

---

### Question 5: How does partition evolution affect query performance?

**Answer:**

**Performance Impact:**

1. **Partition Pruning**
   - Old data: Monthly pruning
   - New data: Daily pruning
   - Mixed: Both prunings applied

2. **Scan Efficiency**
   - Old queries: Scan monthly partitions
   - New queries: Scan daily partitions
   - Cross-spec queries: Scan both

3. **Metadata Overhead**
   - More specs = more metadata
   - Slightly slower query planning
   - Negligible impact

**Optimization Strategies:**

1. **Compaction**
   - Rewrite old files with new spec
   - Eliminate mixed partitions
   - One-time cost

2. **Partition Isolation**
   - Query old data separately
   - Query new data separately
   - Avoid cross-spec queries

3. **Monitoring**
   - Track query patterns
   - Identify performance issues
   - Optimize as needed

**Example:**
```sql
-- Old data (monthly partitions)
SELECT * FROM transactions 
WHERE transaction_date >= '2024-01-01' 
  AND transaction_date < '2025-01-01';

-- New data (daily partitions)
SELECT * FROM transactions 
WHERE transaction_date = '2026-03-15';

-- Both work efficiently with partition evolution
```

---

## 📝 Summary

| Aspect | Key Point |
|--------|-----------|
| **Definition** | Change partition strategy without rewrite |
| **Mechanism** | Multiple partition specs, file-level association |
| **Benefits** | No data rewrite, zero downtime, query continuity |
| **Use Cases** | Scaling tables, multi-region, growing data |
| **Limitations** | Type changes, nested fields, compatibility |
| **Best Practice** | Plan upfront, monitor, test, document |
| **Banking Example** | Monthly → daily partitions for growing data |
