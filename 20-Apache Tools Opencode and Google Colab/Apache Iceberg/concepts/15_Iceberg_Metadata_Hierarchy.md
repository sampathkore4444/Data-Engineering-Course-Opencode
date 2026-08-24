# Concept 15: Iceberg Metadata Hierarchy

## 📚 Detailed Explanation

The **Iceberg Metadata Hierarchy** is the core architecture that enables all Iceberg features: time travel, ACID transactions, schema evolution, and efficient queries. Understanding this hierarchy is fundamental to mastering Iceberg.

### The Complete Hierarchy

```
Catalog
   ↓
Table Metadata
   ↓
Snapshot
   ↓
Manifest List
   ↓
Manifests
   ↓
Data Files
   ↓
Parquet (Rows)
```

### Layer-by-Layer Breakdown

#### Layer 1: Catalog
**Purpose**: Maps table names to metadata locations

**Content:**
```
banking.transactions → s3://lake/transactions/metadata/v1.json
banking.customers → s3://lake/customers/metadata/v1.json
```

**Role:**
- Entry point for queries
- Namespace management
- Transaction coordination

#### Layer 2: Table Metadata
**Purpose**: Contains table-level information

**Content (JSON):**
```json
{
  "format-version": 2,
  "table-uuid": "d20125c8-7284-442c-9aea-15fee620737c",
  "location": "s3://lake/transactions/",
  "last-updated-ms": 1724500000000,
  "last-column-id": 11,
  "current-snapshot-id": 102,
  "snapshots": [100, 101, 102],
  "properties": {...}
}
```

**Role:**
- Schema definition
- Current snapshot pointer
- Snapshot history
- Table properties

#### Layer 3: Snapshot
**Purpose**: Immutable record of table state at point in time

**Content:**
```json
{
  "snapshot-id": 102,
  "parent-snapshot-id": 101,
  "timestamp-ms": 1724500000000,
  "operation": "append",
  "manifest-list": "s3://lake/metadata/snap-102.avro",
  "summary": {
    "added-files-size": "128MB",
    "added-records": "1000000"
  }
}
```

**Role:**
- Point-in-time state
- Time travel reference
- Operation tracking

#### Layer 4: Manifest List
**Purpose**: Lists all manifests for a snapshot

**Content (Avro):**
```
manifest-1.avro
manifest-2.avro
manifest-3.avro
```

**Role:**
- Manifest discovery
- Partition-level metadata
- File count statistics

#### Layer 5: Manifests
**Purpose**: Lists data files with statistics

**Content (Avro):**
```json
{
  "manifest-path": "s3://lake/metadata/manifest-1.avro",
  "partition-spec-id": 0,
  "added-files-count": 100,
  "existing-files-count": 0,
  "deleted-files-count": 0,
  "partitions": [
    {"field-id": 1, "lower": "2026-08-01", "upper": "2026-08-31"}
  ]
}
```

**Role:**
- File discovery
- Statistics for pruning
- Partition information

#### Layer 6: Data Files
**Purpose**: Actual data storage (Parquet)

**Content:**
```
s3://lake/data/file-001.parquet
s3://lake/data/file-002.parquet
s3://lake/data/file-003.parquet
```

**Role:**
- Row storage
- Columnar format
- Compression

---

## 💡 Example: Query Execution Flow

### Query: `SELECT * FROM transactions WHERE date = '2026-08-24'`

**Step-by-Step:**

```
1. Query Engine → Catalog
   "Give me banking.transactions"
   ↓
2. Catalog → Table Metadata
   "Here's s3://lake/transactions/metadata/v1.json"
   ↓
3. Table Metadata → Current Snapshot
   "Current snapshot: 102"
   ↓
4. Snapshot → Manifest List
   "Manifest list: s3://lake/metadata/snap-102.avro"
   ↓
5. Manifest List → Manifests
   "3 manifests: manifest-1, manifest-2, manifest-3"
   ↓
6. Manifests → Statistics
   "manifest-1: date range [2026-08-01, 2026-08-15]"
   "manifest-2: date range [2026-08-16, 2026-08-24]"
   "manifest-3: date range [2026-08-25, 2026-08-31]"
   ↓
7. Pruning
   Skip manifest-1 (date range doesn't include Aug 24)
   Skip manifest-3 (date range doesn't include Aug 24)
   Read manifest-2 only
   ↓
8. Manifest-2 → Data Files
   "file-005.parquet: date range [2026-08-21, 2026-08-24]"
   "file-006.parquet: date range [2026-08-23, 2026-08-24]"
   ↓
9. Data Files → Query Engine
   Read file-005 and file-006
   ↓
10. Result
    Return matching rows
```

**Efficiency:**
- Without metadata: Scan all 10,000 files
- With metadata: Scan only 2 files (99.98% reduction)

---

## 🏦 Real-World Banking Scenario 1: Metadata-Based Query Optimization

### Scenario
A bank's **transaction table** has **10 billion rows** across **50,000 Parquet files**. The analytics team runs **thousands of queries daily**. Without metadata optimization, queries would be extremely slow.

### Problem
- Queries scan too many files
- Performance degrades with data growth
- Query costs increase

### Solution
Iceberg metadata hierarchy enables:
- Metadata-based pruning
- Statistics-driven optimization
- Efficient query planning

### Python Code

```python
"""
Banking Scenario 1: Metadata-Based Query Optimization
Using Iceberg Metadata Hierarchy
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
# STEP 2: Create Optimized Transaction Table
# ============================================================

print("=== METADATA-BASED QUERY OPTIMIZATION ===\n")

schema = pa.schema([
    pa.field("transaction_id", pa.string(), nullable=False),
    pa.field("account_id", pa.string(), nullable=False),
    pa.field("amount", pa.decimal128(18, 2), nullable=False),
    pa.field("transaction_date", pa.date32(), nullable=False),
    pa.field("status", pa.string(), nullable=False),
])

# Create table with partitioning
try:
    txn_table = catalog.create_table(
        identifier="banking.optimized_transactions",
        schema=schema,
        partition_spec={"transform": "month", "source": "transaction_date"}
    )
except Exception:
    txn_table = catalog.load_table("banking.optimized_transactions")

# ============================================================
# STEP 3: Load Data Across Multiple Months
# ============================================================

print("--- Loading Data Across Multiple Months ---")

def generate_monthly_data(year: int, month: int, rows: int) -> pa.Table:
    """Generate data for a specific month."""
    
    start_date = datetime(year, month, 1)
    if month == 12:
        end_date = datetime(year + 1, 1, 1)
    else:
        end_date = datetime(year, month + 1, 1)
    
    data = {
        "transaction_id": [f"TXN-{year}{month:02d}-{i:06d}" for i in range(1, rows + 1)],
        "account_id": [f"ACC-{1000 + (i % 100):06d}" for i in range(rows)],
        "amount": [round(random.uniform(100, 50000), 2) for _ in range(rows)],
        "transaction_date": [
            start_date + timedelta(days=random.randint(0, (end_date - start_date).days - 1))
            for _ in range(rows)
        ],
        "status": ["COMPLETED"] * rows,
    }
    return pa.table(data)

# Load data for 12 months
print("Loading 12 months of data...")
for month in range(1, 13):
    monthly_data = generate_monthly_data(2026, month, 10000)
    txn_table.append(monthly_data)
    print(f"  Month {month:02d}: {len(monthly_data):,} rows")

# ============================================================
# STEP 4: Analyze Metadata Hierarchy
# ============================================================

print("\n--- Analyzing Metadata Hierarchy ---")

# Get metadata
metadata = txn_table.metadata

print(f"\nTable Metadata:")
print(f"  Format Version: {metadata.format_version}")
print(f"  Table UUID: {metadata.table_uuid}")
print(f"  Current Snapshot ID: {metadata.current_snapshot_id}")
print(f"  Total Snapshots: {len(metadata.snapshots)}")
print(f"  Last Column ID: {metadata.last_column_id()}")

# Analyze current snapshot
current_snapshot = metadata.current_snapshot()
print(f"\nCurrent Snapshot:")
print(f"  Snapshot ID: {current_snapshot.snapshot_id}")
print(f"  Operation: {current_snapshot.operation}")
print(f"  Timestamp: {datetime.fromtimestamp(current_snapshot.timestamp_ms / 1000)}")
print(f"  Manifest List: {current_snapshot.manifest_list.manifest_list_location}")

# Analyze manifest list
manifest_list = current_snapshot.manifest_list
manifests = manifest_list.manifests()

print(f"\nManifest List:")
print(f"  Total Manifests: {len(manifests)}")

# Analyze manifests
total_files = 0
total_rows = 0

for i, manifest in enumerate(manifests[:5]):  # Show first 5
    files = manifest.data_files()
    manifest_rows = sum(f.record_count for f in files)
    
    print(f"\n  Manifest {i+1}:")
    print(f"    Files: {len(files)}")
    print(f"    Rows: {manifest_rows:,}")
    
    total_files += len(files)
    total_rows += manifest_rows

print(f"\n  Total Files (first 5 manifests): {total_files}")
print(f"  Total Rows (first 5 manifests): {total_rows:,}")

# ============================================================
# STEP 5: Demonstrate Query Optimization
# ============================================================

print("\n--- Demonstrating Query Optimization ---")

# Query 1: Full table scan (no optimization)
print("\nQuery 1: Full table scan (no optimization)")
start = time.time()
result1 = txn_table.scan().to_arrow()
query1_time = time.time() - start

print(f"  Rows: {len(result1):,}")
print(f"  Time: {query1_time:.3f} seconds")

# Query 2: With partition pruning
print("\nQuery 2: With partition pruning (date filter)")
start = time.time()
result2 = txn_table.scan(
    row_filter="transaction_date >= '2026-06-01' AND transaction_date < '2026-07-01'"
).to_arrow()
query2_time = time.time() - start

print(f"  Rows: {len(result2):,}")
print(f"  Time: {query2_time:.3f} seconds")
print(f"  Improvement: {query1_time/query2_time:.1f}x faster")

# Query 3: With partition + column pruning
print("\nQuery 3: With partition + column pruning")
start = time.time()
result3 = txn_table.scan(
    row_filter="transaction_date >= '2026-06-01' AND transaction_date < '2026-07-01'",
    selected_fields=["transaction_id", "amount"]
).to_arrow()
query3_time = time.time() - start

print(f"  Rows: {len(result3):,}")
print(f"  Columns: {len(result3.column_names)}")
print(f"  Time: {query3_time:.3f} seconds")
print(f"  Improvement vs full scan: {query1_time/query3_time:.1f}x faster")

# ============================================================
# STEP 6: Metadata Statistics
# ============================================================

print("\n--- Metadata Statistics ---")

# Get statistics from manifests
print("\nFile Statistics (for pruning):")

for i, manifest in enumerate(manifests[:3]):
    files = manifest.data_files()
    print(f"\n  Manifest {i+1}:")
    
    for j, file_entry in enumerate(files[:2]):  # Show first 2 files
        print(f"    File {j+1}: {file_entry.file_path.split('/')[-1]}")
        print(f"      Record Count: {file_entry.record_count}")
        
        if hasattr(file_entry, 'lower_bounds') and file_entry.lower_bounds:
            print(f"      Lower Bounds: {dict(list(file_entry.lower_bounds.items())[:3])}")
        if hasattr(file_entry, 'upper_bounds') and file_entry.upper_bounds:
            print(f"      Upper Bounds: {dict(list(file_entry.upper_bounds.items())[:3])}")

# ============================================================
# STEP 7: Metadata Optimization Benefits
# ============================================================

print("\n--- Metadata Optimization Benefits ---")

print("""
METADATA HIERARCHY BENEFITS:

1. EFFICIENT QUERY PLANNING
   - Load only relevant metadata
   - Skip irrelevant manifests
   - Prune files using statistics

2. TIME TRAVEL
   - Immutable snapshots
   - Point-in-time queries
   - Complete history

3. ACID TRANSACTIONS
   - Atomic metadata updates
   - Consistent snapshots
   - Conflict detection

4. SCHEMA EVOLUTION
   - Schema history in metadata
   - Field ID tracking
   - Backward compatibility

5. PERFORMANCE
   - Partition pruning
   - File statistics
   - Column pruning

QUERY OPTIMIZATION:
  Without metadata: Scan 50,000 files
  With metadata: Scan 100 files (99.8% reduction)
""")
```

---

## 🏦 Real-World Banking Scenario 2: Time Travel with Metadata

### Scenario
A bank's **compliance team** needs to investigate a transaction discrepancy from **3 months ago**. They need to see the exact data state at that time.

### Problem
- Cannot reconstruct historical state
- No audit trail
- Regulatory compliance risk

### Solution
Iceberg metadata enables:
- Point-in-time queries
- Complete snapshot history
- Immutable evidence

### Python Code

```python
"""
Banking Scenario 2: Time Travel with Metadata
Using Iceberg Metadata for Historical Queries
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
# STEP 2: Create Transaction Table
# ============================================================

print("=== TIME TRAVEL WITH METADATA ===\n")

schema = pa.schema([
    pa.field("transaction_id", pa.string(), nullable=False),
    pa.field("account_id", pa.string(), nullable=False),
    pa.field("amount", pa.decimal128(18, 2), nullable=False),
    pa.field("status", pa.string(), nullable=False),
    pa.field("created_at", pa.timestamp("us"), nullable=False),
])

try:
    txn_table = catalog.create_table(
        identifier="banking.time_travel_transactions",
        schema=schema
    )
except Exception:
    txn_table = catalog.load_table("banking.time_travel_transactions")

# ============================================================
# STEP 3: Create Multiple Snapshots Over Time
# ============================================================

print("--- Creating Multiple Snapshots ---")

# Simulate transactions over several days
base_date = datetime(2026, 8, 1)

for day in range(1, 25):  # 24 days
    current_date = base_date + timedelta(days=day - 1)
    
    # Generate transactions for this day
    daily_data = pa.table({
        "transaction_id": [f"TXN-{day:02d}-{i:04d}" for i in range(1, 101)],
        "account_id": [f"ACC-{1000 + (i % 50):06d}" for i in range(100)],
        "amount": [round(random.uniform(100, 50000), 2) for _ in range(100)],
        "status": ["COMPLETED"] * 100,
        "created_at": [current_date + timedelta(hours=random.randint(0, 23)) for _ in range(100)],
    })
    
    txn_table.append(daily_data)
    
    if day % 6 == 0:  # Log every 6 days
        print(f"  Day {day:02d}: Snapshot {txn_table.metadata.current_snapshot_id}")

# ============================================================
# STEP 4: Analyze Metadata History
# ============================================================

print("\n--- Analyzing Metadata History ---")

metadata = txn_table.metadata
snapshots = metadata.snapshots

print(f"\nSnapshot History:")
print(f"  Total Snapshots: {len(snapshots)}")

for i, snap in enumerate(snapshots[-10:]):  # Last 10 snapshots
    timestamp = datetime.fromtimestamp(snap.timestamp_ms / 1000)
    print(f"  Snapshot {snap.snapshot_id}: {snap.operation} at {timestamp}")

# ============================================================
# STEP 5: Time Travel Query
# ============================================================

print("\n--- Time Travel Query ---")

# Query at specific snapshot
target_snapshot = snapshots[-10]  # 10 snapshots ago
target_time = datetime.fromtimestamp(target_snapshot.timestamp_ms / 1000)

print(f"\nQuerying at historical point:")
print(f"  Snapshot ID: {target_snapshot.snapshot_id}")
print(f"  Timestamp: {target_time}")

# In production, use time travel SQL:
# SELECT * FROM transactions
# FOR SYSTEM_TIME AS OF '2026-08-14 10:00:00'

# For demonstration, scan with filter
historical_result = txn_table.scan(
    row_filter=f"created_at >= TIMESTAMP '{target_time}' AND created_at < TIMESTAMP '{target_time + timedelta(days=1)}'"
).to_arrow()

print(f"\nResults at historical point:")
print(f"  Rows: {len(historical_result)}")
print(f"  Snapshot: {target_snapshot.snapshot_id}")

# ============================================================
# STEP 6: Compare Current vs Historical
# ============================================================

print("\n--- Comparing Current vs Historical ---")

# Current state
current_result = txn_table.scan().to_arrow()

# Historical state (simulated)
historical_result = txn_table.scan(
    row_filter=f"created_at >= TIMESTAMP '{target_time}' AND created_at < TIMESTAMP '{target_time + timedelta(days=1)}'"
).to_arrow()

print(f"\nComparison:")
print(f"  Current total rows: {len(current_result):,}")
print(f"  Historical rows at {target_time}: {len(historical_result):,}")
print(f"  Difference: {len(current_result) - len(historical_result):,} rows")

# ============================================================
# STEP 7: Metadata-Based Investigation
# ============================================================

print("\n--- Metadata-Based Investigation ---")

def investigate_snapshot(table: Table, snapshot_id: int) -> dict:
    """
    Investigate a specific snapshot using metadata.
    """
    metadata = table.metadata
    
    # Find snapshot
    target_snap = None
    for snap in metadata.snapshots:
        if snap.snapshot_id == snapshot_id:
            target_snap = snap
            break
    
    if not target_snap:
        return {"error": "Snapshot not found"}
    
    # Get manifest list
    manifest_list = target_snap.manifest_list
    manifests = manifest_list.manifests()
    
    # Analyze manifests
    total_files = 0
    total_rows = 0
    
    for manifest in manifests:
        files = manifest.data_files()
        total_files += len(files)
        total_rows += sum(f.record_count for f in files)
    
    return {
        "snapshot_id": target_snap.snapshot_id,
        "timestamp": datetime.fromtimestamp(target_snap.timestamp_ms / 1000),
        "operation": target_snap.operation,
        "manifest_count": len(manifests),
        "file_count": total_files,
        "row_count": total_rows,
        "manifest_list_location": manifest_list.manifest_list_location
    }

# Investigate target snapshot
investigation = investigate_snapshot(txn_table, target_snapshot.snapshot_id)

print(f"\nSnapshot Investigation:")
print(f"  Snapshot ID: {investigation['snapshot_id']}")
print(f"  Timestamp: {investigation['timestamp']}")
print(f"  Operation: {investigation['operation']}")
print(f"  Manifest Count: {investigation['manifest_count']}")
print(f"  File Count: {investigation['file_count']}")
print(f"  Row Count: {investigation['row_count']:,}")

# ============================================================
# STEP 8: Metadata Benefits Summary
# ============================================================

print("\n--- Metadata Benefits Summary ---")

print("""
METADATA HIERARCHY BENEFITS:

1. TIME TRAVEL
   - Query any historical state
   - Immutable snapshots
   - Point-in-time accuracy

2. AUDIT TRAIL
   - Complete snapshot history
   - Timestamped evidence
   - Operation tracking

3. INVESTIGATION
   - Analyze specific snapshots
   - Track changes over time
   - Root cause analysis

4. COMPLIANCE
   - Regulatory evidence
   - Data lineage
   - Retention policies

5. PERFORMANCE
   - Efficient metadata access
   - Pruning optimizations
   - Query planning

INVESTIGATION USE CASES:
  - Transaction discrepancy
  - Data corruption
  - Regulatory audit
  - Debugging ETL
""")
```

---

## 🎯 5 Real-World Interview Questions

### Question 1: Explain the complete Iceberg metadata hierarchy.

**Answer:**

**Hierarchy:**
```
Catalog → Table Metadata → Snapshot → Manifest List → Manifest → Data Files
```

**Layer Details:**

1. **Catalog**: Maps table names to metadata locations
2. **Table Metadata**: Schema, current snapshot, properties
3. **Snapshot**: Immutable table state at point in time
4. **Manifest List**: Lists all manifests for a snapshot
5. **Manifest**: Lists data files with statistics
6. **Data Files**: Parquet files with actual data

**Key Points:**
- Each layer adds efficiency
- Enables time travel, ACID, schema evolution
- Metadata pruning reduces scan scope

---

### Question 2: How does Iceberg use metadata for query optimization?

**Answer:**

**Optimization Techniques:**

1. **Partition Pruning**
   - Metadata contains partition info
   - Skip irrelevant partitions
   - Reduce scan scope

2. **File Statistics**
   - Min/max values per file
   - Null counts
   - Skip files based on predicates

3. **Column Pruning**
   - Only read requested columns
   - Reduce I/O

**Example:**
```sql
SELECT * FROM transactions WHERE date = '2026-08-24'
```

**Optimization Flow:**
1. Load current snapshot
2. Load manifest list
3. Identify relevant manifests (partition pruning)
4. Check file statistics (skip irrelevant files)
5. Read only relevant Parquet files

**Result:** Scan 0.1% of total data

---

### Question 3: What is the role of manifests in Iceberg?

**Answer:**

**Manifest Role:**

1. **File Discovery**: List all data files in a partition
2. **Statistics**: Min/max, null counts for pruning
3. **Partition Info**: Partition values for each file
4. **File Metadata**: Record count, file size

**Manifest Content:**
```json
{
  "file-path": "s3://lake/data/file-001.parquet",
  "record-count": 1000000,
  "file-size-in-bytes": 134217728,
  "column-sizes": {...},
  "value-counts": {...},
  "null-value-counts": {...},
  "lower-bounds": {...},
  "upper-bounds": {...}
}
```

**Benefits:**
- Efficient query planning
- Data file pruning
- Statistics-driven optimization

---

### Question 4: How does Iceberg handle metadata for time travel?

**Answer:**

**Time Travel Mechanism:**

1. **Immutable Snapshots**: Never modified after creation
2. **Snapshot Chain**: Each snapshot points to parent
3. **Metadata Retention**: Old snapshots retained
4. **Point-in-Time Query**: Find closest snapshot

**Example:**
```sql
SELECT * FROM transactions
FOR SYSTEM_TIME AS OF '2026-08-24 10:00:00'
```

**Metadata Flow:**
1. Find snapshot closest to requested time
2. Load that snapshot's manifest list
3. Load manifests
4. Read data files from that snapshot

**Benefits:**
- No data rewrite
- Immutable evidence
- Complete audit trail

---

### Question 5: What are the best practices for managing Iceberg metadata?

**Answer:**

**Best Practices:**

1. **Snapshot Expiration**
   - Remove old snapshots
   - Reduce metadata size
   - Maintain retention policy

2. **Manifest Compaction**
   - Merge small manifests
   - Reduce manifest count
   - Improve query planning

3. **File Compaction**
   - Merge small data files
   - Optimize file sizes
   - Improve read performance

4. **Metadata Monitoring**
   - Track file counts
   - Monitor query performance
   - Alert on degradation

5. **Retention Policy**
   - Define retention periods
   - Automate cleanup
   - Balance cost vs history

**Example:**
```sql
-- Expire old snapshots
CALL system.expire_snapshots(
    table => 'db.transactions',
    older_than => TIMESTAMP '2026-07-24 00:00:00',
    retain_last => 10
);

-- Rewrite manifests
CALL system.rewrite_manifests(
    table => 'db.transactions'
);
```

---

## 📝 Summary

| Layer | Purpose | Key Benefit |
|-------|---------|-------------|
| **Catalog** | Table name → metadata | Entry point |
| **Table Metadata** | Schema, snapshots | Version control |
| **Snapshot** | Immutable state | Time travel |
| **Manifest List** | Lists manifests | Efficient access |
| **Manifest** | Lists files + stats | Data pruning |
| **Data Files** | Parquet storage | Actual data |

**Key Insight**: Each layer adds efficiency by reducing scan scope. The hierarchy enables O(1) to O(M) access patterns instead of O(N) full scans.
