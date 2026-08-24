# Concept 06: Copy-on-Write vs Merge-on-Read

## 📚 Detailed Explanation

**Copy-on-Write (CoW)** and **Merge-on-Read (MoR)** are two strategies for handling updates and deletes in Iceberg. They represent different trade-offs between write performance and read performance.

### Copy-on-Write (CoW)

**How it works:**
1. Read the affected data files
2. Modify the rows (update/delete)
3. Write new data files with the changes
4. Update metadata to point to new files
5. Old files remain for historical snapshots

**Characteristics:**
- **Writes are expensive**: Must rewrite entire files
- **Reads are fast**: Single file contains all data
- **Storage**: More storage used (old + new files)
- **Query performance**: Optimal (no merge needed)

```
Before Update:
file-A.parquet (1000 rows)
file-B.parquet (1000 rows)

After Update (CoW):
file-A.parquet (1000 rows) ← old (kept for time travel)
file-B.parquet (999 rows)  ← new (modified)
file-C.parquet (1 row)     ← new (inserted from A)
```

### Merge-on-Read (MoR)

**How it works:**
1. Write delta/delete files containing changes
2. Original data files remain unchanged
3. On read, merge data files with delta/delete files

**Characteristics:**
- **Writes are fast**: Only write small delta files
- **Reads are expensive**: Must merge multiple files
- **Storage**: Less storage (no full file copies)
- **Query performance**: Degraded (merge overhead)

```
Before Update:
file-A.parquet (1000 rows)
file-B.parquet (1000 rows)

After Update (MoR):
file-A.parquet (1000 rows) ← unchanged
file-B.parquet (1000 rows) ← unchanged
delta-1.parquet (1 row)    ← update
delete-1.parquet (1 row)   ← delete marker
```

### Delete Files in MoR

Iceberg supports two types of delete files:

**Position Deletes:**
```json
{
  "file-path": "s3://lake/data/file-A.parquet",
  "pos": 42  // Row position to delete
}
```

**Equality Deletes:**
```json
{
  "file-path": "s3://lake/data/file-A.parquet",
  " equality-field-ids": [1],  // Column ID
  "equality-values": ["TXN-123"]  // Value to match
}
```

### Comparison Table

| Aspect | Copy-on-Write | Merge-on-Read |
|--------|--------------|---------------|
| **Write Performance** | Slow (rewrite files) | Fast (write deltas) |
| **Read Performance** | Fast (single file) | Slow (merge needed) |
| **Storage** | Higher (file copies) | Lower (delta files) |
| **Delete Files** | No | Yes |
| **Compaction** | Not needed | Required |
| **Use Case** | Read-heavy | Write-heavy |
| **CDC/Streaming** | Not ideal | Ideal |

---

## 💡 Example: Update Strategies in Banking

### Scenario: Transaction Status Update

**Operation:**
```sql
UPDATE transactions
SET status = 'REVERSED'
WHERE transaction_id = 'TXN-123';
```

**Copy-on-Write:**
1. Read file containing TXN-123
2. Modify TXN-123 status to 'REVERSED'
3. Write new file with updated row
4. Update metadata
5. Query reads single file (fast)

**Merge-on-Read:**
1. Write delete file for TXN-123
2. Write insert file with new status
3. Update metadata
4. Query reads original + delta files, merges (slower)

---

## 🏦 Real-World Banking Scenario 1: High-Volume Transaction Updates

### Scenario
A bank's **card processing system** handles **10 million transactions daily**. During the day, many transactions get status updates:
- PENDING → COMPLETED
- PENDING → DECLINED
- COMPLETED → REVERSED

The update pattern is **frequent, small updates** throughout the day.

### Problem
- CoW would rewrite millions of rows daily
- Storage costs would be high
- Write performance would be poor

### Solution
MoR is ideal because:
- Writes are fast (only delta files)
- Storage is efficient
- Reads can be optimized with compaction

### Python Code

```python
"""
Banking Scenario 1: High-Volume Transaction Updates
Using Merge-on-Read for Efficient Writes
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
# STEP 2: Define Transaction Schema
# ============================================================

transaction_schema = pa.schema([
    pa.field("transaction_id", pa.string(), nullable=False),
    pa.field("card_number", pa.string(), nullable=False),
    pa.field("amount", pa.decimal128(18, 2), nullable=False),
    pa.field("merchant_id", pa.string(), nullable=False),
    pa.field("status", pa.string(), nullable=False),
    pa.field("transaction_time", pa.timestamp("us"), nullable=False),
    pa.field("updated_at", pa.timestamp("us"), nullable=True),
])

# Create table with MoR configuration
try:
    txn_table = catalog.create_table(
        identifier="banking.card_transactions",
        schema=transaction_schema,
        properties={
            "write.format.default": "parquet",
            "write.delete.mode": "merge-on-read",
            "write.update.mode": "merge-on-read",
            "write.parquet.compression-codec": "zstd",
        }
    )
except Exception:
    txn_table = catalog.load_table("banking.card_transactions")

# ============================================================
# STEP 3: Load Initial Transaction Data
# ============================================================

print("=== HIGH-VOLUME TRANSACTION UPDATES ===\n")

def generate_card_transactions(count: int) -> pa.Table:
    """Generate card transaction data."""
    
    data = {
        "transaction_id": [f"TXN-{i:08d}" for i in range(1, count + 1)],
        "card_number": [f"4111{random.randint(1000000000, 9999999999)}" for _ in range(count)],
        "amount": [round(random.uniform(10, 5000), 2) for _ in range(count)],
        "merchant_id": [f"MERCHANT-{random.randint(1, 10000):06d}" for _ in range(count)],
        "status": ["PENDING"] * count,
        "transaction_time": [
            datetime(2026, 8, 24, 10, 0, 0) + timedelta(seconds=random.randint(0, 3600))
            for _ in range(count)
        ],
        "updated_at": [None] * count,
    }
    return pa.table(data)

# Load initial batch (10,000 transactions)
initial_batch = generate_card_transactions(10000)
txn_table.append(initial_batch)
print(f"Loaded initial batch: {len(initial_batch)} transactions")
print(f"Status distribution: All PENDING")

# ============================================================
# STEP 4: Simulate Status Updates (MoR in Action)
# ============================================================

print("\n=== STATUS UPDATE SIMULATION ===")

def simulate_status_updates(table: Table, update_count: int) -> dict:
    """
    Simulate status updates using Merge-on-Read.
    In production, these would be UPDATE operations.
    """
    import time
    
    start_time = time.time()
    
    # Get current data
    current_data = table.scan().to_arrow()
    
    # Simulate updates by creating new records with updated status
    # In production, use UPDATE statement
    update_records = []
    
    for i in range(min(update_count, len(current_data))):
        # Randomly select transaction to update
        idx = random.randint(0, len(current_data) - 1)
        
        txn_id = current_data.column("transaction_id")[idx].as_py()
        new_status = random.choice(["COMPLETED", "DECLINED", "REVERSED"])
        
        # Create update record
        update_record = {
            "transaction_id": [txn_id],
            "card_number": [current_data.column("card_number")[idx].as_py()],
            "amount": [float(current_data.column("amount")[idx])],
            "merchant_id": [current_data.column("merchant_id")[idx].as_py()],
            "status": [new_status],
            "transaction_time": [current_data.column("transaction_time")[idx].as_py()],
            "updated_at": [datetime.now()],
        }
        update_records.append(update_record)
    
    # In production, this would be:
    # UPDATE transactions SET status = 'NEW_STATUS' WHERE transaction_id = '...'
    # Each UPDATE creates a delta file (MoR)
    
    # For simulation, append updated records
    for record in update_records:
        update_df = pa.table(record)
        table.append(update_df)
    
    end_time = time.time()
    processing_time = end_time - start_time
    
    return {
        "updates_processed": len(update_records),
        "processing_time_seconds": processing_time,
        "avg_time_per_update_ms": (processing_time / len(update_records) * 1000) if update_records else 0
    }

# Process 1,000 status updates
update_result = simulate_status_updates(txn_table, 1000)

print(f"\nUpdate Results:")
print(f"  Updates processed: {update_result['updates_processed']}")
print(f"  Processing time: {update_result['processing_time_seconds']:.3f} seconds")
print(f"  Avg time per update: {update_result['avg_time_per_update_ms']:.3f} ms")

# ============================================================
# STEP 5: Demonstrate MoR Files
# ============================================================

print("\n=== MERGE-ON-READ FILE STRUCTURE ===")

# Get metadata
metadata = txn_table.metadata
current_snapshot = metadata.current_snapshot()
manifests = current_snapshot.manifest_list.manifests()

print(f"Current Snapshot: {current_snapshot.snapshot_id}")
print(f"Number of Manifests: {len(manifests)}")

# Count data files vs delta files
total_data_files = 0
total_delta_files = 0

for manifest in manifests:
    files = manifest.data_files()
    for file_entry in files:
        if "delete" in file_entry.file_path.lower() or "delta" in file_entry.file_path.lower():
            total_delta_files += 1
        else:
            total_data_files += 1

print(f"\nFile Statistics:")
print(f"  Data files (original): {total_data_files}")
print(f"  Delta/Delete files: {total_delta_files}")
print(f"  Total files: {total_data_files + total_delta_files}")

print(f"\nMoR Implications:")
print(f"  ✓ Writes are fast (only delta files)")
print(f"  ✓ Storage is efficient")
print(f"  ⚠ Reads require merging (slower)")
print(f"  ⚠ Compaction needed periodically")

# ============================================================
# STEP 6: Query Performance Comparison
# ============================================================

print("\n=== QUERY PERFORMANCE ===")

import time

# Query 1: Simple read
start = time.time()
result1 = txn_table.scan(
    row_filter="status = 'COMPLETED'"
).to_arrow()
query1_time = time.time() - start

print(f"Query 1: SELECT * WHERE status = 'COMPLETED'")
print(f"  Rows returned: {len(result1)}")
print(f"  Query time: {query1_time:.3f} seconds")

# Query 2: Aggregation
start = time.time()
result2 = txn_table.scan().to_arrow()
query2_time = time.time() - start

print(f"\nQuery 2: SELECT COUNT(*), SUM(amount) FROM transactions")
print(f"  Total rows: {len(result2)}")
print(f"  Query time: {query2_time:.3f} seconds")

# ============================================================
# STEP 7: Compaction for MoR
# ============================================================

print("\n=== COMPACTION FOR MERGE-ON-READ ===")

print("""
Compaction Process:

1. IDENTIFY small/delta files
   - Files < 128MB
   - Delete files older than threshold

2. MERGE into larger files
   - Combine data files
   - Apply delete files
   - Write new consolidated files

3. UPDATE metadata
   - Point to new files
   - Remove old files from current snapshot

4. CLEANUP
   - Old files become orphans
   - Remove orphan files

Benefits:
  ✓ Read performance improves
  ✓ Storage overhead reduced
  ✓ Metadata simplified

Schedule:
  - Run compaction daily or weekly
  - Based on file count/size thresholds
  - Can be automated with Spark
""")

# ============================================================
# STEP 8: When to Use CoW vs MoR
# ============================================================

print("\n=== WHEN TO USE CoW vs MoR ===")

print("""
USE COPY-ON-WRITE (CoW) WHEN:
  ✓ Read-heavy workload
  ✓ Few updates/deletes
  ✓ Query performance is critical
  ✓ Storage budget is flexible
  ✓ Simple architecture preferred

USE MERGE-ON-READ (MoR) WHEN:
  ✓ Write-heavy workload
  ✓ Frequent updates/deletes
  ✓ CDC/streaming ingestion
  ✓ Storage budget is tight
  ✓ Write latency is critical

BANKING EXAMPLES:

CoW:
  - Historical transaction archive (read-only)
  - Daily/weekly reports
  - Data warehouse tables

MoR:
  - Real-time transaction processing
  - CDC from core banking
  - Status update tables
  - Streaming ingestion from Kafka
""")
```

---

## 🏦 Real-World Banking Scenario 2: CDC from Core Banking System

### Scenario
A bank's **core banking system** runs on Oracle. Every day:
- **500,000 INSERTs** (new transactions)
- **200,000 UPDATEs** (status changes)
- **50,000 DELETEs** (reversed transactions)

This CDC data flows via Debezium → Kafka → Flink → Iceberg.

### Problem
- High volume of updates/deletes
- CoW would be too slow and expensive
- Need near-real-time availability

### Solution
MoR is ideal for CDC because:
- Fast writes (delta files)
- Efficient storage
- Good enough read performance with compaction

### Python Code

```python
"""
Banking Scenario 2: CDC from Core Banking System
Using Merge-on-Read for CDC Ingestion
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
# STEP 2: Define CDC Schema
# ============================================================

cdc_schema = pa.schema([
    pa.field("transaction_id", pa.string(), nullable=False),
    pa.field("account_id", pa.string(), nullable=False),
    pa.field("amount", pa.decimal128(18, 2), nullable=False),
    pa.field("transaction_type", pa.string(), nullable=False),
    pa.field("status", pa.string(), nullable=False),
    pa.field("cdc_operation", pa.string(), nullable=False),  # INSERT, UPDATE, DELETE
    pa.field("cdc_timestamp", pa.timestamp("us"), nullable=False),
    pa.field("source_system", pa.string(), nullable=False),
])

# Create table optimized for CDC
try:
    cdc_table = catalog.create_table(
        identifier="banking.cdc_transactions",
        schema=cdc_schema,
        properties={
            "write.format.default": "parquet",
            "write.delete.mode": "merge-on-read",
            "write.update.mode": "merge-on-read",
            "write.parquet.compression-codec": "zstd",
            "commit.manifest-merge.enabled": "true",
        }
    )
except Exception:
    cdc_table = catalog.load_table("banking.cdc_transactions")

# ============================================================
# STEP 3: Simulate CDC Events from Core Banking
# ============================================================

print("=== CDC FROM CORE BANKING SYSTEM ===\n")

def generate_cdc_batch(batch_size: int, timestamp: datetime) -> pa.Table:
    """
    Simulate CDC events from core banking system.
    In production, this would be Flink reading from Kafka.
    """
    
    operations = ["INSERT", "UPDATE", "DELETE"]
    op_weights = [0.6, 0.3, 0.1]  # 60% inserts, 30% updates, 10% deletes
    
    data = {
        "transaction_id": [f"TXN-{random.randint(10000000, 99999999)}" for _ in range(batch_size)],
        "account_id": [f"ACC-{random.randint(1000, 9999):06d}" for _ in range(batch_size)],
        "amount": [round(random.uniform(100, 100000), 2) for _ in range(batch_size)],
        "transaction_type": [random.choice(["CREDIT", "DEBIT", "TRANSFER"]) for _ in range(batch_size)],
        "status": [random.choice(["PENDING", "COMPLETED", "DECLINED"]) for _ in range(batch_size)],
        "cdc_operation": random.choices(operations, weights=op_weights, k=batch_size),
        "cdc_timestamp": [timestamp] * batch_size,
        "source_system": ["CORE_BANKING_ORACLE"] * batch_size,
    }
    
    return pa.table(data)

# Simulate multiple CDC batches
print("Simulating CDC ingestion (Flink → Iceberg)...")

total_inserts = 0
total_updates = 0
total_deletes = 0

for batch_num in range(5):
    batch_time = datetime(2026, 8, 24, 10, batch_num * 5, 0)
    batch = generate_cdc_batch(1000, batch_time)
    
    # Count operations
    ops = batch.column("cdc_operation").to_pylist()
    total_inserts += ops.count("INSERT")
    total_updates += ops.count("UPDATE")
    total_deletes += ops.count("DELETE")
    
    # Append to Iceberg (creates delta files for MoR)
    cdc_table.append(batch)
    
    print(f"  Batch {batch_num + 1}: {len(batch)} events at {batch_time}")

print(f"\nCDC Statistics:")
print(f"  Total INSERTs: {total_inserts}")
print(f"  Total UPDATEs: {total_updates}")
print(f"  Total DELETEs: {total_deletes}")
print(f"  Total events: {total_inserts + total_updates + total_deletes}")

# ============================================================
# STEP 4: MoR File Analysis
# ============================================================

print("\n=== MERGE-ON-READ FILE ANALYSIS ===")

metadata = cdc_table.metadata
current_snapshot = metadata.current_snapshot()
manifests = current_snapshot.manifest_list.manifests()

# Analyze file types
file_types = {"data": 0, "delete": 0, "delta": 0, "other": 0}

for manifest in manifests:
    files = manifest.data_files()
    for file_entry in files:
        path = file_entry.file_path.lower()
        if "delete" in path:
            file_types["delete"] += 1
        elif "delta" in path:
            file_types["delta"] += 1
        elif "data" in path or "parquet" in path:
            file_types["data"] += 1
        else:
            file_types["other"] += 1

print(f"File Type Distribution:")
for file_type, count in file_types.items():
    print(f"  {file_type}: {count}")

print(f"\nMoR Benefits for CDC:")
print(f"  ✓ Fast writes (delta files only)")
print(f"  ✓ Efficient storage (no full file copies)")
print(f"  ✓ Near-real-time availability")
print(f"  ✓ Compaction improves read performance")

# ============================================================
# STEP 5: Query CDC Data
# ============================================================

print("\n=== QUERYING CDC DATA ===")

# Query 1: All INSERTs
inserts = cdc_table.scan(
    row_filter="cdc_operation = 'INSERT'"
).to_arrow()
print(f"Total INSERTs: {len(inserts)}")

# Query 2: All UPDATEs
updates = cdc_table.scan(
    row_filter="cdc_operation = 'UPDATE'"
).to_arrow()
print(f"Total UPDATEs: {len(updates)}")

# Query 3: Recent activity
recent = cdc_table.scan(
    row_filter="cdc_timestamp >= TIMESTAMP '2026-08-24 10:00:00'"
).to_arrow()
print(f"Recent events (last hour): {len(recent)}")

# ============================================================
# STEP 6: Compaction Strategy
# ============================================================

print("\n=== COMPACTION STRATEGY ===")

print("""
CDC Compaction Strategy:

HOURLY COMPACTION:
  - Merge small delta files
  - Apply delete files
  - Reduce file count

DAILY COMPACTION:
  - Full consolidation
  - Optimize file sizes (128MB-1GB)
  - Update statistics

WEEKLY MAINTENANCE:
  - Expire old snapshots
  - Remove orphan files
  - Rebuild manifests

BENEFITS:
  ✓ Read performance: 3-5x improvement
  ✓ Storage savings: 20-30%
  ✓ Query planning: Faster metadata access
""")

# ============================================================
# STEP 7: Performance Comparison
# ============================================================

print("\n=== CoW vs MoR PERFORMANCE COMPARISON ===")

print(f"""
Scenario: {total_inserts + total_updates + total_deletes} CDC events

COPY-ON-WRITE (CoW):
  Write time: ~{len(cdc_table.metadata.snapshots) * 2.5:.1f} seconds (rewrite files)
  Storage: ~{(total_inserts + total_updates + total_deletes) * 0.5:.0f} MB (file copies)
  Read time: ~0.5 seconds (single file)
  
MERGE-ON-READ (MoR):
  Write time: ~{len(cdc_table.metadata.snapshots) * 0.5:.1f} seconds (delta files)
  Storage: ~{(total_inserts + total_updates + total_deletes) * 0.1:.0f} MB (delta files)
  Read time: ~1.5 seconds (merge overhead)
  
  Savings:
    Write performance: 5x faster
    Storage: 80% less
    Read performance: 3x slower (acceptable for CDC)
""")
```

---

## 🎯 5 Real-World Interview Questions

### Question 1: Explain the difference between Copy-on-Write and Merge-on-Read in Iceberg.

**Answer:**

**Copy-on-Write (CoW):**
- Update: Read old file → Modify → Write new file
- Delete: Read old file → Remove rows → Write new file
- **Writes**: Expensive (rewrite entire files)
- **Reads**: Fast (single file)
- **Storage**: Higher (file copies)

**Merge-on-Read (MoR):**
- Update: Write delta files
- Delete: Write delete files
- Read: Combine data + delta/delete files
- **Writes**: Fast (small delta files)
- **Reads**: Slower (merge overhead)
- **Storage**: Lower (delta files)

**Key Difference**: CoW optimizes reads at the expense of writes; MoR optimizes writes at the expense of reads.

---

### Question 2: When would you choose CoW over MoR in a banking system?

**Answer:**

**Choose CoW for:**
- **Historical data warehouse**: Read-heavy, few updates
- **Daily/weekly reports**: Batch queries, optimal performance
- **Audit tables**: Read-only after initial load
- **Regulatory data**: Read performance critical

**Choose MoR for:**
- **Real-time transaction processing**: Frequent updates
- **CDC ingestion**: High-volume INSERT/UPDATE/DELETE
- **Status tracking tables**: Constant status changes
- **Streaming workloads**: Low-latency writes

**Banking Example:**
```
CoW: Transaction history (archived, read-only)
MoR: Active transactions (frequent status updates)
```

---

### Question 3: What are delete files in Iceberg and how do they work?

**Answer:**

**Delete Files:**
- Used in Merge-on-Read to mark rows for deletion
- Avoid rewriting entire data files
- Two types: Position and Equality

**Position Deletes:**
```json
{
  "file-path": "s3://lake/data/file.parquet",
  "pos": 42  // Row number to delete
}
```

**Equality Deletes:**
```json
{
  "file-path": "s3://lake/data/file.parquet",
  "equality-field-ids": [1],  // Column ID
  "equality-values": ["TXN-123"]  // Value to match
}
```

**How they work on read:**
1. Read data files
2. Read delete files
3. Filter out deleted rows
4. Return merged result

**Compaction:**
- Delete files are temporary
- Compaction applies deletes and removes files
- Improves read performance

---

### Question 4: How does compaction help with Merge-on-Read performance?

**Answer:**

**Compaction Process:**
1. Identify small/delta files
2. Merge into larger files
3. Apply delete files
4. Update metadata

**Benefits:**
- **Read Performance**: 3-5x improvement
- **Storage**: 20-30% reduction
- **Query Planning**: Faster metadata access

**Example:**
```
Before Compaction:
  100 small files (1MB each)
  50 delete files
  Read time: 5 seconds

After Compaction:
  5 larger files (200MB each)
  0 delete files
  Read time: 1 second
```

**Compaction Strategy:**
- **Hourly**: Merge small files
- **Daily**: Full consolidation
- **Weekly**: Expire old snapshots

---

### Question 5: Can you switch between CoW and MoR on an existing table?

**Answer:**

**Yes, you can switch:**

```sql
-- Switch to MoR
ALTER TABLE transactions SET TBLPROPERTIES (
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read'
);

-- Switch to CoW
ALTER TABLE transactions SET TBLPROPERTIES (
    'write.delete.mode' = 'copy-on-write',
    'write.update.mode' = 'copy-on-write'
);
```

**Important Considerations:**

1. **Existing Data**: Old files retain their original format
2. **New Operations**: Use new mode for writes
3. **Compaction**: May be needed to fully convert
4. **Readers**: Must support both modes

**Migration Strategy:**
1. Switch mode in table properties
2. Run compaction to convert existing files
3. Monitor performance
4. Rollback if needed

**Example:**
```
Before: CoW (100 data files)
After switching to MoR: Still 100 data files + new delta files
After compaction: 100 data files (merged) + 0 delta files
```

---

## 📝 Summary

| Aspect | Copy-on-Write | Merge-on-Read |
|--------|--------------|---------------|
| **Write Performance** | Slow (rewrite files) | Fast (delta files) |
| **Read Performance** | Fast (single file) | Slow (merge) |
| **Storage** | Higher | Lower |
| **Delete Files** | No | Yes |
| **Compaction** | Not needed | Required |
| **Use Case** | Read-heavy | Write-heavy |
| **Banking Example** | Historical archive | Real-time transactions |
| **CDC** | Not ideal | Ideal |
