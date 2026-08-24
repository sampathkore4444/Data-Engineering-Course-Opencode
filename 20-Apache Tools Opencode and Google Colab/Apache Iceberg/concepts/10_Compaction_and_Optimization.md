# Concept 10: Compaction and Optimization

## 📚 Detailed Explanation

**Compaction** in Apache Iceberg is the process of merging small data files into larger ones and optimizing file organization. It's essential for maintaining query performance in production data lakes.

### Why Compaction is Needed

**The Small File Problem:**
```
Streaming writes (Kafka → Flink → Iceberg):
  - 1 file every 5 minutes
  - After 1 month: 8,640 files
  - After 1 year: 103,680 files

Result:
  - Poor query performance
  - High metadata overhead
  - Expensive object storage operations
```

**Without Compaction:**
```
Query: SELECT * FROM transactions WHERE date = '2026-08-24'
  - Scan 103,680 files
  - Open each file
  - Read metadata
  - Filter rows
  - Time: 10 minutes
```

**With Compaction:**
```
Query: SELECT * FROM transactions WHERE date = '2026-08-24'
  - Scan 100 files (compacted)
  - Open each file
  - Read metadata
  - Filter rows
  - Time: 6 seconds
```

### Types of Compaction

| Type | Description | When to Use |
|------|-------------|-------------|
| **Data File Compaction** | Merge small data files | Streaming ingestion |
| **Manifest Compaction** | Merge small manifests | Many small files |
| **Delete File Compaction** | Apply delete files | MoR tables |

### Compaction Strategies

**1. Size-Based Compaction**
```python
# Merge files smaller than 128MB
compaction_threshold = 128 * 1024 * 1024  # 128MB
```

**2. Count-Based Compaction**
```python
# Merge when file count exceeds threshold
max_files_per_partition = 100
```

**3. Time-Based Compaction**
```python
# Compact daily/hourly
compaction_schedule = "daily"
```

### Compaction Benefits

1. **Read Performance**: 3-10x faster queries
2. **Storage Efficiency**: 20-30% space savings
3. **Metadata Overhead**: Reduced file count
4. **Query Planning**: Faster metadata access
5. **Object Storage**: Fewer API calls

---

## 💡 Example: Compaction in Streaming

### Scenario: Real-Time Transaction Ingestion

**Without Compaction:**
```
Day 1: 288 files (1 per 5 minutes)
Day 7: 2,016 files
Day 30: 8,640 files
Query time: 5 minutes
```

**With Daily Compaction:**
```
Day 1: 288 files → 5 files (compacted)
Day 7: 35 files (5 per day × 7)
Day 30: 150 files (5 per day × 30)
Query time: 3 seconds
```

---

## 🏦 Real-World Banking Scenario 1: Streaming Transaction Compaction

### Scenario
A bank's **card processing system** streams **100,000 transactions per hour** to Iceberg. Without compaction, the table accumulates thousands of small files daily, degrading query performance.

### Problem
- 100,000 transactions/hour = 2,400,000/day
- Small files accumulate rapidly
- Query performance degrades

### Solution
Daily compaction:
- Merge small files into larger ones
- Maintain optimal file sizes (128MB-1GB)
- Improve query performance

### Python Code

```python
"""
Banking Scenario 1: Streaming Transaction Compaction
Using Iceberg Compaction for Performance
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
# STEP 2: Define Streaming Transaction Schema
# ============================================================

print("=== STREAMING TRANSACTION COMPACTION ===\n")

schema = pa.schema([
    pa.field("transaction_id", pa.string(), nullable=False),
    pa.field("card_number", pa.string(), nullable=False),
    pa.field("amount", pa.decimal128(18, 2), nullable=False),
    pa.field("merchant_id", pa.string(), nullable=False),
    pa.field("status", pa.string(), nullable=False),
    pa.field("transaction_time", pa.timestamp("us"), nullable=False),
])

# Create table for streaming data
try:
    streaming_table = catalog.create_table(
        identifier="banking.streaming_transactions",
        schema=schema,
        properties={
            "write.format.default": "parquet",
            "write.parquet.compression-codec": "zstd",
            "write.target-file-size-bytes": "134217728",  # 128MB
        }
    )
except Exception:
    streaming_table = catalog.load_table("banking.streaming_transactions")

# ============================================================
# STEP 3: Simulate Streaming Ingestion
# ============================================================

print("--- Simulating Streaming Ingestion ---")

def generate_streaming_batch(batch_num: int, batch_size: int = 1000) -> pa.Table:
    """Generate a batch of streaming transactions."""
    
    base_time = datetime(2026, 8, 24, 0, 0, 0)
    
    data = {
        "transaction_id": [f"TXN-{batch_num}-{i:06d}" for i in range(batch_size)],
        "card_number": [f"4111{random.randint(1000000000, 9999999999)}" for _ in range(batch_size)],
        "amount": [round(random.uniform(10, 5000), 2) for _ in range(batch_size)],
        "merchant_id": [f"MERCHANT-{random.randint(1, 10000):06d}" for _ in range(batch_size)],
        "status": [random.choice(["COMPLETED", "PENDING", "DECLINED"]) for _ in range(batch_size)],
        "transaction_time": [
            base_time + timedelta(minutes=batch_num * 5 + i)
            for i in range(batch_size)
        ],
    }
    return pa.table(data)

# Simulate 24 hours of streaming (288 batches of 5 minutes each)
print("Simulating 24 hours of streaming data...")
total_records = 0
num_batches = 288  # 24 hours * 12 batches/hour

start_time = time.time()

for batch_num in range(num_batches):
    batch = generate_streaming_batch(batch_num)
    streaming_table.append(batch)
    total_records += len(batch)
    
    if batch_num % 48 == 0:  # Log every 4 hours
        print(f"  Batch {batch_num}: {total_records:,} total records")

streaming_time = time.time() - start_time
print(f"\nStreaming completed: {total_records:,} records in {streaming_time:.2f} seconds")

# ============================================================
# STEP 4: Analyze File Count (Before Compaction)
# ============================================================

print("\n--- File Analysis (Before Compaction) ---")

metadata = streaming_table.metadata
current_snapshot = metadata.current_snapshot()
manifests = current_snapshot.manifest_list.manifests()

# Count files
total_files = 0
total_rows = 0
small_files = 0

for manifest in manifests:
    files = manifest.data_files()
    for file_entry in files:
        total_files += 1
        total_rows += file_entry.record_count
        
        # Check file size (estimate based on rows)
        if file_entry.record_count < 10000:  # Small file threshold
            small_files += 1

print(f"Total Files: {total_files}")
print(f"Total Rows: {total_rows:,}")
print(f"Small Files (<10K rows): {small_files}")
print(f"Avg Rows per File: {total_rows // total_files if total_files > 0 else 0}")

# ============================================================
# STEP 5: Query Performance Test (Before Compaction)
# ============================================================

print("\n--- Query Performance (Before Compaction) ---")

# Query 1: Simple select
start = time.time()
result1 = streaming_table.scan().to_arrow()
query1_time = time.time() - start

print(f"Query 1: Full table scan")
print(f"  Rows: {len(result1):,}")
print(f"  Time: {query1_time:.3f} seconds")

# Query 2: Filtered query
start = time.time()
result2 = streaming_table.scan(
    row_filter="amount > 1000"
).to_arrow()
query2_time = time.time() - start

print(f"\nQuery 2: Filtered (amount > 1000)")
print(f"  Rows: {len(result2):,}")
print(f"  Time: {query2_time:.3f} seconds")

# ============================================================
# STEP 6: Perform Compaction
# ============================================================

print("\n--- Performing Compaction ---")

print("Compaction Strategy:")
print("  1. Merge files < 128MB")
print("  2. Target file size: 128MB-1GB")
print("  3. Maintain partition structure")

# Simulate compaction (in production, use Spark)
print("\nSimulating compaction...")

# In production, you would run:
# spark.sql("""
#     CALL catalog.system.rewrite_data_files(
#         table => 'banking.streaming_transactions',
#         options => map(
#             'min-file-size-bytes', '134217728',
#             'max-file-size-bytes', '1073741824'
#         )
#     )
# """)

# For demonstration, we'll create compacted data
def create_compacted_data(table: Table) -> pa.Table:
    """Simulate compaction by reading all data and creating larger files."""
    
    # Read all data
    all_data = table.scan().to_arrow()
    
    # In production, this would be done by Spark
    # Here we simulate by creating a single large batch
    
    return all_data

# Perform compaction
compacted_data = create_compacted_data(streaming_table)

# In production, you would:
# 1. Read small files
# 2. Merge into larger files
# 3. Write new files
# 4. Update metadata atomically

print("Compaction completed:")
print(f"  Files merged: {total_files} → ~{total_files // 10}")
print(f"  Small files eliminated: {small_files}")
print(f"  Expected performance improvement: 5-10x")

# ============================================================
# STEP 7: Query Performance Test (After Compaction)
# ============================================================

print("\n--- Query Performance (After Compaction) ---")

# Note: In production, after compaction, file count would be lower
# Here we simulate the improvement

print("Expected Performance After Compaction:")
print("  Query 1: Full table scan")
print("    Before: {query1_time:.3f} seconds")
print("    After: ~{query1_time/5:.3f} seconds (5x faster)")
print("")
print("  Query 2: Filtered query")
print("    Before: {query2_time:.3f} seconds")
print("    After: ~{query2_time/3:.3f} seconds (3x faster)")

# ============================================================
# STEP 8: Compaction Scheduling
# ============================================================

print("\n--- Compaction Scheduling ---")

print("""
COMPACTION SCHEDULE FOR BANKING:

HOURLY:
  - Not recommended (too frequent)
  - Only for critical tables

DAILY (Recommended):
  - Compact streaming tables
  - Merge small files
  - Optimize file sizes

WEEKLY:
  - Full table optimization
  - Manifest compaction
  - Snapshot cleanup

MONTHLY:
  - Partition optimization
  - Historical data archival
  - Performance review

EXAMPLE SCHEDULE:
  Daily 2:00 AM: Compact streaming tables
  Weekly Sunday 3:00 AM: Full optimization
  Monthly 1st 4:00 AM: Archive old data
""")

# ============================================================
# STEP 9: Compaction Monitoring
# ============================================================

print("--- Compaction Monitoring ---")

print("""
KEY METRICS TO MONITOR:

1. FILE COUNT
   - Before compaction
   - After compaction
   - Target: <100 files per partition

2. FILE SIZE
   - Average file size
   - Target: 128MB-1GB
   - Alert if <10MB

3. QUERY PERFORMANCE
   - Query time before/after
   - Target: <10 seconds for common queries

4. STORAGE USAGE
   - Total storage before/after
   - Savings from compaction

5. COMPACTION DURATION
   - Time to complete
   - Resource usage

MONITORING TOOLS:
  - Iceberg metadata queries
  - Spark UI
  - Custom dashboards
  - Alerts for degradation
""")
```

---

## 🏦 Real-World Banking Scenario 2: CDC Compaction for Fraud Detection

### Scenario
A bank's **fraud detection system** receives CDC events from core banking. Events include INSERT, UPDATE, and DELETE operations. Without compaction:
- Delete files accumulate
- Query performance degrades
- Storage costs increase

### Problem
- 500,000 CDC events/day
- 100,000 UPDATEs, 50,000 DELETEs
- Delete files accumulate
- MoR performance degrades

### Solution
Daily compaction:
- Apply delete files
- Merge delta files
- Optimize for queries

### Python Code

```python
"""
Banking Scenario 2: CDC Compaction for Fraud Detection
Using Iceberg Compaction for CDC Workloads
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
# STEP 2: Define CDC Transaction Schema
# ============================================================

print("=== CDC COMPACTION FOR FRAUD DETECTION ===\n")

cdc_schema = pa.schema([
    pa.field("transaction_id", pa.string(), nullable=False),
    pa.field("account_id", pa.string(), nullable=False),
    pa.field("amount", pa.decimal128(18, 2), nullable=False),
    pa.field("status", pa.string(), nullable=False),
    pa.field("cdc_operation", pa.string(), nullable=False),
    pa.field("cdc_timestamp", pa.timestamp("us"), nullable=False),
])

# Create table for CDC data
try:
    cdc_table = catalog.create_table(
        identifier="banking.cdc_fraud_transactions",
        schema=cdc_schema,
        properties={
            "write.format.default": "parquet",
            "write.delete.mode": "merge-on-read",
            "write.update.mode": "merge-on-read",
        }
    )
except Exception:
    cdc_table = catalog.load_table("banking.cdc_fraud_transactions")

# ============================================================
# STEP 3: Simulate CDC Events
# ============================================================

print("--- Simulating CDC Events ---")

def generate_cdc_batch(batch_num: int, batch_size: int = 500) -> pa.Table:
    """Generate CDC events."""
    
    operations = ["INSERT", "UPDATE", "DELETE"]
    op_weights = [0.7, 0.2, 0.1]  # 70% inserts, 20% updates, 10% deletes
    
    data = {
        "transaction_id": [f"TXN-{random.randint(10000000, 99999999)}" for _ in range(batch_size)],
        "account_id": [f"ACC-{random.randint(1000, 9999):06d}" for _ in range(batch_size)],
        "amount": [round(random.uniform(100, 100000), 2) for _ in range(batch_size)],
        "status": [random.choice(["PENDING", "COMPLETED", "DECLINED"]) for _ in range(batch_size)],
        "cdc_operation": random.choices(operations, weights=op_weights, k=batch_size),
        "cdc_timestamp": [
            datetime(2026, 8, 24, 0, 0, 0) + timedelta(minutes=batch_num * 5 + i)
            for i in range(batch_size)
        ],
    }
    return pa.table(data)

# Simulate 100 batches (500,000 events)
print("Simulating CDC events (500,000 events)...")
total_events = 0
total_inserts = 0
total_updates = 0
total_deletes = 0

for batch_num in range(100):
    batch = generate_cdc_batch(batch_num)
    cdc_table.append(batch)
    
    total_events += len(batch)
    
    # Count operations
    ops = batch.column("cdc_operation").to_pylist()
    total_inserts += ops.count("INSERT")
    total_updates += ops.count("UPDATE")
    total_deletes += ops.count("DELETE")

print(f"\nCDC Statistics:")
print(f"  Total Events: {total_events:,}")
print(f"  INSERTs: {total_inserts:,}")
print(f"  UPDATEs: {total_updates:,}")
print(f"  DELETEs: {total_deletes:,}")

# ============================================================
# STEP 4: Analyze File Structure (Before Compaction)
# ============================================================

print("\n--- File Analysis (Before Compaction) ---")

metadata = cdc_table.metadata
current_snapshot = metadata.current_snapshot()
manifests = current_snapshot.manifest_list.manifests()

# Count file types
file_types = {"data": 0, "delete": 0, "delta": 0}

for manifest in manifests:
    files = manifest.data_files()
    for file_entry in files:
        path = file_entry.file_path.lower()
        if "delete" in path:
            file_types["delete"] += 1
        elif "delta" in path:
            file_types["delta"] += 1
        else:
            file_types["data"] += 1

print(f"File Type Distribution:")
print(f"  Data files: {file_types['data']}")
print(f"  Delete files: {file_types['delete']}")
print(f"  Delta files: {file_types['delta']}")
print(f"  Total files: {sum(file_types.values())}")

# ============================================================
# STEP 5: Query Performance Test (Before Compaction)
# ============================================================

print("\n--- Query Performance (Before Compaction) ---")

import time

# Query 1: All transactions
start = time.time()
result1 = cdc_table.scan().to_arrow()
query1_time = time.time() - start

print(f"Query 1: All transactions")
print(f"  Rows: {len(result1):,}")
print(f"  Time: {query1_time:.3f} seconds")

# Query 2: Fraud detection query
start = time.time()
result2 = cdc_table.scan(
    row_filter="amount > 50000 AND status = 'COMPLETED'"
).to_arrow()
query2_time = time.time() - start

print(f"\nQuery 2: High-value completed transactions")
print(f"  Rows: {len(result2):,}")
print(f"  Time: {query2_time:.3f} seconds")

# ============================================================
# STEP 6: Perform CDC Compaction
# ============================================================

print("\n--- Performing CDC Compaction ---")

print("CDC Compaction Strategy:")
print("  1. Apply delete files (remove deleted rows)")
print("  2. Merge delta files (apply updates)")
print("  3. Merge small data files")
print("  4. Update metadata atomically")

# Simulate compaction
print("\nSimulating compaction...")

# In production, you would run:
# spark.sql("""
#     CALL catalog.system.rewrite_data_files(
#         table => 'banking.cdc_fraud_transactions',
#         options => map(
#             'min-file-size-bytes', '134217728',
#             'max-file-size-bytes', '1073741824'
#         )
#     )
# """)
#
# spark.sql("""
#     CALL catalog.system.rewrite_manifests(
#         table => 'banking.cdc_fraud_transactions'
#     )
# """)
#
# spark.sql("""
#     CALL catalog.system.expire_snapshots(
#         table => 'banking.cdc_fraud_transactions',
#         older_than => TIMESTAMP '2026-08-23 00:00:00',
#         retain_last => 5
#     )
# """)

print("Compaction completed:")
print("  ✓ Delete files applied")
print("  ✓ Delta files merged")
print("  ✓ Small files consolidated")
print("  ✓ Manifests rewritten")
print("  ✓ Old snapshots expired")

# ============================================================
# STEP 7: Query Performance Test (After Compaction)
# ============================================================

print("\n--- Query Performance (After Compaction) ---")

print("Expected Performance After Compaction:")
print("  Query 1: All transactions")
print("    Before: {query1_time:.3f} seconds")
print("    After: ~{query1_time/4:.3f} seconds (4x faster)")
print("")
print("  Query 2: High-value completed transactions")
print("    Before: {query2_time:.3f} seconds")
print("    After: ~{query2_time/3:.3f} seconds (3x faster)")

# ============================================================
# STEP 8: Compaction Benefits for CDC
# ============================================================

print("\n--- Compaction Benefits for CDC ---")

print("""
CDC COMPACTION BENEFITS:

1. DELETE FILE APPLICATION
   - Remove deleted rows physically
   - Reduce read overhead
   - Improve query performance

2. DELTA FILE MERGING
   - Apply updates to data files
   - Eliminate merge-on-read overhead
   - Faster queries

3. FILE CONSOLIDATION
   - Merge small files
   - Optimize file sizes
   - Reduce metadata overhead

4. STORAGE OPTIMIZATION
   - Remove redundant data
   - Compress files efficiently
   - Reduce storage costs

5. QUERY PERFORMANCE
   - Fewer files to scan
   - No delete/delta merging
   - Faster query planning

REAL-WORLD IMPACT:
  Before: 100,000 files, 5 minute queries
  After: 1,000 files, 3 second queries
  Improvement: 100x faster
""")

# ============================================================
# STEP 9: Compaction Monitoring
# ============================================================

print("--- Compaction Monitoring ---")

print("""
MONITORING CDC COMPACTION:

KEY METRICS:
  1. Delete file count
     - Target: <100 per partition
     - Alert: >1000

  2. Delta file count
     - Target: <50 per partition
     - Alert: >500

  3. Data file count
     - Target: <100 per partition
     - Alert: >1000

  4. Query performance
     - Target: <10 seconds
     - Alert: >30 seconds

  5. Compaction duration
     - Track runtime
     - Optimize schedule

ALERTS:
  - High file count
  - Slow queries
  - Compaction failures
  - Storage growth

DASHBOARDS:
  - File count over time
  - Query performance trends
  - Compaction history
  - Storage usage
""")
```

---

## 🎯 5 Real-World Interview Questions

### Question 1: What is compaction in Iceberg and why is it necessary?

**Answer:**

**Compaction:**
- Merging small data files into larger ones
- Applying delete/delta files
- Optimizing file organization

**Why Necessary:**

1. **Small File Problem**: Streaming creates many small files
2. **Query Performance**: Many files = slow queries
3. **Metadata Overhead**: Too many files slow planning
4. **Storage Costs**: Many files = more API calls
5. **Delete Files**: MoR tables accumulate delete files

**Example:**
```
Before: 10,000 small files (1MB each)
After: 100 large files (100MB each)
Result: 100x faster queries
```

---

### Question 2: What are the different types of compaction in Iceberg?

**Answer:**

**1. Data File Compaction**
- Merge small data files
- Target: 128MB-1GB per file
- Use case: Streaming ingestion

**2. Manifest Compaction**
- Merge small manifests
- Reduce manifest count
- Use case: Many small files

**3. Delete File Compaction**
- Apply delete files to data files
- Remove deleted rows physically
- Use case: MoR tables

**SQL Examples:**
```sql
-- Data file compaction
CALL system.rewrite_data_files(
    table => 'db.table',
    options => map('min-file-size-bytes', '134217728')
);

-- Manifest compaction
CALL system.rewrite_manifests(
    table => 'db.table'
);
```

---

### Question 3: How do you schedule compaction in production?

**Answer:**

**Scheduling Strategies:**

1. **Daily Compaction**
   - Compact streaming tables
   - Merge small files
   - Run during off-peak hours

2. **Weekly Compaction**
   - Full table optimization
   - Manifest compaction
   - Snapshot cleanup

3. **Event-Based**
   - Trigger when file count > threshold
   - Trigger when query time > threshold
   - Automated based on metrics

**Example Schedule:**
```
Daily 2:00 AM: Compact streaming tables
Weekly Sunday 3:00 AM: Full optimization
Monthly 1st 4:00 AM: Archive old data
```

**Monitoring:**
- Track file counts
- Monitor query performance
- Alert on degradation

---

### Question 4: How does compaction affect query performance?

**Answer:**

**Performance Impact:**

1. **File Count Reduction**
   - Fewer files to scan
   - Faster file opening
   - Reduced metadata overhead

2. **Delete File Application**
   - No merge-on-read overhead
   - Direct data access
   - Faster filtering

3. **File Size Optimization**
   - Larger sequential reads
   - Better compression
   - Reduced I/O

**Metrics:**
```
Before Compaction:
  Files: 10,000
  Query Time: 30 seconds

After Compaction:
  Files: 100
  Query Time: 3 seconds
  
Improvement: 10x faster
```

**Best Practices:**
- Monitor file counts
- Track query performance
- Compact before performance degrades
- Test compaction impact

---

### Question 5: What are the risks of compaction and how to mitigate them?

**Answer:**

**Risks:**

1. **Resource Usage**
   - CPU/memory intensive
   - May affect production queries
   - Network bandwidth

2. **Storage Overhead**
   - Temporary space for new files
   - Old files not immediately deleted
   - Cost increase during compaction

3. **Failure Recovery**
   - Compaction may fail midway
   - Need to handle partial compaction
   - Metadata consistency

4. **Time Travel Impact**
   - Old snapshots reference old files
   - May slow time travel queries
   - Snapshot expiration needed

**Mitigation Strategies:**

1. **Resource Management**
   - Run during off-peak hours
   - Limit concurrent compactions
   - Use cluster resources efficiently

2. **Storage Planning**
   - Ensure enough temporary space
   - Clean up old files promptly
   - Monitor storage costs

3. **Failure Handling**
   - Implement retry logic
   - Checkpoint progress
   - Maintain metadata consistency

4. **Snapshot Management**
   - Expire old snapshots
   - Keep reasonable retention
   - Monitor time travel performance

---

## 📝 Summary

| Aspect | Key Point |
|--------|-----------|
| **Definition** | Merge small files, apply deletes, optimize |
| **Types** | Data, Manifest, Delete file compaction |
| **Benefits** | Faster queries, lower costs, better performance |
| **Schedule** | Daily for streaming, weekly for full optimization |
| **Monitoring** | File counts, query performance, storage |
| **Risks** | Resource usage, storage overhead, failures |
| **Mitigation** | Off-peak scheduling, monitoring, retry logic |
