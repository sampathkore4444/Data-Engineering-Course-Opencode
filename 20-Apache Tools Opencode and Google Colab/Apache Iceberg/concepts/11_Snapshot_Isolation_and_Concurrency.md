# Concept 11: Snapshot Isolation and Concurrency

## 📚 Detailed Explanation

**Snapshot Isolation** and **Optimistic Concurrency Control (OCC)** are key features that enable Iceberg to handle concurrent operations safely and efficiently.

### What is Snapshot Isolation?

Snapshot Isolation means each reader sees a **consistent snapshot** of the table at a specific point in time, regardless of concurrent modifications.

**Example:**
```
Time 10:00:00
  Query A starts → Reads Snapshot 100
  
Time 10:00:05
  ETL Job writes → Creates Snapshot 101
  
Time 10:00:10
  Query A continues → Still reads Snapshot 100
  Query B starts → Reads Snapshot 101
```

**Key Properties:**
- Readers never see partial writes
- Each query sees consistent data
- Concurrent operations don't interfere

### What is Optimistic Concurrency Control?

OCC assumes conflicts are rare and allows concurrent operations without locking:

**Traditional Locking:**
```
Job A: Lock table → Write → Unlock
Job B: Wait for lock → Write → Unlock
(B is blocked while A writes)
```

**Optimistic Concurrency:**
```
Job A: Read → Prepare → Commit
Job B: Read → Prepare → Commit
(Both proceed without blocking)
```

**Conflict Detection:**
```
Job A: Read Snapshot 100 → Prepare → Commit → Snapshot 101 ✓
Job B: Read Snapshot 100 → Prepare → Commit → FAIL (parent changed)
Job B: Retry with Snapshot 101 → Commit → Snapshot 102 ✓
```

### How Iceberg Implements OCC

**1. Read Phase**
- Job reads current snapshot (e.g., Snapshot 100)
- Gets metadata, manifests, data files

**2. Prepare Phase**
- Job prepares new files
- Creates new metadata (but doesn't commit)

**3. Commit Phase**
- Job attempts to commit
- Catalog checks if parent snapshot is still current

**4. Validation**
- If parent changed: Commit fails, job retries
- If parent unchanged: Commit succeeds

**5. Success/Fail**
- Success: New snapshot visible
- Fail: Job retries with new parent

---

## 💡 Example: Concurrent Operations in Banking

### Scenario: Multiple ETL Jobs

**Job A**: Load daily transactions (Snapshot 100 → 101)
**Job B**: Update fraud scores (Snapshot 100 → 101)

**Without OCC:**
```
Job A: Read → Write → Commit ✓
Job B: Read → Write → Commit ✗ (conflict, data overwritten)
```

**With OCC:**
```
Job A: Read Snapshot 100 → Write → Commit → Snapshot 101 ✓
Job B: Read Snapshot 100 → Write → Commit → FAIL (parent changed)
Job B: Retry → Read Snapshot 101 → Write → Commit → Snapshot 102 ✓
```

---

## 🏦 Real-World Banking Scenario 1: Concurrent ETL Jobs

### Scenario
A bank runs **multiple ETL jobs simultaneously**:
- **Job A**: Daily transaction load (2 AM - 4 AM)
- **Job B**: Fraud score calculation (2 AM - 3 AM)
- **Job C**: Customer 360 update (3 AM - 5 AM)

All jobs write to the same Iceberg table.

### Problem
- Jobs may conflict
- Need consistent data for each job
- Must handle failures gracefully

### Solution
Iceberg's snapshot isolation and OCC:
- Each job sees consistent snapshot
- Conflicts detected and resolved
- No data corruption

### Python Code

```python
"""
Banking Scenario 1: Concurrent ETL Jobs
Using Iceberg Snapshot Isolation and OCC
"""

from pyiceberg.catalog import load_catalog
from pyiceberg.table import Table
from datetime import datetime
import pyarrow as pa
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

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

print("=== CONCURRENT ETL JOBS ===\n")

schema = pa.schema([
    pa.field("transaction_id", pa.string(), nullable=False),
    pa.field("account_id", pa.string(), nullable=False),
    pa.field("amount", pa.decimal128(18, 2), nullable=False),
    pa.field("status", pa.string(), nullable=False),
    pa.field("fraud_score", pa.float64(), nullable=True),
    pa.field("risk_level", pa.string(), nullable=True),
    pa.field("updated_at", pa.timestamp("us"), nullable=True),
])

# Create table
try:
    etl_table = catalog.create_table(
        identifier="banking.concurrent_etl",
        schema=schema
    )
except Exception:
    etl_table = catalog.load_table("banking.concurrent_etl")

# ============================================================
# STEP 3: Load Initial Data
# ============================================================

print("--- Loading Initial Data ---")

initial_data = pa.table({
    "transaction_id": [f"TXN-{i:06d}" for i in range(1, 1001)],
    "account_id": [f"ACC-{1000 + (i % 100):06d}" for i in range(1000)],
    "amount": [round(random.uniform(100, 50000), 2) for _ in range(1000)],
    "status": ["COMPLETED"] * 1000,
    "fraud_score": [None] * 1000,
    "risk_level": [None] * 1000,
    "updated_at": [None] * 1000,
})

etl_table.append(initial_data)
print(f"Loaded {len(initial_data)} initial transactions")

# ============================================================
# STEP 4: Define Concurrent ETL Jobs
# ============================================================

print("\n--- Defining Concurrent ETL Jobs ---")

def job_a_daily_load(table: Table, job_name: str) -> dict:
    """
    Job A: Daily transaction load.
    Simulates loading new transactions.
    """
    print(f"\n{job_name}: Starting...")
    start_time = time.time()
    
    # Simulate processing time
    time.sleep(0.1)
    
    # Generate new transactions
    new_data = pa.table({
        "transaction_id": [f"TXN-NEW-{i:06d}" for i in range(1, 51)],
        "account_id": [f"ACC-{1000 + (i % 50):06d}" for i in range(50)],
        "amount": [round(random.uniform(100, 50000), 2) for _ in range(50)],
        "status": ["COMPLETED"] * 50,
        "fraud_score": [None] * 50,
        "risk_level": [None] * 50,
        "updated_at": [datetime.now()] * 50,
    })
    
    # Commit (atomic)
    table.append(new_data)
    
    elapsed = time.time() - start_time
    return {
        "job": job_name,
        "status": "SUCCESS",
        "records_added": len(new_data),
        "elapsed_seconds": elapsed,
        "snapshot_id": table.metadata.current_snapshot_id
    }

def job_b_fraud_scores(table: Table, job_name: str) -> dict:
    """
    Job B: Fraud score calculation.
    Simulates updating fraud scores.
    """
    print(f"\n{job_name}: Starting...")
    start_time = time.time()
    
    # Simulate processing time
    time.sleep(0.15)
    
    # Read current data (snapshot isolation)
    current_data = table.scan().to_arrow()
    
    # Calculate fraud scores (simulated)
    updated_data = current_data.set_column(
        current_data.schema.get_field_index("fraud_score"),
        "fraud_score",
        pa.array([round(random.uniform(0, 1), 4) for _ in range(len(current_data))])
    )
    
    updated_data = updated_data.set_column(
        updated_data.schema.get_field_index("risk_level"),
        "risk_level",
        pa.array([random.choice(["LOW", "MEDIUM", "HIGH"]) for _ in range(len(current_data))])
    )
    
    updated_data = updated_data.set_column(
        updated_data.schema.get_field_index("updated_at"),
        "updated_at",
        pa.array([datetime.now()] * len(current_data))
    )
    
    # Commit (atomic)
    table.append(updated_data)
    
    elapsed = time.time() - start_time
    return {
        "job": job_name,
        "status": "SUCCESS",
        "records_updated": len(updated_data),
        "elapsed_seconds": elapsed,
        "snapshot_id": table.metadata.current_snapshot_id
    }

def job_c_customer_360(table: Table, job_name: str) -> dict:
    """
    Job C: Customer 360 update.
    Simulates updating customer aggregates.
    """
    print(f"\n{job_name}: Starting...")
    start_time = time.time()
    
    # Simulate processing time
    time.sleep(0.12)
    
    # Read current data (snapshot isolation)
    current_data = table.scan().to_arrow()
    
    # Calculate customer aggregates (simulated)
    customer_agg = pa.table({
        "transaction_id": [f"TXN-AGG-{i:06d}" for i in range(1, 21)],
        "account_id": [f"ACC-{1000 + (i % 20):06d}" for i in range(20)],
        "amount": [round(random.uniform(1000, 100000), 2) for _ in range(20)],
        "status": ["AGGREGATED"] * 20,
        "fraud_score": [0.0] * 20,
        "risk_level": ["AGGREGATE"] * 20,
        "updated_at": [datetime.now()] * 20,
    })
    
    # Commit (atomic)
    table.append(customer_agg)
    
    elapsed = time.time() - start_time
    return {
        "job": job_name,
        "status": "SUCCESS",
        "records_aggregated": len(customer_agg),
        "elapsed_seconds": elapsed,
        "snapshot_id": table.metadata.current_snapshot_id
    }

# ============================================================
# STEP 5: Run Concurrent Jobs
# ============================================================

print("\n--- Running Concurrent Jobs ---")

# Run jobs concurrently
with ThreadPoolExecutor(max_workers=3) as executor:
    futures = {
        executor.submit(job_a_daily_load, etl_table, "Job A: Daily Load"): "A",
        executor.submit(job_b_fraud_scores, etl_table, "Job B: Fraud Scores"): "B",
        executor.submit(job_c_customer_360, etl_table, "Job C: Customer 360"): "C",
    }
    
    results = []
    for future in as_completed(futures):
        result = future.result()
        results.append(result)
        print(f"\n{result['job']} completed:")
        print(f"  Status: {result['status']}")
        print(f"  Snapshot ID: {result['snapshot_id']}")

# ============================================================
# STEP 6: Verify Results
# ============================================================

print("\n--- Verifying Results ---")

# Get final state
final_data = etl_table.scan().to_arrow()
print(f"Total records: {len(final_data)}")

# Check snapshot chain
metadata = etl_table.metadata
print(f"\nSnapshot Chain:")
for i, snap in enumerate(metadata.snapshots[-5:]):  # Last 5 snapshots
    timestamp = datetime.fromtimestamp(snap.timestamp_ms / 1000)
    print(f"  Snapshot {snap.snapshot_id}: {snap.operation} at {timestamp}")

# ============================================================
# STEP 7: Demonstrate Snapshot Isolation
# ============================================================

print("\n--- Demonstrating Snapshot Isolation ---")

print("""
SNAPSHOT ISOLATION IN ACTION:

Time 10:00:00
  Job A starts → Reads Snapshot 100
  Job B starts → Reads Snapshot 100
  Job C starts → Reads Snapshot 100

Time 10:00:05
  Job A commits → Creates Snapshot 101
  Job B still reading Snapshot 100 (isolated)
  Job C still reading Snapshot 100 (isolated)

Time 10:00:10
  Job B commits → Creates Snapshot 102
  Job C still reading Snapshot 100 (isolated)

Time 10:00:15
  Job C commits → Creates Snapshot 103

Result:
  ✓ Each job saw consistent data
  ✓ No partial writes visible
  ✓ No conflicts (assuming no overlapping data)
""")

# ============================================================
# STEP 8: Demonstrate Optimistic Concurrency
# ============================================================

print("--- Demonstrating Optimistic Concurrency ---")

print("""
OPTIMISTIC CONCURRENCY CONTROL:

Scenario: Two jobs try to update same records

Job A: Read Snapshot 100 → Prepare → Commit → Snapshot 101 ✓
Job B: Read Snapshot 100 → Prepare → Commit → FAIL (parent changed)
Job B: Retry → Read Snapshot 101 → Prepare → Commit → Snapshot 102 ✓

Key Points:
  ✓ No locking required
  ✓ Conflicts detected at commit time
  ✓ Automatic retry mechanism
  ✓ High throughput for read-heavy workloads
""")
```

---

## 🏦 Real-World Banking Scenario 2: Real-Time Analytics with Concurrent Writers

### Scenario
A bank's **real-time analytics platform** has multiple writers:
- **Flink**: Streaming transactions (continuous)
- **Spark**: Batch aggregations (hourly)
- **Trino**: Ad-hoc queries (concurrent)

All access the same Iceberg table.

### Problem
- Streaming writes must not block batch jobs
- Queries must see consistent data
- Need high throughput

### Solution
Iceberg's snapshot isolation and OCC:
- Streaming writes create new snapshots
- Batch jobs read consistent snapshots
- Queries see point-in-time consistency

### Python Code

```python
"""
Banking Scenario 2: Real-Time Analytics with Concurrent Writers
Using Iceberg for Multi-Writer Architecture
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
# STEP 2: Define Real-Time Transaction Schema
# ============================================================

print("=== REAL-TIME ANALYTICS WITH CONCURRENT WRITERS ===\n")

schema = pa.schema([
    pa.field("transaction_id", pa.string(), nullable=False),
    pa.field("account_id", pa.string(), nullable=False),
    pa.field("amount", pa.decimal128(18, 2), nullable=False),
    pa.field("channel", pa.string(), nullable=False),
    pa.field("transaction_time", pa.timestamp("us"), nullable=False),
    pa.field("batch_id", pa.string(), nullable=True),
])

# Create table
try:
    realtime_table = catalog.create_table(
        identifier="banking.realtime_transactions",
        schema=schema
    )
except Exception:
    realtime_table = catalog.load_table("banking.realtime_transactions")

# ============================================================
# STEP 3: Simulate Streaming Writer (Flink)
# ============================================================

print("--- Simulating Streaming Writer (Flink) ---")

def flink_streaming_write(table: Table, batch_num: int) -> dict:
    """
    Simulate Flink streaming writer.
    Writes small batches continuously.
    """
    start_time = time.time()
    
    # Generate streaming batch
    batch_size = 100
    base_time = datetime(2026, 8, 24, 10, 0, 0)
    
    data = pa.table({
        "transaction_id": [f"TXN-STREAM-{batch_num}-{i:04d}" for i in range(batch_size)],
        "account_id": [f"ACC-{1000 + (i % 50):06d}" for i in range(batch_size)],
        "amount": [round(random.uniform(10, 5000), 2) for _ in range(batch_size)],
        "channel": ["STREAMING"] * batch_size,
        "transaction_time": [
            base_time + timedelta(seconds=batch_num * 60 + i)
            for i in range(batch_size)
        ],
        "batch_id": [f"BATCH-{batch_num:04d}"] * batch_size,
    })
    
    # Commit to Iceberg
    table.append(data)
    
    elapsed = time.time() - start_time
    return {
        "writer": "Flink",
        "batch_num": batch_num,
        "records": len(data),
        "elapsed": elapsed,
        "snapshot_id": table.metadata.current_snapshot_id
    }

# Simulate 10 streaming batches
print("Simulating 10 streaming batches...")
for batch_num in range(10):
    result = flink_streaming_write(realtime_table, batch_num)
    if batch_num % 5 == 0:
        print(f"  Batch {batch_num}: {result['records']} records, Snapshot {result['snapshot_id']}")

# ============================================================
# STEP 4: Simulate Batch Writer (Spark)
# ============================================================

print("\n--- Simulating Batch Writer (Spark) ---")

def spark_batch_write(table: Table) -> dict:
    """
    Simulate Spark batch writer.
    Writes hourly aggregations.
    """
    start_time = time.time()
    
    # Read current data (snapshot isolation)
    current_data = table.scan().to_arrow()
    
    # Calculate aggregations (simulated)
    batch_data = pa.table({
        "transaction_id": [f"TXN-BATCH-{i:04d}" for i in range(1, 21)],
        "account_id": [f"ACC-{1000 + (i % 20):06d}" for i in range(20)],
        "amount": [round(random.uniform(1000, 100000), 2) for _ in range(20)],
        "channel": ["BATCH"] * 20,
        "transaction_time": [datetime.now()] * 20,
        "batch_id": ["HOURLY-AGG"] * 20,
    })
    
    # Commit to Iceberg
    table.append(batch_data)
    
    elapsed = time.time() - start_time
    return {
        "writer": "Spark",
        "records": len(batch_data),
        "elapsed": elapsed,
        "snapshot_id": table.metadata.current_snapshot_id
    }

# Run batch write
spark_result = spark_batch_write(realtime_table)
print(f"  Spark batch: {spark_result['records']} records, Snapshot {spark_result['snapshot_id']}")

# ============================================================
# STEP 5: Simulate Concurrent Query (Trino)
# ============================================================

print("\n--- Simulating Concurrent Query (Trino) ---")

def trino_query(table: Table, query_name: str) -> dict:
    """
    Simulate Trino query.
    Reads consistent snapshot.
    """
    start_time = time.time()
    
    # Query sees consistent snapshot (snapshot isolation)
    result = table.scan().to_arrow()
    
    elapsed = time.time() - start_time
    return {
        "query": query_name,
        "rows": len(result),
        "elapsed": elapsed,
        "snapshot_id": table.metadata.current_snapshot_id
    }

# Run multiple queries
print("Running concurrent queries...")
for i in range(3):
    query_result = trino_query(realtime_table, f"Query {i+1}")
    print(f"  {query_result['query']}: {query_result['rows']} rows, Snapshot {query_result['snapshot_id']}")

# ============================================================
# STEP 6: Demonstrate Snapshot Isolation
# ============================================================

print("\n--- Demonstrating Snapshot Isolation ---")

print("""
SNAPSHOT ISOLATION IN MULTI-WRITER SCENARIO:

Timeline:
  10:00:00 - Flink writes Batch 1 → Snapshot 100
  10:00:05 - Spark starts batch job → Reads Snapshot 100
  10:00:10 - Flink writes Batch 2 → Snapshot 101
  10:00:15 - Spark continues → Still reads Snapshot 100
  10:00:20 - Trino query starts → Reads Snapshot 101
  10:00:25 - Spark commits → Snapshot 102
  10:00:30 - Trino query completes → Consistent view

Key Points:
  ✓ Spark saw consistent Snapshot 100 throughout
  ✓ Trino saw consistent Snapshot 101 throughout
  ✓ No partial writes visible to any reader
  ✓ Each writer creates new snapshot atomically
""")

# ============================================================
# STEP 7: Demonstrate Optimistic Concurrency
# ============================================================

print("--- Demonstrating Optimistic Concurrency ---")

print("""
OPTIMISTIC CONCURRENCY IN MULTI-WRITER SCENARIO:

Scenario: Flink and Spark try to commit simultaneously

Flink: Read Snapshot 102 → Write → Commit → Snapshot 103 ✓
Spark: Read Snapshot 102 → Write → Commit → FAIL (parent changed)
Spark: Retry → Read Snapshot 103 → Write → Commit → Snapshot 104 ✓

Key Points:
  ✓ No locking between Flink and Spark
  ✓ Conflicts detected at commit time
  ✓ Automatic retry with new parent
  ✓ High throughput for concurrent writers
""")

# ============================================================
# STEP 8: Performance Metrics
# ============================================================

print("--- Performance Metrics ---")

# Get final metadata
metadata = realtime_table.metadata
current_snapshot = metadata.current_snapshot()

print(f"Final State:")
print(f"  Current Snapshot: {current_snapshot.snapshot_id}")
print(f"  Total Snapshots: {len(metadata.snapshots)}")
print(f"  Total Records: {sum(manifest.data_files()[i].record_count for manifest in metadata.current_snapshot().manifest_list.manifests() for i in range(len(manifest.data_files())))}")

print(f"\nConcurrency Benefits:")
print(f"  ✓ No blocking between writers")
print(f"  ✓ Consistent reads for queries")
print(f"  ✓ High throughput")
print(f"  ✓ Automatic conflict resolution")
```

---

## 🎯 5 Real-World Interview Questions

### Question 1: Explain snapshot isolation in Iceberg and why it's important.

**Answer:**

**Snapshot Isolation:**
- Each reader sees consistent table state at a point in time
- Concurrent modifications don't affect ongoing reads
- No partial writes visible

**Example:**
```
Query A starts at Snapshot 100
ETL job creates Snapshot 101
Query A continues reading Snapshot 100 (isolated)
Query B starts reading Snapshot 101
```

**Importance:**
1. **Consistency**: Readers always see complete data
2. **No Blocking**: Reads don't block writes
3. **Time Travel**: Historical queries work correctly
4. **Concurrent Operations**: Multiple jobs can run simultaneously

---

### Question 2: How does optimistic concurrency control work in Iceberg?

**Answer:**

**OCC Process:**

1. **Read Phase**: Job reads current snapshot
2. **Prepare Phase**: Job prepares new files/metadata
3. **Commit Phase**: Job attempts atomic commit
4. **Validation**: Catalog checks parent snapshot
5. **Success/Fail**: Retry if conflict detected

**Example:**
```
Job A: Read Snapshot 100 → Commit → Snapshot 101 ✓
Job B: Read Snapshot 100 → Commit → FAIL (parent changed)
Job B: Retry → Read Snapshot 101 → Commit → Snapshot 102 ✓
```

**Benefits:**
- No locking overhead
- High throughput for reads
- Automatic conflict detection
- Safe concurrent writes

---

### Question 3: What happens when two jobs try to commit simultaneously?

**Answer:**

**Conflict Detection:**
- Both jobs read same parent snapshot
- Both prepare new metadata
- First to commit wins (creates new snapshot)
- Second job fails (parent changed)

**Resolution:**
- Failed job retries automatically
- Reads new snapshot as parent
- Prepares new metadata
- Commits successfully

**Example:**
```
Job A: Read Snapshot 100 → Prepare → Commit → Snapshot 101 ✓
Job B: Read Snapshot 100 → Prepare → Commit → FAIL
Job B: Retry → Read Snapshot 101 → Prepare → Commit → Snapshot 102 ✓
```

**Key Points:**
- No data corruption
- Automatic retry
- Consistent state maintained

---

### Question 4: How does snapshot isolation affect query performance?

**Answer:**

**Performance Impact:**

1. **No Locking**: Queries don't block writes
2. **Consistent Reads**: No partial data scanning
3. **Metadata Overhead**: Minor (snapshot lookup)

**Optimizations:**
- Snapshots are immutable (cached)
- Metadata is lightweight
- File references are efficient

**Example:**
```
Query at Snapshot 100:
  - Load snapshot metadata (O(1))
  - Load manifest list (O(1))
  - Load manifests (O(M))
  - Read data files (O(N))

Total: O(M + N) where M << N
```

**Benefits:**
- High query throughput
- No write contention
- Consistent performance

---

### Question 5: Compare Iceberg's concurrency model with traditional databases.

**Answer:**

**Traditional Databases:**
- Lock-based concurrency
- Readers may block writers
- Writers may block readers
- Deadlock possible

**Iceberg:**
- Optimistic concurrency
- Readers never block
- Writers may retry (rare conflicts)
- No deadlocks

**Comparison:**

| Aspect | Traditional DB | Iceberg |
|--------|---------------|---------|
| **Locking** | Pessimistic | Optimistic |
| **Read Blocking** | Possible | Never |
| **Write Blocking** | Possible | Rare (retry) |
| **Deadlock** | Possible | Impossible |
| **Throughput** | Limited by locks | High |
| **Conflict Rate** | N/A | Rare |

**Iceberg Advantages:**
- Better read performance
- Higher throughput
- No deadlocks
- Suitable for data lakes

---

## 📝 Summary

| Aspect | Key Point |
|--------|-----------|
| **Snapshot Isolation** | Consistent reads at point in time |
| **Optimistic Concurrency** | No locking, retry on conflict |
| **Conflict Detection** | Parent snapshot validation |
| **Benefits** | High throughput, no blocking |
| **Use Cases** | Concurrent ETL, streaming + batch |
| **Performance** | Minimal overhead |
| **Safety** | No data corruption |
