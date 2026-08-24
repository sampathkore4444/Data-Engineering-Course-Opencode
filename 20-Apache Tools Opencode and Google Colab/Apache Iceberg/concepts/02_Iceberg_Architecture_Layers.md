# Concept 02: Iceberg Architecture Layers

## 📚 Detailed Explanation

Apache Iceberg follows a **layered architecture** that separates concerns between metadata management and data storage. Understanding these layers is fundamental to mastering Iceberg.

### The Four Core Layers

```
┌─────────────────────────────────────────────┐
│                CATALOG                       │
│    (Table name → Metadata location mapping)  │
├─────────────────────────────────────────────┤
│              TABLE METADATA                  │
│    (Schema, partition spec, snapshot list)   │
├─────────────────────────────────────────────┤
│               SNAPSHOTS                      │
│    (Immutable table state at point in time)  │
├─────────────────────────────────────────────┤
│          MANIFESTS + DATA FILES              │
│    (File lists with statistics + Parquet)    │
└─────────────────────────────────────────────┘
```

### Layer 1: Catalog

The **catalog** is the entry point. It answers:
> "Where is this table's metadata?"

**Catalog Types:**
- **REST Catalog**: HTTP-based, engine-agnostic
- **Hive Catalog**: Uses Hive Metastore
- **JDBC Catalog**: Stores in relational database
- **Glue Catalog**: AWS managed service
- **Hadoop Catalog**: File-based, simple

**What Catalog Stores:**
```
table_name → metadata_file_location
```

### Layer 2: Table Metadata

The **metadata file** (JSON) contains:
- Table schema (columns, types, IDs)
- Current snapshot ID
- List of all snapshots
- Partition specifications
- Properties (table settings)
- Schema history

**Example Metadata JSON:**
```json
{
  "format-version": 2,
  "table-uuid": "d20125c8-7284-442c-9aea-15fee620737c",
  "location": "s3://banking-lakehouse/transactions/",
  "last-updated-ms": 1724500000000,
  "last-column-id": 11,
  "current-snapshot-id": 102,
  "snapshots": [...],
  "properties": {...}
}
```

### Layer 3: Snapshots

A **snapshot** represents the table's state at a point in time.

**Snapshot contains:**
- Snapshot ID (unique identifier)
- Timestamp
- Operation type (append, overwrite, delete)
- Manifest list location
- Summary (operation counts)

**Key Points:**
- Snapshots are **immutable** (never modified)
- New operations create **new snapshots**
- Old snapshots remain for time travel until expired

### Layer 4: Manifests and Data Files

**Manifest List** (one per snapshot):
- Points to all manifests for that snapshot

**Manifest** (many per snapshot):
- Lists data files belonging to the manifest
- Contains file-level statistics (min/max, null counts)
- Records partition values for each file

**Data Files** (Parquet):
- Actual row data
- Stored in object storage (S3, GCS, ADLS)

---

## 💡 Example: Complete Metadata Flow

### Query Execution Flow

```sql
SELECT SUM(amount) 
FROM transactions 
WHERE transaction_date = '2026-08-24';
```

**Step-by-step:**

```
1. Query Engine (Spark/Trino)
   ↓
2. Catalog Lookup
   "banking.transactions" → s3://lake/metadata/v102.metadata.json
   ↓
3. Read Metadata JSON
   current-snapshot-id: 102
   ↓
4. Read Snapshot 102
   manifest-list: s3://lake/metadata/snap-102.avro
   ↓
5. Read Manifest List
   manifest-1.avro, manifest-2.avro, manifest-3.avro
   ↓
6. Read Manifests (with statistics)
   manifest-1: files with date range [2026-08-01, 2026-08-15]
   manifest-2: files with date range [2026-08-16, 2026-08-24]
   manifest-3: files with date range [2026-08-25, 2026-08-31]
   ↓
7. Metadata Pruning
   Skip manifest-1 (date range doesn't include Aug 24)
   Skip manifest-3 (date range doesn't include Aug 24)
   ↓
8. Read Manifest 2 Statistics
   File-001: date min=2026-08-16, max=2026-08-20 → SKIP
   File-002: date min=2026-08-21, max=2026-08-24 → READ
   File-003: date min=2026-08-23, max=2026-08-24 → READ
   ↓
9. Read Relevant Parquet Files
   ↓
10. Compute SUM(amount)
    ↓
11. Return Result
```

**Without Iceberg metadata:** Scan all 10,000 files
**With Iceberg metadata:** Scan only 2-3 relevant files

---

## 🏦 Real-World Banking Scenario 1: Multi-Region Transaction Aggregation

### Scenario
A multinational bank operates in **10 regions** with **5 billion transactions** stored across multiple S3 buckets. The headquarters needs to run **quarterly aggregation reports** across all regions. Without proper metadata layers, this would require scanning billions of files.

### Problem
- Queries scan too many files (performance)
- Cross-region data requires efficient metadata routing
- Regulatory reports must complete within SLA (4 hours)

### Solution
Iceberg's layered architecture enables:
- **Catalog** routes queries to correct region
- **Metadata** prunes irrelevant files
- **Manifests** contain statistics for data skipping

### Python Code

```python
"""
Banking Scenario 1: Multi-Region Transaction Aggregation
Leveraging Iceberg's Architecture Layers
"""

from pyiceberg.catalog import load_catalog
from pyiceberg.table import Table
from datetime import datetime
import pyarrow as pa
import time

# ============================================================
# STEP 1: Setup Multi-Region Catalog
# ============================================================

# In production, each region would have its own catalog
# Here we simulate with a REST catalog
catalog = load_catalog(
    "global_banking_catalog",
    **{
        "uri": "http://catalog-service:8181",
        "warehouse": "s3a://global-banking-lakehouse/"
    }
)

# ============================================================
# STEP 2: Define Region-Specific Tables
# ============================================================

# Schema for regional transactions
transaction_schema = pa.schema([
    pa.field("transaction_id", pa.string(), nullable=False),
    pa.field("region", pa.string(), nullable=False),
    pa.field("branch_id", pa.string(), nullable=False),
    pa.field("customer_id", pa.string(), nullable=False),
    pa.field("amount", pa.decimal128(18, 2), nullable=False),
    pa.field("currency", pa.string(), nullable=False),
    pa.field("amount_usd", pa.decimal128(18, 2), nullable=False),
    pa.field("transaction_type", pa.string(), nullable=False),
    pa.field("transaction_date", pa.date32(), nullable=False),
    pa.field("transaction_timestamp", pa.timestamp("us"), nullable=False),
])

# Regional table identifiers
regions = ["US-EAST", "US-WEST", "EU-WEST", "ASIA-PAC", "LATAM"]

# ============================================================
# STEP 3: Load Data for Each Region
# ============================================================

def generate_regional_transactions(region: str, num_records: int) -> pa.Table:
    """Generate transaction data for a specific region."""
    import random
    
    data = {
        "transaction_id": [f"TXN-{region}-{i:08d}" for i in range(num_records)],
        "region": [region] * num_records,
        "branch_id": [f"BR-{region}-{random.randint(1, 100):03d}" for _ in range(num_records)],
        "customer_id": [f"CUST-{random.randint(1, 10000):06d}" for _ in range(num_records)],
        "amount": [round(random.uniform(100, 100000), 2) for _ in range(num_records)],
        "currency": ["USD" if "US" in region else "EUR" if "EU" in region else "INR" for _ in range(num_records)],
        "amount_usd": [round(random.uniform(100, 100000), 2) for _ in range(num_records)],
        "transaction_type": [random.choice(["CREDIT", "DEBIT", "TRANSFER"]) for _ in range(num_records)],
        "transaction_date": [datetime(2026, 8, 24).date()] * num_records,
        "transaction_timestamp": [datetime.now()] * num_records,
    }
    return pa.table(data)

# Load data for each region
print("Loading regional transaction data...")
for region in regions:
    try:
        table = catalog.create_table(
            identifier=f"banking.regional_transactions_{region.lower().replace('-', '_')}",
            schema=transaction_schema,
            partition_spec={"transform": "day", "source": "transaction_date"}
        )
    except Exception:
        table = catalog.load_table(f"banking.regional_transactions_{region.lower().replace('-', '_')}")
    
    # Generate and load data
    transactions = generate_regional_transactions(region, 10000)
    table.append(transactions)
    print(f"  Loaded {len(transactions)} transactions for {region}")

# ============================================================
# STEP 4: Demonstrate Catalog Layer
# ============================================================

print("\n=== CATALOG LAYER DEMONSTRATION ===")

# List all tables in the banking namespace
tables = catalog.list_tables("banking")
print(f"Tables in 'banking' namespace: {len(tables)}")

for table_id in tables:
    print(f"  - {table_id}")

# ============================================================
# STEP 5: Demonstrate Metadata Layer
# ============================================================

print("\n=== METADATA LAYER DEMONSTRATION ===")

# Load a specific table
sample_table = catalog.load_table("banking.regional_transactions_us_east")

# Access metadata
metadata = sample_table.metadata
print(f"Table UUID: {metadata.table_uuid}")
print(f"Format Version: {metadata.format_version}")
print(f"Current Snapshot ID: {metadata.current_snapshot_id}")
print(f"Total Snapshots: {len(metadata.snapshots)}")
print(f"Last Column ID: {metadata.last_column_id}")

# Schema information
print(f"\nSchema Columns:")
for field in metadata.schema().fields:
    print(f"  - {field.name}: {field.field_type} (ID: {field.field_id})")

# ============================================================
# STEP 6: Demonstrate Snapshot Layer
# ============================================================

print("\n=== SNAPSHOT LAYER DEMONSTRATION ===")

# Get current snapshot
current_snapshot = metadata.current_snapshot()
print(f"Current Snapshot:")
print(f"  ID: {current_snapshot.snapshot_id}")
print(f"  Operation: {current_snapshot.operation}")
print(f"  Timestamp: {datetime.fromtimestamp(current_snapshot.timestamp_ms / 1000)}")

# List all snapshots
print(f"\nAll Snapshots:")
for snap in metadata.snapshots:
    print(f"  Snapshot {snap.snapshot_id}: {snap.operation} at {datetime.fromtimestamp(snap.timestamp_ms / 1000)}")

# ============================================================
# STEP 7: Demonstrate Manifest Layer
# ============================================================

print("\n=== MANIFEST LAYER DEMONSTRATION ===")

# Access manifest list
manifest_list = current_snapshot.manifest_list
print(f"Manifest List Location: {manifest_list.manifest_list_location}")

# List manifests
manifests = manifest_list.manifests()
print(f"Number of Manifests: {len(manifests)}")

for i, manifest in enumerate(manifests[:3]):  # Show first 3
    print(f"\nManifest {i+1}:")
    print(f"  Partition Spec ID: {manifest.partition_spec_id}")
    print(f"  Added Files: {manifest.added_files_count}")
    print(f"  Existing Files: {manifest.existing_files_count}")
    print(f"  Deleted Files: {manifest.deleted_files_count}")
    
    # Show file statistics (first 2 files)
    files = manifest.files()
    for j, file_entry in enumerate(files[:2]):
        print(f"  File {j+1}: {file_entry.file_path}")
        if file_entry.lower_bounds:
            print(f"    Lower Bounds: {dict(list(file_entry.lower_bounds.items())[:3])}")
        if file_entry.upper_bounds:
            print(f"    Upper Bounds: {dict(list(file_entry.upper_bounds.items())[:3])}")

# ============================================================
# STEP 8: Query Performance Comparison
# ============================================================

print("\n=== QUERY PERFORMANCE WITH METADATA PRUNING ===")

# Simulate a query with date filter
start_time = time.time()

# Query that benefits from metadata pruning
result = sample_table.scan(
    row_filter="transaction_date = '2026-08-24'"
).to_arrow()

query_time = time.time() - start_time

print(f"Query: SELECT * FROM transactions WHERE transaction_date = '2026-08-24'")
print(f"Rows returned: {len(result)}")
print(f"Query time: {query_time:.3f} seconds")
print(f"\nMetadata Pruning Benefits:")
print(f"  - Scan only relevant manifests (not all)")
print(f"  - Skip files based on statistics (min/max)")
print(f"  - Read only matching Parquet files")

# ============================================================
# STEP 9: Demonstrate Layer Interaction
# ============================================================

print("\n=== LAYER INTERACTION SUMMARY ===")

print("""
Query Flow:
1. CATALOG → Finds metadata location
2. METADATA → Gets current snapshot, schema, partition spec
3. SNAPSHOT → Gets manifest list location
4. MANIFEST LIST → Gets all manifests
5. MANIFESTS → Contains file lists with statistics
6. STATISTICS → Prune irrelevant files
7. DATA FILES → Read only relevant Parquet files
8. RESULT → Return to query engine

Each layer adds efficiency:
- Catalog: O(1) lookup
- Metadata: O(1) snapshot access
- Manifests: O(M) where M << N (number of files)
- Statistics: O(F) where F << total files
""")
```

---

## 🏦 Real-World Banking Scenario 2: Real-Time Fraud Detection Pipeline

### Scenario
A bank's fraud detection system processes **100,000 transactions per minute**. The system must:
1. Ingest streaming transactions via Kafka
2. Write to Iceberg table via Flink
3. Enable real-time analytics via Trino
4. Maintain complete audit trail

### Problem
- High-velocity writes create many small files
- Query performance degrades with unmanaged files
- Need consistent metadata across streaming and batch

### Solution
Iceberg's architecture layers handle this:
- **Catalog**: REST catalog for Flink/Trino coordination
- **Metadata**: Atomic commits for streaming writes
- **Manifests**: Efficient query planning for fraud detection
- **Data Files**: Compacted for query performance

### Python Code

```python
"""
Banking Scenario 2: Real-Time Fraud Detection Pipeline
Iceberg Architecture for Streaming + Batch Analytics
"""

from pyiceberg.catalog import load_catalog
from pyiceberg.table import Table
from datetime import datetime, timedelta
import pyarrow as pa
import json

# ============================================================
# STEP 1: Setup Streaming-Optimized Catalog
# ============================================================

catalog = load_catalog(
    "fraud_detection_catalog",
    **{
        "uri": "http://catalog-service:8181",
        "warehouse": "s3a://fraud-detection-lakehouse/"
    }
)

# ============================================================
# STEP 2: Define Streaming Transaction Schema
# ============================================================

streaming_schema = pa.schema([
    pa.field("transaction_id", pa.string(), nullable=False),
    pa.field("account_id", pa.string(), nullable=False),
    pa.field("amount", pa.decimal128(18, 2), nullable=False),
    pa.field("merchant_id", pa.string(), nullable=True),
    pa.field("merchant_category", pa.string(), nullable=True),
    pa.field("transaction_timestamp", pa.timestamp("us"), nullable=False),
    pa.field("location_lat", pa.float64(), nullable=True),
    pa.field("location_lon", pa.float64(), nullable=True),
    pa.field("device_id", pa.string(), nullable=True),
    pa.field("is_fraud", pa.boolean(), nullable=True),
    pa.field("fraud_score", pa.float64(), nullable=True),
])

# Create Iceberg table optimized for streaming
try:
    fraud_table = catalog.create_table(
        identifier="fraud_detection.realtime_transactions",
        schema=streaming_schema,
        partition_spec={"transform": "hour", "source": "transaction_timestamp"},
        properties={
            "write.format.default": "parquet",
            "write.parquet.compression-codec": "zstd",
            "commit.manifest-merge.enabled": "true",
            "write.target-file-size-bytes": "134217728",  # 128MB
        }
    )
except Exception:
    fraud_table = catalog.load_table("fraud_detection.realtime_transactions")

# ============================================================
# STEP 3: Simulate Streaming Ingestion (Flink-like)
# ============================================================

def simulate_streaming_batch(batch_number: int, batch_size: int = 1000) -> pa.Table:
    """
    Simulate a batch of streaming transactions.
    In production, this would be Flink writing to Iceberg.
    """
    import random
    
    base_time = datetime(2026, 8, 24, 10, 0, 0)
    
    data = {
        "transaction_id": [f"TXN-{batch_number}-{i:06d}" for i in range(batch_size)],
        "account_id": [f"ACC-{random.randint(10000, 99999):06d}" for _ in range(batch_size)],
        "amount": [round(random.uniform(10, 50000), 2) for _ in range(batch_size)],
        "merchant_id": [f"MERCH-{random.randint(1, 5000):06d}" for _ in range(batch_size)],
        "merchant_category": [random.choice(["GROCERY", "ELECTRONICS", "RESTAURANT", "ATM"]) for _ in range(batch_size)],
        "transaction_timestamp": [
            base_time + timedelta(seconds=batch_number * 60 + i)
            for i in range(batch_size)
        ],
        "location_lat": [round(random.uniform(28.0, 29.0), 6) for _ in range(batch_size)],
        "location_lon": [round(random.uniform(77.0, 78.0), 6) for _ in range(batch_size)],
        "device_id": [f"DEV-{random.randint(1, 100000):06d}" for _ in range(batch_size)],
        "is_fraud": [None] * batch_size,  # Initially unknown
        "fraud_score": [None] * batch_size,
    }
    return pa.table(data)

# Simulate multiple streaming batches
print("Simulating streaming ingestion (Flink → Iceberg)...")
for batch_num in range(5):
    batch = simulate_streaming_batch(batch_num)
    fraud_table.append(batch)
    print(f"  Batch {batch_num}: {len(batch)} records appended")

# ============================================================
# STEP 4: Demonstrate Catalog Layer for Streaming
# ============================================================

print("\n=== CATALOG LAYER FOR STREAMING ===")

# In Flink, the catalog is used to:
# 1. Register the table
# 2. Get metadata location for writing
# 3. Coordinate between multiple Flink jobs

# Simulate Flink catalog interaction
table_location = fraud_table.metadata.location
print(f"Flink would write to: {table_location}")
print(f"Catalog URI: http://catalog-service:8181")
print(f"\nCatalog ensures:")
print(f"  - Multiple Flink jobs don't conflict")
print(f"  - Metadata updates are atomic")
print(f"  - Query engines see consistent state")

# ============================================================
# STEP 5: Demonstrate Metadata Layer for Streaming
# ============================================================

print("\n=== METADATA LAYER FOR STREAMING ===")

metadata = fraud_table.metadata

# Show how metadata tracks streaming writes
print(f"Current Snapshot ID: {metadata.current_snapshot_id}")
print(f"Total Snapshots: {len(metadata.snapshots)}")
print(f"\nSnapshot History (Streaming Batches):")
for snap in metadata.snapshots:
    timestamp = datetime.fromtimestamp(snap.timestamp_ms / 1000)
    print(f"  Snapshot {snap.snapshot_id}: {snap.operation} at {timestamp}")

# ============================================================
# STEP 6: Demonstrate Manifest Layer for Fraud Detection
# ============================================================

print("\n=== MANIFEST LAYER FOR FRAUD DETECTION ===")

current_snapshot = metadata.current_snapshot()
manifests = current_snapshot.manifest_list.manifests()

print(f"Number of Manifests: {len(manifests)}")

# Fraud detection query example
print(f"\nFraud Detection Query:")
print(f"SELECT * FROM transactions")
print(f"WHERE transaction_timestamp >= TIMESTAMP '2026-08-24 10:00:00'")
print(f"  AND transaction_timestamp < TIMESTAMP '2026-08-24 10:05:00'")
print(f"  AND amount > 10000;")

print(f"\nManifest-Based Optimization:")
print(f"  1. Load current snapshot (1 metadata file)")
print(f"  2. Load manifest list (1 metadata file)")
print(f"  3. Identify relevant manifests (skip old hours)")
print(f"  4. Read manifest statistics (skip files with amount_max < 10000)")
print(f"  5. Read only relevant Parquet files")
print(f"\nResult: Query scans ~0.1% of total data")

# ============================================================
# STEP 7: Demonstrate Data File Layer
# ============================================================

print("\n=== DATA FILE LAYER ===")

# Get data files from current manifest
manifest_files = manifests[0].data_files() if manifests else []
print(f"Data Files in First Manifest: {len(manifest_files)}")

for i, file_entry in enumerate(manifest_files[:3]):
    print(f"\n  File {i+1}: {file_entry.file_path}")
    print(f"    Record Count: {file_entry.record_count}")
    print(f"    File Size: {file_entry.file_size_in_bytes / 1024 / 1024:.2f} MB")

# ============================================================
# STEP 8: Streaming + Batch Query Coordination
# ============================================================

print("\n=== STREAMING + BATCH QUERY COORDINATION ===")

# Flink streaming job writes continuously
# Trino/Batch queries read consistent snapshots

print("""
Production Architecture:

Flink Streaming Job
    │
    │ Write batch every 5 minutes
    ▼
Iceberg Table (banking.transactions)
    │
    ├─► Snapshot N (10:00)
    ├─► Snapshot N+1 (10:05)
    ├─► Snapshot N+2 (10:10)
    └─► Snapshot N+3 (10:15) ← Current

Trino Fraud Detection Query
    │
    │ Reads Snapshot N+3 (consistent view)
    │ No interference with Flink writes
    ▼
Fraud Alert System

Benefits:
✓ Flink writes don't block queries
✓ Queries see consistent snapshot
✓ No partial reads during writes
✓ ACID guarantees for both streaming and batch
""")

# ============================================================
# STEP 9: Compaction for Streaming Workloads
# ============================================================

print("\n=== COMPACTION FOR STREAMING WORKLOADS ===")

# After many streaming batches, small files accumulate
print("After 100 streaming batches (500,000 records):")
print("  - 100 small Parquet files")
print("  - Metadata overhead")
print("  - Query performance degradation")

print("\nCompaction Process:")
print("  1. Identify small files (< 128MB)")
print("  2. Rewrite into larger files")
print("  3. Update metadata atomically")
print("  4. Old files become orphans (cleaned later)")

print("\nResult:")
print("  - 100 small files → 4-5 larger files")
print("  - 90% reduction in metadata overhead")
print("  - 3-5x improvement in query performance")
```

---

## 🎯 5 Real-World Interview Questions

### Question 1: Explain the Iceberg metadata hierarchy and how it enables efficient queries.

**Answer:**

The hierarchy is:
```
Catalog → Table Metadata → Snapshot → Manifest List → Manifest → Data Files
```

**How it enables efficiency:**

1. **Catalog**: O(1) lookup to find table metadata location
2. **Table Metadata**: O(1) access to current snapshot
3. **Snapshot**: O(1) access to manifest list
4. **Manifest List**: O(M) where M is number of manifests (typically 10s-100s)
5. **Manifest**: Contains file statistics (min/max, null counts)
6. **Data Files**: Only relevant files are read

**Example**: Query with `WHERE date = '2026-08-24'`
- Without metadata: Scan 10,000 files
- With metadata: Scan 5-10 files (99.9% reduction)

---

### Question 2: What is the difference between a manifest list and a manifest?

**Answer:**

**Manifest List:**
- One per snapshot
- Lists all manifests for that snapshot
- Contains manifest-level metadata (partition spec, file counts)
- Small file, loaded into memory

**Manifest:**
- Many per snapshot
- Lists individual data files
- Contains file-level statistics (min/max, null counts, column sizes)
- Used for data file pruning

**Analogy:**
- Manifest List = Table of contents (lists chapters)
- Manifest = Chapter index (lists pages with summaries)

---

### Question 3: How does Iceberg handle concurrent writes from multiple engines?

**Answer:**

Iceberg uses **optimistic concurrency control**:

1. **Read Phase**: Job reads current snapshot (e.g., Snapshot 100)
2. **Prepare Phase**: Job prepares new files and metadata
3. **Commit Phase**: Job attempts to commit (creates Snapshot 101)
4. **Validation**: Catalog checks if parent snapshot is still current
5. **Success/Fail**: If parent changed, commit fails; job retries

**Key Points:**
- Metadata updates are atomic
- No locking required for reads
- Writes are serialized at commit time
- Conflict detection prevents corruption

**Example:**
```
Job A reads Snapshot 100
Job B reads Snapshot 100
Job A commits → Snapshot 101 (success)
Job B tries to commit → Fails (parent changed)
Job B retries with Snapshot 101 as parent
```

---

### Question 4: Why is the catalog layer important in Iceberg?

**Answer:**

The catalog serves several critical functions:

1. **Namespace Management**: Organizes tables (database.schema.table)
2. **Metadata Routing**: Maps table names to metadata file locations
3. **Transaction Coordination**: Serializes concurrent writes
4. **Engine Interoperability**: Different engines share same catalog
5. **Security**: Central point for access control

**Without Catalog:**
- Each engine would need to know metadata locations
- No central coordination for concurrent writes
- No namespace management
- Each engine would manage its own table registry

**With Catalog:**
- Single source of truth for table locations
- Coordinated concurrent access
- Unified security model
- Engine-agnostic table discovery

---

### Question 5: Explain how Iceberg's manifest statistics enable data file pruning.

**Answer:**

Each manifest contains file-level statistics:

```json
{
  "file_path": "s3://lake/data/file-001.parquet",
  "record_count": 1000000,
  "column_sizes": {"amount": 8000000, "date": 4000000},
  "value_counts": {"amount": 1000000, "date": 1000000},
  "null_value_counts": {"amount": 0, "date": 0},
  "lower_bounds": {"amount": "100", "date": "2026-08-01"},
  "upper_bounds": {"amount": "50000", "date": "2026-08-15"}
}
```

**Pruning Process:**
1. Query: `WHERE date = '2026-08-24'`
2. Check file statistics:
   - File-001: date range [2026-08-01, 2026-08-15] → **SKIP**
   - File-002: date range [2026-08-16, 2026-08-24] → **READ**
   - File-003: date range [2026-08-25, 2026-08-31] → **SKIP**

**Benefit**: Instead of reading 3 files, only read 1 file (67% reduction)

**Advanced**: Partition-level statistics in manifest list provide even earlier pruning.

---

## 📝 Summary

| Layer | Purpose | Key Benefit |
|-------|---------|-------------|
| **Catalog** | Table name → metadata location | Central coordination |
| **Metadata** | Schema, snapshots, properties | Version control |
| **Snapshot** | Immutable table state | Time travel |
| **Manifest List** | Lists manifests for snapshot | Efficient access |
| **Manifest** | Lists files with statistics | Data pruning |
| **Data Files** | Parquet storage | Actual data |

**Key Insight**: Each layer adds efficiency by reducing the amount of data scanned. The hierarchy enables O(1) to O(M) access patterns instead of O(N) full scans.
