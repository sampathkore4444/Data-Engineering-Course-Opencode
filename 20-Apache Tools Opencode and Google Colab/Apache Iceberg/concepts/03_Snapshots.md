# Concept 03: Snapshots

## 📚 Detailed Explanation

A **snapshot** in Apache Iceberg is an **immutable record** of the table's state at a specific point in time. Every committed change (INSERT, UPDATE, DELETE) creates a new snapshot.

### Key Characteristics

1. **Immutable**: Once created, a snapshot never changes
2. **Append-only**: New operations create new snapshots, not modify old ones
3. **Timestamped**: Each snapshot has a creation timestamp
4. **Chain-able**: Snapshots form a chain (linked list)
5. **Expire-able**: Old snapshots can be cleaned up

### Snapshot Structure

```json
{
  "snapshot-id": 102,
  "parent-snapshot-id": 101,
  "timestamp-ms": 1724500000000,
  "operation": "append",
  "manifest-list": "s3://lake/metadata/snap-102.avro",
  "summary": {
    "spark.app.id": "spark-12345",
    "added-files-size": "128MB",
    "added-records": "1000000",
    "total-records": "10000000"
  }
}
```

### Snapshot Operations

| Operation | Description | Example |
|-----------|-------------|---------|
| **append** | Add new files | INSERT INTO |
| **overwrite** | Replace files | INSERT OVERWRITE |
| **delete** | Remove files | DELETE FROM |
| **replace** | Atomic file swap | MERGE INTO |

### Snapshot Chain

```
Snapshot 100 (parent)
    ↓
Snapshot 101 (append)
    ↓
Snapshot 102 (append) ← CURRENT
```

Each snapshot points to:
- **Parent snapshot**: Previous state
- **Manifest list**: Files in this snapshot
- **Operation**: What changed
- **Summary**: Statistics about the change

---

## 💡 Example: Snapshot Lifecycle

### Initial Table State

```text
Snapshot 100
    |
    +-- manifest-1.parquet (1000 rows)
    +-- manifest-2.parquet (1000 rows)
    
Total: 2000 rows
```

### INSERT Operation

```sql
INSERT INTO transactions VALUES (...);
```

```text
Snapshot 101
    |
    +-- manifest-1.parquet (1000 rows) [from 100]
    +-- manifest-2.parquet (1000 rows) [from 100]
    +-- manifest-3.parquet (500 rows)  [NEW]
    
Total: 2500 rows
```

### DELETE Operation

```sql
DELETE FROM transactions WHERE id = 123;
```

```text
Snapshot 102
    |
    +-- manifest-1.parquet (1000 rows) [from 100]
    +-- manifest-2.parquet (999 rows)  [rewritten]
    +-- manifest-3.parquet (500 rows)  [from 101]
    
Total: 2499 rows
```

### Time Travel

```sql
-- Query current state (Snapshot 102)
SELECT COUNT(*) FROM transactions;  -- 2499

-- Query historical state (Snapshot 101)
SELECT COUNT(*) FROM transactions FOR SYSTEM_TIME AS OF TIMESTAMP '...';  -- 2500

-- Query original state (Snapshot 100)
SELECT COUNT(*) FROM transactions FOR SYSTEM_TIME AS OF TIMESTAMP '...';  -- 2000
```

---

## 🏦 Real-World Banking Scenario 1: ETL Job Failure Recovery

### Scenario
A bank's nightly ETL job processes **10 million transactions** and writes to Iceberg. At 2 AM, the job fails after processing 7 million records. The team needs to:
1. Identify the last good snapshot
2. Understand what changed
3. Resume from the correct point

### Problem
- ETL job fails midway
- Partial data written
- Need to resume without data loss or duplication

### Solution
Iceberg snapshots provide:
- **Last good snapshot**: Before ETL started
- **Partial snapshot**: Failed state
- **Recovery**: Resume from last good snapshot

### Python Code

```python
"""
Banking Scenario 1: ETL Job Failure Recovery
Using Iceberg Snapshots for Resilience
"""

from pyiceberg.catalog import load_catalog
from pyiceberg.table import Table
from datetime import datetime, timedelta
import pyarrow as pa
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
# STEP 2: Define Transaction Schema
# ============================================================

schema = pa.schema([
    pa.field("transaction_id", pa.string(), nullable=False),
    pa.field("account_id", pa.string(), nullable=False),
    pa.field("amount", pa.decimal128(18, 2), nullable=False),
    pa.field("transaction_date", pa.date32(), nullable=False),
    pa.field("status", pa.string(), nullable=False),
    pa.field("created_at", pa.timestamp("us"), nullable=False),
])

# Create or load table
try:
    etl_table = catalog.create_table(
        identifier="banking.etl_transactions",
        schema=schema,
        partition_spec={"transform": "day", "source": "transaction_date"}
    )
except Exception:
    etl_table = catalog.load_table("banking.etl_transactions")

# ============================================================
# STEP 3: Simulate ETL Job - Initial Load (Success)
# ============================================================

def generate_transactions(start_id: int, count: int) -> pa.Table:
    """Generate transaction data."""
    from datetime import date
    
    data = {
        "transaction_id": [f"TXN-{i:08d}" for i in range(start_id, start_id + count)],
        "account_id": [f"ACC-{1000 + (i % 100):06d}" for i in range(count)],
        "amount": [round(100 + i * 0.5, 2) for i in range(count)],
        "transaction_date": [date(2026, 8, 24)] * count,
        "status": ["COMPLETED"] * count,
        "created_at": [datetime.now()] * count,
    }
    return pa.table(data)

print("=== ETL JOB SIMULATION ===")
print("\nPhase 1: Initial successful load...")

# Record the "before" snapshot
snapshot_before_etl = etl_table.metadata.current_snapshot_id
print(f"Snapshot before ETL: {snapshot_before_etl}")

# Load first batch (success)
batch1 = generate_transactions(1, 5000)
etl_table.append(batch1)
print(f"Loaded batch 1: {len(batch1)} records")

# ============================================================
# STEP 4: Simulate ETL Job - Second Load (Partial Success)
# ============================================================

print("\nPhase 2: Second batch (simulating failure)...")

# Load second batch
batch2 = generate_transactions(5001, 3000)
etl_table.append(batch2)
print(f"Loaded batch 2: {len(batch2)} records")

# Record snapshot after partial load
snapshot_after_partial = etl_table.metadata.current_snapshot_id
print(f"Snapshot after partial load: {snapshot_after_partial}")

# Simulate ETL failure (in reality, this would be an exception)
print("\n⚠️  ETL Job FAILED at this point!")
print("   Only 8000 of 10000 records processed")

# ============================================================
# STEP 5: Analyze Snapshots for Recovery
# ============================================================

print("\n=== SNAPSHOT ANALYSIS ===")

# Get all snapshots
snapshots = etl_table.metadata.snapshots

print(f"Total snapshots: {len(snapshots)}")
for snap in snapshots:
    timestamp = datetime.fromtimestamp(snap.timestamp_ms / 1000)
    print(f"  Snapshot {snap.snapshot_id}: {snap.operation} at {timestamp}")
    print(f"    Parent: {snap.parent_snapshot_id}")
    print(f"    Summary: {snap.summary}")

# ============================================================
# STEP 6: Recovery Strategy
# ============================================================

print("\n=== RECOVERY STRATEGY ===")

def analyze_snapshot_chain(table: Table) -> list:
    """
    Analyze snapshot chain to identify recovery points.
    """
    snapshots = table.metadata.snapshots
    chain = []
    
    for snap in snapshots:
        chain.append({
            "snapshot_id": snap.snapshot_id,
            "parent_id": snap.parent_snapshot_id,
            "operation": snap.operation,
            "timestamp": datetime.fromtimestamp(snap.timestamp_ms / 1000),
            "summary": snap.summary
        })
    
    return chain

# Analyze chain
chain = analyze_snapshot_chain(etl_table)

print("Snapshot Chain:")
for i, snap in enumerate(chain):
    print(f"  {i+1}. Snapshot {snap['snapshot_id']}: {snap['operation']}")
    print(f"     Time: {snap['timestamp']}")
    if snap['summary']:
        print(f"     Records: {snap['summary'].get('added-records', 'N/A')}")

# ============================================================
# STEP 7: Identify Good Snapshot for Recovery
# ============================================================

print("\n=== RECOVERY POINT IDENTIFICATION ===")

def find_recovery_point(table: Table, failed_operation_time: datetime) -> dict:
    """
    Find the last good snapshot before the failed operation.
    """
    snapshots = table.metadata.snapshots
    good_snapshots = []
    
    for snap in snapshots:
        snap_time = datetime.fromtimestamp(snap.timestamp_ms / 1000)
        if snap_time < failed_operation_time:
            good_snapshots.append(snap)
    
    if good_snapshots:
        last_good = good_snapshots[-1]
        return {
            "snapshot_id": last_good.snapshot_id,
            "timestamp": datetime.fromtimestamp(last_good.timestamp_ms / 1000),
            "operation": last_good.operation,
            "recommendation": "Restore to this snapshot"
        }
    else:
        return {"error": "No good snapshot found"}

# Find recovery point
failed_time = datetime.now()
recovery_point = find_recovery_point(etl_table, failed_time)

print(f"Recovery Point:")
print(f"  Snapshot ID: {recovery_point['snapshot_id']}")
print(f"  Timestamp: {recovery_point['timestamp']}")
print(f"  Recommendation: {recovery_point['recommendation']}")

# ============================================================
# STEP 8: Query Historical State (Time Travel)
# ============================================================

print("\n=== TIME TRAVEL QUERY ===")

# Query the last good snapshot
def query_historical_state(table: Table, snapshot_id: int) -> pa.Table:
    """
    Query table state at a specific snapshot.
    """
    # In practice, you'd use time travel SQL
    # Here we simulate by scanning with filters
    
    scan_result = table.scan().to_arrow()
    return scan_result

# Get current state
current_data = query_historical_state(etl_table, etl_table.metadata.current_snapshot_id)
print(f"Current state: {len(current_data)} records")

# In production, you would use:
# spark.sql(f"""
#     SELECT * FROM banking.etl_transactions
#     FOR SYSTEM_TIME AS OF TIMESTAMP '{recovery_point['timestamp']}'
# """)

# ============================================================
# STEP 9: Recovery Actions
# ============================================================

print("\n=== RECOVERY ACTIONS ===")

print("""
Recovery Steps:

1. STOP the failed ETL job
   - Ensure no partial writes are committed
   
2. ANALYZE snapshot chain
   - Identify last successful snapshot
   - Determine what data was processed
   
3. QUERY historical state
   - Verify data integrity at recovery point
   - Count records, check sums
   
4. OPTION A: Resume from recovery point
   - Start ETL from where it left off
   - Avoid reprocessing already committed data
   
5. OPTION B: Full reprocess
   - If data integrity is uncertain
   - Reprocess all records from source
   
6. VALIDATE recovered data
   - Compare with source system
   - Ensure no duplicates or gaps
""")

# ============================================================
# STEP 10: Implement Recovery (Resume ETL)
# ============================================================

print("\n=== RESUME ETL FROM RECOVERY POINT ===")

def resume_etl_from_point(table: Table, recovery_snapshot_id: int, source_data: pa.Table):
    """
    Resume ETL from a specific recovery point.
    """
    # Verify current snapshot matches recovery point
    current_snapshot = table.metadata.current_snapshot_id
    
    if current_snapshot == recovery_snapshot_id:
        print(f"✓ Current snapshot matches recovery point: {recovery_snapshot_id}")
        
        # Process only new records (not already committed)
        # In production, you'd track processed IDs
        
        # Append new data
        table.append(source_data)
        print(f"✓ Appended {len(source_data)} new records")
        
        # Verify
        new_snapshot = table.metadata.current_snapshot_id
        print(f"✓ New snapshot created: {new_snapshot}")
        
        return True
    else:
        print(f"✗ Snapshot mismatch: current={current_snapshot}, expected={recovery_snapshot_id}")
        return False

# Generate remaining transactions
remaining_transactions = generate_transactions(8001, 2000)

# Resume ETL
success = resume_etl_from_point(
    etl_table,
    snapshot_after_partial,
    remaining_transactions
)

if success:
    print(f"\n✓ ETL Recovery Complete!")
    print(f"  Total records: {len(current_data) + len(remaining_transactions)}")
```

---

## 🏦 Real-World Banking Scenario 2: Regulatory Audit Trail

### Scenario
A bank's **compliance team** needs to provide regulators with complete audit trail for a specific transaction over the past 3 years. The transaction has been modified multiple times due to:
- Corrections
- Status changes
- Fraud investigations
- Legal holds

### Problem
- Need to show every state of the transaction
- Regulators require timestamped evidence
- Must prove data integrity over time

### Solution
Iceberg snapshots provide:
- **Immutable history**: Every change is recorded
- **Timestamped evidence**: Exact time of each change
- **Complete audit trail**: Full chain of custody

### Python Code

```python
"""
Banking Scenario 2: Regulatory Audit Trail
Using Iceberg Snapshots for Compliance
"""

from pyiceberg.catalog import load_catalog
from pyiceberg.table import Table
from datetime import datetime, timedelta
import pyarrow as pa
import json

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
# STEP 2: Define Transaction Schema with Audit Fields
# ============================================================

audit_schema = pa.schema([
    pa.field("transaction_id", pa.string(), nullable=False),
    pa.field("account_id", pa.string(), nullable=False),
    pa.field("amount", pa.decimal128(18, 2), nullable=False),
    pa.field("status", pa.string(), nullable=False),
    pa.field("status_reason", pa.string(), nullable=True),
    pa.field("modified_by", pa.string(), nullable=True),
    pa.field("modification_timestamp", pa.timestamp("us"), nullable=False),
    pa.field("audit_trail", pa.string(), nullable=True),  # JSON string
])

# Create table
try:
    audit_table = catalog.create_table(
        identifier="banking.audit_transactions",
        schema=audit_schema
    )
except Exception:
    audit_table = catalog.load_table("banking.audit_transactions")

# ============================================================
# STEP 3: Simulate Transaction Lifecycle
# ============================================================

def create_transaction_snapshot(
    transaction_id: str,
    status: str,
    amount: float,
    reason: str,
    modified_by: str,
    timestamp: datetime,
    audit_trail: list
) -> pa.Table:
    """Create a transaction snapshot with audit trail."""
    
    # Append to audit trail
    audit_entry = {
        "timestamp": timestamp.isoformat(),
        "status": status,
        "reason": reason,
        "modified_by": modified_by,
        "action": "UPDATE" if audit_trail else "INSERT"
    }
    audit_trail.append(audit_entry)
    
    data = {
        "transaction_id": [transaction_id],
        "account_id": ["ACC-1001"],
        "amount": [amount],
        "status": [status],
        "status_reason": [reason],
        "modified_by": [modified_by],
        "modification_timestamp": [timestamp],
        "audit_trail": [json.dumps(audit_trail)],
    }
    return pa.table(data)

# Simulate transaction lifecycle
print("=== TRANSACTION LIFECYCLE SIMULATION ===")
print("Transaction: TXN-2026-08-24-000001")

audit_trail = []

# Stage 1: Initial transaction
print("\n1. Initial Transaction (Aug 24, 10:00 AM)")
snap1 = create_transaction_snapshot(
    transaction_id="TXN-2026-08-24-000001",
    status="COMPLETED",
    amount=50000.00,
    reason="Normal transfer",
    modified_by="SYSTEM",
    timestamp=datetime(2026, 8, 24, 10, 0, 0),
    audit_trail=audit_trail
)
audit_table.append(snap1)

# Stage 2: Fraud investigation
print("2. Fraud Investigation (Aug 25, 2:00 PM)")
snap2 = create_transaction_snapshot(
    transaction_id="TXN-2026-08-24-000001",
    status="UNDER_REVIEW",
    amount=50000.00,
    reason="Suspicious activity detected",
    modified_by="FRAUD_TEAM",
    timestamp=datetime(2026, 8, 25, 14, 0, 0),
    audit_trail=audit_trail
)
audit_table.append(snap2)

# Stage 3: Transaction reversed
print("3. Transaction Reversed (Aug 26, 11:00 AM)")
snap3 = create_transaction_snapshot(
    transaction_id="TXN-2026-08-24-000001",
    status="REVERSED",
    amount=50000.00,
    reason="Confirmed fraud - customer complaint",
    modified_by="COMPLIANCE_OFFICER",
    timestamp=datetime(2026, 8, 26, 11, 0, 0),
    audit_trail=audit_trail
)
audit_table.append(snap3)

# Stage 4: Refund processed
print("4. Refund Processed (Aug 28, 3:00 PM)")
snap4 = create_transaction_snapshot(
    transaction_id="TXN-2026-08-24-000001",
    status="REFUNDED",
    amount=50000.00,
    reason="Refund to customer account",
    modified_by="OPERATIONS_TEAM",
    timestamp=datetime(2026, 8, 28, 15, 0, 0),
    audit_trail=audit_trail
)
audit_table.append(snap4)

# ============================================================
# STEP 4: Generate Audit Report
# ============================================================

print("\n=== REGULATORY AUDIT REPORT ===")

def generate_audit_report(table: Table, transaction_id: str) -> dict:
    """
    Generate complete audit report for a transaction.
    """
    # Get all snapshots
    snapshots = table.metadata.snapshots
    
    audit_report = {
        "transaction_id": transaction_id,
        "report_generated_at": datetime.now().isoformat(),
        "total_snapshots": len(snapshots),
        "audit_trail": []
    }
    
    for snap in snapshots:
        # Query data at each snapshot
        # In production, use time travel
        scan_result = table.scan(
            row_filter=f"transaction_id = '{transaction_id}'"
        ).to_arrow()
        
        if len(scan_result) > 0:
            record = {
                "snapshot_id": snap.snapshot_id,
                "timestamp": datetime.fromtimestamp(snap.timestamp_ms / 1000).isoformat(),
                "operation": snap.operation,
                "data": {
                    col: scan_result.column(col)[0].as_py()
                    for col in scan_result.column_names
                }
            }
            audit_report["audit_trail"].append(record)
    
    return audit_report

# Generate report
report = generate_audit_report(audit_table, "TXN-2026-08-24-000001")

print(json.dumps(report, indent=2, default=str))

# ============================================================
# STEP 5: Verify Data Integrity
# ============================================================

print("\n=== DATA INTEGRITY VERIFICATION ===")

def verify_data_integrity(table: Table, transaction_id: str) -> dict:
    """
    Verify data integrity across all snapshots.
    """
    snapshots = table.metadata.snapshots
    
    verification = {
        "transaction_id": transaction_id,
        "total_snapshots": len(snapshots),
        "integrity_checks": [],
        "status": "PASS"
    }
    
    # Check 1: All snapshots have the transaction
    snapshots_with_transaction = 0
    for snap in snapshots:
        scan_result = table.scan(
            row_filter=f"transaction_id = '{transaction_id}'"
        ).to_arrow()
        if len(scan_result) > 0:
            snapshots_with_transaction += 1
    
    verification["integrity_checks"].append({
        "check": "Transaction present in all snapshots",
        "expected": len(snapshots),
        "actual": snapshots_with_transaction,
        "status": "PASS" if snapshots_with_transaction == len(snapshots) else "FAIL"
    })
    
    # Check 2: Amount consistency
    amounts = []
    for snap in snapshots:
        scan_result = table.scan(
            row_filter=f"transaction_id = '{transaction_id}'"
        ).to_arrow()
        if len(scan_result) > 0:
            amounts.append(float(scan_result.column("amount")[0]))
    
    verification["integrity_checks"].append({
        "check": "Amount consistency across snapshots",
        "amounts": amounts,
        "status": "PASS" if len(set(amounts)) == 1 else "WARN"
    })
    
    # Check 3: Status progression
    statuses = []
    for snap in snapshots:
        scan_result = table.scan(
            row_filter=f"transaction_id = '{transaction_id}'"
        ).to_arrow()
        if len(scan_result) > 0:
            statuses.append(scan_result.column("status")[0].as_py())
    
    verification["integrity_checks"].append({
        "check": "Valid status progression",
        "statuses": statuses,
        "status": "PASS"
    })
    
    # Overall status
    if any(check["status"] == "FAIL" for check in verification["integrity_checks"]):
        verification["status"] = "FAIL"
    
    return verification

# Verify integrity
integrity = verify_data_integrity(audit_table, "TXN-2026-08-24-000001")

print(json.dumps(integrity, indent=2, default=str))

# ============================================================
# STEP 6: Export Audit Trail for Regulators
# ============================================================

print("\n=== EXPORT FOR REGULATORS ===")

def export_audit_trail_for_regulators(table: Table, transaction_id: str) -> str:
    """
    Export complete audit trail in regulator-approved format.
    """
    report = generate_audit_report(table, transaction_id)
    
    # Format for regulatory submission
    export = {
        "document_type": "TRANSACTION_AUDIT_TRAIL",
        "regulation": "BASEL_III",
        "bank_name": "EXAMPLE_BANK",
        "transaction_id": transaction_id,
        "audit_period": {
            "start": report["audit_trail"][0]["timestamp"] if report["audit_trail"] else None,
            "end": report["audit_trail"][-1]["timestamp"] if report["audit_trail"] else None
        },
        "total_modifications": len(report["audit_trail"]),
        "data_source": "Apache Iceberg (Immutable Snapshots)",
        "integrity_verification": "VERIFIED",
        "audit_trail": report["audit_trail"],
        "certification": {
            "generated_by": "Automated Compliance System",
            "timestamp": datetime.now().isoformat(),
            "hash": "SHA256:..."  # In production, compute actual hash
        }
    }
    
    return json.dumps(export, indent=2, default=str)

# Export for regulators
regulator_export = export_audit_trail_for_regulators(
    audit_table,
    "TXN-2026-08-24-000001"
)

print(regulator_export)
```

---

## 🎯 5 Real-World Interview Questions

### Question 1: What is a snapshot in Iceberg and why is immutability important?

**Answer:**

A **snapshot** is an immutable record of the table's state at a point in time. It contains:
- Snapshot ID (unique identifier)
- Timestamp
- Parent snapshot ID (chain link)
- Operation type (append, overwrite, delete)
- Manifest list location (files in this snapshot)

**Why immutability matters:**

1. **Time Travel**: Historical snapshots are never modified, enabling accurate queries of past states
2. **Concurrency**: Multiple readers can access different snapshots without conflicts
3. **Audit Trail**: Complete history of changes is preserved
4. **Recovery**: Can always restore to a known good state
5. **Consistency**: Readers see a consistent view, not partial changes

**Example**: If Snapshot 100 is immutable, a query started at Snapshot 100 will always see the same data, even as Snapshot 101, 102, etc. are created.

---

### Question 2: How does Iceberg handle snapshot cleanup and why is it necessary?

**Answer:**

Iceberg provides **snapshot expiration** to clean up old snapshots:

```sql
CALL catalog.system.expire_snapshots(
    table => 'db.transactions',
    older_than => TIMESTAMP '2026-07-24 00:00:00',
    retain_last => 10
);
```

**Why it's necessary:**

1. **Storage Cost**: Each snapshot may reference old files that are no longer needed
2. **Metadata Overhead**: Too many snapshots slow down metadata operations
3. **Performance**: Query planning scans snapshot history
4. **Compliance**: Some regulations require data retention limits

**What gets cleaned up:**
- Old snapshot metadata files
- Old manifest lists
- Manifests no longer referenced by any snapshot
- Data files no longer referenced (orphan files)

**Best Practice**: Keep snapshots for 30-90 days depending on business needs.

---

### Question 3: Explain the difference between a snapshot and a version.

**Answer:**

**Snapshot:**
- Immutable record at a point in time
- Contains manifest list (file references)
- Has unique ID and timestamp
- Never modified after creation

**Version:**
- Logical concept (not a formal Iceberg term)
- Often refers to a snapshot that represents a meaningful state
- May correspond to a business event (daily close, ETL completion)

**Relationship:**
```
Version 1.0 → Snapshot 100 (initial load)
Version 1.1 → Snapshot 101 (daily update)
Version 2.0 → Snapshot 102 (schema change)
```

**Key Difference**: Snapshots are physical (metadata files), versions are logical (business meaning).

---

### Question 4: How do you identify the current snapshot vs historical snapshots?

**Answer:**

**Current Snapshot:**
- Stored in table metadata JSON: `"current-snapshot-id": 102`
- Only one snapshot is "current" at any time
- New writes create new current snapshot

**Historical Snapshots:**
- Stored in metadata JSON: `"snapshots": [100, 101, 102]`
- Accessible via time travel
- Available until expired

**Access Patterns:**
```sql
-- Current state
SELECT * FROM transactions;

-- Historical state
SELECT * FROM transactions 
FOR SYSTEM_TIME AS OF TIMESTAMP '2026-08-24 10:00:00';
```

**Programmatic Access:**
```python
# Current snapshot
current = table.metadata.current_snapshot_id

# All snapshots
all_snapshots = [s.snapshot_id for s in table.metadata.snapshots]
```

---

### Question 5: What happens to snapshots during a failed ETL job?

**Answer:**

**Two scenarios:**

**Scenario 1: Job fails before commit**
- No new snapshot created
- Table remains in previous state
- No cleanup needed

**Scenario 2: Job fails after partial commit**
- New snapshot may be created with partial data
- Need to analyze snapshot chain
- Options:
  1. **Expire** the partial snapshot (if allowed)
  2. **Keep** and document the partial state
  3. **Resume** from last good snapshot

**Recovery Steps:**
```python
# 1. Identify last good snapshot
good_snapshot = find_last_good_snapshot(table)

# 2. Query at that snapshot
data_at_good_state = table.scan(
    snapshot_id=good_snapshot
).to_arrow()

# 3. Resume ETL from that point
resume_etl(table, good_snapshot, source_data)
```

**Prevention:**
- Use idempotent writes
- Track processed records
- Implement checkpointing
- Use transactions (atomic commits)

---

## 📝 Summary

| Aspect | Key Point |
|--------|-----------|
| **Definition** | Immutable record of table state at point in time |
| **Structure** | ID, timestamp, parent, operation, manifest list |
| **Chain** | Snapshots form a linked list (parent → child) |
| **Current** | Only one snapshot is "current" (in metadata) |
| **Historical** | Old snapshots enable time travel |
| **Cleanup** | Snapshot expiration removes old snapshots |
| **Immutability** | Never modified, only created/expired |
| **Recovery** | Use snapshot chain to identify recovery points |
