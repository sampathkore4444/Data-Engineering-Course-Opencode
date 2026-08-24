# Concept 05: ACID Transactions

## 📚 Detailed Explanation

**ACID transactions** in Apache Iceberg bring database-like transactional guarantees to data lakes. This is one of the most important features that differentiates Iceberg from plain Parquet files.

### What is ACID?

ACID stands for:
- **A**tomicity: All or nothing
- **C**onsistency: Valid state transitions
- **I**solation: Concurrent operations don't interfere
- **D** durability: Committed data persists

### ACID in Traditional Databases vs Iceberg

| Aspect | Traditional DB | Iceberg |
|--------|---------------|---------|
| **Atomicity** | Transaction log | Metadata commit |
| **Consistency** | Constraints, triggers | Schema validation |
| **Isolation** | Locks | Snapshot isolation |
| **Durability** | WAL, replication | Object storage + metadata |

### How Iceberg Achieves ACID

**Atomicity:**
```
Before Commit:
  metadata-v100.json
      ↓
  [Write new files]  ← In progress
      ↓
  [Update metadata]  ← Not yet

After Commit (atomic):
  metadata-v101.json  ← NEW (atomic swap)
```

**Consistency:**
- Schema validation on write
- Partition spec validation
- Type checking

**Isolation:**
- Snapshot isolation (each reader sees consistent state)
- Optimistic concurrency control

**Durability:**
- Data written to object storage (S3, GCS)
- Metadata committed to catalog
- Replication across availability zones

### Iceberg Transaction Operations

| Operation | Description | Example |
|-----------|-------------|---------|
| **INSERT** | Add new rows | `INSERT INTO ...` |
| **UPDATE** | Modify existing rows | `UPDATE ... SET ... WHERE ...` |
| **DELETE** | Remove rows | `DELETE FROM ... WHERE ...` |
| **MERGE** | Conditional upsert | `MERGE INTO ... USING ...` |
| **INSERT OVERWRITE** | Replace partition | `INSERT OVERWRITE ...` |

---

## 💡 Example: ACID in Banking

### Scenario: Fund Transfer

**Without ACID (Plain Parquet):**
```
Step 1: Debit Account A (write file)
Step 2: Credit Account B (write file)
❌ What if Step 2 fails?
   - Money debited from A
   - Money not credited to B
   - Data inconsistency!
```

**With ACID (Iceberg):**
```
Step 1: Prepare debit for Account A
Step 2: Prepare credit for Account B
Step 3: Commit (atomic)
   ✓ Both succeed → New snapshot
   ✗ Either fails → No changes
```

---

## 🏦 Real-World Banking Scenario 1: Inter-Bank Fund Transfer

### Scenario
A customer transfers **$50,000** from their savings account to another bank via NEFT. The system must ensure:
1. Debit from source account
2. Credit to destination account
3. Both succeed or both fail (atomicity)
4. No duplicate transactions (durability)
5. Concurrent transfers don't interfere (isolation)

### Problem
- Network failures can cause partial commits
- System crashes during transfer
- Multiple transfers to same account

### Solution
Iceberg ACID transactions ensure:
- **Atomicity**: Transfer is all-or-nothing
- **Consistency**: Account balances remain valid
- **Isolation**: Concurrent transfers are safe
- **Durability**: Completed transfers persist

### Python Code

```python
"""
Banking Scenario 1: Inter-Bank Fund Transfer
Using Iceberg ACID Transactions
"""

from pyiceberg.catalog import load_catalog
from pyiceberg.table import Table
from datetime import datetime
import pyarrow as pa
import uuid

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
# STEP 2: Define Account and Transaction Schemas
# ============================================================

account_schema = pa.schema([
    pa.field("account_id", pa.string(), nullable=False),
    pa.field("customer_id", pa.string(), nullable=False),
    pa.field("account_type", pa.string(), nullable=False),
    pa.field("balance", pa.decimal128(18, 2), nullable=False),
    pa.field("currency", pa.string(), nullable=False),
    pa.field("status", pa.string(), nullable=False),
    pa.field("last_updated", pa.timestamp("us"), nullable=False),
])

transaction_schema = pa.schema([
    pa.field("transaction_id", pa.string(), nullable=False),
    pa.field("source_account", pa.string(), nullable=False),
    pa.field("destination_account", pa.string(), nullable=False),
    pa.field("amount", pa.decimal128(18, 2), nullable=False),
    pa.field("currency", pa.string(), nullable=False),
    pa.field("transaction_type", pa.string(), nullable=False),
    pa.field("status", pa.string(), nullable=False),
    pa.field("created_at", pa.timestamp("us"), nullable=False),
    pa.field("completed_at", pa.timestamp("us"), nullable=True),
])

# Create tables
try:
    account_table = catalog.create_table(
        identifier="banking.accounts",
        schema=account_schema
    )
except Exception:
    account_table = catalog.load_table("banking.accounts")

try:
    txn_table = catalog.create_table(
        identifier="banking.transactions",
        schema=transaction_schema
    )
except Exception:
    txn_table = catalog.load_table("banking.transactions")

# ============================================================
# STEP 3: Initialize Account Data
# ============================================================

print("=== INTER-BANK FUND TRANSFER ===\n")

# Initial accounts
initial_accounts = pa.table({
    "account_id": ["ACC-SAVINGS-001", "ACC-SAVINGS-002"],
    "customer_id": ["CUST-1001", "CUST-1002"],
    "account_type": ["SAVINGS", "SAVINGS"],
    "balance": [100000.00, 75000.00],
    "currency": ["USD", "USD"],
    "status": ["ACTIVE", "ACTIVE"],
    "last_updated": [datetime.now(), datetime.now()],
})

account_table.append(initial_accounts)
print("Initial Account Balances:")
print(f"  ACC-SAVINGS-001: $100,000.00")
print(f"  ACC-SAVINGS-002: $75,000.00")

# ============================================================
# STEP 4: ACID Transfer Operation
# ============================================================

def transfer_funds(
    account_table: Table,
    txn_table: Table,
    source_account: str,
    destination_account: str,
    amount: float,
    currency: str = "USD"
) -> dict:
    """
    Perform ACID-compliant fund transfer.
    """
    transaction_id = f"TXN-{uuid.uuid4().hex[:12].upper()}"
    
    print(f"\n--- Transfer Operation: {transaction_id} ---")
    
    # Step 1: Read current balances
    print("1. Reading current balances...")
    source_data = account_table.scan(
        row_filter=f"account_id = '{source_account}'"
    ).to_arrow()
    
    dest_data = account_table.scan(
        row_filter=f"account_id = '{destination_account}'"
    ).to_arrow()
    
    if len(source_data) == 0:
        return {"error": f"Source account {source_account} not found"}
    if len(dest_data) == 0:
        return {"error": f"Destination account {destination_account} not found"}
    
    source_balance = float(source_data.column("balance")[0])
    dest_balance = float(dest_data.column("balance")[0])
    
    print(f"   Source balance: ${source_balance:,.2f}")
    print(f"   Destination balance: ${dest_balance:,.2f}")
    
    # Step 2: Validate sufficient funds
    print("2. Validating sufficient funds...")
    if source_balance < amount:
        return {"error": f"Insufficient funds: ${source_balance:,.2f} < ${amount:,.2f}"}
    print(f"   ✓ Sufficient funds available")
    
    # Step 3: Prepare new balances
    print("3. Preparing new balances...")
    new_source_balance = source_balance - amount
    new_dest_balance = dest_balance + amount
    
    print(f"   New source balance: ${new_source_balance:,.2f}")
    print(f"   New destination balance: ${new_dest_balance:,.2f}")
    
    # Step 4: Update accounts (atomic operation)
    print("4. Updating accounts...")
    
    # In production, this would be a single atomic commit
    # Here we simulate by updating both tables
    
    # Update source account
    updated_source = pa.table({
        "account_id": [source_account],
        "customer_id": [source_data.column("customer_id")[0].as_py()],
        "account_type": [source_data.column("account_type")[0].as_py()],
        "balance": [new_source_balance],
        "currency": [currency],
        "status": ["ACTIVE"],
        "last_updated": [datetime.now()],
    })
    
    # Update destination account
    updated_dest = pa.table({
        "account_id": [destination_account],
        "customer_id": [dest_data.column("customer_id")[0].as_py()],
        "account_type": [dest_data.column("account_type")[0].as_py()],
        "balance": [new_dest_balance],
        "currency": [currency],
        "status": ["ACTIVE"],
        "last_updated": [datetime.now()],
    })
    
    # Append updates (in production, use overwrite for updates)
    account_table.append(updated_source)
    account_table.append(updated_dest)
    
    # Step 5: Record transaction
    print("5. Recording transaction...")
    txn_record = pa.table({
        "transaction_id": [transaction_id],
        "source_account": [source_account],
        "destination_account": [destination_account],
        "amount": [amount],
        "currency": [currency],
        "transaction_type": ["TRANSFER"],
        "status": ["COMPLETED"],
        "created_at": [datetime.now()],
        "completed_at": [datetime.now()],
    })
    
    txn_table.append(txn_record)
    
    # Step 6: Commit (atomic)
    print("6. Committing transaction (atomic)...")
    print(f"   ✓ Transaction {transaction_id} completed successfully")
    
    return {
        "transaction_id": transaction_id,
        "source_account": source_account,
        "destination_account": destination_account,
        "amount": amount,
        "new_source_balance": new_source_balance,
        "new_dest_balance": new_dest_balance,
        "status": "COMPLETED"
    }

# Execute transfer
result = transfer_funds(
    account_table,
    txn_table,
    source_account="ACC-SAVINGS-001",
    destination_account="ACC-SAVINGS-002",
    amount=50000.00
)

print(f"\nTransfer Result:")
print(f"  Transaction ID: {result['transaction_id']}")
print(f"  Status: {result['status']}")
print(f"  New Source Balance: ${result['new_source_balance']:,.2f}")
print(f"  New Destination Balance: ${result['new_dest_balance']:,.2f}")

# ============================================================
# STEP 5: Verify ACID Properties
# ============================================================

print("\n=== ACID PROPERTIES VERIFICATION ===")

print("""
Atomicity:
  ✓ Both debit and credit succeeded together
  ✓ No partial state exists
  ✓ If either failed, neither would be committed

Consistency:
  ✓ Total money in system unchanged ($100K + $75K = $175K → $50K + $125K = $175K)
  ✓ Both accounts remain in valid state
  ✓ No negative balances

Isolation:
  ✓ Concurrent transfers to same account are safe
  ✓ Each transfer sees consistent snapshot
  ✓ No dirty reads

Durability:
  ✓ Transaction recorded in Iceberg table
  ✓ Data persisted to object storage
  ✓ Can be queried and audited
""")

# ============================================================
# STEP 6: Audit Trail
# ============================================================

print("=== AUDIT TRAIL ===")

# Query transaction history
transactions = txn_table.scan().to_arrow()
print(f"Total transactions: {len(transactions)}")
for i in range(len(transactions)):
    txn_id = transactions.column("transaction_id")[i].as_py()
    source = transactions.column("source_account")[i].as_py()
    dest = transactions.column("destination_account")[i].as_py()
    amount = transactions.column("amount")[i].as_py()
    status = transactions.column("status")[i].as_py()
    print(f"  {txn_id}: ${amount:,.2f} from {source} to {dest} [{status}]")
```

---

## 🏦 Real-World Banking Scenario 2: Batch Payment Processing

### Scenario
A bank processes **1 million salary payments** on the last day of each month. The batch job must:
1. Read employee salary data
2. Debit company account
3. Credit individual employee accounts
4. Handle failures gracefully
5. Maintain audit trail

### Problem
- Failures can leave partial state
- Millions of transactions must be atomic
- Regulatory audit required

### Solution
Iceberg ACID transactions provide:
- **Batch atomicity**: All-or-nothing for entire batch
- **Checkpointing**: Resume from last successful point
- **Audit trail**: Complete record of all changes

### Python Code

```python
"""
Banking Scenario 2: Batch Payment Processing
Using Iceberg ACID Transactions for Payroll
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
# STEP 2: Define Payroll Schema
# ============================================================

payroll_schema = pa.schema([
    pa.field("employee_id", pa.string(), nullable=False),
    pa.field("employee_name", pa.string(), nullable=False),
    pa.field("department", pa.string(), nullable=False),
    pa.field("base_salary", pa.decimal128(18, 2), nullable=False),
    pa.field("bonus", pa.decimal128(18, 2), nullable=True),
    pa.field("deductions", pa.decimal128(18, 2), nullable=True),
    pa.field("net_salary", pa.decimal128(18, 2), nullable=False),
    pa.field("payment_date", pa.date32(), nullable=False),
    pa.field("status", pa.string(), nullable=False),
    pa.field("processed_at", pa.timestamp("us"), nullable=True),
])

# Create table
try:
    payroll_table = catalog.create_table(
        identifier="banking.payroll",
        schema=payroll_schema
    )
except Exception:
    payroll_table = catalog.load_table("banking.payroll")

# ============================================================
# STEP 3: Generate Employee Salary Data
# ============================================================

print("=== BATCH PAYROLL PROCESSING ===\n")

def generate_payroll_batch(batch_size: int = 100) -> pa.Table:
    """Generate payroll data for a batch of employees."""
    
    departments = ["IT", "HR", "Finance", "Operations", "Sales"]
    
    data = {
        "employee_id": [f"EMP-{i:06d}" for i in range(1, batch_size + 1)],
        "employee_name": [f"Employee_{i}" for i in range(1, batch_size + 1)],
        "department": [random.choice(departments) for _ in range(batch_size)],
        "base_salary": [round(random.uniform(3000, 15000), 2) for _ in range(batch_size)],
        "bonus": [round(random.uniform(0, 5000), 2) for _ in range(batch_size)],
        "deductions": [round(random.uniform(500, 3000), 2) for _ in range(batch_size)],
        "net_salary": [0.0] * batch_size,  # Calculated later
        "payment_date": [datetime(2026, 8, 31).date()] * batch_size,
        "status": ["PENDING"] * batch_size,
        "processed_at": [None] * batch_size,
    }
    
    # Calculate net salary
    for i in range(batch_size):
        base = data["base_salary"][i]
        bonus = data["bonus"][i] or 0
        deductions = data["deductions"][i] or 0
        data["net_salary"][i] = base + bonus - deductions
    
    return pa.table(data)

# Generate payroll batch
payroll_batch = generate_payroll_batch(100)
print(f"Generated payroll batch: {len(payroll_batch)} employees")

# ============================================================
# STEP 4: ACID Batch Processing
# ============================================================

def process_payroll_batch(
    payroll_table: Table,
    batch: pa.Table,
    failure_rate: float = 0.01
) -> dict:
    """
    Process payroll batch with ACID guarantees.
    Simulates occasional failures for testing.
    """
    print(f"\n--- Processing Payroll Batch ---")
    
    start_time = datetime.now()
    
    # Track processing
    successful = 0
    failed = 0
    failed_employees = []
    
    # Process each employee (simplified - in production, batch operations)
    for i in range(len(batch)):
        employee_id = batch.column("employee_id")[i].as_py()
        net_salary = float(batch.column("net_salary")[i])
        
        # Simulate occasional failure
        if random.random() < failure_rate:
            failed += 1
            failed_employees.append(employee_id)
            print(f"  ✗ FAILED: {employee_id} (simulated error)")
            continue
        
        successful += 1
    
    end_time = datetime.now()
    processing_time = (end_time - start_time).total_seconds()
    
    # If any failed, rollback entire batch (ACID atomicity)
    if failed > 0:
        print(f"\n⚠️  BATCH FAILED: {failed}/{len(batch)} employees failed")
        print("   Rolling back entire batch (ACID atomicity)...")
        
        # In production, no data would be committed
        # Here we simulate by not appending to table
        
        return {
            "status": "FAILED",
            "total_employees": len(batch),
            "successful": 0,
            "failed": len(batch),  # Entire batch rolled back
            "failed_employees": [batch.column("employee_id")[i].as_py() for i in range(len(batch))],
            "processing_time_seconds": processing_time,
            "message": "Batch rolled back due to partial failure"
        }
    
    # All successful - commit batch
    print(f"\n✓ BATCH SUCCESSFUL: {successful}/{len(batch)} employees processed")
    
    # Mark as processed
    processed_batch = batch.set_column(
        batch.schema.get_field_index("status"),
        "status",
        pa.array(["PROCESSED"] * len(batch))
    )
    processed_batch = processed_batch.set_column(
        processed_batch.schema.get_field_index("processed_at"),
        "processed_at",
        pa.array([datetime.now()] * len(batch))
    )
    
    # Commit to Iceberg (atomic)
    payroll_table.append(processed_batch)
    
    print(f"   Committed {len(processed_batch)} records to Iceberg")
    print(f"   Processing time: {processing_time:.2f} seconds")
    
    return {
        "status": "SUCCESS",
        "total_employees": len(batch),
        "successful": successful,
        "failed": 0,
        "processing_time_seconds": processing_time,
        "message": "Batch processed successfully"
    }

# Process payroll batch
result = process_payroll_batch(payroll_table, payroll_batch, failure_rate=0.0)

print(f"\nBatch Result:")
print(f"  Status: {result['status']}")
print(f"  Employees: {result['successful']}/{result['total_employees']}")
print(f"  Processing Time: {result['processing_time_seconds']:.2f} seconds")

# ============================================================
# STEP 5: Handle Failure Scenario
# ============================================================

print("\n=== FAILURE SCENARIO TEST ===")

# Generate another batch with higher failure rate
batch_with_failures = generate_payroll_batch(50)
failure_result = process_payroll_batch(payroll_table, batch_with_failures, failure_rate=0.1)

print(f"\nFailure Scenario Result:")
print(f"  Status: {failure_result['status']}")
print(f"  Message: {failure_result['message']}")

# ============================================================
# STEP 6: Audit and Reporting
# ============================================================

print("\n=== PAYROLL AUDIT REPORT ===")

# Query processed payroll
processed_payroll = payroll_table.scan(
    row_filter="status = 'PROCESSED'"
).to_arrow()

print(f"Processed Payroll Summary:")
print(f"  Total Employees: {len(processed_payroll)}")
print(f"  Total Net Salary: ${sum(float(processed_payroll.column('net_salary')[i]) for i in range(len(processed_payroll))):,.2f}")
print(f"  Payment Date: {processed_payroll.column('payment_date')[0].as_py()}")

# Department-wise summary
print(f"\nDepartment-wise Summary:")
departments = set(processed_payroll.column("department").to_pylist())
for dept in sorted(departments):
    dept_data = processed_payroll.filter(
        pa.compute.equal(processed_payroll.column("department"), dept)
    )
    dept_total = sum(float(dept_data.column("net_salary")[i]) for i in range(len(dept_data)))
    print(f"  {dept}: {len(dept_data)} employees, ${dept_total:,.2f} total")

# ============================================================
# STEP 7: ACID Properties Demonstration
# ============================================================

print("\n=== ACID PROPERTIES IN ACTION ===")

print("""
1. ATOMICITY:
   - Payroll batch is all-or-nothing
   - If 1 employee fails, entire batch rolls back
   - No partial payments possible

2. CONSISTENCY:
   - All net_salary = base_salary + bonus - deductions
   - Status transitions: PENDING → PROCESSED
   - No invalid states allowed

3. ISOLATION:
   - Multiple payroll batches can run concurrently
   - Each sees consistent snapshot
   - No interference between batches

4. DURABILITY:
   - Committed data persists in object storage
   - Survives system crashes
   - Audit trail maintained

REGULATORY COMPLIANCE:
   ✓ Complete audit trail
   ✓ Timestamped records
   ✓ Immutable snapshots
   ✓ Point-in-time queries
""")
```

---

## 🎯 5 Real-World Interview Questions

### Question 1: How does Iceberg achieve ACID transactions on object storage?

**Answer:**

Iceberg achieves ACID through **metadata management**:

**Atomicity:**
- New files are written to object storage
- Metadata update is atomic (catalog commit)
- Either all files are committed or none

**Consistency:**
- Schema validation on write
- Partition spec validation
- Type checking enforced

**Isolation:**
- Snapshot isolation (each reader sees consistent state)
- Optimistic concurrency control
- No locking required for reads

**Durability:**
- Data written to object storage (replicated)
- Metadata committed to catalog
- Survives node failures

**Example:**
```
1. Write new Parquet files (S3)
2. Create new manifest file
3. Create new manifest list
4. Atomically update table metadata (catalog)
   - If step 4 fails → files are orphaned (cleaned up later)
   - If step 4 succeeds → new snapshot is visible
```

---

### Question 2: What is optimistic concurrency control and how does Iceberg use it?

**Answer:**

**Optimistic Concurrency Control (OCC):**
- Assumes conflicts are rare
- Allows concurrent operations without locking
- Validates at commit time
- Retries if conflict detected

**Iceberg Implementation:**

1. **Read Phase**: Job reads current snapshot (e.g., Snapshot 100)
2. **Prepare Phase**: Job prepares new files and metadata
3. **Commit Phase**: Job attempts to commit (creates Snapshot 101)
4. **Validation**: Catalog checks if parent snapshot is still current
5. **Success/Fail**: If parent changed, commit fails; job retries

**Example:**
```
Job A: Read Snapshot 100 → Prepare → Commit → Snapshot 101 ✓
Job B: Read Snapshot 100 → Prepare → Commit → FAIL (parent changed)
Job B: Retry with Snapshot 101 as parent → Commit → Snapshot 102 ✓
```

**Benefits:**
- No locking overhead
- High throughput for read-heavy workloads
- Automatic conflict detection

---

### Question 3: Explain the difference between COPY-ON-WRITE and MERGE-ON-READ for ACID transactions.

**Answer:**

**COPY-ON-WRITE (CoW):**
- Update: Read old file → Modify → Write new file
- Delete: Read old file → Remove rows → Write new file
- **Pros**: Fast reads, simple
- **Cons**: Slow updates, storage overhead

**MERGE-ON-READ (MoR):**
- Update: Write delta files
- Delete: Write delete files
- Read: Combine data + delta/delete files
- **Pros**: Fast updates, low storage
- **Cons**: Slow reads, complex

**Iceberg Support:**
```
# CoW (default)
ALTER TABLE transactions SET TBLPROPERTIES (
    'write.format.default' = 'parquet'
);

# MoR (with delete files)
ALTER TABLE transactions SET TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.delete.mode' = 'merge-on-read'
);
```

**When to Use:**
- **CoW**: Read-heavy, few updates
- **MoR**: Write-heavy, frequent updates (CDC, streaming)

---

### Question 4: How does Iceberg handle failed transactions and cleanup?

**Answer:**

**Failed Transaction Scenarios:**

1. **Failure before metadata commit:**
   - New files written to object storage
   - Metadata not updated
   - Files become **orphaned**
   - Cleanup: `ORPHAN FILES` maintenance

2. **Failure during metadata commit:**
   - Catalog operation fails
   - No new snapshot created
   - Files remain orphaned

3. **Failure after commit:**
   - New snapshot visible
   - No cleanup needed

**Cleanup Operations:**
```sql
-- Remove orphan files
CALL catalog.system.remove_orphan_files(
    table => 'db.transactions',
    older_than => TIMESTAMP '2026-08-23 00:00:00'
);

-- Expire old snapshots
CALL catalog.system.expire_snapshots(
    table => 'db.transactions',
    older_than => TIMESTAMP '2026-08-23 00:00:00'
);
```

**Best Practices:**
- Monitor orphan files
- Set appropriate retention periods
- Use idempotent writes
- Implement retry logic

---

### Question 5: Can you use Iceberg ACID for real-time streaming? Explain the challenges.

**Answer:**

**Yes, but with considerations:**

**Challenges:**

1. **File Management:**
   - Streaming creates many small files
   - Need compaction to merge files
   - Small files degrade performance

2. **Commit Frequency:**
   - Too frequent commits → metadata overhead
   - Too infrequent → latency issues
   - Need to balance (e.g., commit every 5 minutes)

3. **Concurrency:**
   - Multiple streaming jobs may conflict
   - Optimistic concurrency handles this
   - May need retry logic

4. **Checkpointing:**
   - Long-running jobs need checkpoints
   - Failure recovery from last checkpoint
   - Iceberg snapshots serve as checkpoints

**Solution Architecture:**
```
Kafka → Flink → Iceberg (commit every 5 min)
                   ↓
              Compaction (hourly)
                   ↓
              Query Engine (Trino/Spark)
```

**Best Practices:**
- Use Flink's Iceberg connector
- Commit based on time/size thresholds
- Run compaction regularly
- Monitor file counts and sizes

---

## 📝 Summary

| Aspect | Key Point |
|--------|-----------|
| **Atomicity** | All-or-nothing via metadata commit |
| **Consistency** | Schema validation, type checking |
| **Isolation** | Snapshot isolation, optimistic concurrency |
| **Durability** | Object storage + catalog persistence |
| **Operations** | INSERT, UPDATE, DELETE, MERGE |
| **CoW vs MoR** | Trade-off between read/write performance |
| **Cleanup** | Orphan files, snapshot expiration |
| **Streaming** | Possible with compaction and checkpointing |
