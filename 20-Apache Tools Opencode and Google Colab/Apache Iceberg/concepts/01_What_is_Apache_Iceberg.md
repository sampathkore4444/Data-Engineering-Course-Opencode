# Concept 01: What is Apache Iceberg

## 📚 Detailed Explanation

Apache Iceberg is an **open-source table format** designed for huge analytic datasets. It is NOT a database and NOT a file format — it is a **metadata and transaction layer** that sits between your query engine and your data files.

### The Core Definition

Think of Iceberg as a **"table management system"** for your data lake. It tells your query engines:

- **Which files** belong to the table
- **Which version** of the table is current
- **What schema** the table has
- **What changed** between versions
- **How data is organized** (partitions, sorting)

### Where Iceberg Fits

```
┌───────────────────────────────┐
│         BI / ML / Apps        │
└───────────────┬───────────────┘
                │
        Query / Data Access
                │
       Trino / Spark / DuckDB
                │
       ┌────────▼────────┐
       │  Apache Iceberg │  ◄── Table Format (metadata layer)
       │   Table Format  │
       └────────┬────────┘
                │
       ┌────────▼────────┐
       │    Parquet      │  ◄── File Format (data storage)
       │   File Format   │
       └────────┬────────┘
                │
       Object Storage (S3/GCS/ADLS)
```

### Key Characteristics

1. **Open Format**: Any engine (Spark, Trino, Flink, DuckDB) can read/write
2. **ACID Transactions**: Full transactional support for data lakes
3. **Schema Evolution**: Change schema without rewriting files
4. **Time Travel**: Query data as it existed at any point in time
5. **Hidden Partitioning**: Users don't need to know physical layout
6. **Partition Evolution**: Change partitioning strategy without rewriting data

### The One-Liner

> **Apache Iceberg turns a collection of Parquet files in a data lake into a reliable, transactional, evolvable analytical table.**

---

## 💡 Example

### Without Iceberg (Plain Data Lake)

```text
/data/lake/transactions/
    ├── part-001.parquet
    ├── part-002.parquet
    ├── part-003.parquet
    ├── ...
    └── part-500000.parquet

Problems:
- No table versioning
- No ACID transactions
- No schema evolution
- No time travel
- No concurrent write safety
```

### With Iceberg (Managed Table)

```text
/data/lake/transactions/
    ├── metadata/
    │   ├── v1.metadata.json
    │   ├── v2.metadata.json
    │   ├── snap-001.avro
    │   ├── snap-002.avro
    │   └── ...
    ├── data/
    │   ├── part-001.parquet
    │   ├── part-002.parquet
    │   └── part-003.parquet
    └── schema.json

Benefits:
✓ Full table management
✓ Version control
✓ ACID compliance
✓ Time travel
✓ Schema evolution
```

---

## 🏦 Real-World Banking Scenario 1: Daily Transaction Reconciliation

### Scenario
A bank processes **50 million transactions daily**. The reconciliation team needs to verify that all transactions from yesterday match between the core banking system and the data lake. If there's a discrepancy, they need to see exactly what changed and when.

### Problem Without Iceberg
- No way to query "yesterday's snapshot" accurately
- ETL jobs might partially fail, leaving inconsistent data
- Can't track what changed between reconciliation runs

### Solution With Iceberg
- Each ETL run creates a new snapshot
- Reconciliation queries can time-travel to yesterday's exact state
- ACID guarantees ensure partial failures don't corrupt the table

### Python Code

```python
"""
Banking Scenario 1: Daily Transaction Reconciliation
Using Apache Iceberg with PyIceberg
"""

from pyiceberg.catalog import load_catalog
from pyiceberg.table import Table
from datetime import datetime, timedelta
import pyarrow.parquet as pq
import pyarrow as pa

# ============================================================
# STEP 1: Configure Iceberg Catalog Connection
# ============================================================

# Load the Iceberg catalog (REST catalog example)
catalog = load_catalog(
    "banking_catalog",
    **{
        "uri": "http://localhost:8181",
        "warehouse": "s3a://banking-lakehouse/"
    }
)

# ============================================================
# STEP 2: Define Transaction Table Schema
# ============================================================

# Define schema for banking transactions
schema = pa.schema([
    pa.field("transaction_id", pa.string(), nullable=False),
    pa.field("account_id", pa.string(), nullable=False),
    pa.field("customer_id", pa.string(), nullable=False),
    pa.field("branch_id", pa.string(), nullable=False),
    pa.field("amount", pa.decimal128(18, 2), nullable=False),
    pa.field("currency", pa.string(), nullable=False),
    pa.field("transaction_type", pa.string(), nullable=False),
    pa.field("transaction_timestamp", pa.timestamp("us"), nullable=False),
    pa.field("status", pa.string(), nullable=False),
    pa.field("created_at", pa.timestamp("us"), nullable=False),
])

# Create the Iceberg table if it doesn't exist
try:
    table = catalog.create_table(
        identifier="banking.daily_transactions",
        schema=schema,
        partition_spec={"transform": "day", "source": "transaction_timestamp"}
    )
    print("Created new Iceberg table: banking.daily_transactions")
except Exception as e:
    table = catalog.load_table("banking.daily_transactions")
    print(f"Loaded existing table: {table}")

# ============================================================
# STEP 3: Load Today's Transactions (ETL Job)
# ============================================================

def load_daily_transactions(date: datetime) -> pa.Table:
    """
    Simulate loading transactions from core banking system.
    In production, this would connect to Oracle/MySQL via Debezium.
    """
    # Simulated transaction data
    transactions = {
        "transaction_id": [f"TXN-{date.strftime('%Y%m%d')}-{i:06d}" for i in range(1, 101)],
        "account_id": [f"ACC-{1000 + (i % 50):06d}" for i in range(100)],
        "customer_id": [f"CUST-{5000 + (i % 30):06d}" for i in range(100)],
        "branch_id": [f"BR-{10 + (i % 10):03d}" for i in range(100)],
        "amount": [float(100 + i * 10) for i in range(100)],
        "currency": ["INR"] * 100,
        "transaction_type": ["CREDIT" if i % 3 == 0 else "DEBIT" for i in range(100)],
        "transaction_timestamp": [
            datetime(date.year, date.month, date.day, 9 + (i // 12), i % 60)
            for i in range(100)
        ],
        "status": ["COMPLETED"] * 100,
        "created_at": [datetime.now()] * 100,
    }
    return pa.table(transactions)

# Load transactions for today
today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
transactions_today = load_daily_transactions(today)

# Append to Iceberg table
table.append(transactions_today)
print(f"Loaded {len(transactions_today)} transactions for {today.date()}")

# ============================================================
# STEP 4: Reconciliation - Time Travel Query
# ============================================================

# Get yesterday's date
yesterday = today - timedelta(days=1)

# Query using time travel - see data as it was yesterday
yesterday_snapshot = table.scan(
    row_filter=f"transaction_timestamp >= TIMESTAMP '{yesterday}' AND "
               f"transaction_timestamp < TIMESTAMP '{today}'"
).to_arrow()

print(f"\nReconciliation Report:")
print(f"Yesterday's transactions: {len(yesterday_snapshot)}")
print(f"Today's transactions: {len(transactions_today)}")

# ============================================================
# STEP 5: Compare Snapshots for Discrepancies
# ============================================================

def reconcile_transactions(previous_day: pa.Table, current_day: pa.Table) -> dict:
    """
    Compare two daily snapshots to find discrepancies.
    """
    # Get transaction IDs from both days
    prev_ids = set(previous_day.column("transaction_id").to_pylist())
    curr_ids = set(current_day.column("transaction_id").to_pylist())
    
    # Find discrepancies
    missing_in_current = prev_ids - curr_ids
    new_in_current = curr_ids - prev_ids
    common = prev_ids & curr_ids
    
    # Check for amount discrepancies
    amount_discrepancies = []
    prev_dict = {
        row["transaction_id"]: row["amount"] 
        for row in previous_day.to_pydict().items()
    }
    curr_dict = {
        row["transaction_id"]: row["amount"] 
        for row in current_day.to_pydict().items()
    }
    
    for txn_id in common:
        if prev_dict.get(txn_id) != curr_dict.get(txn_id):
            amount_discrepancies.append({
                "transaction_id": txn_id,
                "previous_amount": prev_dict.get(txn_id),
                "current_amount": curr_dict.get(txn_id)
            })
    
    return {
        "missing_in_current": missing_in_current,
        "new_in_current": new_in_current,
        "amount_discrepancies": amount_discrepancies,
        "total_previous": len(prev_ids),
        "total_current": len(curr_ids)
    }

# Perform reconciliation
reconciliation_result = reconcile_transactions(yesterday_snapshot, transactions_today)

print(f"\n=== RECONCILIATION RESULTS ===")
print(f"Missing in current: {len(reconciliation_result['missing_in_current'])}")
print(f"New in current: {len(reconciliation_result['new_in_current'])}")
print(f"Amount discrepancies: {len(reconciliation_result['amount_discrepancies'])}")

# ============================================================
# STEP 6: Audit Trail - List All Snapshots
# ============================================================

# List all snapshots for audit purposes
snapshots = table.metadata.snapshots
print(f"\n=== AUDIT TRAIL ===")
print(f"Total snapshots: {len(snapshots)}")
for snap in snapshots:
    print(f"  Snapshot {snap.snapshot_id}: "
          f"Created {datetime.fromtimestamp(snap.timestamp_ms / 1000)} | "
          f"Operation: {snap.operation}")
```

---

## 🏦 Real-World Banking Scenario 2: Loan Application Data Lineage

### Scenario
A bank's **loan approval system** needs to provide complete data lineage for regulatory audits (Basel III, RBI guidelines). When a loan is approved or rejected, regulators want to see the exact data state used for the decision, including all historical changes.

### Problem Without Iceberg
- Can't reconstruct exact data state at approval time
- No audit trail for data changes
- Regulatory compliance violations

### Solution With Iceberg
- Time travel provides exact historical snapshots
- Metadata tracks all changes with timestamps
- Complete audit trail for regulatory compliance

### Python Code

```python
"""
Banking Scenario 2: Loan Application Data Lineage
Using Apache Iceberg for Regulatory Compliance
"""

from pyiceberg.catalog import load_catalog
from pyiceberg.table import Table
from datetime import datetime, timedelta
import pyarrow as pa
import json

# ============================================================
# STEP 1: Setup Iceberg Catalog
# ============================================================

catalog = load_catalog(
    "banking_catalog",
    **{
        "uri": "http://localhost:8181",
        "warehouse": "s3a://banking-lakehouse/"
    }
)

# ============================================================
# STEP 2: Define Loan Application Schema
# ============================================================

loan_schema = pa.schema([
    pa.field("application_id", pa.string(), nullable=False),
    pa.field("customer_id", pa.string(), nullable=False),
    pa.field("loan_type", pa.string(), nullable=False),
    pa.field("loan_amount", pa.decimal128(18, 2), nullable=False),
    pa.field("interest_rate", pa.float64(), nullable=False),
    pa.field("tenure_months", pa.int32(), nullable=False),
    pa.field("credit_score", pa.int32(), nullable=False),
    pa.field("income", pa.decimal128(18, 2), nullable=False),
    pa.field("risk_category", pa.string(), nullable=False),
    pa.field("decision", pa.string(), nullable=False),
    pa.field("decision_timestamp", pa.timestamp("us"), nullable=False),
    pa.field("data_snapshot_timestamp", pa.timestamp("us"), nullable=False),
])

# Create Iceberg table
try:
    loan_table = catalog.create_table(
        identifier="banking.loan_applications",
        schema=loan_schema,
        partition_spec={"transform": "month", "source": "decision_timestamp"}
    )
except Exception:
    loan_table = catalog.load_table("banking.loan_applications")

# ============================================================
# STEP 3: Simulate Loan Data Changes Over Time
# ============================================================

def create_loan_data(timestamp: datetime, snapshot_timestamp: datetime) -> pa.Table:
    """Create loan application data with snapshot tracking."""
    data = {
        "application_id": ["LOAN-001", "LOAN-002", "LOAN-003"],
        "customer_id": ["CUST-1001", "CUST-1002", "CUST-1003"],
        "loan_type": ["HOME", "PERSONAL", "BUSINESS"],
        "loan_amount": [5000000.00, 500000.00, 10000000.00],
        "interest_rate": [8.5, 12.0, 10.5],
        "tenure_months": [240, 60, 120],
        "credit_score": [750, 680, 820],
        "income": [150000.00, 50000.00, 300000.00],
        "risk_category": ["LOW", "MEDIUM", "LOW"],
        "decision": ["APPROVED", "PENDING", "APPROVED"],
        "decision_timestamp": [timestamp] * 3,
        "data_snapshot_timestamp": [snapshot_timestamp] * 3,
    }
    return pa.table(data)

# ============================================================
# STEP 4: Create Multiple Snapshots (Simulating Time)
# ============================================================

# Snapshot 1: Initial application state
snap1_time = datetime(2026, 8, 20, 10, 0, 0)
data_snap1 = create_loan_data(snap1_time, snap1_time)
loan_table.append(data_snap1)
print(f"Snapshot 1 created at {snap1_time}")

# Snapshot 2: Updated credit scores (new data from bureau)
snap2_time = datetime(2026, 8, 21, 14, 30, 0)
data_snap2 = create_loan_data(snap2_time, snap2_time)
# Simulate credit score update for LOAN-002
data_snap2 = data_snap2.set_column(
    6, "credit_score", pa.array([750, 720, 820])
)
loan_table.append(data_snap2)
print(f"Snapshot 2 created at {snap2_time}")

# Snapshot 3: Final decision
snap3_time = datetime(2026, 8, 22, 9, 0, 0)
data_snap3 = create_loan_data(snap3_time, snap3_time)
data_snap3 = data_snap3.set_column(
    9, "decision", pa.array(["APPROVED", "APPROVED", "REJECTED"])
)
loan_table.append(data_snap3)
print(f"Snapshot 3 created at {snap3_time}")

# ============================================================
# STEP 5: Regulatory Audit - Time Travel Query
# ============================================================

def audit_loan_decision(
    loan_table: Table,
    application_id: str,
    decision_timestamp: datetime
) -> dict:
    """
    Retrieve exact data state used for a loan decision.
    Required for regulatory audit compliance.
    """
    # Query the table at the exact decision timestamp
    scan_result = loan_table.scan(
        row_filter=f"application_id = '{application_id}' AND "
                   f"decision_timestamp <= TIMESTAMP '{decision_timestamp}'"
    ).to_arrow()
    
    if len(scan_result) == 0:
        return {"error": "No record found for the specified criteria"}
    
    # Get the most recent record before decision
    last_record = {
        col: scan_result.column(col)[-1].as_py()
        for col in scan_result.column_names
    }
    
    return {
        "application_id": last_record["application_id"],
        "customer_id": last_record["customer_id"],
        "loan_type": last_record["loan_type"],
        "loan_amount": float(last_record["loan_amount"]),
        "credit_score_at_decision": last_record["credit_score"],
        "risk_category": last_record["risk_category"],
        "decision": last_record["decision"],
        "data_used_timestamp": last_record["data_snapshot_timestamp"],
        "audit_note": "This data was used for the loan decision as per regulatory requirements"
    }

# Perform audit for LOAN-002
audit_result = audit_loan_decision(
    loan_table,
    "LOAN-002",
    datetime(2026, 8, 22, 9, 0, 0)
)

print("\n=== REGULATORY AUDIT REPORT ===")
print(json.dumps(audit_result, indent=2, default=str))

# ============================================================
# STEP 6: Track Data Lineage Across Snapshots
# ============================================================

def track_data_lineage(loan_table: Table, application_id: str) -> list:
    """
    Track how data changed across all snapshots for an application.
    Complete audit trail for regulatory compliance.
    """
    lineage = []
    
    # Get all snapshots
    snapshots = loan_table.metadata.snapshots
    
    for snapshot in snapshots:
        # Scan table at each snapshot
        scan_result = loan_table.scan(
            row_filter=f"application_id = '{application_id}'"
        ).to_arrow()
        
        if len(scan_result) > 0:
            record = {
                "snapshot_id": snapshot.snapshot_id,
                "timestamp": datetime.fromtimestamp(snapshot.timestamp_ms / 1000),
                "operation": snapshot.operation,
                "data": {
                    col: scan_result.column(col)[0].as_py()
                    for col in scan_result.column_names
                }
            }
            lineage.append(record)
    
    return lineage

# Get complete lineage for LOAN-002
lineage = track_data_lineage(loan_table, "LOAN-002")

print("\n=== DATA LINEAGE TRAIL ===")
for i, entry in enumerate(lineage, 1):
    print(f"\nSnapshot {i}: {entry['timestamp']}")
    print(f"  Operation: {entry['operation']}")
    print(f"  Credit Score: {entry['data']['credit_score']}")
    print(f"  Decision: {entry['data']['decision']}")

# ============================================================
# STEP 7: Generate Compliance Report
# ============================================================

def generate_compliance_report(loan_table: Table) -> dict:
    """
    Generate compliance report showing data integrity.
    """
    # Get table metadata
    metadata = loan_table.metadata
    
    report = {
        "table_name": str(loan_table identifier),
        "current_snapshot": metadata.current_snapshot_id,
        "total_snapshots": len(metadata.snapshots),
        "total_data_files": sum(
            len(manifest.files()) 
            for manifest in metadata.current_snapshot().manifest_list.manifests()
        ),
        "schema_version": metadata.last_column_id(),
        "partition_spec": str(metadata.specs()),
        "compliance_status": "COMPLIANT",
        "audit_trail_available": True,
        "time_travel_capable": True,
        "generated_at": datetime.now().isoformat()
    }
    
    return report

# Generate and display compliance report
compliance_report = generate_compliance_report(loan_table)
print("\n=== COMPLIANCE REPORT ===")
print(json.dumps(compliance_report, indent=2, default=str))
```

---

## 🎯 5 Real-World Interview Questions

### Question 1: What is Apache Iceberg and why was it created?

**Answer:**
Apache Iceberg is an open-source table format for huge analytic datasets. It was created to solve the limitations of plain data lakes, including:
- **No ACID transactions**: Plain Parquet files don't support atomic commits
- **No schema evolution**: Changing schemas required rewriting all files
- **No time travel**: Can't query historical states
- **No concurrent write safety**: Multiple writers can corrupt data
- **No metadata management**: No efficient way to track which files belong to a table

Iceberg provides a metadata layer that brings database-like capabilities to data stored in files like Parquet.

---

### Question 2: How does Iceberg differ from a traditional database?

**Answer:**
| Aspect | Traditional Database | Apache Iceberg |
|--------|---------------------|----------------|
| Storage | Integrated | Separated (Parquet on S3) |
| Compute | Tightly coupled | Decoupled (Spark, Trino, DuckDB) |
| Schema | Fixed | Evolvable |
| Time Travel | Limited (binlog) | Full snapshot history |
| Concurrency | Row-level locks | Optimistic concurrency |
| Scale | Vertical | Horizontal (object storage) |

Iceberg is a **table format**, not a database. It manages metadata and transactions for files stored in object storage, while traditional databases manage both metadata and data in integrated systems.

---

### Question 3: What are the main components of an Iceberg table?

**Answer:**
An Iceberg table has four main components:

1. **Catalog**: Maps table names to metadata locations
2. **Table Metadata**: Contains schema, partition spec, current snapshot ID
3. **Snapshots**: Immutable records of table state at a point in time
4. **Manifests**: Lists of data files with statistics for each snapshot

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
Data Files (Parquet)
```

Each component serves a specific purpose in enabling efficient queries, time travel, and ACID transactions.

---

### Question 4: When would you choose Iceberg over Delta Lake?

**Answer:**
Choose Iceberg when:
- **Multi-engine support is critical**: Iceberg works with Spark, Trino, Flink, DuckDB; Delta is Spark-native
- **Partition evolution is needed**: Iceberg supports changing partition strategies without rewriting data
- **Open ecosystem is preferred**: Iceberg is vendor-neutral with broad industry adoption
- **Hidden partitioning is important**: Users shouldn't need to know physical layout

Choose Delta Lake when:
- **Spark-only environment**: Delta is optimized for Spark
- **Streaming is primary use case**: Delta has tighter Spark Structured Streaming integration
- **Databricks ecosystem**: If using Databricks, Delta is native

---

### Question 5: Explain the metadata hierarchy in Iceberg and why it matters.

**Answer:**
The metadata hierarchy is:

```
Catalog → Table Metadata → Snapshot → Manifest List → Manifest → Data Files
```

**Why it matters:**

1. **Efficient Query Planning**: Instead of scanning all files, Iceberg uses metadata to identify relevant files
2. **File Statistics**: Manifests contain min/max statistics enabling data file pruning
3. **Time Travel**: Each snapshot points to its own manifest list, enabling historical queries
4. **ACID Transactions**: New snapshots are atomically committed via metadata updates
5. **Concurrency Control**: Optimistic concurrency compares metadata to detect conflicts

Example: Querying `WHERE date = '2026-08-24'` might only read 5 out of 1000 files because metadata statistics eliminate irrelevant files.

---

## 📝 Summary

| Aspect | Key Point |
|--------|-----------|
| **What** | Open table format for analytic datasets |
| **Why** | Brings ACID, schema evolution, time travel to data lakes |
| **Components** | Catalog, Metadata, Snapshots, Manifests, Data Files |
| **Storage** | Object storage (S3, GCS, ADLS) |
| **Compute** | Spark, Trino, Flink, DuckDB |
| **Key Feature** | Separates metadata from data for efficiency |
