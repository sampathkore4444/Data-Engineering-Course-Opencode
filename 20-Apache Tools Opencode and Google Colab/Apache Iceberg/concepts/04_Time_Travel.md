# Concept 04: Time Travel

## 📚 Detailed Explanation

**Time Travel** in Apache Iceberg is the ability to query data as it existed at any point in the past. It's one of Iceberg's most powerful features, enabling historical queries, auditing, and recovery.

### How Time Travel Works

Every Iceberg snapshot is immutable and timestamped. Time travel uses these snapshots to reconstruct historical states:

```
Current Time (2026-08-24 12:00:00)
    │
    ▼
Snapshot 102 (12:00:00) ← CURRENT
    │
    ▼
Snapshot 101 (11:00:00) ← 1 hour ago
    │
    ▼
Snapshot 100 (10:00:00) ← 2 hours ago
```

**Querying at a specific time:**
```sql
SELECT * FROM transactions
FOR SYSTEM_TIME AS OF TIMESTAMP '2026-08-24 10:00:00';
```

Iceberg will:
1. Find the snapshot closest to (but not after) the requested time
2. Use that snapshot's manifest list
3. Read the data files from that snapshot
4. Return the historical state

### Time Travel SQL Syntax

**Spark:**
```sql
-- By timestamp
SELECT * FROM transactions
FOR SYSTEM_TIME AS OF '2026-08-24 10:00:00';

-- By snapshot ID
SELECT * FROM transactions
FOR SYSTEM_VERSION AS OF 100;

-- Delta Lake syntax (also supported by Iceberg)
SELECT * FROM transactions@v100;
SELECT * FROM transactions TIMESTAMP AS OF '2026-08-24 10:00:00';
```

**Trino:**
```sql
SELECT * FROM transactions
FOR SYSTEM_TIME AS OF TIMESTAMP '2026-08-24 10:00:00';
```

**DuckDB:**
```sql
SELECT * FROM transactions
FOR SYSTEM_TIME AS OF '2026-08-24 10:00:00';
```

### Time Travel Use Cases

| Use Case | Description |
|----------|-------------|
| **Audit** | Show data state at regulatory reporting time |
| **Recovery** | Restore to pre-failure state |
| **Debugging** | Investigate what data looked like before a bug |
| **Compliance** | Prove data integrity over time |
| **Testing** | Compare current vs historical results |
| **ML Training** | Train models on historical snapshots |

### Time Travel vs Database Snapshots

| Aspect | Database Snapshots | Iceberg Time Travel |
|--------|-------------------|---------------------|
| **Granularity** | Often hourly/daily | Per-transaction |
| **Storage** | Copy-on-write (expensive) | Metadata-only (cheap) |
| **Retention** | Limited (storage cost) | Configurable (days/weeks) |
| **Access** | Database-specific | Standard SQL |
| **Performance** | May be slow | Optimized (metadata pruning) |

---

## 💡 Example: Time Travel in Banking

### Scenario: Investigating a Data Issue

**Monday 9 AM**: Customer reports wrong balance
**Monday 10 AM**: Support team investigates
**Monday 11 AM**: Issue identified - ETL bug at 8 AM

**Without Time Travel:**
- Cannot see what balance was at 8 AM
- Must reconstruct from logs
- Time-consuming, error-prone

**With Time Travel:**
```sql
-- See balance at 8 AM (before bug)
SELECT balance FROM accounts
FOR SYSTEM_TIME AS OF '2026-08-24 08:00:00'
WHERE account_id = 'ACC-1001';

-- See balance at 9 AM (after bug)
SELECT balance FROM accounts
FOR SYSTEM_TIME AS OF '2026-08-24 09:00:00'
WHERE account_id = 'ACC-1001';

-- Compare
SELECT 
    a.balance as balance_8am,
    b.balance as balance_9am,
    b.balance - a.balance as difference
FROM 
    (SELECT balance FROM accounts FOR SYSTEM_TIME AS OF '2026-08-24 08:00:00' WHERE account_id = 'ACC-1001') a,
    (SELECT balance FROM accounts FOR SYSTEM_TIME AS OF '2026-08-24 09:00:00' WHERE account_id = 'ACC-1001') b;
```

---

## 🏦 Real-World Banking Scenario 1: ETL Bug Investigation

### Scenario
A bank's **daily interest calculation** ETL job runs at 2 AM. At 6 AM, the analytics team notices interest amounts are **10x higher than expected**. They need to:
1. Identify when the bug occurred
2. See the exact data state before the bug
3. Calculate the impact
4. Restore correct data

### Problem
- Bug discovered 4 hours after execution
- No way to see "before" state
- Manual reconstruction needed

### Solution
Iceberg time travel enables:
- **Pinpoint** the exact snapshot when bug occurred
- **Compare** before/after states
- **Calculate** impact precisely
- **Restore** to correct state

### Python Code

```python
"""
Banking Scenario 1: ETL Bug Investigation
Using Iceberg Time Travel for Debugging
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
# STEP 2: Define Account Schema
# ============================================================

account_schema = pa.schema([
    pa.field("account_id", pa.string(), nullable=False),
    pa.field("customer_id", pa.string(), nullable=False),
    pa.field("balance", pa.decimal128(18, 2), nullable=False),
    pa.field("interest_rate", pa.float64(), nullable=False),
    pa.field("interest_amount", pa.decimal128(18, 2), nullable=False),
    pa.field("last_updated", pa.timestamp("us"), nullable=False),
    pa.field("etl_batch_id", pa.string(), nullable=True),
])

# Create table
try:
    account_table = catalog.create_table(
        identifier="banking.account_balances",
        schema=account_schema
    )
except Exception:
    account_table = catalog.load_table("banking.account_balances")

# ============================================================
# STEP 3: Simulate Normal ETL Run (Before Bug)
# ============================================================

print("=== ETL BUG INVESTIGATION ===")
print("Simulating interest calculation ETL job...\n")

# Step 1: Normal ETL run at 2 AM (correct)
print("1. Normal ETL run at 2:00 AM (CORRECT)")

normal_data = pa.table({
    "account_id": ["ACC-1001", "ACC-1002", "ACC-1003"],
    "customer_id": ["CUST-1001", "CUST-1002", "CUST-1003"],
    "balance": [100000.00, 250000.00, 50000.00],
    "interest_rate": [0.04, 0.035, 0.045],
    "interest_amount": [4000.00, 8750.00, 2250.00],  # CORRECT: balance * rate / 12
    "last_updated": [datetime(2026, 8, 24, 2, 0, 0)] * 3,
    "etl_batch_id": ["BATCH-20260824-0200"] * 3,
})

account_table.append(normal_data)
snapshot_correct = account_table.metadata.current_snapshot_id
print(f"   Snapshot: {snapshot_correct}")
print(f"   Interest amounts: 4000, 8750, 2250 (CORRECT)")

# Step 2: Buggy ETL run at 3 AM (10x bug)
print("\n2. Buggy ETL run at 3:00 AM (BUG - 10x interest)")

buggy_data = pa.table({
    "account_id": ["ACC-1001", "ACC-1002", "ACC-1003"],
    "customer_id": ["CUST-1001", "CUST-1002", "CUST-1003"],
    "balance": [100000.00, 250000.00, 50000.00],
    "interest_rate": [0.04, 0.035, 0.045],
    "interest_amount": [40000.00, 87500.00, 22500.00],  # BUG: 10x too high
    "last_updated": [datetime(2026, 8, 24, 3, 0, 0)] * 3,
    "etl_batch_id": ["BATCH-20260824-0300-BUGGY"] * 3,
})

account_table.append(buggy_data)
snapshot_buggy = account_table.metadata.current_snapshot_id
print(f"   Snapshot: {snapshot_buggy}")
print(f"   Interest amounts: 40000, 87500, 22500 (BUGGY)")

# ============================================================
# STEP 4: Time Travel Investigation
# ============================================================

print("\n=== TIME TRAVEL INVESTIGATION ===")

# Step 1: Query current state (buggy)
print("3. Query current state (buggy data):")
current_data = account_table.scan().to_arrow()
print(f"   Current interest amounts:")
for i in range(len(current_data)):
    account = current_data.column("account_id")[i].as_py()
    interest = current_data.column("interest_amount")[i].as_py()
    print(f"     {account}: {interest}")

# Step 2: Query at 2 AM (correct data)
print("\n4. Time travel to 2:00 AM (correct data):")
# In practice, use time travel SQL
# Here we simulate by scanning with filters

# Get snapshot at 2 AM
snapshot_2am = None
for snap in account_table.metadata.snapshots:
    snap_time = datetime.fromtimestamp(snap.timestamp_ms / 1000)
    if snap_time.hour == 2:
        snapshot_2am = snap.snapshot_id
        break

if snapshot_2am:
    print(f"   Found snapshot at 2 AM: {snapshot_2am}")
    # In production: SELECT * FROM table FOR SYSTEM_VERSION AS OF {snapshot_2am}

# ============================================================
# STEP 5: Impact Analysis
# ============================================================

print("\n=== IMPACT ANALYSIS ===")

def calculate_impact(correct_data, buggy_data) -> dict:
    """Calculate impact of the bug."""
    
    impact = {
        "total_accounts_affected": 0,
        "total_overcharge": 0,
        "details": []
    }
    
    for i in range(len(correct_data)):
        account_id = correct_data.column("account_id")[i].as_py()
        correct_interest = float(correct_data.column("interest_amount")[i])
        buggy_interest = float(buggy_data.column("interest_amount")[i])
        
        overcharge = buggy_interest - correct_interest
        
        impact["details"].append({
            "account_id": account_id,
            "correct_interest": correct_interest,
            "buggy_interest": buggy_interest,
            "overcharge": overcharge
        })
        
        impact["total_accounts_affected"] += 1
        impact["total_overcharge"] += overcharge
    
    return impact

# Calculate impact
impact = calculate_impact(normal_data, buggy_data)

print("Impact Summary:")
print(f"  Accounts affected: {impact['total_accounts_affected']}")
print(f"  Total overcharge: ${impact['total_overcharge']:,.2f}")
print(f"\nDetailed breakdown:")
for detail in impact["details"]:
    print(f"  {detail['account_id']}: ${detail['overcharge']:,.2f} overcharged")

# ============================================================
# STEP 6: Generate Investigation Report
# ============================================================

print("\n=== INVESTIGATION REPORT ===")

def generate_investigation_report(table: Table) -> dict:
    """Generate investigation report for the ETL bug."""
    
    snapshots = table.metadata.snapshots
    
    report = {
        "investigation_id": "INV-2026-08-24-001",
        "incident_date": "2026-08-24",
        "bug_discovered_at": "2026-08-24 06:00:00",
        "bug_occurred_at": "2026-08-24 03:00:00",
        "root_cause": "Interest calculation multiplier bug in ETL code",
        "snapshots_analyzed": len(snapshots),
        "correct_snapshot": snapshot_correct,
        "buggy_snapshot": snapshot_buggy,
        "impact": impact,
        "remediation_steps": [
            "1. Restore to correct snapshot",
            "2. Fix ETL code",
            "3. Re-run ETL for affected date",
            "4. Verify corrected data",
            "5. Notify affected customers"
        ],
        "time_travel_query_used": """
            SELECT * FROM banking.account_balances
            FOR SYSTEM_TIME AS OF '2026-08-24 02:00:00'
        """
    }
    
    return report

report = generate_investigation_report(account_table)
print(json.dumps(report, indent=2, default=str))

# ============================================================
# STEP 7: Restoration Plan
# ============================================================

print("\n=== RESTORATION PLAN ===")

print("""
Restoration Steps:

1. IDENTIFY correct snapshot
   - Snapshot 100 (2:00 AM) - CORRECT
   - Snapshot 101 (3:00 AM) - BUGGY

2. OPTION A: Time travel query (for analysis)
   SELECT * FROM banking.account_balances
   FOR SYSTEM_VERSION AS OF 100;

3. OPTION B: Full restore (if needed)
   - Create new table from historical snapshot
   - Replace current table with restored data
   - Or use Iceberg's overwrite operation

4. VERIFY restored data
   - Compare with source system
   - Ensure no data loss
   - Check all accounts

5. RE-RUN ETL with fixed code
   - Apply corrected interest calculation
   - Create new snapshot
   - Validate results
""")
```

---

## 🏦 Real-World Banking Scenario 2: Regulatory Reporting Audit

### Scenario
A bank's **quarterly financial report** was submitted to regulators on July 15, 2026. On August 20, 2026, regulators request evidence that the data used for the report was accurate at submission time.

### Problem
- Need to prove data state on July 15
- No way to reconstruct exact data
- Risk of regulatory penalties

### Solution
Iceberg time travel provides:
- **Immutable evidence**: Exact data state on July 15
- **Timestamped proof**: When data was queried
- **Complete audit trail**: All changes since then

### Python Code

```python
"""
Banking Scenario 2: Regulatory Reporting Audit
Using Iceberg Time Travel for Compliance
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
# STEP 2: Define Financial Report Schema
# ============================================================

report_schema = pa.schema([
    pa.field("report_id", pa.string(), nullable=False),
    pa.field("report_date", pa.date32(), nullable=False),
    pa.field("total_deposits", pa.decimal128(18, 2), nullable=False),
    pa.field("total_loans", pa.decimal128(18, 2), nullable=False),
    pa.field("total_interest_income", pa.decimal128(18, 2), nullable=False),
    pa.field("total_interest_expense", pa.decimal128(18, 2), nullable=False),
    pa.field("net_interest_margin", pa.float64(), nullable=False),
    pa.field("non_performing_assets", pa.decimal128(18, 2), nullable=False),
    pa.field("capital_adequacy_ratio", pa.float64(), nullable=False),
    pa.field("generated_at", pa.timestamp("us"), nullable=False),
    pa.field("submitted_to_regulator", pa.boolean(), nullable=False),
])

# Create table
try:
    report_table = catalog.create_table(
        identifier="banking.quarterly_reports",
        schema=report_schema
    )
except Exception:
    report_table = catalog.load_table("banking.quarterly_reports")

# ============================================================
# STEP 3: Simulate Report Generation Over Time
# ============================================================

print("=== REGULATORY REPORT AUDIT ===")
print("Simulating quarterly report generation...\n")

# Generate multiple reports over time
reports = [
    {
        "report_id": "RPT-Q2-2026-001",
        "report_date": datetime(2026, 6, 30).date(),
        "total_deposits": 50000000000.00,  # 50 billion
        "total_loans": 35000000000.00,     # 35 billion
        "total_interest_income": 1750000000.00,  # 1.75 billion
        "total_interest_expense": 875000000.00,   # 875 million
        "net_interest_margin": 0.0175,  # 1.75%
        "non_performing_assets": 1750000000.00,   # 1.75 billion
        "capital_adequacy_ratio": 0.15,  # 15%
        "generated_at": datetime(2026, 7, 15, 10, 0, 0),
        "submitted_to_regulator": True,
    },
    {
        "report_id": "RPT-Q2-2026-002",
        "report_date": datetime(2026, 6, 30).date(),
        "total_deposits": 50500000000.00,  # Slightly different (correction)
        "total_loans": 35200000000.00,
        "total_interest_income": 1760000000.00,
        "total_interest_expense": 880000000.00,
        "net_interest_margin": 0.0176,
        "non_performing_assets": 1760000000.00,
        "capital_adequacy_ratio": 0.151,
        "generated_at": datetime(2026, 7, 16, 14, 0, 0),
        "submitted_to_regulator": False,  # Not submitted
    },
    {
        "report_id": "RPT-Q3-2026-001",
        "report_date": datetime(2026, 9, 30).date(),
        "total_deposits": 52000000000.00,
        "total_loans": 36000000000.00,
        "total_interest_income": 1800000000.00,
        "total_interest_expense": 900000000.00,
        "net_interest_margin": 0.0180,
        "non_performing_assets": 1800000000.00,
        "capital_adequacy_ratio": 0.152,
        "generated_at": datetime(2026, 10, 15, 10, 0, 0),
        "submitted_to_regulator": True,
    },
]

# Load reports
for report in reports:
    data = pa.table({
        "report_id": [report["report_id"]],
        "report_date": [report["report_date"]],
        "total_deposits": [report["total_deposits"]],
        "total_loans": [report["total_loans"]],
        "total_interest_income": [report["total_interest_income"]],
        "total_interest_expense": [report["total_interest_expense"]],
        "net_interest_margin": [report["net_interest_margin"]],
        "non_performing_assets": [report["non_performing_assets"]],
        "capital_adequacy_ratio": [report["capital_adequacy_ratio"]],
        "generated_at": [report["generated_at"]],
        "submitted_to_regulator": [report["submitted_to_regulator"]],
    })
    report_table.append(data)
    print(f"Loaded report: {report['report_id']} generated at {report['generated_at']}")

# ============================================================
# STEP 4: Regulatory Request Simulation
# ============================================================

print("\n=== REGULATORY REQUEST ===")

# Regulator requests evidence for Q2 2026 report
regulator_request = {
    "request_id": "REG-2026-08-20-001",
    "request_date": "2026-08-20",
    "report_period": "Q2-2026",
    "submission_date": "2026-07-15",
    "requested_by": "Central Bank - Compliance Division",
    "requirements": [
        "Evidence of data used for Q2 2026 financial report",
        "Proof of data accuracy at submission time",
        "Audit trail of any changes since submission"
    ]
}

print("Regulator Request:")
print(json.dumps(regulator_request, indent=2))

# ============================================================
# STEP 5: Time Travel Evidence Generation
# ============================================================

print("\n=== TIME TRAVEL EVIDENCE GENERATION ===")

def generate_regulatory_evidence(table: Table, submission_date: datetime) -> dict:
    """
    Generate evidence for regulators using time travel.
    """
    # Get all snapshots
    snapshots = table.metadata.snapshots
    
    # Find snapshot at submission time
    submission_snapshot = None
    for snap in snapshots:
        snap_time = datetime.fromtimestamp(snap.timestamp_ms / 1000)
        if snap_time <= submission_date:
            submission_snapshot = snap
    
    if not submission_snapshot:
        return {"error": "No snapshot found at submission time"}
    
    # Get data at that snapshot
    # In practice, use time travel SQL
    scan_result = table.scan(
        row_filter="submitted_to_regulator = true"
    ).to_arrow()
    
    # Find the specific report
    report_data = None
    for i in range(len(scan_result)):
        if scan_result.column("report_date")[i].as_py() == datetime(2026, 6, 30).date():
            report_data = {
                col: scan_result.column(col)[i].as_py()
                for col in scan_result.column_names
            }
            break
    
    evidence = {
        "evidence_id": f"EVID-{datetime.now().strftime('%Y%m%d-%H%M%S')}",
        "regulator_request_id": regulator_request["request_id"],
        "snapshot_used": submission_snapshot.snapshot_id,
        "snapshot_timestamp": datetime.fromtimestamp(
            submission_snapshot.timestamp_ms / 1000
        ).isoformat(),
        "data_at_submission": report_data,
        "time_travel_query": f"""
            SELECT * FROM banking.quarterly_reports
            FOR SYSTEM_VERSION AS OF {submission_snapshot.snapshot_id}
            WHERE report_date = '2026-06-30'
            AND submitted_to_regulator = true
        """,
        "integrity_verification": {
            "snapshot_immutable": True,
            "data_not_modified": True,
            "audit_trail_available": True
        }
    }
    
    return evidence

# Generate evidence
evidence = generate_regulatory_evidence(report_table, datetime(2026, 7, 15, 10, 0, 0))

print("Regulatory Evidence:")
print(json.dumps(evidence, indent=2, default=str))

# ============================================================
# STEP 6: Audit Trail Since Submission
# ============================================================

print("\n=== AUDIT TRAIL SINCE SUBMISSION ===")

def generate_audit_trail(table: Table, submission_date: datetime) -> dict:
    """
    Generate audit trail of changes since submission.
    """
    snapshots = table.metadata.snapshots
    
    changes_since_submission = []
    
    for snap in snapshots:
        snap_time = datetime.fromtimestamp(snap.timestamp_ms / 1000)
        if snap_time > submission_date:
            changes_since_submission.append({
                "snapshot_id": snap.snapshot_id,
                "timestamp": snap_time.isoformat(),
                "operation": snap.operation,
                "summary": snap.summary
            })
    
    audit_trail = {
        "submission_date": submission_date.isoformat(),
        "current_date": datetime.now().isoformat(),
        "total_changes": len(changes_since_submission),
        "changes": changes_since_submission,
        "conclusion": "No modifications to submitted report data since submission"
    }
    
    return audit_trail

# Generate audit trail
audit_trail = generate_audit_trail(report_table, datetime(2026, 7, 15, 10, 0, 0))

print("Audit Trail:")
print(json.dumps(audit_trail, indent=2, default=str))

# ============================================================
# STEP 7: Compliance Certification
# ============================================================

print("\n=== COMPLIANCE CERTIFICATION ===")

def generate_compliance_certificate(evidence: dict, audit_trail: dict) -> dict:
    """
    Generate compliance certificate for regulators.
    """
    certificate = {
        "certificate_id": f"CERT-{datetime.now().strftime('%Y%m%d-%H%M%S')}",
        "bank_name": "EXAMPLE BANK LTD",
        "regulation": "BASEL_III",
        "report_period": "Q2-2026",
        "submission_date": "2026-07-15",
        "evidence_snapshot_id": evidence["snapshot_used"],
        "snapshot_timestamp": evidence["snapshot_timestamp"],
        "data_integrity": "VERIFIED",
        "audit_trail_status": "COMPLETE",
        "changes_since_submission": audit_trail["total_changes"],
        "certification": {
            "certified_by": "Chief Compliance Officer",
            "certification_date": datetime.now().isoformat(),
            "statement": """
                This certificate confirms that the data used for the Q2 2026 
                financial report was accurate at the time of submission (July 15, 2026).
                The data has been retrieved using Apache Iceberg time travel feature,
                which provides immutable, timestamped snapshots of the data.
                No modifications have been made to the submitted data since submission.
            """
        },
        "technical_evidence": {
            "technology": "Apache Iceberg",
            "time_travel_capability": "Immutable snapshots with point-in-time queries",
            "snapshot_chain": "Complete and verifiable",
            "data_storage": "Object storage (S3) with Parquet format"
        }
    }
    
    return certificate

# Generate certificate
certificate = generate_compliance_certificate(evidence, audit_trail)

print("Compliance Certificate:")
print(json.dumps(certificate, indent=2, default=str))
```

---

## 🎯 5 Real-World Interview Questions

### Question 1: How does time travel work in Iceberg and what are its limitations?

**Answer:**

**How it works:**
1. Each snapshot is immutable and timestamped
2. Time travel queries find the snapshot closest to the requested time
3. Data is read from that snapshot's manifest list
4. Returns historical state without modifying current data

**SQL syntax:**
```sql
SELECT * FROM table
FOR SYSTEM_TIME AS OF '2026-08-24 10:00:00';
```

**Limitations:**
1. **Snapshot retention**: Old snapshots must be expired (storage cost)
2. **Precision**: Limited to snapshot granularity (not sub-second)
3. **Performance**: Historical queries may scan more files
4. **Storage**: Historical data files must be retained
5. **Catalog dependency**: Requires catalog support for time travel

---

### Question 2: Compare time travel in Iceberg vs traditional databases.

**Answer:**

| Aspect | Traditional DB | Iceberg Time Travel |
|--------|---------------|---------------------|
| **Mechanism** | Transaction logs/binlogs | Immutable snapshots |
| **Granularity** | Per-transaction | Per-batch/ETL |
| **Storage** | Copy-on-write (expensive) | Metadata-only (cheap) |
| **Retention** | Limited (hours/days) | Configurable (weeks/months) |
| **Access** | DB-specific | Standard SQL |
| **Performance** | May be slow | Optimized with metadata |
| **Cost** | High (duplicates data) | Low (reference-based) |

**Key Difference**: Iceberg uses **metadata references** instead of data copies, making time travel cheap and scalable.

---

### Question 3: What happens to time travel when snapshots are expired?

**Answer:**

**When snapshots are expired:**
- Historical data becomes inaccessible
- Time travel to that period fails
- Metadata files are cleaned up
- Orphan data files may be removed

**Example:**
```sql
-- Expire snapshots older than 30 days
CALL catalog.system.expire_snapshots(
    table => 'db.transactions',
    older_than => TIMESTAMP '2026-07-24 00:00:00'
);

-- Time travel to July 1 now fails
SELECT * FROM transactions
FOR SYSTEM_TIME AS OF '2026-07-01 00:00:00';
-- ERROR: Snapshot not found
```

**Best Practices:**
1. Keep snapshots for regulatory retention period
2. Archive old data to separate tables before expiration
3. Document snapshot expiration policy
4. Test time travel after expiration

---

### Question 4: How do you use time travel for debugging ETL issues?

**Answer:**

**Debugging Steps:**

1. **Identify the issue timestamp**
   ```sql
   -- Find when data changed unexpectedly
   SELECT snapshot_id, timestamp, operation
   FROM table_metadata
   WHERE timestamp BETWEEN '2026-08-24 02:00' AND '2026-08-24 04:00';
   ```

2. **Query before the issue**
   ```sql
   SELECT * FROM transactions
   FOR SYSTEM_TIME AS OF '2026-08-24 02:00:00'
   WHERE ...;
   ```

3. **Query after the issue**
   ```sql
   SELECT * FROM transactions
   FOR SYSTEM_TIME AS OF '2026-08-24 03:00:00'
   WHERE ...;
   ```

4. **Compare results**
   ```sql
   -- Find differences
   SELECT a.*, b.*
   FROM (
       SELECT * FROM transactions FOR SYSTEM_TIME AS OF '2026-08-24 02:00:00'
   ) a
   JOIN (
       SELECT * FROM transactions FOR SYSTEM_TIME AS OF '2026-08-24 03:00:00'
   ) b ON a.id = b.id
   WHERE a.value != b.value;
   ```

5. **Restore if needed**
   ```sql
   -- Restore to pre-issue state
   INSERT OVERWRITE TABLE transactions
   SELECT * FROM transactions
   FOR SYSTEM_VERSION AS OF {good_snapshot_id};
   ```

---

### Question 5: Can you use time travel for ML model training? Explain the benefits.

**Answer:**

**Yes, time travel is valuable for ML:**

**Benefits:**

1. **Reproducible Training**
   - Train on exact historical snapshot
   - Reproduce results years later
   - Compare model versions fairly

2. **Point-in-Time Features**
   - Use features as they were at prediction time
   - Avoid data leakage
   - Proper train/test split

3. **A/B Testing**
   - Train multiple models on different snapshots
   - Compare performance objectively
   - No data contamination

4. **Regulatory Compliance**
   - Prove model training data
   - Audit trail for model decisions
   - Explainable AI requirements

**Example:**
```python
# Train on snapshot from 6 months ago
training_data = spark.sql("""
    SELECT * FROM customer_features
    FOR SYSTEM_TIME AS OF '2026-02-24 00:00:00'
""")

# Features are exactly as they were
# No leakage from future data
model = train_model(training_data)

# Evaluate on current data
current_data = spark.sql("SELECT * FROM customer_features")
evaluate_model(model, current_data)
```

**Key Insight**: Time travel enables **point-in-time correctness** in ML, which is critical for production models.

---

## 📝 Summary

| Aspect | Key Point |
|--------|-----------|
| **Definition** | Query data as it existed at any point in time |
| **Mechanism** | Immutable snapshots with timestamps |
| **SQL** | `FOR SYSTEM_TIME AS OF` or `FOR SYSTEM_VERSION AS OF` |
| **Use Cases** | Audit, debugging, recovery, compliance, ML |
| **Limitations** | Snapshot retention, precision, performance |
| **Best Practice** | Keep snapshots for regulatory retention period |
| **Cost** | Low (metadata references, not data copies) |
| **Key Benefit** | Immutable, verifiable historical evidence |
