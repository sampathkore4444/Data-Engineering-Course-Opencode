# Concept 12: CDC with Iceberg

## 📚 Detailed Explanation

**Change Data Capture (CDC)** with Apache Iceberg enables real-time synchronization of data from operational databases (Oracle, MySQL, PostgreSQL) to analytical data lakes. This is a critical pattern for modern banking architectures.

### What is CDC?

CDC captures changes (INSERT, UPDATE, DELETE) from source systems and replicates them to target systems in real-time or near-real-time.

**Traditional ETL:**
```
Source DB → Batch ETL → Data Lake (nightly)
  - 24-hour latency
  - Complex reconciliation
  - Data drift issues
```

**CDC:**
```
Source DB → Debezium → Kafka → Flink → Iceberg (real-time)
  - Sub-second latency
  - Consistent data
  - No reconciliation needed
```

### CDC Components

| Component | Description | Example |
|-----------|-------------|---------|
| **Source Database** | Operational system | Oracle, MySQL, PostgreSQL |
| **CDC Tool** | Captures changes | Debezium, AWS DMS, GoldenGate |
| **Message Queue** | Streams changes | Kafka, Kinesis, Pulsar |
| **Stream Processor** | Processes changes | Flink, Spark Streaming |
| **Target Table** | Stores changes | Iceberg table |

### CDC Operations

| Operation | Description | Iceberg Action |
|-----------|-------------|----------------|
| **INSERT** | New row added | Append new file |
| **UPDATE** | Row modified | Write delta file (MoR) or new file (CoW) |
| **DELETE** | Row removed | Write delete file (MoR) or new file (CoW) |

### CDC Patterns

**1. Full Load + CDC**
```
Initial: Full table copy
Ongoing: CDC streaming changes
```

**2. Snapshot + CDC**
```
Initial: Iceberg snapshot
Ongoing: CDC streaming changes
```

**3. Hybrid**
```
Initial: Parallel full load + CDC
Ongoing: CDC only
```

---

## 💡 Example: CDC in Banking

### Scenario: Core Banking to Data Lake

**Source**: Oracle Core Banking System
**Target**: Iceberg Data Lake

**Flow:**
```
Oracle → Debezium → Kafka → Flink → Iceberg
  │         │         │       │        │
  │         │         │       │        └── transactions table
  │         │         │       └── Process CDC events
  │         │         └── Stream changes
  │         └── Capture changes
  └── Operational system
```

**Benefits:**
- Real-time analytics
- No batch windows
- Consistent data
- Audit trail

---

## 🏦 Real-World Banking Scenario 1: Core Banking CDC to Data Lake

### Scenario
A bank's **core banking system** (Oracle) processes **10 million transactions daily**. The analytics team needs real-time access to transaction data for:
- Real-time dashboards
- Fraud detection
- Regulatory reporting

### Problem
- Traditional ETL: 24-hour latency
- Batch processing: Complex reconciliation
- Data drift: Source-target inconsistencies

### Solution
CDC pipeline: Oracle → Debezium → Kafka → Flink → Iceberg

### Python Code

```python
"""
Banking Scenario 1: Core Banking CDC to Data Lake
Using Iceberg for Real-Time Analytics
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
# STEP 2: Define Transaction Schema (Target)
# ============================================================

print("=== CORE BANKING CDC TO DATA LAKE ===\n")

schema = pa.schema([
    pa.field("transaction_id", pa.string(), nullable=False),
    pa.field("account_id", pa.string(), nullable=False),
    pa.field("amount", pa.decimal128(18, 2), nullable=False),
    pa.field("transaction_type", pa.string(), nullable=False),
    pa.field("status", pa.string(), nullable=False),
    pa.field("transaction_time", pa.timestamp("us"), nullable=False),
    pa.field("cdc_operation", pa.string(), nullable=False),
    pa.field("cdc_timestamp", pa.timestamp("us"), nullable=False),
    pa.field("source_system", pa.string(), nullable=False),
])

# Create Iceberg table
try:
    cdc_table = catalog.create_table(
        identifier="banking.cdc_transactions",
        schema=schema,
        properties={
            "write.format.default": "parquet",
            "write.delete.mode": "merge-on-read",
            "write.update.mode": "merge-on-read",
        }
    )
except Exception:
    cdc_table = catalog.load_table("banking.cdc_transactions")

# ============================================================
# STEP 3: Simulate Source Database Changes
# ============================================================

print("--- Simulating Source Database (Oracle) ---")

def generate_oracle_changes(num_changes: int) -> pa.Table:
    """
    Simulate changes from Oracle core banking system.
    In production, Debezium captures these changes.
    """
    
    operations = ["INSERT", "UPDATE", "DELETE"]
    op_weights = [0.7, 0.2, 0.1]  # 70% inserts, 20% updates, 10% deletes
    
    data = {
        "transaction_id": [f"TXN-{random.randint(10000000, 99999999)}" for _ in range(num_changes)],
        "account_id": [f"ACC-{random.randint(1000, 9999):06d}" for _ in range(num_changes)],
        "amount": [round(random.uniform(100, 100000), 2) for _ in range(num_changes)],
        "transaction_type": [random.choice(["CREDIT", "DEBIT", "TRANSFER"]) for _ in range(num_changes)],
        "status": [random.choice(["PENDING", "COMPLETED", "DECLINED"]) for _ in range(num_changes)],
        "transaction_time": [
            datetime.now() - timedelta(minutes=random.randint(0, 60))
            for _ in range(num_changes)
        ],
        "cdc_operation": random.choices(operations, weights=op_weights, k=num_changes),
        "cdc_timestamp": [datetime.now()] * num_changes,
        "source_system": ["ORACLE_CORE_BANKING"] * num_changes,
    }
    
    return pa.table(data)

# Generate initial changes
print("Generating initial changes from Oracle...")
initial_changes = generate_oracle_changes(1000)
print(f"  Generated {len(initial_changes)} changes")

# ============================================================
# STEP 4: Debezium Kafka Topic Simulation
# ============================================================

print("\n--- Simulating Debezium Kafka Topic ---")

# In production, Debezium would:
# 1. Read Oracle redo logs
# 2. Capture changes
# 3. Publish to Kafka topics

# Kafka topics would be:
# - orcl.CORE_BANKING.TRANSACTIONS (INSERT)
# - orcl.CORE_BANKING.TRANSACTIONS (UPDATE)
# - orcl.CORE_BANKING.TRANSACTIONS (DELETE)

print("Kafka Topics:")
print("  - orcl.CORE_BANKING.TRANSACTIONS")
print("  - Format: Debezium JSON")
print("  - Partition: By transaction_id")

# ============================================================
# STEP 5: Flink Processing Simulation
# ============================================================

print("\n--- Simulating Flink Processing ---")

def flink_process_cdc_events(kafka_messages: pa.Table) -> pa.Table:
    """
    Simulate Flink processing CDC events.
    In production, Flink reads from Kafka and writes to Iceberg.
    """
    
    # In production, Flink would:
    # 1. Read from Kafka
    # 2. Deserialize CDC events
    # 3. Transform/enrich data
    # 4. Write to Iceberg
    
    # For simulation, we process the table directly
    processed = kafka_messages
    
    # Add processing metadata
    processed = processed.set_column(
        processed.schema.get_field_index("cdc_timestamp"),
        "cdc_timestamp",
        pa.array([datetime.now()] * len(processed))
    )
    
    return processed

# Process CDC events
print("Processing CDC events with Flink...")
processed_events = flink_process_cdc_events(initial_changes)
print(f"  Processed {len(processed_events)} events")

# ============================================================
# STEP 6: Write to Iceberg
# ============================================================

print("\n--- Writing to Iceberg ---")

# Write to Iceberg (atomic commit)
cdc_table.append(processed_events)
print(f"  Written {len(processed_events)} events to Iceberg")
print(f"  Snapshot ID: {cdc_table.metadata.current_snapshot_id}")

# ============================================================
# STEP 7: Continuous CDC Stream
# ============================================================

print("\n--- Simulating Continuous CDC Stream ---")

# Simulate continuous streaming
for batch_num in range(5):
    # Generate new changes
    new_changes = generate_oracle_changes(200)
    
    # Process with Flink
    processed = flink_process_cdc_events(new_changes)
    
    # Write to Iceberg
    cdc_table.append(processed)
    
    print(f"  Batch {batch_num + 1}: {len(processed)} events, Snapshot {cdc_table.metadata.current_snapshot_id}")
    time.sleep(0.1)  # Simulate streaming delay

# ============================================================
# STEP 8: Query CDC Data
# ============================================================

print("\n--- Querying CDC Data ---")

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

# Query 3: All DELETEs
deletes = cdc_table.scan(
    row_filter="cdc_operation = 'DELETE'"
).to_arrow()
print(f"Total DELETEs: {len(deletes)}")

# Query 4: Recent activity
recent = cdc_table.scan(
    row_filter="cdc_timestamp >= TIMESTAMP '2026-08-24 10:00:00'"
).to_arrow()
print(f"Recent activity (last hour): {len(recent)}")

# ============================================================
# STEP 9: CDC Benefits
# ============================================================

print("\n--- CDC Benefits ---")

print("""
CDC PIPELINE BENEFITS:

1. REAL-TIME DATA
   - Sub-second latency
   - No batch windows
   - Always fresh data

2. CONSISTENT DATA
   - No reconciliation needed
   - Source-target sync
   - Audit trail

3. OPERATIONAL EFFICIENCY
   - No complex ETL
   - Reduced batch processing
   - Lower maintenance

4. ANALYTICAL CAPABILITIES
   - Real-time dashboards
   - Fraud detection
   - Regulatory reporting

5. COST SAVINGS
   - Reduced batch infrastructure
   - Lower operational costs
   - Better resource utilization

ARCHITECTURE:
  Oracle → Debezium → Kafka → Flink → Iceberg
    │         │         │       │        │
    │         │         │       │        └── Data Lake
    │         │         │       └── Stream Processing
    │         │         └── Message Queue
    │         └── Change Capture
    └── Operational System
""")
```

---

## 🏦 Real-World Banking Scenario 2: Multi-Source CDC for Customer 360

### Scenario
A bank wants to build a **Customer 360 view** by combining data from multiple source systems:
- **Core Banking**: Account and transaction data
- **CRM**: Customer relationship data
- **Card System**: Card transaction data
- **Loan System**: Loan application data

### Problem
- Multiple source systems
- Different update frequencies
- Need unified customer view

### Solution
CDC from each source → Iceberg tables → Unified view

### Python Code

```python
"""
Banking Scenario 2: Multi-Source CDC for Customer 360
Using Iceberg for Unified Customer View
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
# STEP 2: Define Source Tables
# ============================================================

print("=== MULTI-SOURCE CDC FOR CUSTOMER 360 ===\n")

# Core Banking Schema
core_banking_schema = pa.schema([
    pa.field("account_id", pa.string(), nullable=False),
    pa.field("customer_id", pa.string(), nullable=False),
    pa.field("account_type", pa.string(), nullable=False),
    pa.field("balance", pa.decimal128(18, 2), nullable=False),
    pa.field("status", pa.string(), nullable=False),
    pa.field("cdc_operation", pa.string(), nullable=False),
    pa.field("cdc_timestamp", pa.timestamp("us"), nullable=False),
])

# CRM Schema
crm_schema = pa.schema([
    pa.field("customer_id", pa.string(), nullable=False),
    pa.field("customer_name", pa.string(), nullable=False),
    pa.field("email", pa.string(), nullable=True),
    pa.field("phone", pa.string(), nullable=True),
    pa.field("segment", pa.string(), nullable=True),
    pa.field("cdc_operation", pa.string(), nullable=False),
    pa.field("cdc_timestamp", pa.timestamp("us"), nullable=False),
])

# Card System Schema
card_schema = pa.schema([
    pa.field("card_id", pa.string(), nullable=False),
    pa.field("customer_id", pa.string(), nullable=False),
    pa.field("card_type", pa.string(), nullable=False),
    pa.field("credit_limit", pa.decimal128(18, 2), nullable=False),
    pa.field("status", pa.string(), nullable=False),
    pa.field("cdc_operation", pa.string(), nullable=False),
    pa.field("cdc_timestamp", pa.timestamp("us"), nullable=False),
])

# Loan System Schema
loan_schema = pa.schema([
    pa.field("loan_id", pa.string(), nullable=False),
    pa.field("customer_id", pa.string(), nullable=False),
    pa.field("loan_type", pa.string(), nullable=False),
    pa.field("loan_amount", pa.decimal128(18, 2), nullable=False),
    pa.field("status", pa.string(), nullable=False),
    pa.field("cdc_operation", pa.string(), nullable=False),
    pa.field("cdc_timestamp", pa.timestamp("us"), nullable=False),
])

# Create tables
tables = {}
for name, schema in [
    ("core_banking", core_banking_schema),
    ("crm", crm_schema),
    ("card_system", card_schema),
    ("loan_system", loan_schema)
]:
    try:
        table = catalog.create_table(
            identifier=f"banking.{name}",
            schema=schema
        )
        tables[name] = table
        print(f"Created table: banking.{name}")
    except Exception:
        table = catalog.load_table(f"banking.{name}")
        tables[name] = table
        print(f"Loaded table: banking.{name}")

# ============================================================
# STEP 3: Simulate CDC from Each Source
# ============================================================

print("\n--- Simulating CDC from Each Source ---")

# Core Banking CDC
core_data = pa.table({
    "account_id": [f"ACC-{i:06d}" for i in range(1, 101)],
    "customer_id": [f"CUST-{i:06d}" for i in range(1, 101)],
    "account_type": [random.choice(["SAVINGS", "CURRENT", "FIXED"]) for _ in range(100)],
    "balance": [round(random.uniform(1000, 1000000), 2) for _ in range(100)],
    "status": ["ACTIVE"] * 100,
    "cdc_operation": ["INSERT"] * 100,
    "cdc_timestamp": [datetime.now()] * 100,
})
tables["core_banking"].append(core_data)
print(f"  Core Banking: {len(core_data)} records")

# CRM CDC
crm_data = pa.table({
    "customer_id": [f"CUST-{i:06d}" for i in range(1, 101)],
    "customer_name": [f"Customer_{i}" for i in range(1, 101)],
    "email": [f"customer{i}@bank.com" for i in range(1, 101)],
    "phone": [f"+1-555-{i:04d}" for i in range(1, 101)],
    "segment": [random.choice(["PREMIUM", "STANDARD", "BASIC"]) for _ in range(100)],
    "cdc_operation": ["INSERT"] * 100,
    "cdc_timestamp": [datetime.now()] * 100,
})
tables["crm"].append(crm_data)
print(f"  CRM: {len(crm_data)} records")

# Card System CDC
card_data = pa.table({
    "card_id": [f"CARD-{i:06d}" for i in range(1, 81)],
    "customer_id": [f"CUST-{random.randint(1, 100):06d}" for _ in range(80)],
    "card_type": [random.choice(["VISA", "MASTERCARD", "AMEX"]) for _ in range(80)],
    "credit_limit": [round(random.uniform(1000, 100000), 2) for _ in range(80)],
    "status": ["ACTIVE"] * 80,
    "cdc_operation": ["INSERT"] * 80,
    "cdc_timestamp": [datetime.now()] * 80,
})
tables["card_system"].append(card_data)
print(f"  Card System: {len(card_data)} records")

# Loan System CDC
loan_data = pa.table({
    "loan_id": [f"LOAN-{i:06d}" for i in range(1, 61)],
    "customer_id": [f"CUST-{random.randint(1, 100):06d}" for _ in range(60)],
    "loan_type": [random.choice(["HOME", "PERSONAL", "BUSINESS"]) for _ in range(60)],
    "loan_amount": [round(random.uniform(10000, 1000000), 2) for _ in range(60)],
    "status": ["ACTIVE"] * 60,
    "cdc_operation": ["INSERT"] * 60,
    "cdc_timestamp": [datetime.now()] * 60,
})
tables["loan_system"].append(loan_data)
print(f"  Loan System: {len(loan_data)} records")

# ============================================================
# STEP 4: Create Unified Customer 360 View
# ============================================================

print("\n--- Creating Customer 360 View ---")

# Query each source
core_df = tables["core_banking"].scan().to_arrow()
crm_df = tables["crm"].scan().to_arrow()
card_df = tables["card_system"].scan().to_arrow()
loan_df = tables["loan_system"].scan().to_arrow()

# Create unified view (simulated with dictionaries)
customer_360 = {}

# Core Banking data
for i in range(len(core_df)):
    customer_id = core_df.column("customer_id")[i].as_py()
    if customer_id not in customer_360:
        customer_360[customer_id] = {
            "customer_id": customer_id,
            "accounts": [],
            "cards": [],
            "loans": [],
            "segment": None,
            "email": None,
        }
    customer_360[customer_id]["accounts"].append({
        "account_id": core_df.column("account_id")[i].as_py(),
        "balance": float(core_df.column("balance")[i]),
    })

# CRM data
for i in range(len(crm_df)):
    customer_id = crm_df.column("customer_id")[i].as_py()
    if customer_id in customer_360:
        customer_360[customer_id]["segment"] = crm_df.column("segment")[i].as_py()
        customer_360[customer_id]["email"] = crm_df.column("email")[i].as_py()

# Card data
for i in range(len(card_df)):
    customer_id = card_df.column("customer_id")[i].as_py()
    if customer_id in customer_360:
        customer_360[customer_id]["cards"].append({
            "card_id": card_df.column("card_id")[i].as_py(),
            "card_type": card_df.column("card_type")[i].as_py(),
        })

# Loan data
for i in range(len(loan_df)):
    customer_id = loan_df.column("customer_id")[i].as_py()
    if customer_id in customer_360:
        customer_360[customer_id]["loans"].append({
            "loan_id": loan_df.column("loan_id")[i].as_py(),
            "loan_type": loan_df.column("loan_type")[i].as_py(),
        })

print(f"Unified Customer 360 view created for {len(customer_360)} customers")

# ============================================================
# STEP 5: Query Customer 360
# ============================================================

print("\n--- Querying Customer 360 ---")

# Sample customer
sample_customer = "CUST-000001"
if sample_customer in customer_360:
    customer = customer_360[sample_customer]
    print(f"\nCustomer 360 View for {sample_customer}:")
    print(f"  Segment: {customer['segment']}")
    print(f"  Email: {customer['email']}")
    print(f"  Accounts: {len(customer['accounts'])}")
    print(f"  Cards: {len(customer['cards'])}")
    print(f"  Loans: {len(customer['loans'])}")

# ============================================================
# STEP 6: CDC Benefits for Customer 360
# ============================================================

print("\n--- CDC Benefits for Customer 360 ---")

print("""
MULTI-SOURCE CDC BENEFITS:

1. REAL-TIME UNIFIED VIEW
   - All sources synchronized
   - Always fresh data
   - No batch delays

2. CONSISTENT DATA
   - CDC ensures source-target sync
   - No reconciliation needed
   - Audit trail available

3. OPERATIONAL EFFICIENCY
   - No complex ETL
   - Reduced batch processing
   - Lower maintenance

4. ANALYTICAL CAPABILITIES
   - Real-time customer insights
   - Personalized recommendations
   - Fraud detection

5. COST SAVINGS
   - Reduced infrastructure
   - Lower operational costs
   - Better resource utilization

ARCHITECTURE:
  Oracle → Debezium → Kafka → Flink → Iceberg (Core Banking)
  MySQL  → Debezium → Kafka → Flink → Iceberg (CRM)
  Card   → Debezium → Kafka → Flink → Iceberg (Cards)
  Loan   → Debezium → Kafka → Flink → Iceberg (Loans)
                                    │
                                    ▼
                              Customer 360 View
""")
```

---

## 🎯 5 Real-World Interview Questions

### Question 1: What is CDC and why is it important for banking?

**Answer:**

**CDC (Change Data Capture):**
- Captures INSERT, UPDATE, DELETE from source systems
- Streams changes to target systems in real-time
- Enables real-time analytics and reporting

**Importance for Banking:**

1. **Real-Time Analytics**: Sub-second data availability
2. **Regulatory Compliance**: Audit trail, real-time reporting
3. **Fraud Detection**: Immediate pattern analysis
4. **Operational Efficiency**: No batch windows
5. **Customer Experience**: Real-time personalization

**Example:**
```
Traditional: Oracle → ETL → Data Lake (24-hour latency)
CDC: Oracle → Debezium → Kafka → Flink → Iceberg (sub-second)
```

---

### Question 2: How does Debezium work with Iceberg?

**Answer:**

**Architecture:**
```
Oracle/MySQL → Debezium → Kafka → Flink → Iceberg
```

**Debezium Role:**
- Reads database logs (redo/binlog)
- Captures changes
- Publishes to Kafka topics

**Flink Role:**
- Reads from Kafka
- Transforms/enriches data
- Writes to Iceberg

**Iceberg Role:**
- Stores changes efficiently
- Provides ACID transactions
- Enables time travel

**Example Flow:**
1. Oracle inserts row → Debezium captures → Kafka topic
2. Flink reads Kafka → Transforms → Writes to Iceberg
3. Iceberg creates new snapshot → Available for queries

---

### Question 3: How do you handle DELETE operations in CDC with Iceberg?

**Answer:**

**DELETE Handling:**

**Copy-on-Write (CoW):**
- Read affected file
- Remove deleted rows
- Write new file
- Update metadata

**Merge-on-Read (MoR):**
- Write delete file
- Original file unchanged
- On read, merge with delete file

**Example:**
```
DELETE FROM transactions WHERE id = 123;

CoW:
  1. Read file containing id=123
  2. Remove row
  3. Write new file
  4. Update metadata

MoR:
  1. Write delete file for id=123
  2. Original file unchanged
  3. Query merges at read time
```

**Best Practice:**
- Use MoR for high-volume deletes
- Compact periodically
- Monitor delete file count

---

### Question 4: How do you ensure exactly-once delivery in CDC?

**Answer:**

**Exactly-Once Delivery:**

1. **Idempotent Writes**
   - Write same data multiple times → Same result
   - Use transaction IDs
   - Deduplicate at target

2. **Checkpointing**
   - Flink checkpoints Kafka offsets
   - Restart from last checkpoint
   - No data loss/duplication

3. **Transactional Writes**
   - Iceberg atomic commits
   - All-or-nothing writes
   - Consistent state

**Example:**
```
Flink Job:
  1. Read batch from Kafka
  2. Process events
  3. Write to Iceberg (atomic commit)
  4. Checkpoint Kafka offset
  5. If failure: Restart from checkpoint
```

**Key Points:**
- Idempotent operations
- Checkpointing
- Atomic commits
- Deduplication

---

### Question 5: What are the challenges of CDC in banking?

**Answer:**

**Challenges:**

1. **Data Volume**
   - Millions of transactions daily
   - High throughput requirements
   - Storage costs

2. **Latency**
   - Sub-second requirements
   - Network delays
   - Processing overhead

3. **Consistency**
   - Source-target sync
   - Schema evolution
   - Data quality

4. **Reliability**
   - Failure handling
   - Recovery mechanisms
   - Monitoring

5. **Security**
   - Data encryption
   - Access control
   - Compliance

**Solutions:**
- Partitioning for volume
- Optimization for latency
- Schema evolution for consistency
- Checkpointing for reliability
- Encryption for security

---

## 📝 Summary

| Aspect | Key Point |
|--------|-----------|
| **Definition** | Capture and stream changes from source systems |
| **Components** | Source DB, CDC tool, Kafka, Flink, Iceberg |
| **Operations** | INSERT, UPDATE, DELETE |
| **Benefits** | Real-time data, consistency, efficiency |
| **Challenges** | Volume, latency, consistency, reliability |
| **Use Cases** | Core banking, Customer 360, fraud detection |
