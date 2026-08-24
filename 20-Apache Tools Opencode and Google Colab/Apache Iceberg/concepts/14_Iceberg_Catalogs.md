# Concept 14: Iceberg Catalogs

## 📚 Detailed Explanation

An **Iceberg Catalog** is the entry point that maps table names to their metadata locations. It's the first layer in the Iceberg architecture and is critical for table management.

### What is a Catalog?

The catalog answers the question:
> "Where is this table's metadata?"

**Without Catalog:**
```
Query Engine: "Give me table 'banking.transactions'"
  → Where is it?
  → How do I find metadata?
  → Multiple engines need to coordinate?
```

**With Catalog:**
```
Query Engine: "Give me table 'banking.transactions'"
  → Catalog: "Here's the metadata location: s3://lake/transactions/metadata/v1.json"
  → Query Engine: Reads metadata → Reads data
```

### Catalog Responsibilities

1. **Namespace Management**: Organize tables (database.schema.table)
2. **Metadata Routing**: Map table names to metadata files
3. **Transaction Coordination**: Serialize concurrent writes
4. **Security**: Central point for access control
5. **Engine Interoperability**: Different engines share same catalog

### Catalog Types

| Catalog Type | Description | Use Case |
|--------------|-------------|----------|
| **REST** | HTTP-based, engine-agnostic | Multi-engine environments |
| **Hive** | Uses Hive Metastore | Existing Hive deployments |
| **JDBC** | Stores in relational DB | Simple deployments |
| **Glue** | AWS managed service | AWS environments |
| **Hadoop** | File-based | Testing/development |
| **Nessie** | Git-like versioning | Data versioning |

### Catalog Architecture

```
Spark / Trino / Flink / DuckDB
           │
           ▼
       Catalog
           │
           ▼
   Table Metadata Location
           │
           ▼
   Iceberg Metadata (S3/GCS/ADLS)
```

---

## 💡 Example: Catalog in Banking

### Scenario: Multi-Engine Access

**Engine Access:**
- **Spark**: ETL jobs (batch processing)
- **Trino**: Ad-hoc queries (interactive)
- **Flink**: Streaming (real-time)
- **DuckDB**: Local analysis

**Without Catalog:**
- Each engine needs to know metadata locations
- No coordination between engines
- Complex configuration

**With REST Catalog:**
- All engines connect to same catalog
- Automatic metadata routing
- Coordinated transactions

---

## 🏦 Real-World Banking Scenario 1: Multi-Engine Analytics Platform

### Scenario
A bank's **analytics platform** uses multiple engines:
- **Spark**: Daily ETL jobs
- **Trino**: BI dashboards
- **Flink**: Real-time analytics
- **DuckDB**: Data science exploration

All engines need to access the same Iceberg tables.

### Problem
- Each engine has different catalog requirements
- Need centralized table management
- Must coordinate concurrent writes

### Solution
REST Catalog provides:
- Engine-agnostic access
- Centralized metadata
- Transaction coordination

### Python Code

```python
"""
Banking Scenario 1: Multi-Engine Analytics Platform
Using Iceberg REST Catalog
"""

from pyiceberg.catalog import load_catalog
from pyiceberg.table import Table
from datetime import datetime
import pyarrow as pa
import random

# ============================================================
# STEP 1: Setup REST Catalog
# ============================================================

print("=== MULTI-ENGINE ANALYTICS PLATFORM ===\n")

# Load REST Catalog
catalog = load_catalog(
    "banking_analytics",
    **{
        "uri": "http://catalog-service:8181",
        "warehouse": "s3a://banking-analytics-lakehouse/"
    }
)

print("Connected to REST Catalog:")
print(f"  URI: http://catalog-service:8181")
print(f"  Warehouse: s3a://banking-analytics-lakehouse/")

# ============================================================
# STEP 2: Create Namespaces and Tables
# ============================================================

print("\n--- Creating Namespaces and Tables ---")

# Create namespaces (databases)
catalog.create_namespace("bronze")
catalog.create_namespace("silver")
catalog.create_namespace("gold")

print("Created namespaces: bronze, silver, gold")

# Define schemas
transaction_schema = pa.schema([
    pa.field("transaction_id", pa.string(), nullable=False),
    pa.field("account_id", pa.string(), nullable=False),
    pa.field("amount", pa.decimal128(18, 2), nullable=False),
    pa.field("transaction_date", pa.date32(), nullable=False),
    pa.field("status", pa.string(), nullable=False),
])

# Create tables
tables = {
    "bronze.transactions_raw": None,
    "silver.transactions_clean": None,
    "gold.transactions_daily": None,
}

for table_id in tables.keys():
    try:
        table = catalog.create_table(
            identifier=table_id,
            schema=transaction_schema
        )
        tables[table_id] = table
        print(f"Created table: {table_id}")
    except Exception as e:
        table = catalog.load_table(table_id)
        tables[table_id] = table
        print(f"Loaded table: {table_id}")

# ============================================================
# STEP 3: Simulate Spark ETL Job
# ============================================================

print("\n--- Simulating Spark ETL Job ---")

def spark_etl_job(catalog, table_id: str) -> dict:
    """
    Simulate Spark ETL job writing to Iceberg.
    In production, this would be a Spark application.
    """
    start_time = datetime.now()
    
    # Load table from catalog
    table = catalog.load_table(table_id)
    
    # Generate data
    data = pa.table({
        "transaction_id": [f"TXN-{i:08d}" for i in range(1, 1001)],
        "account_id": [f"ACC-{1000 + (i % 100):06d}" for i in range(1000)],
        "amount": [round(random.uniform(100, 50000), 2) for _ in range(1000)],
        "transaction_date": [datetime(2026, 8, 24).date()] * 1000,
        "status": ["COMPLETED"] * 1000,
    })
    
    # Append to table
    table.append(data)
    
    return {
        "engine": "Spark",
        "table": table_id,
        "records": len(data),
        "snapshot_id": table.metadata.current_snapshot_id,
        "duration": (datetime.now() - start_time).total_seconds()
    }

# Run ETL job
spark_result = spark_etl_job(catalog, "bronze.transactions_raw")
print(f"\nSpark ETL Result:")
print(f"  Engine: {spark_result['engine']}")
print(f"  Table: {spark_result['table']}")
print(f"  Records: {spark_result['records']}")
print(f"  Snapshot ID: {spark_result['snapshot_id']}")

# ============================================================
# STEP 4: Simulate Trino BI Query
# ============================================================

print("\n--- Simulating Trino BI Query ---")

def trino_bi_query(catalog, table_id: str) -> dict:
    """
    Simulate Trino BI query reading from Iceberg.
    In production, this would be a Trino query.
    """
    start_time = datetime.now()
    
    # Load table from catalog
    table = catalog.load_table(table_id)
    
    # Query table
    result = table.scan().to_arrow()
    
    return {
        "engine": "Trino",
        "table": table_id,
        "rows": len(result),
        "duration": (datetime.now() - start_time).total_seconds()
    }

# Run BI query
trino_result = trino_bi_query(catalog, "bronze.transactions_raw")
print(f"\nTrino BI Query Result:")
print(f"  Engine: {trino_result['engine']}")
print(f"  Table: {trino_result['table']}")
print(f"  Rows: {trino_result['rows']}")

# ============================================================
# STEP 5: Simulate Flink Streaming Job
# ============================================================

print("\n--- Simulating Flink Streaming Job ---")

def flink_streaming_job(catalog, table_id: str) -> dict:
    """
    Simulate Flink streaming job writing to Iceberg.
    In production, this would be a Flink application.
    """
    start_time = datetime.now()
    
    # Load table from catalog
    table = catalog.load_table(table_id)
    
    # Simulate streaming batch
    data = pa.table({
        "transaction_id": [f"TXN-STREAM-{i:06d}" for i in range(1, 501)],
        "account_id": [f"ACC-{1000 + (i % 50):06d}" for i in range(500)],
        "amount": [round(random.uniform(10, 5000), 2) for _ in range(500)],
        "transaction_date": [datetime(2026, 8, 24).date()] * 500,
        "status": ["COMPLETED"] * 500,
    })
    
    # Append to table
    table.append(data)
    
    return {
        "engine": "Flink",
        "table": table_id,
        "records": len(data),
        "snapshot_id": table.metadata.current_snapshot_id,
        "duration": (datetime.now() - start_time).total_seconds()
    }

# Run streaming job
flink_result = flink_streaming_job(catalog, "bronze.transactions_raw")
print(f"\nFlink Streaming Result:")
print(f"  Engine: {flink_result['engine']}")
print(f"  Table: {flink_result['table']}")
print(f"  Records: {flink_result['records']}")
print(f"  Snapshot ID: {flink_result['snapshot_id']}")

# ============================================================
# STEP 6: Simulate DuckDB Local Analysis
# ============================================================

print("\n--- Simulating DuckDB Local Analysis ---")

def duckdb_analysis(catalog, table_id: str) -> dict:
    """
    Simulate DuckDB local analysis reading from Iceberg.
    In production, this would be a DuckDB query.
    """
    start_time = datetime.now()
    
    # Load table from catalog
    table = catalog.load_table(table_id)
    
    # Query table
    result = table.scan().to_arrow()
    
    return {
        "engine": "DuckDB",
        "table": table_id,
        "rows": len(result),
        "duration": (datetime.now() - start_time).total_seconds()
    }

# Run DuckDB analysis
duckdb_result = duckdb_analysis(catalog, "bronze.transactions_raw")
print(f"\nDuckDB Analysis Result:")
print(f"  Engine: {duckdb_result['engine']}")
print(f"  Table: {duckdb_result['table']}")
print(f"  Rows: {duckdb_result['rows']}")

# ============================================================
# STEP 7: Catalog Benefits
# ============================================================

print("\n--- Catalog Benefits ---")

print("""
REST CATALOG BENEFITS:

1. ENGINE AGNOSTIC
   - Same catalog for Spark, Trino, Flink, DuckDB
   - No engine-specific configuration
   - Unified access

2. CENTRALIZED MANAGEMENT
   - Single source of truth
   - Consistent metadata
   - Coordinated transactions

3. SECURITY
   - Central access control
   - Authentication/authorization
   - Audit logging

4. FLEXIBILITY
   - HTTP-based interface
   - Easy integration
   - Scalable

5. OPERATIONAL EFFICIENCY
   - Simplified deployment
   - Reduced complexity
   - Easier monitoring

ARCHITECTURE:
  Spark ─┐
  Trino ─┼─► REST Catalog ─► Iceberg Metadata ─► Object Storage
  Flink ─┤
  DuckDB─┘
""")
```

---

## 🏦 Real-World Banking Scenario 2: Data Governance with Catalog

### Scenario
A bank needs **data governance** for regulatory compliance:
- Track table lineage
- Control access
- Audit changes
- Manage retention

### Problem
- No centralized governance
- Difficult to track changes
- Complex access control

### Solution
Catalog provides governance features:
- Table metadata tracking
- Access control
- Audit trail
- Retention policies

### Python Code

```python
"""
Banking Scenario 2: Data Governance with Catalog
Using Iceberg Catalog for Compliance
"""

from pyiceberg.catalog import load_catalog
from pyiceberg.table import Table
from datetime import datetime
import pyarrow as pa
import random

# ============================================================
# STEP 1: Setup Catalog with Governance
# ============================================================

print("=== DATA GOVERNANCE WITH CATALOG ===\n")

catalog = load_catalog(
    "banking_governance",
    **{
        "uri": "http://catalog-service:8181",
        "warehouse": "s3a://banking-governance-lakehouse/"
    }
)

# ============================================================
# STEP 2: Create Governed Tables
# ============================================================

print("--- Creating Governed Tables ---")

# Transaction table with governance properties
transaction_schema = pa.schema([
    pa.field("transaction_id", pa.string(), nullable=False),
    pa.field("account_id", pa.string(), nullable=False),
    pa.field("amount", pa.decimal128(18, 2), nullable=False),
    pa.field("transaction_date", pa.date32(), nullable=False),
    pa.field("status", pa.string(), nullable=False),
])

try:
    governed_table = catalog.create_table(
        identifier="governance.transactions",
        schema=transaction_schema,
        properties={
            "owner": "data_engineering_team",
            "retention_days": "2555",  # 7 years
            "classification": "CONFIDENTIAL",
            "regulation": "BASEL_III",
            "created_by": "admin@bank.com",
            "created_at": datetime.now().isoformat(),
        }
    )
    print("Created governed table: governance.transactions")
except Exception:
    governed_table = catalog.load_table("governance.transactions")
    print("Loaded governed table: governance.transactions")

# ============================================================
# STEP 3: Load Data with Audit Trail
# ============================================================

print("\n--- Loading Data with Audit Trail ---")

def load_with_audit(table: Table, data: pa.Table, user: str) -> dict:
    """
    Load data with audit information.
    """
    start_time = datetime.now()
    
    # Append data
    table.append(data)
    
    return {
        "user": user,
        "timestamp": start_time.isoformat(),
        "records": len(data),
        "snapshot_id": table.metadata.current_snapshot_id,
        "operation": "INSERT"
    }

# Generate data
data = pa.table({
    "transaction_id": [f"TXN-{i:06d}" for i in range(1, 501)],
    "account_id": [f"ACC-{1000 + (i % 50):06d}" for i in range(500)],
    "amount": [round(random.uniform(100, 50000), 2) for _ in range(500)],
    "transaction_date": [datetime(2026, 8, 24).date()] * 500,
    "status": ["COMPLETED"] * 500,
})

# Load with audit
audit_record = load_with_audit(governed_table, data, "etl_user@bank.com")
print(f"\nAudit Record:")
print(f"  User: {audit_record['user']}")
print(f"  Timestamp: {audit_record['timestamp']}")
print(f"  Records: {audit_record['records']}")
print(f"  Snapshot ID: {audit_record['snapshot_id']}")

# ============================================================
# STEP 4: Track Table Lineage
# ============================================================

print("\n--- Tracking Table Lineage ---")

def track_lineage(table: Table) -> dict:
    """
    Track table lineage from metadata.
    """
    metadata = table.metadata
    
    lineage = {
        "table_name": str(table.identifier),
        "current_snapshot": metadata.current_snapshot_id,
        "total_snapshots": len(metadata.snapshots),
        "schema_version": metadata.last_column_id(),
        "properties": dict(metadata.properties),
        "snapshots": []
    }
    
    for snap in metadata.snapshots[-5:]:  # Last 5 snapshots
        lineage["snapshots"].append({
            "snapshot_id": snap.snapshot_id,
            "timestamp": datetime.fromtimestamp(snap.timestamp_ms / 1000).isoformat(),
            "operation": snap.operation,
            "summary": snap.summary
        })
    
    return lineage

# Track lineage
lineage = track_lineage(governed_table)
print(f"\nTable Lineage:")
print(f"  Table: {lineage['table_name']}")
print(f"  Current Snapshot: {lineage['current_snapshot']}")
print(f"  Total Snapshots: {lineage['total_snapshots']}")

# ============================================================
# STEP 5: Access Control
# ============================================================

print("\n--- Access Control ---")

def check_access(user: str, table: Table, operation: str) -> bool:
    """
    Check access control (simplified).
    In production, use catalog's access control.
    """
    # Simplified access control logic
    admin_users = ["admin@bank.com", "data_engineering_lead@bank.com"]
    read_users = ["analyst@bank.com", "bi_team@bank.com"]
    
    if operation == "WRITE":
        return user in admin_users
    elif operation == "READ":
        return user in admin_users or user in read_users
    return False

# Check access
users_to_check = [
    ("admin@bank.com", "WRITE"),
    ("analyst@bank.com", "READ"),
    ("analyst@bank.com", "WRITE"),
    ("external@partner.com", "READ"),
]

print("\nAccess Control Results:")
for user, operation in users_to_check:
    allowed = check_access(user, governed_table, operation)
    status = "✓ ALLOWED" if allowed else "✗ DENIED"
    print(f"  {user} ({operation}): {status}")

# ============================================================
# STEP 6: Data Retention
# ============================================================

print("\n--- Data Retention ---")

def check_retention(table: Table) -> dict:
    """
    Check data retention based on table properties.
    """
    properties = table.metadata.properties
    
    retention_days = int(properties.get("retention_days", 365))
    created_at = properties.get("created_at", datetime.now().isoformat())
    
    return {
        "retention_days": retention_days,
        "retention_years": retention_days / 365,
        "created_at": created_at,
        "compliance": "BASEL_III" if retention_days >= 2555 else "STANDARD"
    }

# Check retention
retention = check_retention(governed_table)
print(f"\nData Retention Policy:")
print(f"  Retention Period: {retention['retention_days']} days ({retention['retention_years']:.1f} years)")
print(f"  Compliance: {retention['compliance']}")

# ============================================================
# STEP 7: Governance Benefits
# ============================================================

print("\n--- Governance Benefits ---")

print("""
CATALOG GOVERNANCE BENEFITS:

1. TABLE LINEAGE
   - Track table creation
   - Monitor changes
   - Audit trail

2. ACCESS CONTROL
   - Role-based access
   - User authentication
   - Operation-level control

3. DATA RETENTION
   - Policy enforcement
   - Automatic cleanup
   - Compliance support

4. AUDIT TRAIL
   - User actions
   - Timestamps
   - Operation types

5. COMPLIANCE
   - Regulatory requirements
   - Data classification
   - Security policies

REGULATORY COMPLIANCE:
  ✓ BASEL_III: 7-year retention
  ✓ GDPR: Right to be forgotten
  ✓ SOX: Financial data audit
  ✓ HIPAA: Healthcare data protection
""")
```

---

## 🎯 5 Real-World Interview Questions

### Question 1: What is an Iceberg catalog and why is it important?

**Answer:**

**Catalog:**
- Entry point for table access
- Maps table names to metadata locations
- Manages namespaces and tables

**Importance:**
1. **Centralized Management**: Single source of truth
2. **Engine Interoperability**: Multiple engines share same catalog
3. **Transaction Coordination**: Serializes concurrent writes
4. **Security**: Central access control
5. **Governance**: Audit trail and lineage

**Example:**
```
Spark: "Give me banking.transactions"
Catalog: "Here's the metadata location: s3://..."
Query Engine: Reads metadata → Reads data
```

---

### Question 2: Compare different Iceberg catalog implementations.

**Answer:**

| Catalog | Description | Use Case |
|---------|-------------|----------|
| **REST** | HTTP-based, engine-agnostic | Multi-engine environments |
| **Hive** | Uses Hive Metastore | Existing Hive deployments |
| **JDBC** | Stores in relational DB | Simple deployments |
| **Glue** | AWS managed service | AWS environments |
| **Hadoop** | File-based | Testing/development |
| **Nessie** | Git-like versioning | Data versioning |

**REST Catalog Advantages:**
- Engine-agnostic
- HTTP-based (easy integration)
- Scalable
- No dependencies

**When to Use:**
- **REST**: Multi-engine, cloud environments
- **Hive**: Existing Hive infrastructure
- **Glue**: AWS-native environments
- **JDBC**: Simple, small deployments

---

### Question 3: How does a catalog handle concurrent writes?

**Answer:**

**Mechanism:**

1. **Optimistic Concurrency**: Assume conflicts rare
2. **Parent Snapshot Validation**: Check if parent changed
3. **Atomic Commit**: Metadata update is atomic
4. **Retry on Conflict**: Failed commits retry automatically

**Example:**
```
Job A: Read Snapshot 100 → Prepare → Commit → Snapshot 101 ✓
Job B: Read Snapshot 100 → Prepare → Commit → FAIL (parent changed)
Job B: Retry → Read Snapshot 101 → Commit → Snapshot 102 ✓
```

**Catalog Role:**
- Validates parent snapshot
- Ensures atomic commit
- Detects conflicts
- Manages retry logic

---

### Question 4: How do you secure an Iceberg catalog?

**Answer:**

**Security Measures:**

1. **Authentication**
   - User/password
   - Kerberos
   - OAuth2
   - API keys

2. **Authorization**
   - Role-based access control
   - Table-level permissions
   - Operation-level control

3. **Encryption**
   - Data at rest (S3 encryption)
   - Data in transit (TLS)
   - Metadata encryption

4. **Audit Logging**
   - User actions
   - Timestamps
   - Operation types

**Example:**
```python
# REST Catalog with authentication
catalog = load_catalog(
    "secure_catalog",
    **{
        "uri": "https://catalog.bank.com",
        "token": "bearer_token_here"
    }
)
```

---

### Question 5: How does catalog choice affect performance?

**Answer:**

**Performance Impact:**

1. **Metadata Access**
   - REST: Network latency
   - Hive: Metastore latency
   - JDBC: Database latency
   - Glue: AWS API latency

2. **Transaction Coordination**
   - All catalogs: Similar performance
   - REST: HTTP overhead (minimal)
   - Hive: Metastore overhead

3. **Scalability**
   - REST: Highly scalable
   - Hive: Limited by Metastore
   - JDBC: Limited by database
   - Glue: AWS-managed scalability

**Optimization:**
- Use caching
- Connection pooling
- Regional deployment
- Load balancing

**Example:**
```
REST Catalog:
  - Metadata access: 10ms
  - Transaction commit: 50ms
  - Scales horizontally

Hive Catalog:
  - Metadata access: 20ms
  - Transaction commit: 100ms
  - Limited by Metastore
```

---

## 📝 Summary

| Aspect | Key Point |
|--------|-----------|
| **Definition** | Entry point for table access |
| **Responsibilities** | Namespace, metadata routing, transactions, security |
| **Types** | REST, Hive, JDBC, Glue, Hadoop, Nessie |
| **Importance** | Centralized management, engine interoperability |
| **Security** | Authentication, authorization, encryption, audit |
| **Performance** | Metadata access, transaction coordination |
| **Use Case** | Multi-engine analytics, data governance |
