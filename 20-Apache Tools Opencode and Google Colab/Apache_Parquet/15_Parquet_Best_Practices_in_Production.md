# Parquet Best Practices in Production

## Table of Contents
1. [Detailed Explanation](#1-detailed-explanation)
2. [Example](#2-example)
3. [Real-World Banking Scenario 1](#3-banking-scenario-1-production-data-pipeline)
4. [Python Code - Scenario 1](#4-python-code---scenario-1)
5. [Real-World Banking Scenario 2](#5-banking-scenario-2-data-governance)
6. [Python Code - Scenario 2](#6-python-code---scenario-2)
7. [Interview Questions](#7-interview-questions)

---

## 1. Detailed Explanation

### Production Best Practices

> **Production Parquet requires careful attention to schema design, file organization, compression, monitoring, and governance. These practices ensure reliable, performant, and cost-effective data storage.**

### Schema Design Best Practices

```
DO:
  + Use explicit schemas (never infer)
  + Use appropriate types (DATE not STRING)
  + Use DECIMAL for financial amounts (not FLOAT)
  + Add metadata (created_by, version, description)
  + Keep schemas flat when possible

DON'T:
  - Use STRING for dates/numbers
  - Use FLOAT for financial amounts
  - Create deeply nested structures (>3 levels)
  - Use overly generic names (col1, col2)
  - Mix null and non-null types
```

### File Organization Best Practices

```
File Sizing:
  Optimal: 256MB - 1GB per file
  Too small: < 64MB (metadata overhead)
  Too large: > 2GB (poor parallelism)

Partitioning:
  Partition by: date, region, category
  Avoid: high-cardinality columns (user_id)
  Rule: Each partition should have > 100K rows

Naming:
  Good: transactions_2026_08_24_001.parquet
  Bad:  part-00000-abc123.parquet
```

### Compression Best Practices

```
Hot Data (frequent reads):
  Codec: Snappy or Zstd level 1-3
  Rationale: Fast decompression

Warm Data (occasional reads):
  Codec: Zstd level 3-6
  Rationale: Balanced speed and ratio

Cold Data (rare reads):
  Codec: Gzip level 9 or Zstd level 9
  Rationale: Maximum compression

Archival (compliance):
  Codec: Brotli or Gzip level 9
  Rationale: Minimal reads, maximum savings
```

### Metadata Best Practices

```python
# Always add metadata
metadata = {
    "created_by": "etl_pipeline_v2.1",
    "source_system": "core_banking",
    "data_owner": "data_engineering_team",
    "retention_days": 2555,  # 7 years
    "schema_version": "3.1",
    "description": "Daily transaction archive",
}

pq.write_table(
    table,
    "output.parquet",
    metadata=metadata,
)
```

### Monitoring Best Practices

```
Monitor:
  + File count per partition
  + Average file size
  + Compression ratio
  + Query performance (p50, p95, p99)
  + Storage growth rate
  + Write latency

Alert on:
  - File count > 10,000 per partition
  - Average file size < 64MB
  - Query latency > 30 seconds
  - Storage growth > 10% month-over-month
```

### Governance Best Practices

```
Data Lineage:
  + Track source → transformation → destination
  + Log ETL job runs and dependencies
  + Maintain data dictionary

Access Control:
  + Column-level security (sensitive data)
  + Row-level security (customer data)
  + Audit logging

Retention:
  + Define retention policies per data type
  + Automate deletion/archival
  + Comply with regulations (GDPR, SOX)
```

---

## 2. Example

### Production Parquet Configuration

```python
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
import os

# Define production schema
schema = pa.schema([
    ("transaction_id", pa.int64()),
    ("account_id", pa.string()),
    ("amount", pa.decimal128(18, 2)),
    ("currency", pa.string()),
    ("status", pa.string()),
    ("transaction_date", pa.date32()),
], metadata={
    "created_by": "etl_pipeline_v2.1",
    "source_system": "core_banking",
    "schema_version": "1.0",
    "retention_years": "7",
})

# Write with production settings
pq.write_table(
    table,
    "transactions.parquet",
    schema=schema,
    compression="zstd",
    compression_level=3,
    use_dictionary=True,
    write_statistics=True,
    data_page_size=1_048_576,
    version="2.6",
    metadata=schema.metadata,
)

# Verify metadata
metadata = pq.read_metadata("transactions.parquet")
print(f"Created by: {metadata.metadata[b'created_by']}")
print(f"Schema version: {metadata.metadata[b'schema_version']}")
```

---

## 3. Banking Scenario 1: Production Data Pipeline

### Problem
A bank needs a production-ready Parquet pipeline with:
- Schema validation
- Data quality checks
- Monitoring and alerting
- Cost optimization
- Compliance with regulations

### Requirements
- 99.9% uptime
- < 5 second query latency
- 7-year data retention
- SOC 2 compliance
- Cost optimization (storage budget)

### Architecture
```
Data Sources
       |
       v
  ETL Pipeline (with validation)
       |
       +-- Schema validation
       +-- Data quality checks
       +-- Type enforcement
       |
       v
  Parquet Writer (optimized)
       |
       +-- Zstd compression
       +-- Dictionary encoding
       +-- Statistics
       |
       v
  S3 Data Lake
       |
       +-- Lifecycle policies
       +-- Access control
       +-- Monitoring
       |
       v
  Analytics Layer
```

---

## 4. Python Code - Scenario 1

```python
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
import numpy as np
from datetime import datetime, timedelta
import random
import os
import tempfile
import json

# ============================================================
# BANKING SCENARIO: Production Data Pipeline
# ============================================================

class ProductionParquetPipeline:
    """Production-ready Parquet pipeline with validation and monitoring."""

    def __init__(self, base_path):
        self.base_path = base_path
        self.schema = self._define_schema()
        self.metrics = {
            "rows_written": 0,
            "files_written": 0,
            "validation_errors": 0,
            "start_time": datetime.now(),
        }

    def _define_schema(self):
        """Define production schema with metadata."""
        return pa.schema([
            ("transaction_id", pa.int64()),
            ("account_id", pa.string()),
            ("amount", pa.decimal128(18, 2)),
            ("currency", pa.string()),
            ("status", pa.string()),
            ("transaction_date", pa.date32()),
            ("created_at", pa.timestamp("us")),
        ], metadata={
            "created_by": "production_etl_v3.2",
            "source_system": "core_banking",
            "schema_version": "2.1",
            "data_owner": "data_engineering",
            "retention_years": "7",
            "last_updated": datetime.now().isoformat(),
        })

    def validate_data(self, table):
        """Validate data quality."""
        errors = []

        # Check for nulls in required columns
        required_columns = ["transaction_id", "account_id", "amount", "status"]
        for col_name in required_columns:
            col = table.column(col_name)
            null_count = pc.null_count(col).as_py()
            if null_count > 0:
                errors.append(f"Column {col_name} has {null_count} null values")

        # Check amount is positive
        amounts = table.column("amount")
        negative_mask = pc.less(amounts, pa.scalar(0))
        negative_count = pc.sum(negative_mask).as_py()
        if negative_count > 0:
            errors.append(f"Column amount has {negative_count} negative values")

        # Check status values
        valid_statuses = ["COMPLETED", "PENDING", "FAILED"]
        status_col = table.column("status")
        for status in valid_statuses:
            pass  # Would check each value

        self.metrics["validation_errors"] += len(errors)
        return errors

    def write_optimized(self, table, partition_col=None):
        """Write Parquet with production optimizations."""
        start_time = datetime.now()

        # Validate
        errors = self.validate_data(table)
        if errors:
            print(f"Validation errors: {errors}")
            return None

        # Write
        if partition_col:
            output_path = os.path.join(self.base_path, "partitioned")
            pq.write_to_dataset(
                table,
                root_path=output_path,
                partition_cols=[partition_col],
                compression="zstd",
                compression_level=3,
                use_dictionary=True,
                write_statistics=True,
                data_page_size=1_048_576,
                version="2.6",
            )
        else:
            output_path = os.path.join(self.base_path, f"data_{self.metrics['files_written']:06d}.parquet")
            pq.write_table(
                table,
                output_path,
                compression="zstd",
                compression_level=3,
                use_dictionary=True,
                write_statistics=True,
                schema=self.schema,
            )

        # Update metrics
        self.metrics["rows_written"] += table.num_rows
        self.metrics["files_written"] += 1

        elapsed = (datetime.now() - start_time).total_seconds()
        print(f"Written {table.num_rows:,} rows in {elapsed:.3f}s")

        return output_path

    def get_metrics(self):
        """Get pipeline metrics."""
        elapsed = (datetime.now() - self.metrics["start_time"]).total_seconds()
        return {
            **self.metrics,
            "elapsed_seconds": elapsed,
            "rows_per_second": self.metrics["rows_written"] / elapsed if elapsed > 0 else 0,
        }


def generate_production_data(num_rows=100_000):
    """Generate production-quality transaction data."""
    random.seed(42)
    np.random.seed(42)

    from decimal import Decimal

    table = pa.table({
        "transaction_id": pa.array(list(range(1, num_rows + 1)), type=pa.int64()),
        "account_id": pa.array([f"ACC{random.randint(100000, 999999)}" for _ in range(num_rows)], type=pa.string()),
        "amount": pa.array([Decimal(str(round(random.uniform(1.0, 100000.0), 2))) for _ in range(num_rows)], type=pa.decimal128(18, 2)),
        "currency": pa.array(np.random.choice(["USD", "EUR", "GBP"], num_rows), type=pa.string()),
        "status": pa.array(np.random.choice(["COMPLETED", "PENDING", "FAILED"], num_rows), type=pa.string()),
        "transaction_date": pa.array([
            (datetime(2026, 8, 1) + timedelta(days=random.randint(0, 30))).strftime("%Y-%m-%d")
            for _ in range(num_rows)
        ], type=pa.date32()),
        "created_at": pa.array([datetime.now()] * num_rows, type=pa.timestamp("us")),
    })

    return table


def run_production_pipeline():
    """Run production pipeline with monitoring."""
    base_path = os.path.join(tempfile.gettempdir(), "production_pipeline")
    os.makedirs(base_path, exist_ok=True)

    # Initialize pipeline
    pipeline = ProductionParquetPipeline(base_path)

    # Generate and write data
    print("Generating production data...")
    table = generate_production_data(num_rows=100_000)

    # Write with partitioning
    output_path = pipeline.write_optimized(table, partition_col="transaction_date")

    # Get metrics
    metrics = pipeline.get_metrics()

    print(f"\n=== Pipeline Metrics ===")
    print(f"Rows written: {metrics['rows_written']:,}")
    print(f"Files written: {metrics['files_written']}")
    print(f"Validation errors: {metrics['validation_errors']}")
    print(f"Elapsed time: {metrics['elapsed_seconds']:.3f}s")
    print(f"Throughput: {metrics['rows_per_second']:,.0f} rows/sec")

    return pipeline


# ============================================================
# RUN THE SCENARIO
# ============================================================
if __name__ == "__main__":
    run_production_pipeline()
```

---

## 5. Banking Scenario 2: Data Governance

### Problem
A bank needs to implement data governance for Parquet files:
- Data lineage tracking
- Access control
- Retention policies
- Audit logging
- Compliance reporting

### Requirements
- Track data from source to destination
- Control access by role
- Enforce 7-year retention
- Log all data access
- Generate compliance reports

### Architecture
```
Data Sources
       |
       v
  ETL Pipeline (with lineage)
       |
       +-- Source tracking
       +-- Transformation logging
       +-- Destination mapping
       |
       v
  Parquet Files (with metadata)
       |
       +-- Access control
       +-- Retention policies
       +-- Audit logging
       |
       v
  Governance Layer
       |
       +-- Lineage graph
       +-- Access logs
       +-- Compliance reports
```

---

## 6. Python Code - Scenario 2

```python
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
from datetime import datetime, timedelta
import random
import os
import tempfile
import json

# ============================================================
# BANKING SCENARIO: Data Governance
# ============================================================

class DataGovernanceManager:
    """Manage data governance for Parquet files."""

    def __init__(self, base_path):
        self.base_path = base_path
        self.lineage_file = os.path.join(base_path, "lineage.json")
        self.access_log_file = os.path.join(base_path, "access_log.json")
        self._init_files()

    def _init_files(self):
        """Initialize governance files."""
        if not os.path.exists(self.lineage_file):
            with open(self.lineage_file, "w") as f:
                json.dump([], f)

        if not os.path.exists(self.access_log_file):
            with open(self.access_log_file, "w") as f:
                json.dump([], f)

    def record_lineage(self, source, transformation, destination, metadata=None):
        """Record data lineage."""
        lineage_entry = {
            "timestamp": datetime.now().isoformat(),
            "source": source,
            "transformation": transformation,
            "destination": destination,
            "metadata": metadata or {},
        }

        with open(self.lineage_file, "r") as f:
            lineage = json.load(f)

        lineage.append(lineage_entry)

        with open(self.lineage_file, "w") as f:
            json.dump(lineage, f, indent=2)

        print(f"Recorded lineage: {source} → {destination}")

    def log_access(self, user, file_path, action, metadata=None):
        """Log data access."""
        access_entry = {
            "timestamp": datetime.now().isoformat(),
            "user": user,
            "file": file_path,
            "action": action,
            "metadata": metadata or {},
        }

        with open(self.access_log_file, "r") as f:
            access_log = json.load(f)

        access_log.append(access_entry)

        with open(self.access_log_file, "w") as f:
            json.dump(access_log, f, indent=2)

    def write_governed_parquet(self, table, output_path, data_owner, retention_years=7):
        """Write Parquet with governance metadata."""
        # Add governance metadata
        governance_metadata = {
            "created_by": "governed_pipeline",
            "data_owner": data_owner,
            "retention_years": str(retention_years),
            "classification": "CONFIDENTIAL",
            "created_at": datetime.now().isoformat(),
            "retention_expiry": (datetime.now() + timedelta(days=retention_years * 365)).isoformat(),
        }

        # Merge with existing metadata
        existing_metadata = table.schema.metadata or {}
        all_metadata = {**existing_metadata, **{k.encode(): v.encode() for k, v in governance_metadata.items()}}

        # Create new schema with metadata
        new_schema = table.schema.with_metadata(all_metadata)

        # Write
        pq.write_table(
            table,
            output_path,
            schema=new_schema,
            compression="zstd",
            use_dictionary=True,
            write_statistics=True,
        )

        # Record lineage
        self.record_lineage(
            source="core_banking.oracle.transactions",
            transformation="daily_etl_v2.1",
            destination=output_path,
            metadata=governance_metadata,
        )

        return output_path

    def generate_compliance_report(self):
        """Generate compliance report."""
        with open(self.lineage_file, "r") as f:
            lineage = json.load(f)

        with open(self.access_log_file, "r") as f:
            access_log = json.load(f)

        print(f"\n=== Compliance Report ===")
        print(f"Total lineage entries: {len(lineage)}")
        print(f"Total access logs: {len(access_log)}")

        # Unique sources
        sources = set(entry["source"] for entry in lineage)
        print(f"Unique data sources: {len(sources)}")

        # Unique users
        users = set(entry["user"] for entry in access_log)
        print(f"Unique users: {len(users)}")

        # Recent access
        recent_access = [e for e in access_log if (datetime.now() - datetime.fromisoformat(e["timestamp"])).days <= 30]
        print(f"Access in last 30 days: {len(recent_access)}")


def generate_governed_data(num_rows=50_000):
    """Generate data for governance demo."""
    random.seed(42)
    np.random.seed(42)

    from decimal import Decimal

    table = pa.table({
        "transaction_id": pa.array(list(range(1, num_rows + 1)), type=pa.int64()),
        "account_id": pa.array([f"ACC{random.randint(100000, 999999)}" for _ in range(num_rows)], type=pa.string()),
        "amount": pa.array([Decimal(str(round(random.uniform(1.0, 100000.0), 2))) for _ in range(num_rows)], type=pa.decimal128(18, 2)),
        "status": pa.array(np.random.choice(["COMPLETED", "PENDING", "FAILED"], num_rows), type=pa.string()),
        "transaction_date": pa.array([
            (datetime(2026, 8, 1) + timedelta(days=random.randint(0, 30))).strftime("%Y-%m-%d")
            for _ in range(num_rows)
        ], type=pa.date32()),
    })

    return table


def run_governance_demo():
    """Run data governance demo."""
    base_path = os.path.join(tempfile.gettempdir(), "governance_demo")
    os.makedirs(base_path, exist_ok=True)

    # Initialize governance manager
    manager = DataGovernanceManager(base_path)

    # Generate data
    print("Generating governed data...")
    table = generate_governed_data(num_rows=50_000)

    # Write with governance
    output_path = os.path.join(base_path, "governed_transactions.parquet")
    manager.write_governed_parquet(
        table,
        output_path,
        data_owner="data_engineering_team",
        retention_years=7,
    )

    # Log access
    manager.log_access("analyst_1", output_path, "READ", {"query": "SELECT * LIMIT 100"})
    manager.log_access("analyst_2", output_path, "READ", {"query": "SELECT SUM(amount)"})

    # Generate compliance report
    manager.generate_compliance_report()

    # Verify metadata
    written_table = pq.read_table(output_path)
    print(f"\n=== Metadata Verification ===")
    print(f"Schema metadata keys: {list(written_table.schema.metadata.keys())}")
    for key, value in written_table.schema.metadata.items():
        print(f"  {key.decode()}: {value.decode()}")


# ============================================================
# RUN THE SCENARIO
# ============================================================
if __name__ == "__main__":
    run_governance_demo()
```

---

## 7. Interview Questions

### Q1: What are the most important Parquet best practices for production?

**Answer:**

1. **Always use explicit schemas**:
```python
schema = pa.schema([("id", pa.int64()), ("amount", pa.decimal128(18, 2))])
```

2. **Use DECIMAL for financial amounts** (not FLOAT):
```python
pa.decimal128(18, 2)  # Exact precision
```

3. **Enable dictionary encoding** for low-cardinality columns:
```python
pq.write_table(table, "file.parquet", use_dictionary=True)
```

4. **Write statistics** for predicate pushdown:
```python
pq.write_table(table, "file.parquet", write_statistics=True)
```

5. **Target file size**: 256MB - 1GB per file

6. **Use Zstd compression** for best balance:
```python
pq.write_table(table, "file.parquet", compression="zstd")
```

7. **Partition by low-cardinality columns** (date, region)

8. **Add metadata** for governance:
```python
metadata = {"created_by": "etl_v2.1", "owner": "data_team"}
```

---

### Q2: How do you handle data quality in Parquet pipelines?

**Answer:**

**Validation checklist:**

1. **Schema validation**: Ensure data matches expected schema
```python
expected = pa.schema([("id", pa.int64()), ("amount", pa.float64())])
assert table.schema == expected
```

2. **Null checks**: Validate required columns have no nulls
```python
for col in required_columns:
    assert pc.null_count(table.column(col)).as_py() == 0
```

3. **Range checks**: Validate numeric values are within bounds
```python
assert pc.min(table.column("amount")).as_py() >= 0
```

4. **Uniqueness checks**: Validate primary keys are unique
```python
assert table.column("id").length() == pc.unique(table.column("id")).length()
```

5. **Referential integrity**: Validate foreign keys exist
```python
# Check all account_ids exist in accounts table
```

---

### Q3: How do you monitor Parquet data quality in production?

**Answer:**

**Metrics to monitor:**

1. **File metrics**:
   - File count per partition
   - Average file size
   - Total storage size

2. **Query metrics**:
   - Query latency (p50, p95, p99)
   - Rows scanned vs returned
   - Predicate pushdown effectiveness

3. **Pipeline metrics**:
   - Write latency
   - Throughput (rows/sec)
   - Validation error rate

4. **Storage metrics**:
   - Growth rate
   - Compression ratio
   - Lifecycle policy compliance

**Alerting rules:**
```python
# Alert if file count > 10,000
if file_count > 10_000:
    alert("Too many small files")

# Alert if query latency > 30s
if p99_latency > 30:
    alert("Query performance degraded")

# Alert if validation errors > 1%
if error_rate > 0.01:
    alert("Data quality issues")
```

---

### Q4: How do you implement data retention for Parquet files?

**Answer:**

**Strategies:**

1. **Partition-based retention**:
```python
# Delete partitions older than 7 years
import shutil
cutoff_date = datetime.now() - timedelta(days=7*365)
for partition in os.listdir("data/"):
    partition_date = datetime.strptime(partition, "date=%Y-%m-%d")
    if partition_date < cutoff_date:
        shutil.rmtree(f"data/{partition}")
```

2. **S3 Lifecycle policies**:
```json
{
  "Rules": [
    {
      "ID": "Move to Glacier after 1 year",
      "Transition": {"Days": 365, "StorageClass": "GLACIER"},
      "Expiration": {"Days": 2555}
    }
  ]
}
```

3. **Iceberg/Delta Lake expiration**:
```sql
-- Iceberg
CALL system.expire_snapshots('db.table', TIMESTAMP '2019-01-01 00:00:00')

-- Delta Lake
VACUUM db.table RETAIN 168 HOURS
```

---

### Q5: How do you ensure Parquet files are compliant with regulations?

**Answer:**

**Regulatory requirements:**

1. **Data lineage**: Track source → transformation → destination
```python
metadata = {
    "source": "core_banking.oracle",
    "transformation": "etl_v2.1",
    "timestamp": datetime.now().isoformat(),
}
```

2. **Access control**: Log who accessed what
```python
access_log.append({
    "user": "analyst_1",
    "file": "transactions.parquet",
    "action": "READ",
    "timestamp": datetime.now().isoformat(),
})
```

3. **Retention**: Enforce deletion after required period
```python
# 7-year retention for financial data
retention_expiry = datetime.now() + timedelta(days=7*365)
```

4. **Encryption**: Encrypt sensitive data at rest
```python
# Use S3 server-side encryption
pq.write_table(table, "s3://bucket/file.parquet")
# Enable S3 encryption policy
```

5. **Audit trail**: Maintain complete audit log
```python
# Log all data operations
audit_log.append({
    "operation": "WRITE",
    "user": "etl_job",
    "file": "transactions.parquet",
    "rows": 100000,
    "timestamp": datetime.now().isoformat(),
})
```
