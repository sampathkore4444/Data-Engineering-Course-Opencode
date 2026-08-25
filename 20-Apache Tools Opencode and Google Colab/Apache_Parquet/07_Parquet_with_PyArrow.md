# Parquet with PyArrow

## Table of Contents
1. [Detailed Explanation](#1-detailed-explanation)
2. [Example](#2-example)
3. [Real-World Banking Scenario 1](#3-banking-scenario-1-data-lake-operations)
4. [Python Code - Scenario 1](#4-python-code---scenario-1)
5. [Real-World Banking Scenario 2](#5-banking-scenario-2-data-quality-validation)
6. [Python Code - Scenario 2](#6-python-code---scenario-2)
7. [Interview Questions](#7-interview-questions)

---

## 1. Detailed Explanation

### What is PyArrow?

PyArrow is the **Python binding for Apache Arrow**, providing:
- In-memory columnar data structures (Table, RecordBatch, Array)
- Parquet file reading and writing
- IPC (Inter-Process Communication) for zero-copy data exchange
- Compute functions for vectorized operations
- Integration with Pandas, DuckDB, and other tools

> **PyArrow is the primary Python library for working with Parquet files. It provides the bridge between in-memory Arrow data and on-disk Parquet storage.**

### PyArrow's Role in the Parquet Ecosystem

```
Your Python Code
       |
       v
  PyArrow (API Layer)
       |
       +-- Arrow Tables (in-memory)
       +-- Parquet Reader/Writer
       +-- Compute Functions
       +-- IPC (data exchange)
       |
       v
  Parquet Files (on-disk)
       |
       v
  Object Storage (S3, GCS, ADLS)
```

### Key PyArrow Components for Parquet

#### 1. Arrow Tables

The primary in-memory data structure:

```python
import pyarrow as pa

table = pa.table({
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"],
    "amount": [100.0, 250.0, 500.0],
})

# Write to Parquet
import pyarrow.parquet as pq
pq.write_table(table, "output.parquet")

# Read back
table = pq.read_table("output.parquet")
```

#### 2. Schemas

Define explicit data types:

```python
schema = pa.schema([
    ("id", pa.int64()),
    ("name", pa.string()),
    ("amount", pa.decimal128(18, 2)),
    ("date", pa.date32()),
    ("is_active", pa.bool_()),
])

table = pa.table({...}, schema=schema)
```

#### 3. Compute Functions

Vectorized operations on Arrow data:

```python
import pyarrow.compute as pc

# Filter
filtered = pc.filter(table, pc.field("amount") > 1000)

# Sort
sorted_table = table.sort_by("amount")

# Aggregate
total = pc.sum(table.column("amount"))
```

#### 4. ParquetDataset

Multi-file, partitioned reading:

```python
dataset = pq.ParquetDataset(
    "s3://bucket/data/",
    filters=[("date", ">=", "2026-08-01")],
    use_legacy_dataset=False,
)
table = dataset.read(columns=["amount", "date"])
```

### PyArrow vs Pandas for Parquet

| Feature | PyArrow | Pandas |
|---------|---------|--------|
| Memory efficiency | Columnar (Arrow) | Row-based (NumPy) |
| Large datasets | Yes (streaming) | Limited by RAM |
| Type system | Rich (Decimal, Timestamp) | Limited |
| Parquet integration | Native | Via PyArrow |
| Nested data | Full support | Limited |
| Performance | Faster for large data | Faster for small data |

### Converting Between Formats

```python
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd

# Pandas → Arrow → Parquet
df = pd.DataFrame({...})
table = pa.Table.from_pandas(df)
pq.write_table(table, "output.parquet")

# Parquet → Arrow → Pandas
table = pq.read_table("output.parquet")
df = table.to_pandas()

# Direct Pandas → Parquet (uses PyArrow internally)
df.to_parquet("output.parquet", engine="pyarrow")
```

### Working with Partitions

```python
# Write partitioned
pq.write_to_dataset(
    table,
    root_path="output/",
    partition_cols=["year", "month"],
)

# Read with partition pruning
dataset = pq.ParquetDataset(
    "output/",
    filters=[("year", "=", 2026), ("month", "=", 8)],
)
table = dataset.read()
```

### Schema Evolution with PyArrow

```python
# Write with schema v1
schema_v1 = pa.schema([("id", pa.int64()), ("name", pa.string())])
table_v1 = pa.table({"id": [1, 2], "name": ["A", "B"]}, schema=schema_v1)
pq.write_table(table_v1, "data_v1.parquet")

# Write with schema v2 (added column)
schema_v2 = pa.schema([("id", pa.int64()), ("name", pa.string()), ("email", pa.string())])
table_v2 = pa.table({"id": [3, 4], "name": ["C", "D"], "email": ["c@d.com", "e@f.com"]}, schema=schema_v2)
pq.write_table(table_v2, "data_v2.parquet")

# Read both (PyArrow handles schema mismatch)
table = pq.read_table("data_v1.parquet")  # email column will be null
```

---

## 2. Example

### Complete PyArrow Parquet Workflow

```python
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
import pandas as pd

# 1. Create data with explicit schema
schema = pa.schema([
    ("transaction_id", pa.int64()),
    ("account_id", pa.string()),
    ("amount", pa.decimal128(18, 2)),
    ("currency", pa.string()),
    ("transaction_date", pa.date32()),
    ("status", pa.string()),
])

data = {
    "transaction_id": [1001, 1002, 1003, 1004, 1005],
    "account_id": ["A001", "A002", "A001", "A003", "A002"],
    "amount": [100.00, 2500.00, 50.00, 15000.00, 75.00],
    "currency": ["USD", "USD", "EUR", "GBP", "USD"],
    "transaction_date": ["2026-08-01", "2026-08-01", "2026-08-02", "2026-08-02", "2026-08-03"],
    "status": ["COMPLETED", "COMPLETED", "PENDING", "COMPLETED", "FAILED"],
}

table = pa.table(data, schema=schema)

# 2. Write to Parquet
pq.write_table(table, "transactions.parquet", compression="zstd")

# 3. Read back
table = pq.read_table("transactions.parquet")
print(table.to_pandas())

# 4. Read with column pruning
partial = pq.read_table("transactions.parquet", columns=["account_id", "amount"])
print(f"\nPartial read: {partial.num_columns} columns")

# 5. Read with filter
filtered = pq.read_table(
    "transactions.parquet",
    filters=[("status", "=", "COMPLETED")],
    columns=["transaction_id", "amount"],
)
print(f"\nFiltered: {filtered.num_rows} completed transactions")

# 6. Compute operations
total = pc.sum(table.column("amount"))
print(f"\nTotal amount: {total}")
```

---

## 3. Banking Scenario 1: Data Lake Operations

### Problem
A bank needs to perform common data lake operations on Parquet files:
- Read files from different partitions
- Merge incremental updates
- Validate data quality
- Generate reports

### Why PyArrow Matters?
- Efficient partitioned reading
- Vectorized compute for fast aggregations
- Schema validation for data quality
- Seamless Pandas integration for reporting

### Architecture
```
S3 Data Lake (partitioned Parquet)
       |
       v
  PyArrow (read, filter, transform)
       |
       v
  Data Quality Checks
       |
       v
  Reports (Pandas / Matplotlib)
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

# ============================================================
# BANKING SCENARIO: Data Lake Operations with PyArrow
# ============================================================

def create_transaction_table(num_rows=100_000):
    """Create a realistic transaction table."""
    random.seed(42)
    np.random.seed(42)

    dates = [(datetime(2026, 8, 1) + timedelta(days=i)).strftime("%Y-%m-%d")
             for i in range(31)]
    statuses = ["COMPLETED"] * 95 + ["PENDING"] * 3 + ["FAILED"] * 2
    channels = ["ONLINE", "MOBILE", "BRANCH", "ATM", "POS"]
    currencies = ["USD", "EUR", "GBP"]

    table = pa.table({
        "transaction_id": pa.array(list(range(1, num_rows + 1)), type=pa.int64()),
        "account_id": pa.array([f"ACC{random.randint(100000, 999999)}" for _ in range(num_rows)], type=pa.string()),
        "customer_id": pa.array([f"CUST{random.randint(10000, 99999)}" for _ in range(num_rows)], type=pa.string()),
        "amount": pa.array(np.random.uniform(1.0, 100000.0, num_rows).round(2), type=pa.float64()),
        "currency": pa.array(np.random.choice(currencies, num_rows), type=pa.string()),
        "transaction_date": pa.array(np.random.choice(dates, num_rows), type=pa.date32()),
        "status": pa.array(np.random.choice(statuses, num_rows), type=pa.string()),
        "channel": pa.array(np.random.choice(channels, num_rows), type=pa.string()),
    })

    return table


def write_partitioned_data(table, base_path):
    """Write data partitioned by date."""
    pq.write_to_dataset(
        table,
        root_path=base_path,
        partition_cols=["transaction_date"],
        compression="zstd",
        use_dictionary=True,
        write_statistics=True,
    )
    print(f"Wrote {table.num_rows:,} rows to {base_path}")


def read_specific_partition(base_path, target_date):
    """Read data for a specific date partition."""
    filters = [("transaction_date", "=", target_date)]
    dataset = pq.ParquetDataset(base_path, filters=filters, use_legacy_dataset=False)
    table = dataset.read()
    return table


def merge_incremental(base_path, new_data_path, output_path):
    """Merge incremental data with existing partitioned data."""
    # Read existing data
    existing = pq.ParquetDataset(base_path, use_legacy_dataset=False).read()

    # Read new data
    new_data = pq.read_table(new_data_path)

    # Concatenate
    merged = pa.concat_tables([existing, new_data])

    # Write merged data
    pq.write_to_dataset(
        merged,
        root_path=output_path,
        partition_cols=["transaction_date"],
        compression="zstd",
    )

    print(f"Merged: {existing.num_rows:,} + {new_data.num_rows:,} = {merged.num_rows:,}")


def compute_daily_summary(base_path):
    """Compute daily transaction summary using PyArrow compute."""
    dataset = pq.ParquetDataset(base_path, use_legacy_dataset=False)
    table = dataset.read(columns=["transaction_date", "amount", "status", "channel"])

    # Group by date and compute aggregations
    # PyArrow doesn't have a direct group_by like Pandas, so convert
    df = table.to_pandas()

    summary = df.groupby("transaction_date").agg(
        total_amount=("amount", "sum"),
        avg_amount=("amount", "mean"),
        tx_count=("amount", "count"),
        completed_pct=("status", lambda x: (x == "COMPLETED").mean() * 100),
    ).round(2)

    return summary


# ============================================================
# RUN THE SCENARIO
# ============================================================
if __name__ == "__main__":
    base_path = os.path.join(tempfile.gettempdir(), "datalake_ops")
    os.makedirs(base_path, exist_ok=True)

    # Create and write data
    table = create_transaction_table(num_rows=100_000)
    write_partitioned_data(table, base_path)

    # Read specific partition
    print(f"\n=== Partition Read ===")
    partition_data = read_specific_partition(base_path, "2026-08-15")
    print(f"Rows for 2026-08-15: {partition_data.num_rows:,}")

    # Compute summary
    print(f"\n=== Daily Summary ===")
    summary = compute_daily_summary(base_path)
    print(summary.head(10))
```

---

## 5. Banking Scenario 2: Data Quality Validation

### Problem
Before data enters the analytics layer, it must pass quality checks:
- No null values in critical columns
- Amounts within reasonable bounds
- Dates are valid
- Referential integrity (account_id exists in customer table)
- No duplicate transaction_ids

### Why PyArrow Matters?
- Vectorized null checks (pc.null_count)
- Statistical validation (min/max)
- Schema enforcement
- Fast set operations for deduplication

### Architecture
```
Raw Parquet Files
       |
       v
  PyArrow Data Quality Pipeline
       |
       +-- Null checks
       +-- Range validation
       +-- Schema validation
       +-- Deduplication
       +-- Referential integrity
       |
       v
  Clean Parquet Files (passed validation)
       |
       v
  Analytics Layer
```

---

## 6. Python Code - Scenario 2

```python
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
import numpy as np
from datetime import datetime, timedelta
import random
import os
import tempfile

# ============================================================
# BANKING SCENARIO: Data Quality Validation with PyArrow
# ============================================================

def create_data_with_quality_issues(num_rows=10_000):
    """Create transaction data with intentional quality issues."""
    random.seed(42)
    np.random.seed(42)

    # Some nulls
    amounts = np.random.uniform(1.0, 100000.0, num_rows).round(2)
    amounts[random.sample(range(num_rows), 50)] = np.nan  # 50 nulls

    # Some out-of-range values
    amounts[random.sample(range(num_rows), 20)] = -100.0  # 20 negative
    amounts[random.sample(range(num_rows), 10)] = 999999999.0  # 10 extreme

    # Some duplicate IDs
    transaction_ids = list(range(1, num_rows + 1))
    for i in range(100):
        transaction_ids[random.randint(0, num_rows-1)] = transaction_ids[i]  # 100 duplicates

    table = pa.table({
        "transaction_id": pa.array(transaction_ids, type=pa.int64()),
        "account_id": pa.array([f"ACC{random.randint(100000, 999999)}" for _ in range(num_rows)], type=pa.string()),
        "amount": pa.array(amounts, type=pa.float64()),
        "currency": pa.array(np.random.choice(["USD", "EUR", "GBP"], num_rows), type=pa.string()),
        "status": pa.array(np.random.choice(
            ["COMPLETED", "PENDING", "FAILED", None], num_rows
        ), type=pa.string()),
        "transaction_date": pa.array([
            (datetime(2026, 8, 1) + timedelta(days=random.randint(0, 30))).strftime("%Y-%m-%d")
            for _ in range(num_rows)
        ], type=pa.date32()),
    })

    return table


class DataQualityValidator:
    """PyArrow-based data quality validator for Parquet files."""

    def __init__(self):
        self.issues = []

    def validate_nulls(self, table, required_columns):
        """Check for null values in required columns."""
        for col_name in required_columns:
            col = table.column(col_name)
            null_count = pc.null_count(col).as_py()
            if null_count > 0:
                self.issues.append({
                    "check": "null_check",
                    "column": col_name,
                    "null_count": null_count,
                    "severity": "ERROR",
                })
            else:
                print(f"  ✓ {col_name}: no nulls")

    def validate_ranges(self, table, column, min_val, max_val):
        """Check values are within expected range."""
        col = table.column(column).cast(pa.float64())
        valid = pc.and_(
            pc.greater_equal(col, pa.scalar(min_val, type=pa.float64())),
            pc.less_equal(col, pa.scalar(max_val, type=pa.float64()))
        )
        invalid_count = pc.sum(pc.invert(valid)).as_py()

        if invalid_count > 0:
            self.issues.append({
                "check": "range_check",
                "column": column,
                "invalid_count": invalid_count,
                "expected_range": f"[{min_val}, {max_val}]",
                "severity": "ERROR",
            })
        else:
            print(f"  ✓ {column}: all values in range [{min_val}, {max_val}]")

    def validate_uniqueness(self, table, column):
        """Check for duplicate values."""
        col = table.column(column)
        unique_count = pc.unique(col).length()
        total_count = col.length()
        duplicate_count = total_count - unique_count

        if duplicate_count > 0:
            self.issues.append({
                "check": "uniqueness_check",
                "column": column,
                "duplicate_count": duplicate_count,
                "severity": "WARNING",
            })
        else:
            print(f"  ✓ {column}: all values unique")

    def validate_schema(self, table, expected_schema):
        """Validate table matches expected schema."""
        for field in expected_schema:
            if field.name not in table.column_names:
                self.issues.append({
                    "check": "schema_check",
                    "column": field.name,
                    "severity": "ERROR",
                    "message": f"Column {field.name} missing",
                })
            else:
                actual_type = table.schema.field(field.name).type
                if actual_type != field.type:
                    self.issues.append({
                        "check": "schema_check",
                        "column": field.name,
                        "severity": "WARNING",
                        "message": f"Type mismatch: expected {field.type}, got {actual_type}",
                    })
                else:
                    print(f"  ✓ {field.name}: schema matches")

    def validate_not_empty(self, table):
        """Check table is not empty."""
        if table.num_rows == 0:
            self.issues.append({
                "check": "empty_check",
                "severity": "ERROR",
                "message": "Table is empty",
            })
        else:
            print(f"  ✓ Table has {table.num_rows:,} rows")

    def get_report(self):
        """Generate quality report."""
        errors = [i for i in self.issues if i["severity"] == "ERROR"]
        warnings = [i for i in self.issues if i["severity"] == "WARNING"]

        print(f"\n=== Data Quality Report ===")
        print(f"Errors: {len(errors)}")
        print(f"Warnings: {len(warnings)}")

        if errors:
            print(f"\nErrors:")
            for e in errors:
                print(f"  - {e['check']}: {e.get('column', 'N/A')} - {e.get('message', f'{e.get(\"invalid_count\", e.get(\"null_count\", \"N/A\"))} issues')}")

        if warnings:
            print(f"\nWarnings:")
            for w in warnings:
                print(f"  - {w['check']}: {w.get('column', 'N/A')} - {w.get('message', f'{w.get(\"duplicate_count\", \"N/A\")} issues')}")

        return len(errors) == 0


def clean_and_validate(table, output_path):
    """Clean data and validate quality."""
    print("=== Data Quality Validation ===\n")

    validator = DataQualityValidator()

    # Run validations
    print("1. Checking for nulls...")
    validator.validate_nulls(table, ["transaction_id", "amount", "status"])

    print("\n2. Checking amount ranges...")
    validator.validate_ranges(table, "amount", 0.01, 1_000_000.0)

    print("\n3. Checking uniqueness...")
    validator.validate_uniqueness(table, "transaction_id")

    print("\n4. Checking schema...")
    validator.validate_schema(table, table.schema)

    print("\n5. Checking not empty...")
    validator.validate_not_empty(table)

    # Get report
    passed = validator.get_report()

    if passed:
        # Clean the data
        # Remove nulls in critical columns
        mask = pc.is_valid(table.column("amount"))
        cleaned = pc.filter(table, mask)

        # Remove negative amounts
        positive = pc.greater(cleaned.column("amount"), pa.scalar(0.0))
        cleaned = pc.filter(cleaned, positive)

        # Write clean data
        pq.write_table(cleaned, output_path, compression="zstd")
        print(f"\n✓ Clean data written: {cleaned.num_rows:,} rows")
    else:
        print(f"\n✗ Data failed quality checks. Not writing to output.")

    return passed


# ============================================================
# RUN THE SCENARIO
# ============================================================
if __name__ == "__main__":
    # Create data with quality issues
    print("Creating data with quality issues...")
    table = create_data_with_quality_issues(num_rows=10_000)

    # Save raw data
    base_path = os.path.join(tempfile.gettempdir(), "quality_check")
    os.makedirs(base_path, exist_ok=True)
    pq.write_table(table, os.path.join(base_path, "raw.parquet"))

    # Validate and clean
    output_path = os.path.join(base_path, "clean.parquet")
    clean_and_validate(table, output_path)
```

---

## 7. Interview Questions

### Q1: What is PyArrow and how does it relate to Parquet?

**Answer:**
PyArrow is the **Python binding for Apache Arrow**, providing:
- In-memory columnar data structures (Table, RecordBatch)
- Parquet file reading and writing
- Vectorized compute functions
- IPC for zero-copy data exchange
- Integration with Pandas, DuckDB, Spark

**Relationship to Parquet**: PyArrow is the **primary Python library** for reading and writing Parquet files. It uses Arrow's memory format as the intermediate representation, enabling efficient data exchange between Parquet files and Python code.

```python
import pyarrow as pa
import pyarrow.parquet as pq

# PyArrow provides the bridge
table = pa.table({...})  # Arrow in-memory
pq.write_table(table, "file.parquet")  # Parquet on-disk
table = pq.read_table("file.parquet")  # Back to Arrow
df = table.to_pandas()  # To Pandas
```

---

### Q2: When would you use PyArrow directly vs Pandas for Parquet operations?

**Answer:**

**Use PyArrow directly when:**
- Working with datasets larger than RAM
- Need nested data support
- Require explicit schema control
- Building data pipelines with partitioning
- Need vectorized compute operations
- Working with Decimal or Timestamp types

**Use Pandas when:**
- Data fits in memory
- Need familiar DataFrame API
- Doing exploratory analysis
- Quick data transformations
- Integration with ML libraries

**Example**:
```python
# PyArrow: Production data pipeline
import pyarrow.parquet as pq
dataset = pq.ParquetDataset("s3://bucket/data/", filters=[...])
table = dataset.read(columns=["a", "b"])  # Efficient, streaming

# Pandas: Quick analysis
df = pd.read_parquet("file.parquet")  # Simple, familiar
df.groupby("status")["amount"].sum()  # Pandas API
```

---

### Q3: How do you handle large datasets that don't fit in memory?

**Answer:**

**PyArrow strategies:**

1. **Column pruning**: Read only needed columns
```python
table = pq.read_table("file.parquet", columns=["col1", "col2"])
```

2. **Predicate pushdown**: Read only matching rows
```python
dataset = pq.ParquetDataset("data/", filters=[("date", ">=", "2026-08-01")])
```

3. **Batch reading**: Process in chunks
```python
dataset = pq.ParquetDataset("data/")
reader = dataset.read批次(batch_size=100_000)
for batch in reader:
    process(batch)
```

4. **Partitioned reading**: Read only relevant partitions
```python
dataset = pq.ParquetDataset("data/", partition_cols=["date"])
# Read specific partition
table = dataset.read(filter=[("date", "=", "2026-08-01")])
```

5. **Use DuckDB**: Let DuckDB handle memory management
```python
import duckdb
con = duckdb.connect()
result = con.execute("SELECT * FROM read_parquet('data/*.parquet')").fetchall()
```

---

### Q4: What are the best practices for using PyArrow with Parquet?

**Answer:**

1. **Always define explicit schemas**:
```python
schema = pa.schema([("id", pa.int64()), ("amount", pa.decimal128(18, 2))])
```

2. **Use partitioning for large datasets**:
```python
pq.write_to_dataset(table, root_path="output/", partition_cols=["date"])
```

3. **Specify compression explicitly**:
```python
pq.write_table(table, "output.parquet", compression="zstd")
```

4. **Enable statistics for predicate pushdown**:
```python
pq.write_table(table, "output.parquet", write_statistics=True)
```

5. **Use dictionary encoding for low-cardinality columns**:
```python
pq.write_table(table, "output.parquet", use_dictionary=True)
```

6. **Read only what you need**:
```python
table = pq.read_table("file.parquet", columns=["needed_col1", "needed_col2"])
```

7. **Use filters for partitioned datasets**:
```python
dataset = pq.ParquetDataset("data/", filters=[("date", ">=", "2026-08-01")])
```

---

### Q5: How does PyArrow handle schema evolution across Parquet files?

**Answer:**
PyArrow handles schema evolution through **flexible reading**:

**Adding columns:**
```python
# File 1: schema v1
pq.write_table(pa.table({"id": [1], "name": ["A"]}), "v1.parquet")

# File 2: schema v2 (added column)
pq.write_table(pa.table({"id": [2], "name": ["B"], "email": ["b@c.com"]}), "v2.parquet")

# Read both - PyArrow fills NULLs for missing columns
table = pq.ParquetDataset(["v1.parquet", "v2.parquet"]).read()
# id: [1, 2], name: [A, B], email: [NULL, b@c.com]
```

**Type widening:**
- INT32 → INT64: Supported
- FLOAT → DOUBLE: Supported
- Other changes: May fail

**Limitations:**
- Cannot rename columns (name is in the file)
- Cannot remove columns (they persist in old files)
- Cannot change types arbitrarily

**Best practice**: Use Iceberg or Delta Lake for robust schema evolution across files.
