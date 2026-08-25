# Concept 05: Arrow Schemas and DataTypes

## 📚 Detailed Explanation

**Arrow Schemas** define the structure and types of data in Arrow Tables and Arrays. They ensure type safety and enable efficient memory layout.

### What is a Schema?

A schema is a **blueprint** that defines:
- Column names
- Column data types
- Nullability
- Metadata

### Why Schemas Matter

**Without Schema:**
```python
# No type enforcement
data = [1, "two", 3.0]  # Mixed types
# Problem: Inefficient, error-prone
```

**With Schema:**
```python
import pyarrow as pa

# Type-safe
schema = pa.schema([
    pa.field("id", pa.int64()),
    pa.field("name", pa.string()),
    pa.field("amount", pa.float64())
])
# Problem: Guaranteed types, efficient storage
```

### Arrow Data Types

| Category | Types | Description |
|----------|-------|-------------|
| **Integer** | int8, int16, int32, int64 | Signed integers |
| **Unsigned** | uint8, uint16, uint32, uint64 | Unsigned integers |
| **Float** | float16, float32, float64 | Floating point |
| **String** | utf8, large_utf8 | Variable-length strings |
| **Binary** | binary, large_binary | Variable-length bytes |
| **Boolean** | boolean | True/False |
| **Date** | date32, date64 | Date types |
| **Time** | time32, time64 | Time types |
| **Timestamp** | timestamp | Date + Time |
| **Decimal** | decimal128, decimal256 | Precise decimals |
| **Duration** | duration | Time intervals |
| **Interval** | interval | Date intervals |
| **List** | list, large_list | Variable-length lists |
| **Struct** | struct | Fixed named fields |
| **Map** | map | Key-value pairs |
| **Union** | union, sparse_union, dense_union | Heterogeneous types |

### Schema Operations

```python
import pyarrow as pa

# Create schema
schema = pa.schema([
    pa.field("id", pa.int64(), nullable=False),
    pa.field("name", pa.string(), nullable=True),
    pa.field("amount", pa.decimal128(18, 2))
])

# Add metadata
schema = schema.with_metadata({"version": "1.0", "source": "banking"})

# Get field by name
field = schema.field("id")

# Check if field exists
has_id = schema.get_field_index("id") >= 0
```

---

## 💡 Example: Schemas in Banking

### Scenario: Transaction Schema

```python
import pyarrow as pa

# Define banking transaction schema
transaction_schema = pa.schema([
    pa.field("transaction_id", pa.string(), nullable=False),
    pa.field("account_id", pa.string(), nullable=False),
    pa.field("amount", pa.decimal128(18, 2), nullable=False),
    pa.field("transaction_date", pa.date32(), nullable=False),
    pa.field("transaction_time", pa.time64("us"), nullable=True),
    pa.field("status", pa.string(), nullable=False),
    pa.field("branch_id", pa.string(), nullable=True),
])

# Create table with schema
table = pa.table({
    "transaction_id": ["TXN-001", "TXN-002"],
    "account_id": ["ACC-001", "ACC-002"],
    "amount": [50000.00, 75000.00],
    "transaction_date": ["2026-08-24", "2026-08-24"],
    "transaction_time": ["10:30:00", "14:45:00"],
    "status": ["COMPLETED", "PENDING"],
    "branch_id": ["BR-001", "BR-002"],
}, schema=transaction_schema)
```

---

## 🏦 Real-World Banking Scenario 1: Schema Design for Transaction System

### Scenario
A bank is designing a **new transaction system** with:
- 100+ columns
- Complex data types
- Regulatory requirements

### Problem
- Need precise data types
- Must handle currency correctly
- Regulatory compliance

### Solution
Arrow schemas provide:
- Precise decimal types
- Null handling
- Type validation

### Python Code

```python
"""
Banking Scenario 1: Schema Design for Transaction System
Using Arrow Schemas and DataTypes
"""

import pyarrow as pa
import pyarrow.compute as pc
from datetime import datetime, date, time
import random
import time as time_module

# ============================================================
# STEP 1: Design Comprehensive Schema
# ============================================================

print("=== SCHEMA DESIGN FOR TRANSACTION SYSTEM ===\n")

# Design transaction schema
transaction_schema = pa.schema([
    # Primary identifiers
    pa.field("transaction_id", pa.string(), nullable=False),
    pa.field("account_id", pa.string(), nullable=False),
    pa.field("customer_id", pa.string(), nullable=False),
    
    # Transaction details
    pa.field("amount", pa.decimal128(18, 2), nullable=False),
    pa.field("currency", pa.string(), nullable=False),
    pa.field("transaction_type", pa.string(), nullable=False),
    pa.field("transaction_date", pa.date32(), nullable=False),
    pa.field("transaction_time", pa.timestamp("us"), nullable=True),
    
    # Account information
    pa.field("account_type", pa.string(), nullable=True),
    pa.field("account_balance", pa.decimal128(18, 2), nullable=True),
    
    # Location and channel
    pa.field("branch_id", pa.string(), nullable=True),
    pa.field("channel", pa.string(), nullable=True),
    pa.field("device_id", pa.string(), nullable=True),
    pa.field("ip_address", pa.string(), nullable=True),
    
    # Status and processing
    pa.field("status", pa.string(), nullable=False),
    pa.field("status_reason", pa.string(), nullable=True),
    pa.field("processed_at", pa.timestamp("us"), nullable=True),
    
    # Compliance and audit
    pa.field("regulatory_code", pa.string(), nullable=True),
    pa.field("audit_trail_id", pa.string(), nullable=True),
    pa.field("compliance_flag", pa.boolean(), nullable=True),
    
    # Metadata
    pa.field("created_at", pa.timestamp("us"), nullable=False),
    pa.field("updated_at", pa.timestamp("us"), nullable=True),
    pa.field("version", pa.int32(), nullable=False),
])

print(f"Transaction Schema:")
print(f"  Fields: {len(transaction_schema)}")
print(f"\nField Details:")
for field in transaction_schema:
    nullable = "NULLABLE" if field.nullable else "NOT NULL"
    print(f"  {field.name}: {field.type} ({nullable})")

# ============================================================
# STEP 2: Create Table with Schema
# ============================================================

print("\n--- Creating Table with Schema ---")

def generate_transaction_data(num_records: int) -> pa.Table:
    """Generate transaction data matching schema."""
    
    data = {
        "transaction_id": [f"TXN-{i:010d}" for i in range(1, num_records + 1)],
        "account_id": [f"ACC-{random.randint(10000, 99999):06d}" for _ in range(num_records)],
        "customer_id": [f"CUST-{random.randint(100000, 999999):06d}" for _ in range(num_records)],
        "amount": [round(random.uniform(100, 1000000), 2) for _ in range(num_records)],
        "currency": ["INR"] * num_records,
        "transaction_type": [random.choice(["CREDIT", "DEBIT", "TRANSFER"]) for _ in range(num_records)],
        "transaction_date": [date(2026, 8, 24)] * num_records,
        "transaction_time": [datetime.now()] * num_records,
        "account_type": [random.choice(["SAVINGS", "CURRENT"]) for _ in range(num_records)],
        "account_balance": [round(random.uniform(10000, 5000000), 2) for _ in range(num_records)],
        "branch_id": [f"BR-{random.randint(1, 100):03d}" for _ in range(num_records)],
        "channel": [random.choice(["ATM", "MOBILE", "WEB", "BRANCH"]) for _ in range(num_records)],
        "device_id": [f"DEV-{random.randint(1000, 9999):04d}" if random.random() > 0.3 else None for _ in range(num_records)],
        "ip_address": [f"192.168.{random.randint(1, 255)}.{random.randint(1, 255)}" if random.random() > 0.5 else None for _ in range(num_records)],
        "status": [random.choice(["COMPLETED", "PENDING", "FAILED"]) for _ in range(num_records)],
        "status_reason": [None] * num_records,
        "processed_at": [datetime.now()] * num_records,
        "regulatory_code": [f"REG-{random.randint(1000, 9999)}" for _ in range(num_records)],
        "audit_trail_id": [f"AUDIT-{i:010d}" for i in range(1, num_records + 1)],
        "compliance_flag": [random.choice([True, False]) for _ in range(num_records)],
        "created_at": [datetime.now()] * num_records,
        "updated_at": [None] * num_records,
        "version": [1] * num_records,
    }
    
    return pa.table(data, schema=transaction_schema)

# Generate 1 million transactions
print("Generating 1 million transactions...")
start_time = time_module.time()
transactions = generate_transaction_data(1000000)
generation_time = time_module.time() - start_time

print(f"Generated in {generation_time:.3f} seconds")
print(f"Table size: {transactions.nbytes / 1024 / 1024:.2f} MB")

# ============================================================
# STEP 3: Schema Validation
# ============================================================

print("\n--- Schema Validation ---")

# Validate schema
print(f"\nSchema Validation:")
print(f"  Total fields: {len(transactions.schema)}")
print(f"  Nullable fields: {sum(1 for f in transactions.schema if f.nullable)}")
print(f"  Required fields: {sum(1 for f in transactions.schema if not f.nullable)}")

# Check data types
print(f"\nData Type Summary:")
type_counts = {}
for field in transactions.schema:
    type_name = str(field.type)
    type_counts[type_name] = type_counts.get(type_name, 0) + 1

for type_name, count in sorted(type_counts.items()):
    print(f"  {type_name}: {count} fields")

# ============================================================
# STEP 4: Type-Specific Operations
# ============================================================

print("\n--- Type-Specific Operations ---")

# Decimal operations (precise financial calculations)
amounts = transactions.column("amount")
print(f"\nDecimal Operations:")
print(f"  Sum: {pc.sum(amounts)}")
print(f"  Mean: {pc.mean(amounts)}")

# Timestamp operations
timestamps = transactions.column("transaction_time")
print(f"\nTimestamp Operations:")
print(f"  Min: {pc.min(timestamps)}")
print(f"  Max: {pc.max(timestamps)}")

# String operations
statuses = transactions.column("status")
print(f"\nString Operations:")
status_counts = {}
for status in ["COMPLETED", "PENDING", "FAILED"]:
    mask = pc.equal(statuses, status)
    count = pc.sum(pa.array([1 if m else 0 for m in mask]))
    status_counts[status] = count.as_py()

for status, count in status_counts.items():
    print(f"  {status}: {count:,}")

# ============================================================
# STEP 5: Schema Evolution
# ============================================================

print("\n--- Schema Evolution ---")

# Add new column
new_schema = transactions.schema.append(
    pa.field("fraud_score", pa.float64(), nullable=True)
)

# Add column with default values
transactions_with_fraud = transactions.append_column(
    "fraud_score",
    pa.array([round(random.uniform(0, 1), 4) for _ in range(len(transactions))])
)

print(f"\nSchema Evolution:")
print(f"  Original fields: {len(transactions.schema)}")
print(f"  New fields: {len(transactions_with_fraud.schema)}")
print(f"  Added: fraud_score (float64)")

# ============================================================
# STEP 6: Schema Comparison
# ============================================================

print("\n--- Schema Comparison ---")

# Create another table with different schema
other_schema = pa.schema([
    pa.field("id", pa.int64()),
    pa.field("name", pa.string()),
    pa.field("value", pa.float64()),
])

other_table = pa.table({
    "id": [1, 2, 3],
    "name": ["A", "B", "C"],
    "value": [1.0, 2.0, 3.0]
}, schema=other_schema)

print(f"\nSchema Comparison:")
print(f"  Transaction Schema: {len(transactions.schema)} fields")
print(f"  Other Schema: {len(other_schema)} fields")

# ============================================================
# STEP 7: Benefits Summary
# ============================================================

print("\n--- Benefits Summary ---")

print("""
ARROW SCHEMA BENEFITS:

1. TYPE SAFETY
   - Guaranteed data types
   - No runtime type errors
   - Predictable behavior

2. MEMORY EFFICIENCY
   - Optimal storage per type
   - No type conversion overhead
   - Efficient encoding

3. VALIDATION
   - Schema enforcement
   - Null handling
   - Data quality

4. EVOLUTION
   - Add/remove columns
   - Type widening
   - Backward compatibility

5. INTEROPERABILITY
   - Cross-language schemas
   - Parquet compatibility
   - Database mapping

DATA TYPE SELECTION:
  - int64: IDs, counts
  - decimal128: Financial amounts
  - string: Names, codes
  - timestamp: Dates with time
  - boolean: Flags
  - date32: Dates only
""")
```

---

## 🏦 Real-World Banking Scenario 2: Data Migration Schema

### Scenario
A bank is **migrating from Oracle to a data lake**:
- Complex Oracle schemas
- Need to map to Arrow types
- Preserve data accuracy

### Problem
- Type mismatches
- Precision loss
- Null handling

### Solution
Arrow schemas provide:
- Precise decimal types
- Type mapping
- Null preservation

### Python Code

```python
"""
Banking Scenario 2: Data Migration Schema
Using Arrow Schemas for Oracle Migration
"""

import pyarrow as pa
import pyarrow.compute as pc
from datetime import datetime, date
import random

# ============================================================
# STEP 1: Define Oracle to Arrow Type Mapping
# ============================================================

print("=== DATA MIGRATION SCHEMA ===\n")

# Oracle to Arrow type mapping
oracle_to_arrow = {
    "NUMBER(10)": pa.int64(),
    "NUMBER(18,2)": pa.decimal128(18, 2),
    "VARCHAR2(50)": pa.string(),
    "VARCHAR2(100)": pa.string(),
    "DATE": pa.date32(),
    "TIMESTAMP": pa.timestamp("us"),
    "CLOB": pa.string(),
    "BLOB": pa.binary(),
}

print("Oracle to Arrow Type Mapping:")
for oracle_type, arrow_type in oracle_to_arrow.items():
    print(f"  {oracle_type} → {arrow_type}")

# ============================================================
# STEP 2: Design Target Schema
# ============================================================

print("\n--- Designing Target Schema ---")

# Customer table schema
customer_schema = pa.schema([
    pa.field("customer_id", pa.int64(), nullable=False),
    pa.field("first_name", pa.string(), nullable=False),
    pa.field("last_name", pa.string(), nullable=False),
    pa.field("email", pa.string(), nullable=True),
    pa.field("phone", pa.string(), nullable=True),
    pa.field("date_of_birth", pa.date32(), nullable=True),
    pa.field("registration_date", pa.date32(), nullable=False),
    pa.field("account_balance", pa.decimal128(18, 2), nullable=False),
    pa.field("credit_score", pa.int32(), nullable=True),
    pa.field("risk_rating", pa.string(), nullable=False),
    pa.field("is_active", pa.boolean(), nullable=False),
    pa.field("created_at", pa.timestamp("us"), nullable=False),
    pa.field("updated_at", pa.timestamp("us"), nullable=True),
])

print(f"\nCustomer Schema:")
print(f"  Fields: {len(customer_schema)}")

# ============================================================
# STEP 3: Generate Migration Data
# ============================================================

print("\n--- Generating Migration Data ---")

def generate_oracle_data(num_records: int) -> pa.Table:
    """Simulate Oracle data with type variations."""
    
    data = {
        "customer_id": list(range(1, num_records + 1)),
        "first_name": [random.choice(["Alice", "Bob", "Charlie", "David", "Eve"]) for _ in range(num_records)],
        "last_name": [random.choice(["Smith", "Johnson", "Williams", "Brown", "Jones"]) for _ in range(num_records)],
        "email": [f"customer{i}@bank.com" if random.random() > 0.2 else None for i in range(num_records)],
        "phone": [f"+1-555-{random.randint(1000, 9999)}" if random.random() > 0.3 else None for _ in range(num_records)],
        "date_of_birth": [date(random.randint(1960, 2000), random.randint(1, 12), random.randint(1, 28)) for _ in range(num_records)],
        "registration_date": [date(2020, 1, 1) for _ in range(num_records)],
        "account_balance": [round(random.uniform(1000, 1000000), 2) for _ in range(num_records)],
        "credit_score": [random.randint(300, 850) if random.random() > 0.1 else None for _ in range(num_records)],
        "risk_rating": [random.choice(["LOW", "MEDIUM", "HIGH"]) for _ in range(num_records)],
        "is_active": [random.choice([True, False]) for _ in range(num_records)],
        "created_at": [datetime(2020, 1, 1)] * num_records,
        "updated_at": [datetime.now() if random.random() > 0.5 else None for _ in range(num_records)],
    }
    
    return pa.table(data, schema=customer_schema)

# Generate 500,000 customers
print("Generating 500,000 customer records...")
oracle_data = generate_oracle_data(500000)

print(f"Generated: {len(oracle_data):,} records")
print(f"Table size: {oracle_data.nbytes / 1024 / 1024:.2f} MB")

# ============================================================
# STEP 4: Type Conversion Validation
# ============================================================

print("\n--- Type Conversion Validation ---")

# Validate decimal precision
balances = oracle_data.column("account_balance")
print(f"\nDecimal Validation:")
print(f"  Min: {pc.min(balances)}")
print(f"  Max: {pc.max(balances)}")
print(f"  Precision: 18 digits, 2 decimal places")

# Validate string lengths
names = oracle_data.column("first_name")
print(f"\nString Validation:")
print(f"  Min length: {pc.min(pc.utf8_length(names)).as_py()}")
print(f"  Max length: {pc.max(pc.utf8_length(names)).as_py()}")

# Validate null handling
print(f"\nNull Validation:")
for col_name in ["email", "phone", "credit_score", "updated_at"]:
    col = oracle_data.column(col_name)
    null_count = col.null_count
    print(f"  {col_name}: {null_count:,} nulls ({null_count/len(oracle_data)*100:.1f}%)")

# ============================================================
# STEP 5: Data Quality Checks
# ============================================================

print("\n--- Data Quality Checks ---")

# Check 1: No duplicate customer IDs
customer_ids = oracle_data.column("customer_id")
unique_ids = pc.unique(customer_ids)
print(f"\nCheck 1: Unique Customer IDs")
print(f"  Total: {len(customer_ids):,}")
print(f"  Unique: {len(unique_ids):,}")
print(f"  Duplicates: {len(customer_ids) - len(unique_ids):,}")

# Check 2: Valid credit scores
credit_scores = oracle_data.column("credit_score")
valid_scores_mask = pc.and_(
    pc.greater_equal(credit_scores, pa.scalar(300)),
    pc.less_equal(credit_scores, pa.scalar(850))
)
valid_scores = pc.sum(pa.array([1 if m else 0 for m in valid_scores_mask]))
print(f"\nCheck 2: Valid Credit Scores")
print(f"  Valid: {valid_scores:,.0f}")
print(f"  Invalid: {len(credit_scores) - valid_scores:,.0f}")

# Check 3: Positive balances
positive_balances_mask = pc.greater(balances, pa.scalar(0))
positive_balances = pc.sum(pa.array([1 if m else 0 for m in positive_balances_mask]))
print(f"\nCheck 3: Positive Balances")
print(f"  Positive: {positive_balances:,.0f}")
print(f"  Negative/Zero: {len(balances) - positive_balances:,.0f}")

# ============================================================
# STEP 6: Migration Summary
# ============================================================

print("\n--- Migration Summary ---")

print("""
DATA MIGRATION SUMMARY:

Source: Oracle Database
Target: Arrow/Parquet Data Lake

Schema Mapping:
  - NUMBER → int64/decimal128
  - VARCHAR2 → string
  - DATE → date32
  - TIMESTAMP → timestamp

Data Quality:
  ✓ Unique IDs validated
  ✓ Credit scores in range
  ✓ Positive balances
  ✓ Null handling preserved

Performance:
  - 500,000 records migrated
  - Decimal precision maintained
  - All data types mapped correctly

Benefits:
  ✓ Type safety
  ✓ Precision preservation
  ✓ Null handling
  ✓ Cross-platform compatibility
""")
```

---

## 🎯 5 Real-World Interview Questions

### Question 1: What is an Arrow schema and why is it important?

**Answer:**

**Arrow Schema:**
- Defines column names and data types
- Specifies nullability
- Contains metadata

**Importance:**
1. **Type Safety**: Guarantees data types
2. **Memory Efficiency**: Optimal storage per type
3. **Validation**: Ensures data quality
4. **Interoperability**: Cross-language compatibility

**Example:**
```python
import pyarrow as pa

schema = pa.schema([
    pa.field("id", pa.int64(), nullable=False),
    pa.field("name", pa.string(), nullable=True),
    pa.field("amount", pa.decimal128(18, 2))
])
```

---

### Question 2: How do you choose the right Arrow data type for banking data?

**Answer:**

**Data Type Selection:**

| Data | Arrow Type | Why |
|------|------------|-----|
| Transaction ID | string | May contain letters |
| Account Balance | decimal128(18,2) | Precise currency |
| Transaction Date | date32 | Date only |
| Transaction Time | timestamp | Date + Time |
| Customer Name | string | Variable length |
| Is Active | boolean | True/False |
| Credit Score | int32 | Integer range |

**Decimal Precision:**
```python
# Financial amounts
pa.decimal128(18, 2)  # 18 digits, 2 decimal places

# Example: 1234567890123456.78
```

---

### Question 3: How does Arrow handle schema evolution?

**Answer:**

**Schema Evolution Operations:**

1. **Add Column:**
```python
new_schema = schema.append(pa.field("new_col", pa.string()))
```

2. **Remove Column:**
```python
new_schema = pa.schema([f for f in schema if f.name != "old_col"])
```

3. **Type Widening:**
```python
# int32 → int64 (safe)
# float32 → float64 (safe)
# string(50) → string(100) (safe)
```

**Backward Compatibility:**
- New columns have default values
- Old queries still work
- No data rewrite needed

---

### Question 4: What are nullability constraints in Arrow schemas?

**Answer:**

**Nullability:**
- `nullable=True`: Column can have null values
- `nullable=False`: Column cannot have null values

**Example:**
```python
schema = pa.schema([
    pa.field("id", pa.int64(), nullable=False),  # Required
    pa.field("name", pa.string(), nullable=True),  # Optional
])
```

**Null Handling:**
- Null bitmap tracks validity
- Operations skip nulls automatically
- Null count precomputed

**Benefits:**
- Data quality enforcement
- Query optimization
- Memory efficiency

---

### Question 5: How do you validate data against an Arrow schema?

**Answer:**

**Validation Methods:**

1. **Schema Matching:**
```python
# Create table with schema
table = pa.table(data, schema=expected_schema)
```

2. **Type Checking:**
```python
# Verify field types
for field in table.schema:
    assert field.type == expected_type
```

3. **Null Checking:**
```python
# Check required fields have no nulls
for field in schema:
    if not field.nullable:
        assert table.column(field.name).null_count == 0
```

**Example:**
```python
import pyarrow as pa

# Define schema
schema = pa.schema([
    pa.field("id", pa.int64(), nullable=False),
    pa.field("name", pa.string(), nullable=True),
])

# Validate data
table = pa.table({"id": [1, 2], "name": ["A", None]}, schema=schema)

# Check required fields
assert table.column("id").null_count == 0
```

---

## 📝 Summary

| Aspect | Key Point |
|--------|-----------|
| **Definition** | Blueprint defining column names and types |
| **Types** | int, float, string, decimal, timestamp, etc. |
| **Nullability** | nullable=True/False |
| **Importance** | Type safety, memory efficiency, validation |
| **Evolution** | Add/remove columns, type widening |
| **Banking Use** | Decimal for currency, timestamp for audit |
