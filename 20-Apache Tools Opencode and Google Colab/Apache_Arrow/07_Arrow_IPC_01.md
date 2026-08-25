# Concept 07: Arrow IPC (Inter-Process Communication)

## 📚 Detailed Explanation

**Arrow IPC** is a format for efficiently serializing and deserializing Arrow data between processes. It enables zero-copy data sharing between different systems and languages.

### What is Arrow IPC?

Arrow IPC is:
- **Binary Format**: Efficient serialization 
- **Zero-Copy**: No deserialization overhead
- **Cross-Language**: Works with Python, Java, C++, etc.
- **Streamable**: Supports streaming data

### IPC vs Other Formats

| Format | Purpose | Zero-Copy | Use Case |
|--------|---------|-----------|----------|
| **Arrow IPC** | In-memory exchange | Yes | Process communication |
| **Parquet** | On-disk storage | No | Data lakes |
| **CSV** | Text exchange | No | Import/Export |
| **JSON** | Text exchange | No | APIs |

### IPC Components

**1. Schema:**
```python
import pyarrow as pa

# Define schema
schema = pa.schema([
    pa.field("id", pa.int64()),
    pa.field("name", pa.string()),
])
```

**2. Record Batch:**
```python
# Create record batch
batch = pa.record_batch({
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"]
}, schema=schema)
```

**3. IPC Message:**
```python
# Serialize to IPC
sink = pa.BufferOutputStream()
writer = pa.ipc.new_file(sink, schema)
writer.write_batch(batch)
writer.close()

# Get IPC buffer
ipc_buffer = sink.getvalue()
```

### IPC Operations

**Write IPC:**
```python
import pyarrow.ipc as ipc

# Write to file
with ipc.new_file("data.arrow", schema) as writer:
    writer.write_batch(batch)
```

**Read IPC:**
```python
# Read from file
with ipc.open_file("data.arrow") as reader:
    batch = reader.read_batch()
```

---

## 💡 Example: IPC in Banking

### Scenario: Process Communication

```python
import pyarrow as pa
import pyarrow.ipc as ipc

# ETL Process
def etl_process():
    # Process data
    data = pa.table({"id": [1, 2], "amount": [100, 200]})
    
    # Serialize to IPC
    sink = pa.BufferOutputStream()
    writer = ipc.new_file(sink, data.schema)
    writer.write_table(data)
    writer.close()
    
    return sink.getvalue()

# Analytics Process
def analytics_process(ipc_buffer):
    # Deserialize from IPC
    reader = ipc.open_file(pa.BufferReader(ipc_buffer))
    table = reader.read_all()
    
    # Analyze
    return table.column("amount").sum()
```

---

## 🏦 Real-World Banking Scenario 1: Microservices Communication

### Scenario
A bank has **microservices architecture**:
- Transaction Service
- Fraud Detection Service
- Analytics Service

### Problem
- Need efficient data exchange
- Low latency required
- Cross-language support

### Solution
Arrow IPC provides:
- Zero-copy data sharing
- Fast serialization
- Language agnostic

### Python Code

```python
"""
Banking Scenario 1: Microservices Communication
Using Arrow IPC
"""

import pyarrow as pa
import pyarrow.ipc as ipc
import random
import time

# ============================================================
# STEP 1: Define IPC Schema
# ============================================================

print("=== MICROSERVICES COMMUNICATION WITH ARROW IPC ===\n")

# Transaction schema
transaction_schema = pa.schema([
    pa.field("transaction_id", pa.string(), nullable=False),
    pa.field("account_id", pa.string(), nullable=False),
    pa.field("amount", pa.decimal128(18, 2), nullable=False),
    pa.field("timestamp", pa.timestamp("us"), nullable=False),
    pa.field("status", pa.string(), nullable=False),
])

print(f"Transaction Schema: {len(transaction_schema)} fields")

# ============================================================
# STEP 2: Transaction Service (Producer)
# ============================================================

print("\n--- Transaction Service (Producer) ---")

def transaction_service(num_transactions: int) -> pa.Buffer:
    """
    Simulate Transaction Service producing data.
    In production, this would be a REST API or message queue.
    """
    
    # Generate transaction data
    data = {
        "transaction_id": [f"TXN-{random.randint(10000000, 99999999)}" for _ in range(num_transactions)],
        "account_id": [f"ACC-{random.randint(1000, 9999):06d}" for _ in range(num_transactions)],
        "amount": [round(random.uniform(100, 100000), 2) for _ in range(num_transactions)],
        "timestamp": [pa.scalar(time.time() * 1000000, type=pa.timestamp("us")) for _ in range(num_transactions)],
        "status": [random.choice(["COMPLETED", "PENDING", "FAILED"]) for _ in range(num_transactions)],
    }
    
    # Create Arrow Table
    table = pa.table(data, schema=transaction_schema)
    
    # Serialize to IPC
    sink = pa.BufferOutputStream()
    writer = ipc.new_file(sink, transaction_schema)
    writer.write_table(table)
    writer.close()
    
    ipc_buffer = sink.getvalue()
    print(f"  Produced {num_transactions} transactions")
    print(f"  IPC buffer size: {ipc_buffer.size / 1024:.2f} KB")
    
    return ipc_buffer

# Produce transactions
ipc_buffer = transaction_service(10000)

# ============================================================
# STEP 3: Fraud Detection Service (Consumer)
# ============================================================

print("\n--- Fraud Detection Service (Consumer) ---")

def fraud_detection_service(ipc_buffer: pa.Buffer) -> pa.Table:
    """
    Simulate Fraud Detection Service consuming data.
    In production, this would be a separate microservice.
    """
    
    # Deserialize from IPC
    reader = ipc.open_file(pa.BufferReader(ipc_buffer))
    table = reader.read_all()
    
    print(f"  Received {len(table)} transactions")
    
    # Fraud detection logic (simplified)
    amounts = table.column("amount")
    
    # Flag high-value transactions
    high_value_mask = pa.compute.greater(amounts, pa.scalar(50000))
    high_value_count = pa.compute.sum(pa.array([1 if m else 0 for m in high_value_mask]))
    
    # Flag rapid transactions
    # (In production, would check timestamps)
    
    # Add fraud score
    fraud_scores = pa.array([round(random.uniform(0, 1), 4) for _ in range(len(table))])
    table = table.append_column("fraud_score", fraud_scores)
    
    # Add risk level
    risk_levels = pa.array([
        "HIGH" if score > 0.7 else "MEDIUM" if score > 0.4 else "LOW"
        for score in fraud_scores
    ])
    table = table.append_column("risk_level", risk_levels)
    
    print(f"  High-value transactions: {high_value_count.as_py():,}")
    print(f"  Fraud detection completed")
    
    return table

# Process with fraud detection
fraud_table = fraud_detection_service(ipc_buffer)

# ============================================================
# STEP 4: Analytics Service (Consumer)
# ============================================================

print("\n--- Analytics Service (Consumer) ---")

def analytics_service(ipc_buffer: pa.Buffer) -> dict:
    """
    Simulate Analytics Service consuming data.
    """
    
    # Deserialize from IPC
    reader = ipc.open_file(pa.BufferReader(ipc_buffer))
    table = reader.read_all()
    
    print(f"  Received {len(table)} transactions")
    
    # Analytics
    amounts = table.column("amount")
    
    stats = {
        "total": pa.compute.sum(amounts).as_py(),
        "mean": pa.compute.mean(amounts).as_py(),
        "min": pa.compute.min(amounts).as_py(),
        "max": pa.compute.max(amounts).as_py(),
    }
    
    # Status distribution
    status_counts = {}
    for status in ["COMPLETED", "PENDING", "FAILED"]:
        mask = pa.compute.equal(table.column("status"), status)
        count = pa.compute.sum(pa.array([1 if m else 0 for m in mask]))
        status_counts[status] = count.as_py()
    
    stats["status_distribution"] = status_counts
    
    print(f"  Analytics completed")
    
    return stats

# Process with analytics
analytics_stats = analytics_service(ipc_buffer)

# ============================================================
# STEP 5: Performance Comparison
# ============================================================

print("\n--- Performance Comparison ---")

# Compare IPC vs JSON
import json

# Generate sample data
sample_data = {
    "transaction_id": ["TXN-001", "TXN-002", "TXN-003"],
    "amount": [100.00, 200.00, 300.00],
}

# Serialize to IPC
start_time = time.time()
sink = pa.BufferOutputStream()
table = pa.table(sample_data)
writer = ipc.new_file(sink, table.schema)
writer.write_table(table)
writer.close()
ipc_buffer = sink.getvalue()
ipc_time = time.time() - start_time

# Serialize to JSON
start_time = time.time()
json_buffer = json.dumps(sample_data).encode()
json_time = time.time() - start_time

print(f"\nSerialization Comparison:")
print(f"  IPC: {ipc_time * 1000:.3f} ms ({ipc_buffer.size / 1024:.2f} KB)")
print(f"  JSON: {json_time * 1000:.3f} ms ({len(json_buffer) / 1024:.2f} KB)")
print(f"  IPC is {json_time / ipc_time:.1f}x faster")

# ============================================================
# STEP 6: Benefits Summary
# ============================================================

print("\n--- Benefits Summary ---")

print("""
ARROW IPC BENEFITS:

1. ZERO-COPY
   - No serialization overhead
   - Direct memory sharing
   - Fast data transfer

2. CROSS-LANGUAGE
   - Python, Java, C++, Rust
   - Same binary format
   - No conversion needed

3. EFFICIENCY
   - Binary format
   - Compressed
   - Streaming support

4. RELIABILITY
   - Schema validation
   - Type safety
   - Error detection

5. USE CASES
   - Microservices communication
   - Process interop
   - Data exchange
   - Streaming pipelines

MICROSERVICES ARCHITECTURE:
  Transaction Service → IPC → Fraud Detection
  Transaction Service → IPC → Analytics
  Transaction Service → IPC → Reporting
""")
```

---

## 🏦 Real-World Banking Scenario 2: Data Pipeline with IPC

### Scenario
A bank's **data pipeline** needs to:
- Extract data from source systems
- Transform in processing layer
- Load to analytics layer

### Problem
- Multiple stages
- Need efficient data transfer
- Low latency

### Solution
Arrow IPC provides:
- Efficient serialization
- Zero-copy transfers
- Streaming support

### Python Code

```python
"""
Banking Scenario 2: Data Pipeline with IPC
Using Arrow IPC for Efficient Data Transfer
"""

import pyarrow as pa
import pyarrow.ipc as ipc
import random
import time

# ============================================================
# STEP 1: Define Pipeline Stages
# ============================================================

print("=== DATA PIPELINE WITH ARROW IPC ===\n")

# Source schema
source_schema = pa.schema([
    pa.field("id", pa.int64(), nullable=False),
    pa.field("raw_data", pa.string(), nullable=False),
    pa.field("source", pa.string(), nullable=False),
    pa.field("timestamp", pa.timestamp("us"), nullable=False),
])

# Processed schema
processed_schema = pa.schema([
    pa.field("id", pa.int64(), nullable=False),
    pa.field("account_id", pa.string(), nullable=False),
    pa.field("amount", pa.decimal128(18, 2), nullable=False),
    pa.field("status", pa.string(), nullable=False),
    pa.field("processed_at", pa.timestamp("us"), nullable=False),
])

print(f"Pipeline Stages:")
print(f"  1. Extract (Source → Raw)")
print(f"  2. Transform (Raw → Processed)")
print(f"  3. Load (Processed → Analytics)")

# ============================================================
# STEP 2: Extract Stage (Source System)
# ============================================================

print("\n--- Extract Stage ---")

def extract_stage(num_records: int) -> pa.Buffer:
    """
    Extract data from source system.
    Simulates reading from Oracle/MySQL.
    """
    
    # Generate raw data
    data = {
        "id": list(range(1, num_records + 1)),
        "raw_data": [f"RAW-{random.randint(100000, 999999)}" for _ in range(num_records)],
        "source": ["CORE_BANKING"] * num_records,
        "timestamp": [pa.scalar(time.time() * 1000000, type=pa.timestamp("us")) for _ in range(num_records)],
    }
    
    # Create Arrow Table
    table = pa.table(data, schema=source_schema)
    
    # Serialize to IPC
    sink = pa.BufferOutputStream()
    writer = ipc.new_file(sink, source_schema)
    writer.write_table(table)
    writer.close()
    
    ipc_buffer = sink.getvalue()
    print(f"  Extracted {num_records} records")
    print(f"  IPC size: {ipc_buffer.size / 1024:.2f} KB")
    
    return ipc_buffer

# Extract data
extracted_buffer = extract_stage(100000)

# ============================================================
# STEP 3: Transform Stage (Processing Layer)
# ============================================================

print("\n--- Transform Stage ---")

def transform_stage(ipc_buffer: pa.Buffer) -> pa.Buffer:
    """
    Transform raw data to processed format.
    Simulates data cleansing and enrichment.
    """
    
    # Deserialize from IPC
    reader = ipc.open_file(pa.BufferReader(ipc_buffer))
    raw_table = reader.read_all()
    
    print(f"  Received {len(raw_table)} records")
    
    # Transform data
    processed_data = {
        "id": raw_table.column("id"),
        "account_id": pa.array([f"ACC-{random.randint(1000, 9999):06d}" for _ in range(len(raw_table))]),
        "amount": pa.array([round(random.uniform(100, 100000), 2) for _ in range(len(raw_table))]),
        "status": pa.array([random.choice(["COMPLETED", "PENDING"]) for _ in range(len(raw_table))]),
        "processed_at": pa.array([pa.scalar(time.time() * 1000000, type=pa.timestamp("us")) for _ in range(len(raw_table))]),
    }
    
    # Create processed table
    processed_table = pa.table(processed_data, schema=processed_schema)
    
    # Serialize to IPC
    sink = pa.BufferOutputStream()
    writer = ipc.new_file(sink, processed_schema)
    writer.write_table(processed_table)
    writer.close()
    
    ipc_buffer = sink.getvalue()
    print(f"  Transformed to {len(processed_table)} records")
    print(f"  IPC size: {ipc_buffer.size / 1024:.2f} KB")
    
    return ipc_buffer

# Transform data
transformed_buffer = transform_stage(extracted_buffer)

# ============================================================
# STEP 4: Load Stage (Analytics Layer)
# ============================================================

print("\n--- Load Stage ---")

def load_stage(ipc_buffer: pa.Buffer) -> pa.Table:
    """
    Load processed data to analytics layer.
    Simulates writing to data lake.
    """
    
    # Deserialize from IPC
    reader = ipc.open_file(pa.BufferReader(ipc_buffer))
    table = reader.read_all()
    
    print(f"  Loaded {len(table)} records")
    
    # In production, would write to Parquet/Iceberg
    # Here we just return the table
    
    return table

# Load data
analytics_table = load_stage(transformed_buffer)

# ============================================================
# STEP 5: Pipeline Performance
# ============================================================

print("\n--- Pipeline Performance ---")

# Measure end-to-end time
start_time = time.time()

# Run pipeline
extracted = extract_stage(100000)
transformed = transform_stage(extracted)
loaded = load_stage(transformed)

total_time = time.time() - start_time

print(f"\nPipeline Performance:")
print(f"  Total time: {total_time:.3f} seconds")
print(f"  Records per second: {len(loaded) / total_time:,.0f}")

# ============================================================
# STEP 6: Benefits Summary
# ============================================================

print("\n--- Benefits Summary ---")

print("""
ARROW IPC IN DATA PIPELINES:

1. EFFICIENT TRANSFER
   - Binary format
   - Zero-copy
   - Low overhead

2. STREAMING SUPPORT
   - Process data in chunks
   - Memory efficient
   - Scalable

3. RELIABILITY
   - Schema validation
   - Type safety
   - Error handling

4. INTEROPERABILITY
   - Cross-language
   - Cross-platform
   - Standard format

5. PERFORMANCE
   - Fast serialization
   - Fast deserialization
   - Minimal CPU usage

PIPELINE ARCHITECTURE:
  Source → IPC → Transform → IPC → Load → Analytics
  
  Each stage:
    - Deserialize IPC
    - Process data
    - Serialize IPC
""")
```

---

## 🎯 5 Real-World Interview Questions

### Question 1: What is Arrow IPC and how is it different from Parquet?

**Answer:**

**Arrow IPC:**
- In-memory format
- Zero-copy
- Fast serialization
- For process communication

**Parquet:**
- On-disk format
- Columnar storage
- Compressed
- For data lakes

**Key Difference:**
- IPC: RAM → RAM (fast)
- Parquet: RAM → Disk → RAM (slower)

---

### Question 2: How do you serialize and deserialize Arrow data with IPC?

**Answer:**

**Serialization:**
```python
import pyarrow as pa
import pyarrow.ipc as ipc

# Create table
table = pa.table({"a": [1, 2, 3]})

# Serialize to IPC
sink = pa.BufferOutputStream()
writer = ipc.new_file(sink, table.schema)
writer.write_table(table)
writer.close()

ipc_buffer = sink.getvalue()
```

**Deserialization:**
```python
# Deserialize from IPC
reader = ipc.open_file(pa.BufferReader(ipc_buffer))
table = reader.read_all()
```

---

### Question 3: What are the use cases for Arrow IPC?

**Answer:**

**Use Cases:**

1. **Microservices Communication:**
   - REST APIs
   - Message queues
   - Event streaming

2. **Data Pipelines:**
   - ETL processes
   - Stream processing
   - Batch processing

3. **Cross-Language Interop:**
   - Python ↔ Java
   - Python ↔ C++
   - Python ↔ R

4. **Process Communication:**
   - Parent-child processes
   - Parallel processing
   - Distributed computing

---

### Question 4: How does Arrow IPC achieve zero-copy?

**Answer:**

**Zero-Copy Mechanism:**

1. **Memory Mapping:**
   - Map file to memory
   - Direct access
   - No copying

2. **Reference Counting:**
   - Share buffers
   - Automatic cleanup
   - No duplication

3. **Buffer Sharing:**
   - Same memory
   - Multiple readers
   - No serialization

**Example:**
```python
# Both reference same memory
buffer1 = ipc_buffer
buffer2 = pa.BufferReader(ipc_buffer)

# No copy happens
```

---

### Question 5: What are the limitations of Arrow IPC?

**Answer:**

**Limitations:**

1. **In-Memory Only:**
   - Not for persistent storage
   - Use Parquet for disk

2. **Size Limitations:**
   - Large datasets may cause memory issues
   - Use batch processing

3. **Version Compatibility:**
   - Different Arrow versions may not be compatible
   - Need version matching

4. **Security:**
   - No built-in encryption
   - Need external security

**Mitigations:**
- Use Parquet for persistence
- Process in batches
- Version management
- External encryption

---

## 📝 Summary

| Aspect | Key Point |
|--------|-----------|
| **Definition** | Binary format for in-memory data exchange |
| **Key Feature** | Zero-copy |
| **Components** | Schema, Record Batch, IPC Message |
| **Operations** | Serialize, Deserialize |
| **Use Cases** | Microservices, Pipelines, Interop |
| **vs Parquet** | IPC = RAM, Parquet = Disk |
