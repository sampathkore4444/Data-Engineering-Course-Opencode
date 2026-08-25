# Concept 06: Arrow Memory Management

## 📚 Detailed Explanation

**Arrow Memory Management** is crucial for efficient data processing. Arrow provides sophisticated memory allocation, pooling, and zero-copy mechanisms to optimize performance.

### Why Memory Management Matters

**Without Proper Management:**
```
System 1: Allocate 1GB
System 2: Allocate 1GB
System 3: Allocate 1GB
Total: 3GB (with 1GB unused each)
```

**With Arrow Memory Pool:**
```
Memory Pool: 3GB shared
System 1: Allocate from pool
System 2: Allocate from pool
System 3: Allocate from pool
Total: 3GB (efficiently shared)
```

### Arrow Memory Architecture

```
┌─────────────────────────────────────┐
│         Application Layer           │
├─────────────────────────────────────┤
│       Arrow Data Structures         │
│    (Tables, Arrays, RecordBatches)  │
├─────────────────────────────────────┤
│         Memory Pool                 │
│    (Allocation & Deallocation)      │
├─────────────────────────────────────┤
│       System Memory (RAM)           │
└─────────────────────────────────────┘
```

### Memory Pool Types

| Pool Type | Description | Use Case |
|-----------|-------------|----------|
| **System Memory Pool** | Default, uses malloc/free | General purpose |
| **Jemalloc Memory Pool** | Better fragmentation | High-performance |
| **jemalloc** | Thread-safe | Multi-threaded |

### Key Concepts

**1. Buffer:**
```python
import pyarrow as pa

# Create buffer
buffer = pa.allocate_buffer(1024)  # 1KB buffer

# Buffer properties
print(f"Size: {buffer.size}")
print(f"Capacity: {buffer.capacity}")
print(f"Is immutable: {buffer.isImmutable}")
```

**2. Memory Pool:**
```python
# Get default memory pool
pool = pa.default_memory_pool()

# Check memory usage
print(f"Bytes allocated: {pool.bytes_allocated}")
print(f"Max memory: {pool.max_memory}")
```

**3. Zero-Copy:**
```python
# Create array
array = pa.array([1, 2, 3, 4, 5])

# Zero-copy slice (no data copied)
sliced = array.slice(1, 3)  # [2, 3, 4]

# Memory is shared
print(f"Original buffer: {array.buffers()}")
print(f"Sliced buffer: {sliced.buffers()}")
```

---

## 💡 Example: Memory Management in Banking

### Scenario: Large Dataset Processing

```python
import pyarrow as pa
import pyarrow.compute as pc

# Process large dataset efficiently
def process_large_dataset(data_size: int):
    # Allocate memory efficiently
    pool = pa.default_memory_pool()
    
    # Create arrays (memory allocated from pool)
    amounts = pa.array([round(random.uniform(10, 100000), 2) for _ in range(data_size)])
    
    # Process (zero-copy operations)
    total = pc.sum(amounts)
    mean = pc.mean(amounts)
    
    # Memory automatically returned to pool
    return total, mean
```

---

## 🏦 Real-World Banking Scenario 1: Memory-Efficient Batch Processing

### Scenario
A bank processes **10 million transactions daily** in batches. They need to:
- Minimize memory usage
- Process efficiently
- Avoid out-of-memory errors

### Problem
- Large datasets
- Limited memory
- Need efficient processing

### Solution
Arrow memory management provides:
- Efficient allocation
- Zero-copy operations
- Memory pooling

### Python Code

```python
"""
Banking Scenario 1: Memory-Efficient Batch Processing
Using Arrow Memory Management
"""

import pyarrow as pa
import pyarrow.compute as pc
import random
import gc
import sys

# ============================================================
# STEP 1: Setup Memory Monitoring
# ============================================================

print("=== MEMORY-EFFICIENT BATCH PROCESSING ===\n")

def get_memory_usage():
    """Get current memory usage."""
    pool = pa.default_memory_pool()
    return {
        "arrow_allocated": pool.bytes_allocated,
        "arrow_max": pool.max_memory,
    }

def print_memory_usage(label: str):
    """Print memory usage."""
    usage = get_memory_usage()
    print(f"\n{label}:")
    print(f"  Arrow allocated: {usage['arrow_allocated'] / 1024 / 1024:.2f} MB")
    print(f"  Arrow max: {usage['arrow_max'] / 1024 / 1024:.2f} MB")

# ============================================================
# STEP 2: Generate Data in Batches
# ============================================================

print("--- Generating Data in Batches ---")

def generate_batch(batch_size: int) -> pa.Table:
    """Generate a batch of transactions."""
    
    data = {
        "transaction_id": [f"TXN-{random.randint(10000000, 99999999)}" for _ in range(batch_size)],
        "account_id": [f"ACC-{random.randint(1000, 9999):06d}" for _ in range(batch_size)],
        "amount": [round(random.uniform(100, 100000), 2) for _ in range(batch_size)],
        "status": [random.choice(["COMPLETED", "PENDING", "FAILED"]) for _ in range(batch_size)],
    }
    
    return pa.table(data)

# Process in batches
batch_size = 100000
num_batches = 100
total_records = 0

print_memory_usage("Initial")

for batch_num in range(num_batches):
    # Generate batch
    batch = generate_batch(batch_size)
    total_records += len(batch)
    
    # Process batch
    amounts = batch.column("amount")
    batch_total = pc.sum(amounts).as_py()
    
    # Clear batch from memory
    del batch
    gc.collect()
    
    if batch_num % 20 == 0:
        print_memory_usage(f"After batch {batch_num}")

print_memory_usage("Final")
print(f"\nTotal records processed: {total_records:,}")

# ============================================================
# STEP 3: Zero-Copy Operations
# ============================================================

print("\n--- Zero-Copy Operations ---")

# Create large array
large_array = pa.array([round(random.uniform(10, 100000), 2) for _ in range(1000000)])
print(f"\nOriginal array size: {large_array.nbytes / 1024 / 1024:.2f} MB")

# Zero-copy slice
sliced = large_array.slice(0, 100000)
print(f"Sliced array size: {sliced.nbytes / 1024 / 1024:.2f} MB")

# Zero-copy filter
mask = pc.greater(large_array, 50000)
filtered = large_array.filter(mask)
print(f"Filtered array size: {filtered.nbytes / 1024 / 1024:.2f} MB")

# Memory is shared (no copy)
print(f"\nZero-copy benefits:")
print(f"  - No data duplication")
print(f"  - Fast operations")
print(f"  - Memory efficient")

# ============================================================
# STEP 4: Memory Pool Management
# ============================================================

print("\n--- Memory Pool Management ---")

# Get memory pool
pool = pa.default_memory_pool()

print(f"\nMemory Pool Statistics:")
print(f"  Bytes allocated: {pool.bytes_allocated / 1024 / 1024:.2f} MB")
print(f"  Max memory used: {pool.max_memory / 1024 / 1024:.2f} MB")

# Allocate and release
buffer1 = pa.allocate_buffer(1024 * 1024)  # 1MB
print(f"\nAfter allocating 1MB:")
print(f"  Bytes allocated: {pool.bytes_allocated / 1024 / 1024:.2f} MB")

# Release buffer
del buffer1
gc.collect()

print(f"\nAfter releasing 1MB:")
print(f"  Bytes allocated: {pool.bytes_allocated / 1024 / 1024:.2f} MB")

# ============================================================
# STEP 5: Efficient Aggregations
# ============================================================

print("\n--- Efficient Aggregations ---")

# Generate large dataset
large_table = pa.table({
    "category": [random.choice(["A", "B", "C", "D"]) for _ in range(1000000)],
    "amount": [round(random.uniform(100, 100000), 2) for _ in range(1000000)],
})

print(f"\nLarge table size: {large_table.nbytes / 1024 / 1024:.2f} MB")

# Efficient aggregation
result = large_table.group_by("category").aggregate({
    "amount": "sum"
})

print(f"\nAggregation result:")
for i in range(len(result)):
    cat = result.column("category")[i].as_py()
    total = result.column("amount_sum")[i].as_py()
    print(f"  {cat}: ${total:,.2f}")

# ============================================================
# STEP 6: Memory Optimization Tips
# ============================================================

print("\n--- Memory Optimization Tips ---")

print("""
MEMORY OPTIMIZATION TIPS:

1. USE DICTIONARY ENCODING
   - For categorical data
   - Reduces memory 30-50%

2. PROCESS IN BATCHES
   - Don't load all data at once
   - Use record batches

3. ZERO-COPY OPERATIONS
   - Use slicing instead of copying
   - Use filtering with masks

4. RELEASE MEMORY
   - Delete unused variables
   - Use garbage collection

5. CHOOSE RIGHT DATA TYPES
   - int32 vs int64
   - float32 vs float64
   - string vs large_string

EXAMPLES:
  ✓ pc.dictionary_encode() for categories
  ✓ table.to_batches() for streaming
  ✓ array.slice() for zero-copy
  ✓ del + gc.collect() for cleanup
""")
```

---

## 🏦 Real-World Banking Scenario 2: Streaming Data Processing

### Scenario
A bank's **streaming platform** processes **1 million events per minute**. They need:
- Low memory footprint
- Efficient processing
- No memory leaks

### Problem
- Continuous data flow
- Memory pressure
- Long-running processes

### Solution
Arrow memory management provides:
- Efficient streaming
- Memory pooling
- Automatic cleanup

### Python Code

```python
"""
Banking Scenario 2: Streaming Data Processing
Using Arrow Memory Management
"""

import pyarrow as pa
import pyarrow.compute as pc
import random
import gc

# ============================================================
# STEP 1: Setup Streaming Processor
# ============================================================

print("=== STREAMING DATA PROCESSING ===\n")

class StreamingProcessor:
    """Memory-efficient streaming processor."""
    
    def __init__(self):
        self.processed_count = 0
        self.total_amount = 0
        self.pool = pa.default_memory_pool()
    
    def process_event(self, event: pa.Table):
        """Process a single event batch."""
        
        # Extract amounts
        amounts = event.column("amount")
        
        # Update statistics
        self.processed_count += len(amounts)
        self.total_amount += pc.sum(amounts).as_py()
        
        # Clear event from memory
        del event
        gc.collect()
    
    def get_stats(self):
        """Get processing statistics."""
        return {
            "processed": self.processed_count,
            "total_amount": self.total_amount,
            "memory_used": self.pool.bytes_allocated,
        }

# ============================================================
# STEP 2: Generate Streaming Events
# ============================================================

print("--- Generating Streaming Events ---")

def generate_streaming_event(batch_size: int) -> pa.Table:
    """Generate a streaming event batch."""
    
    data = {
        "event_id": [f"EVT-{random.randint(10000000, 99999999)}" for _ in range(batch_size)],
        "account_id": [f"ACC-{random.randint(1000, 9999):06d}" for _ in range(batch_size)],
        "amount": [round(random.uniform(10, 50000), 2) for _ in range(batch_size)],
        "event_type": [random.choice(["TRANSACTION", "TRANSFER", "PAYMENT"]) for _ in range(batch_size)],
        "timestamp": [pa.scalar(1724500000)] * batch_size,
    }
    
    return pa.table(data)

# Process streaming events
processor = StreamingProcessor()
batch_size = 10000
num_batches = 1000

print(f"\nProcessing {num_batches} batches of {batch_size} events...")

for batch_num in range(num_batches):
    # Generate event
    event = generate_streaming_event(batch_size)
    
    # Process event
    processor.process_event(event)
    
    # Print progress every 100 batches
    if batch_num % 100 == 0:
        stats = processor.get_stats()
        print(f"  Batch {batch_num}: {stats['processed']:,} events, "
              f"${stats['total_amount']:,.2f} total")

# Final stats
stats = processor.get_stats()
print(f"\nFinal Statistics:")
print(f"  Total events: {stats['processed']:,}")
print(f"  Total amount: ${stats['total_amount']:,.2f}")
print(f"  Memory used: {stats['memory_used'] / 1024 / 1024:.2f} MB")

# ============================================================
# STEP 3: Memory Efficiency Analysis
# ============================================================

print("\n--- Memory Efficiency Analysis ---")

# Compare with naive approach
print(f"\nMemory Comparison:")
print(f"  Naive approach: {stats['processed'] * 100} bytes (if stored)")
print(f"  Arrow streaming: {stats['memory_used'] / 1024:.2f} KB")
print(f"  Efficiency: {(1 - stats['memory_used'] / (stats['processed'] * 100)) * 100:.1f}% savings")

# ============================================================
# STEP 4: Benefits Summary
# ============================================================

print("\n--- Benefits Summary ---")

print("""
STREAMING MEMORY BENEFITS:

1. EFFICIENT ALLOCATION
   - Memory pool management
   - Automatic deallocation
   - No memory leaks

2. ZERO-COPY OPERATIONS
   - Fast processing
   - No data duplication
   - Memory efficient

3. BATCH PROCESSING
   - Process in chunks
   - Control memory usage
   - Scalable

4. AUTOMATIC CLEANUP
   - Garbage collection
   - Reference counting
   - Memory release

5. MONITORING
   - Memory usage tracking
   - Pool statistics
   - Performance metrics

USE CASES:
  ✓ Real-time analytics
  ✓ Stream processing
  ✓ Long-running jobs
  ✓ Memory-constrained environments
""")
```

---

## 🎯 5 Real-World Interview Questions

### Question 1: What is Arrow's memory pool and why is it important?

**Answer:**

**Memory Pool:**
- Centralized memory allocation
- Shared across operations
- Automatic deallocation

**Importance:**
1. **Efficiency**: Reduces fragmentation
2. **Sharing**: Multiple operations share memory
3. **Tracking**: Monitor memory usage
4. **Control**: Manage memory limits

**Example:**
```python
import pyarrow as pa

pool = pa.default_memory_pool()
print(f"Bytes allocated: {pool.bytes_allocated}")
print(f"Max memory: {pool.max_memory}")
```

---

### Question 2: How does zero-copy work in Arrow?

**Answer:**

**Zero-Copy Mechanism:**
- Share memory buffers
- No data duplication
- Reference counting

**Example:**
```python
import pyarrow as pa

# Create array
array = pa.array([1, 2, 3, 4, 5])

# Zero-copy slice
sliced = array.slice(1, 3)  # Shares memory

# Both reference same buffer
print(f"Original buffers: {array.buffers()}")
print(f"Sliced buffers: {sliced.buffers()}")
```

**Benefits:**
- Fast operations
- Memory efficient
- No serialization overhead

---

### Question 3: How do you manage memory in long-running Arrow processes?

**Answer:**

**Memory Management Strategies:**

1. **Batch Processing:**
```python
for batch in table.to_batches(max_chunksize=10000):
    process(batch)
    del batch  # Release memory
```

2. **Garbage Collection:**
```python
import gc
gc.collect()  # Force garbage collection
```

3. **Memory Monitoring:**
```python
pool = pa.default_memory_pool()
if pool.bytes_allocated > limit:
    gc.collect()
```

4. **Use Streaming:**
```python
# Process in chunks, don't load all data
for batch in pq.read_table('data.parquet', batch_size=10000):
    process(batch)
```

---

### Question 4: What are the memory implications of different Arrow data types?

**Answer:**

**Memory Usage by Type:**

| Type | Size | Use Case |
|------|------|----------|
| int8 | 1 byte | Small integers |
| int32 | 4 bytes | Medium integers |
| int64 | 8 bytes | Large integers |
| float32 | 4 bytes | Moderate precision |
| float64 | 8 bytes | High precision |
| string | Variable | Text data |
| decimal128 | 16 bytes | Precise currency |

**Optimization Tips:**
- Use smallest appropriate type
- Use dictionary encoding for categories
- Use null bitmaps for sparse data

---

### Question 5: How do you detect and fix memory leaks in Arrow applications?

**Answer:**

**Detection Methods:**

1. **Monitor Memory Pool:**
```python
pool = pa.default_memory_pool()
initial = pool.bytes_allocated
# ... process ...
final = pool.bytes_allocated
if final > initial:
    print("Memory leak detected")
```

2. **Use Tracemalloc:**
```python
import tracemalloc
tracemalloc.start()
# ... process ...
print(tracemalloc.get_traced_memory())
```

**Fixes:**
1. Delete unused variables
2. Use explicit garbage collection
3. Process in batches
4. Use context managers

---

## 📝 Summary

| Aspect | Key Point |
|--------|-----------|
| **Memory Pool** | Centralized allocation |
| **Zero-Copy** | Share memory buffers |
| **Batch Processing** | Control memory usage |
| **Monitoring** | Track allocation |
| **Optimization** | Dictionary encoding, right types |
| **Use Cases** | Streaming, long-running, large data |
