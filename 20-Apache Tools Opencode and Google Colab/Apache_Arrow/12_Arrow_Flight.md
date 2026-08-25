# Concept 12: Arrow Flight

## 📚 Detailed Explanation

**Arrow Flight** is a high-performance data transport framework for transferring Arrow data between processes and machines. It provides efficient, columnar data transfer over the network.

### What is Arrow Flight?

Arrow Flight is:
- **High-Performance**: Optimized for large datasets
- **Columnar**: Transfers Arrow data directly
- **gRPC-based**: Uses HTTP/2 for transport
- **Cross-Language**: Works with Python, Java, C++, etc.

### Why Arrow Flight?

**Without Flight:**
```
System A → Serialize → Network → Deserialize → System B
          (slow)                  (slow)
```

**With Flight:**
```
System A → Arrow IPC → Network → Arrow IPC → System B
          (fast)                  (fast)
```

### Arrow Flight Architecture

```
┌─────────────────┐         ┌─────────────────┐
│  Flight Client  │         │  Flight Server  │
│                 │  gRPC   │                 │
│  Arrow Data     │◄──────►│  Arrow Data     │
└─────────────────┘         └─────────────────┘
```

### Key Features

| Feature | Description |
|---------|-------------|
| **Columnar Transfer** | Direct Arrow data transfer |
| **Batching** | Efficient batch processing |
| **Authentication** | Secure data transfer |
| **Middleware** | Custom interceptors |

---

## 💡 Example: Arrow Flight in Banking

### Scenario: Data Transfer

```python
# Server side
from pyarrow import flight

class BankingFlightServer(flight.FlightServerBase):
    def do_get(self, context, ticket):
        # Read data
        table = read_data(ticket)
        return flight.RecordBatchStream(table)

# Client side
client = flight.connect("grpc://localhost:8815")
reader = client.do_get(flight.Ticket("transactions"))
table = reader.read_all()
```

---

## 🏦 Real-World Banking Scenario 1: Data Service

### Scenario
A bank's **data service** needs to:
- Serve data to multiple clients
- Handle large datasets
- Provide fast access

### Problem
- Network overhead
- Serialization costs
- Multiple clients

### Solution
Arrow Flight provides:
- Efficient data transfer
- Columnar format
- gRPC transport

### Python Code

```python
"""
Banking Scenario 1: Data Service
Using Arrow Flight
"""

import pyarrow as pa
import pyarrow.flight as flight
import random
import time
import threading

# ============================================================
# STEP 1: Define Flight Server
# ============================================================

print("=== DATA SERVICE WITH ARROW FLIGHT ===\n")

class BankingDataService(flight.FlightServerBase):
    """Arrow Flight server for banking data."""
    
    def __init__(self, data: pa.Table):
        super().__init__()
        self.data = data
    
    def do_get(self, context, ticket):
        """Handle data retrieval requests."""
        
        ticket_str = ticket.ticket.decode()
        
        if ticket_str == "transactions":
            return flight.RecordBatchStream(self.data)
        elif ticket_str == "summary":
            # Return summary data
            summary = self.data.group_by("branch").aggregate({
                "amount": "sum",
                "transaction_id": "count"
            })
            return flight.RecordBatchStream(summary)
        else:
            raise flight.FlightServerError(f"Unknown ticket: {ticket_str}")
    
    def list_flights(self, context, criteria):
        """List available flights."""
        
        # Return available endpoints
        endpoint = flight.FlightEndpoint(
            flight.Ticket(b"transactions"),
            [flight.Location.for_grpc("localhost", 8815)]
        )
        
        descriptor = flight_descriptor = pa.schema([])
        
        return [
            flight.FlightInfo(
                self.data.schema,
                flight_descriptor,
                [endpoint],
                self.data.num_rows,
                self.data.nbytes
            )
        ]

# ============================================================
# STEP 2: Generate Banking Data
# ============================================================

print("--- Generating Banking Data ---")

def generate_banking_data(num_records: int) -> pa.Table:
    """Generate banking data for Flight service."""
    
    data = {
        "transaction_id": [f"TXN-{i:010d}" for i in range(1, num_records + 1)],
        "account_id": [f"ACC-{random.randint(1000, 9999):06d}" for _ in range(num_records)],
        "amount": [round(random.uniform(100, 100000), 2) for _ in range(num_records)],
        "branch": [random.choice(["Mumbai", "Delhi", "Bangalore"]) for _ in range(num_records)],
        "status": [random.choice(["COMPLETED", "PENDING"]) for _ in range(num_records)],
    }
    
    return pa.table(data)

# Generate 1 million records
print("Generating 1 million records...")
banking_data = generate_banking_data(1000000)
print(f"Generated: {len(banking_data):,} records")

# ============================================================
# STEP 3: Start Flight Server
# ============================================================

print("\n--- Starting Flight Server ---")

# Create server
server = BankingDataService(banking_data)
location = flight.Location.for_grpc("localhost", 8815)
server = flight.FlightServerBase(location, server)

# Start server in background thread
def start_server():
    server.serve()

server_thread = threading.Thread(target=start_server, daemon=True)
server_thread.start()

print(f"Flight server started on localhost:8815")

# ============================================================
# STEP 4: Flight Client Operations
# ============================================================

print("\n--- Flight Client Operations ---")

# Connect to server
client = flight.connect("grpc://localhost:8815")

# Get transactions
start_time = time.time()
reader = client.do_get(flight.Ticket(b"transactions"))
transactions = reader.read_all()
get_time = time.time() - start_time

print(f"\nGet Transactions:")
print(f"  Records: {len(transactions):,}")
print(f"  Time: {get_time:.3f} seconds")

# Get summary
start_time = time.time()
reader = client.do_get(flight.Ticket(b"summary"))
summary = reader.read_all()
summary_time = time.time() - start_time

print(f"\nGet Summary:")
print(f"  Rows: {len(summary)}")
print(f"  Time: {summary_time:.3f} seconds")
print(summary.to_pandas().to_string(index=False))

# ============================================================
# STEP 5: Performance Comparison
# ============================================================

print("\n--- Performance Comparison ---")

# Compare Flight vs HTTP/JSON
import json

# Flight transfer
start_time = time.time()
reader = client.do_get(flight.Ticket(b"transactions"))
flight_data = reader.read_all()
flight_time = time.time() - start_time

# HTTP/JSON transfer (simulated)
start_time = time.time()
json_data = json.dumps(banking_data.to_pydict())
json_time = time.time() - start_time

print(f"\nTransfer Comparison:")
print(f"  Flight: {flight_time:.3f} seconds")
print(f"  JSON: {json_time:.3f} seconds")
print(f"  Flight is {json_time / flight_time:.1f}x faster")

# ============================================================
# STEP 6: Benefits Summary
# ============================================================

print("\n--- Benefits Summary ---")

print("""
ARROW FLIGHT BENEFITS:

1. HIGH PERFORMANCE
   - Columnar transfer
   - Zero-copy
   - gRPC transport

2. SCALABILITY
   - Multiple clients
   - Batch processing
   - Parallel transfers

3. SECURITY
   - Authentication
   - TLS encryption
   - Access control

4. FLEXIBILITY
   - Custom endpoints
   - Middleware support
   - Cross-language

5. USE CASES
   - Data services
   - Microservices
   - Distributed systems
   - Real-time analytics

ARCHITECTURE:
  Client → gRPC → Flight Server → Data Store
""")
```

---

## 🏦 Real-World Banking Scenario 2: Distributed Analytics

### Scenario
A bank's **distributed analytics platform** needs to:
- Transfer data between nodes
- Process large datasets
- Scale horizontally

### Problem
- Network bottleneck
- Data movement
- Scalability

### Solution
Arrow Flight provides:
- Efficient data transfer
- Columnar format
- Horizontal scaling

### Python Code

```python
"""
Banking Scenario 2: Distributed Analytics
Using Arrow Flight
"""

import pyarrow as pa
import pyarrow.flight as flight
import random
import time

# ============================================================
# STEP 1: Define Distributed Architecture
# ============================================================

print("=== DISTRIBUTED ANALYTICS WITH ARROW FLIGHT ===\n")

print("""
DISTRIBUTED ARCHITECTURE:

┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Node 1    │     │   Node 2    │     │   Node 3    │
│  (Master)   │────►│  (Worker)   │────►│  (Worker)   │
└─────────────┘     └─────────────┘     └─────────────┘
       │                   │                   │
       └───────────────────┴───────────────────┘
                           │
                    Arrow Flight
""")

# ============================================================
# STEP 2: Simulate Multi-Node Processing
# ============================================================

print("--- Simulating Multi-Node Processing ---")

def generate_node_data(node_id: int, num_records: int) -> pa.Table:
    """Generate data for a specific node."""
    
    data = {
        "node_id": [f"NODE-{node_id}"] * num_records,
        "transaction_id": [f"TXN-{node_id}-{i:08d}" for i in range(1, num_records + 1)],
        "amount": [round(random.uniform(100, 100000), 2) for _ in range(num_records)],
        "region": [random.choice(["North", "South", "East", "West"]) for _ in range(num_records)],
    }
    
    return pa.table(data)

# Generate data for 3 nodes
print("\nGenerating data for 3 nodes...")
node1_data = generate_node_data(1, 333333)
node2_data = generate_node_data(2, 333333)
node3_data = generate_node_data(3, 333334)

print(f"  Node 1: {len(node1_data):,} records")
print(f"  Node 2: {len(node2_data):,} records")
print(f"  Node 3: {len(node3_data):,} records")

# ============================================================
# STEP 3: Transfer Data Between Nodes
# ============================================================

print("\n--- Transferring Data Between Nodes ---")

# Simulate Flight transfer
start_time = time.time()

# Combine data from all nodes
combined = pa.concat_tables([node1_data, node2_data, node3_data])
transfer_time = time.time() - start_time

print(f"\nData Transfer:")
print(f"  Total records: {len(combined):,}")
print(f"  Transfer time: {transfer_time:.3f} seconds")

# ============================================================
# STEP 4: Distributed Aggregation
# ============================================================

print("\n--- Distributed Aggregation ---")

# Aggregate on each node
start_time = time.time()

# Node 1 aggregation
node1_agg = node1_data.group_by("region").aggregate({
    "amount": "sum",
    "transaction_id": "count"
})

# Node 2 aggregation
node2_agg = node2_data.group_by("region").aggregate({
    "amount": "sum",
    "transaction_id": "count"
})

# Node 3 aggregation
node3_agg = node3_data.group_by("region").aggregate({
    "amount": "sum",
    "transaction_id": "count"
})

agg_time = time.time() - start_time

print(f"\nLocal Aggregations:")
print(f"  Time: {agg_time:.3f} seconds")

# Transfer aggregated results
start_time = time.time()
combined_agg = pa.concat_tables([node1_agg, node2_agg, node3_agg])
transfer_agg_time = time.time() - start_time

print(f"\nAggregation Transfer:")
print(f"  Time: {transfer_agg_time:.3f} seconds")

# Final aggregation
start_time = time.time()
final_agg = combined_agg.group_by("region").aggregate({
    "amount_sum": "sum",
    "transaction_id_count": "sum"
})
final_agg_time = time.time() - start_time

print(f"\nFinal Aggregation:")
print(f"  Time: {final_agg_time:.3f} seconds")
print(final_agg.to_pandas().to_string(index=False))

# ============================================================
# STEP 5: Performance Summary
# ============================================================

print("\n--- Performance Summary ---")

print("""
DISTRIBUTED ANALYTICS PERFORMANCE:

Dataset: 1 million records across 3 nodes

Operations:
  - Data Generation: {transfer_time:.3f} seconds
  - Local Aggregation: {agg_time:.3f} seconds
  - Transfer Aggregation: {transfer_agg_time:.3f} seconds
  - Final Aggregation: {final_agg_time:.3f} seconds

Total Time: {total_time:.3f} seconds

PERFORMANCE CHARACTERISTICS:
  ✓ Efficient data transfer
  ✓ Local processing
  ✓ Minimal data movement
  ✓ Parallel aggregation

ARCHITECTURE BENEFITS:
  - Horizontal scaling
  - Fault tolerance
  - Load balancing
  - Geographic distribution
""")
```

---

## 🎯 5 Real-World Interview Questions

### Question 1: What is Arrow Flight and how is it different from gRPC?

**Answer:**

**Arrow Flight:**
- High-performance data transport
- Columnar data transfer
- Built on gRPC

**gRPC:**
- General-purpose RPC framework
- Protocol buffers
- Not optimized for columnar data

**Key Difference:**
- Flight: Optimized for Arrow data
- gRPC: General-purpose

---

### Question 2: How does Arrow Flight achieve high performance?

**Answer:**

**Performance Mechanisms:**

1. **Columnar Transfer:**
   - Direct Arrow data transfer
   - No serialization overhead
   - Memory-efficient

2. **Batching:**
   - Transfer data in batches
   - Reduce network overhead
   - Parallel processing

3. **gRPC:**
   - HTTP/2 multiplexing
   - Binary protocol
   - Connection pooling

**Example:**
```python
# Efficient batch transfer
reader = client.do_get(flight.Ticket(b"data"))
table = reader.read_all()
```

---

### Question 3: What are the use cases for Arrow Flight?

**Answer:**

**Use Cases:**

1. **Data Services:**
   - Serve data to applications
   - REST API alternatives
   - Real-time data access

2. **Distributed Analytics:**
   - Transfer data between nodes
   - Parallel processing
   - Aggregation

3. **Microservices:**
   - Service-to-service communication
   - Event streaming
   - Data pipelines

4. **Cloud Applications:**
   - Data lake access
   - Multi-region transfer
   - Cross-cloud data

---

### Question 4: How do you secure Arrow Flight connections?

**Answer:**

**Security Measures:**

1. **TLS Encryption:**
```python
# Server with TLS
server = flight.FlightServerBase(
    location,
    tls_certificates=[(cert, key)],
    auth_handler=auth_handler
)
```

2. **Authentication:**
```python
# Client with auth
client = flight.connect(
    "grpc://localhost:8815",
    credentials=flight.Credentials(token)
)
```

3. **Access Control:**
```python
# Middleware for access control
class AuthMiddleware(flight.ServerMiddleware):
    def checking_call_handler(self, context, method):
        # Check permissions
        pass
```

---

### Question 5: How do you monitor Arrow Flight performance?

**Answer:**

**Monitoring Metrics:**

1. **Transfer Metrics:**
   - Bytes transferred
   - Records transferred
   - Transfer time

2. **Latency Metrics:**
   - Request latency
   - Response latency
   - End-to-end latency

3. **Throughput Metrics:**
   - Records per second
   - Bytes per second
   - Concurrent connections

**Example:**
```python
# Monitor transfer
start_time = time.time()
reader = client.do_get(flight.Ticket(b"data"))
table = reader.read_all()
transfer_time = time.time() - start_time

print(f"Transfer: {len(table)} records in {transfer_time:.3f}s")
print(f"Throughput: {len(table)/transfer_time:.0f} records/sec")
```

---

## 📝 Summary

| Aspect | Key Point |
|--------|-----------|
| **Definition** | High-performance data transport |
| **Protocol** | gRPC-based |
| **Format** | Columnar Arrow data |
| **Features** | Batching, authentication, middleware |
| **Use Cases** | Data services, distributed analytics |
| **Performance** | Zero-copy, columnar transfer |
