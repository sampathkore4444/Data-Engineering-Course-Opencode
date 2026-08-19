# 09 - Data Streaming

## Table of Contents
1. [Stream Processing Concepts](#1-stream-processing-concepts)
2. [Windowing Operations](#2-windowing-operations)
3. [State Management](#3-state-management)
4. [Lambda vs Kappa Architecture](#4-lambda-vs-kappa-architecture)
5. [Real-World Streaming Scenarios](#5-real-world-streaming-scenarios)
6. [Banking Examples](#6-banking-examples)
7. [E-Commerce Examples](#7-e-commerce-examples)
8. [Hands-On Exercises](#8-hands-on-exercises)
9. [Interview Questions](#9-interview-questions)

---

## 1. Stream Processing Concepts

### Batch vs Streaming vs Micro-Batch

```
BATCH PROCESSING:
|--- Data ---|--- Data ---|--- Data ---|
      |            |            |
      v            v            v
  [Process]    [Process]    [Process]
  (Hourly)     (Hourly)     (Hourly)
  
Results available after batch completes (minutes to hours)

STREAM PROCESSING:
Data --> [Process] --> Results
Data --> [Process] --> Results
Data --> [Process] --> Results
(Continuous, real-time, event-by-event)

MICRO-BATCH:
|--- Data (1 min) ---|--- Data (1 min) ---|
        |                     |
        v                     v
    [Process]             [Process]
    (Near real-time, 1-5 second latency)
```

### Event-Driven Architecture

```
+--------+     +--------+     +--------+
| Event  |     | Event  |     | Event  |
| Source |     | Source |     | Source |
+---+----+     +---+----+     +---+----+
    |              |              |
    v              v              v
+--------------------------------------------------+
|              EVENT BROKER                        |
|    (Kafka, Kinesis, Pub/Sub, Event Hubs)         |
+--------------------------------------------------+
    |              |              |
    v              v              v
+--------+   +--------+   +--------+
|Consumer|   |Consumer|   |Consumer|
|   A    |   |   B    |   |   C    |
|(Real-  |   |(Batch  |   |(Alerts)|
| Time)  |   | ETL)   |   |        |
+--------+   +--------+   +--------+
```

### Event Sourcing

Store all state changes as a sequence of events.

```
Traditional (Current State):
+------------------+
| Account Balance  |
| =            |
+------------------+

Event Sourcing (History):
+------------------------------------------+
| Event Log                                |
| 1. AccountCreated: balance =           |
| 2. Deposit: +                       |
| 3. Withdrawal: -                     |
| 4. Deposit: +                        |
| 5. Withdrawal: -                     |
| Current:  (replayed from events)     |
+------------------------------------------+
```

### CQRS (Command Query Responsibility Segregation)

Separate read and write models.

```
Commands (Writes)              Queries (Reads)
+------------+                +------------+
|  Command   |                |   Query    |
|  Handler   |                |   Handler  |
+-----+------+                +------+-----+
      |                              |
      v                              v
+------------+                +------------+
|  Write DB  | --- Sync ---> |  Read DB   |
| (Normalized|               |(Denormalized|
|  for writes|               | for reads) |
+------------+                +------------+
```

### Modern Streaming Tools

| **Category**          | **Tools**                                                                | **Description**               |
| --------------------- | ------------------------------------------------------------------------ | ----------------------------- |
| **Message Brokers**   | Apache Kafka, AWS Kinesis, Google Pub/Sub, Azure Event Hubs              | Event ingestion and buffering |
| **Stream Processing** | Apache Flink, Spark Streaming, Kafka Streams, Apache Beam                | Real-time computation         |
| **Managed Services**  | Confluent Cloud, Amazon Kinesis, Google Dataflow, Azure Stream Analytics | Cloud-native streaming        |
| **Monitoring**        | Confluent Control Center, Burrow (Kafka lag), Datadog, Prometheus        | Stream monitoring             |
| **Schema Registry**   | Confluent Schema Registry, AWS Glue Schema Registry                      | Schema evolution              |


### Delivery Semantics

```
AT-MOST-ONCE:
  Message --> Process --> Ack (No retry on failure)
  Risk: Messages may be lost

AT-LEAST-ONCE:
  Message --> Process --> Fail --> Retry --> Process --> Ack
  Risk: Messages may be processed multiple times

EXACTLY-ONCE:
  Message --> Process --> Transaction --> Ack
  (Idempotent processing + transactions)
  Best for financial data
```

---

## 2. Windowing Operations

### Tumbling Window
Fixed-size, non-overlapping windows.

```
Events:  |--e1--e2--e3--e4--e5--e6--e7--e8--|
Windows: [---Window 1---][---Window 2---][---Window 3---]
```

```sql
-- Flink SQL
SELECT 
    TUMBLE_START(event_time, INTERVAL '5' MINUTE) as window_start,
    COUNT(*) as event_count
FROM events
GROUP BY TUMBLE(event_time, INTERVAL '5' MINUTE);
```

### Sliding Window
Fixed-size, overlapping windows.

```
Events:  |--e1--e2--e3--e4--e5--e6--e7--e8--|
Windows: [---Window 1---]
            [---Window 2---]
              [---Window 3---]
                [---Window 4---]
```

```sql
SELECT 
    HOP_START(event_time, INTERVAL '1' MINUTE, INTERVAL '5' MINUTE) as window_start,
    AVG(metric) as moving_avg
FROM events
GROUP BY HOP(event_time, INTERVAL '1' MINUTE, INTERVAL '5' MINUTE);
```

### Session Window
Groups events by activity periods with gaps.

```
Events:  |--e1--e2----e3--e4--e5------e6--e7--|
Sessions: [Session 1][---Session 2---][Session 3]
           (active)  (gap > timeout) (active)
```

```sql
SELECT 
    SESSION_START(event_time, INTERVAL '30' MINUTE) as session_start,
    COUNT(*) as events_in_session
FROM events
GROUP BY SESSION(event_time, INTERVAL '30' MINUTE);
```

### Global Window
All events in a single window (use with triggers).

---

## 3. State Management

### Stateful vs Stateless Processing

```
STATELESS:
  Each event processed independently
  No memory of previous events
  Example: Filter, Map, FlatMap

STATEFUL:
  Processing depends on previous events
  Maintains state across events
  Example: Count, Sum, Join, Window aggregation
```

### State Backends

```
Memory State Backend:
  +------------------+
  | Heap Memory      |
  | (Fast, Limited)  |
  +------------------+

RocksDB State Backend:
  +------------------+
  | Local Disk       |
  | (Persistent,     |
  |  Large state)    |
  +------------------+
```

### Checkpointing

Periodic snapshot of state for fault tolerance.

```
Events:  |--e1--e2--e3--e4--e5--e6--e7--e8--|
                     |                       |
Checkpoints:        [CP1]                  [CP2]
                     |                       |
Recovery:  Start from CP1 if failure occurs between CP1 and CP2
```

### Streaming Platforms Comparison

| **Platform**        | **Latency** | **Throughput** | **Exactly-Once**            | **Managed**          |
| ------------------- | ----------- | -------------- | --------------------------- | -------------------- |
| **Apache Kafka**    | ms          | Very High      | Yes (with Transactions)     | Self / Confluent     |
| **AWS Kinesis**     | ms          | High           | Yes (with enhanced fan-out) | Yes                  |
| **Google Pub/Sub**  | ms          | Very High      | At-least-once               | Yes                  |
| **Apache Flink**    | ms          | Very High      | Yes                         | Self / Managed       |
| **Spark Streaming** | seconds     | Very High      | Yes                         | Self / Databricks    |
| **Kafka Streams**   | ms          | High           | Yes                         | Library (no cluster) |


### Exactly-Once with Checkpointing

```java
// Flink checkpointing configuration
env.enableCheckpointing(60000); // 1 minute
env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
env.getCheckpointConfig().setCheckpointTimeout(120000);
env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
env.setStateBackend(new RocksDBStateBackend("hdfs://checkpoints", true));
```

---

## 4. Lambda vs Kappa Architecture

### Lambda Architecture

Lambda Architecture (coined by Nathan Marz) is a data processing architecture that combines batch and stream processing to handle massive quantities of data. It takes advantage of both batch and real-time processing methods.

```
+--------------------------------------------------+
|                 BATCH LAYER                       |
|  (Historical processing, batch views)            |
|                                                  |
|  Source --> Hadoop/Spark --> Batch Views          |
|  - Processes entire dataset                      |
|  - High latency, high throughput                 |
|  - Handles reprocessing                          |
|  - Provides comprehensive, accurate views        |
+--------------------------------------------------+
                     |
                     v
+--------------------------------------------------+
|              SERVING LAYER                        |
|  (Merge batch and real-time views)               |
|                                                  |
|  - Combines batch and real-time views            |
|  - Responds to queries                           |
|  - Provides unified query interface              |
+--------------------------------------------------+
                     ^
                     |
+--------------------------------------------------+
|              SPEED LAYER                          |
|  (Real-time processing, incremental views)       |
|                                                  |
|  Source --> Kafka/Flink --> Real-time Views       |
|  - Processes new data incrementally              |
|  - Low latency, handles recent data              |
|  - Provides approximate real-time views          |
+--------------------------------------------------+
```

**Key Concepts:**
- **Batch Layer:** Master dataset (immutable, append-only) + batch views (pre-computed)
- **Speed Layer:** Real-time views (supplementary, temporary)
- **Serving Layer:** Merges batch + speed views for query responses

**How it Works:**
1. New data enters both batch and speed layers simultaneously
2. Speed layer processes data quickly for low-latency views
3. Batch layer processes data comprehensively for accurate views
4. Serving layer queries both views and merges results
5. Once batch view is updated, corresponding speed view is discarded

**Pros:**
- Fault-tolerant and scalable
- Handles both batch and real-time use cases
- Batch layer provides complete accuracy
- Speed layer provides low latency
- Easy to reason about and debug

**Cons:**
- Complex (two codebases to maintain)
- Duplicate logic (batch + streaming)
- Higher operational cost
- Serving layer merging adds complexity

**Real-World Example:** Twitter's user timeline
- Batch layer: Compute user follows, interests (daily)
- Speed layer: Process new tweets, retweets (real-time)
- Serving layer: Merge for timeline display

### Kappa Architecture

Kappa Architecture (coined by Jay Kreps) is a simplification of Lambda Architecture that treats all data as streams. Instead of having separate batch and speed layers, it uses a single stream processing layer for all computation.

```
+--------------------------------------------------+
|           SINGLE STREAM PROCESSING               |
|  All data as stream, reprocess by replaying      |
|                                                  |
|  Source --> Kafka --> Stream Processor --> Views  |
|                     (Flink/Spark)                |
|                                                  |
|  Key: Kafka retains all historical data          |
|       Replay from beginning to reprocess         |
+--------------------------------------------------+
```

**Key Concepts:**
- **Single Processing Layer:** One stream processor handles all computation
- **Immutable Event Log:** Kafka retains all events (source of truth)
- **Reprocessing:** To fix errors or change logic, replay from Kafka
- **Versioned Applications:** Deploy new versions alongside old ones

**How it Works:**
1. All data enters Kafka and is retained indefinitely
2. Stream processor consumes events and produces views
3. To reprocess: deploy new version, replay from Kafka beginning
4. Old views are replaced by new views
5. Single codebase for all transformations

**Pros:**
- Simpler architecture (one codebase)
- Less infrastructure to manage
- Easier to maintain and debug
- No serving layer merging needed
- Reprocessing is straightforward

**Cons:**
- Stream processor must handle all use cases
- Replay can be slow for large historical data
- Requires robust stream processor (Flink/Kafka Streams)
- Kafka storage costs for retaining all data

**Real-World Example:** LinkedIn's real-time analytics
- All data flows through Kafka
- Stream processors (Kafka Streams) compute views
- Reprocessing done by replaying Kafka topics

### Lambda vs Kappa: Detailed Comparison

| **Aspect**            | **Lambda Architecture** | **Kappa Architecture**               |
| --------------------- | ----------------------- | ------------------------------------ |
| **Processing Layers** | Batch + Speed           | Single Stream                        |
| **Codebases**         | Two (batch + streaming) | One (streaming only)                 |
| **Reprocessing**      | Batch layer handles it  | Replay Kafka from the beginning      |
| **Complexity**        | Higher (two systems)    | Lower (one system)                   |
| **Operational Cost**  | Higher                  | Lower                                |
| **Latency**           | Low (speed layer)       | Low (stream processing)              |
| **Accuracy**          | High (batch layer)      | Depends on processor                 |
| **Best For**          | Mixed workloads         | Stream-native workloads              |
| **Learning Curve**    | Moderate                | Steep (requires streaming expertise) |
| **Maintenance**       | More complex            | Simpler                              |


### Migration Strategy: Lambda to Kappa

Many organizations start with Lambda and migrate to Kappa over time:

```
Phase 1: Lambda (Initial)
  Batch Layer (Spark) + Speed Layer (Flink)
  Both write to serving layer

Phase 2: Consolidate
  Move batch logic to streaming
  Use Kafka for replay capability
  Maintain both temporarily

Phase 3: Kappa (Target)
  Single Flink/Kafka Streams processor
  All views computed from stream
  Batch layer decommissioned
```

**Migration Tips:**
1. Start with non-critical batch jobs
2. Ensure Kafka retains sufficient history
3. Test replay performance before full migration
4. Maintain batch fallback during transition
5. Validate results match between old and new systems

### When to Use Which?

| **Scenario**                      | **Choose**                | **Reason**                           |
| --------------------------------- | ------------------------- | ------------------------------------ |
| Existing batch + need real-time   | **Lambda**                | Already have batch infrastructure    |
| New greenfield project            | **Kappa**                 | Simpler from the start               |
| Complex batch logic (ML training) | **Lambda**                | Batch is better for ML workloads     |
| Simple transformations only       | **Kappa**                 | Streaming handles them easily        |
| Need historical reprocessing      | **Both** *(Kappa easier)* | Both support it; Kappa is simpler    |
| High accuracy requirements        | **Lambda**                | Batch can ensure completeness        |
| Low-latency requirements          | **Kappa**                 | Single stream processing path        |
| Limited streaming expertise       | **Lambda**                | Easier to debug                      |
| Strong streaming team             | **Kappa**                 | Better leverages streaming expertise |


### Tools for Each Architecture

**Lambda Architecture Tools:**
- **Batch Layer:** Apache Spark, Hadoop MapReduce
- **Speed Layer:** Apache Flink, Apache Storm, Spark Streaming
- **Serving Layer:** Cassandra, HBase, Elasticsearch
- **Orchestration:** Apache Airflow, Oozie

**Kappa Architecture Tools:**
- **Stream Processing:** Apache Flink, Kafka Streams, Apache Beam
- **Message Broker:** Apache Kafka (central component)
- **State Store:** RocksDB, Redis
- **Serving Layer:** Same as processing output

### Example: E-Commerce Order Processing

**Lambda Implementation:**
```
Batch Layer:
  Daily: Process all orders, compute daily sales
  Output: Daily sales table (accurate, complete)

Speed Layer:
  Real-time: Process new orders as they arrive
  Output: Real-time sales feed (approximate)

Serving Layer:
  Query: Combine daily sales + real-time feed
  Display: Current sales totals
```

**Kappa Implementation:**
```
Single Stream:
  All orders flow through Kafka
  Flink processes each order:
    - Emit to 'daily_sales' topic
    - Update running totals
  Views:
    - daily_sales (materialized)
    - customer_totals (materialized)
  Reprocessing:
    - Deploy new Flink job
    - Replay Kafka from beginning
    - New views replace old ones
```

---

## 5. Real-World Streaming Scenarios

### Scenario 1: Real-Time Fraud Detection

```
Transaction Events
        |
        v
+------------------+
|    Kafka         |
| (Buffer/Queue)   |
+------------------+----+
        |              |
        v              v
+----------------+  +----------------+
| Flink Stream   |  | Spark          |
| Processing     |  | Streaming      |
| - Enrichment   |  | - Aggregation  |
| - ML Scoring   |  | - Reporting    |
| - Rule Engine  |  |                |
+--------+-------+  +--------+-------+
         |                   |
         v                   v
+----------------+  +----------------+
| Alert System   |  | Data Lake      |
| (Real-time     |  | (Batch         |
|  notifications)|  |  analytics)    |
+----------------+  +----------------+
```

### Scenario 2: E-Commerce Real-Time Inventory

```
Web Clicks --> Kafka --> Flink --> Real-time Inventory
                                    |
                                    +--> Website Display
                                    |
                                    +--> Alert System (Low Stock)
                                    |
                                    +--> Data Lake (Analytics)
```

---

## 6. Banking Examples

### Example 1: Real-Time Transaction Monitoring

```java
// Flink streaming job for transaction monitoring
DataStream<Transaction> transactions = env
    .addSource(new KafkaSource<>("transactions"))
    .assignTimestampsAndWatermarks(
        WatermarkStrategy.<Transaction>forBoundedOutOfOrderness(Duration.ofSeconds(5))
            .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
    );

// Detect suspicious patterns
DataStream<Alert> alerts = transactions
    .keyBy(Transaction::getCustomerId)
    .window(SlidingEventTimeWindows.of(Time.minutes(5), Time.minutes(1)))
    .aggregate(new TransactionAggregator())
    .filter(agg -> agg.getTotalAmount() > 10000 || agg.getCount() > 10)
    .map(agg -> new Alert(agg.getCustomerId(), "SUSPICIOUS_ACTIVITY", agg));
```

### Example 2: Real-Time P&L Calculation

```sql
-- Kafka Streams SQL
SELECT 
    account_id,
    SUM(CASE WHEN transaction_type = 'CREDIT' THEN amount ELSE 0 END) as credits,
    SUM(CASE WHEN transaction_type = 'DEBIT' THEN amount ELSE 0 END) as debits,
    SUM(CASE WHEN transaction_type = 'CREDIT' THEN amount ELSE -amount END) as net_pnl
FROM transactions
GROUP BY account_id;
```

---

## 7. E-Commerce Examples

### Example 1: Real-Time Recommendations

```python
# Spark Structured Streaming
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("RealTimeRecommendations") \
    .getOrCreate()

# Read clickstream events
clicks = spark.readStream \
    .format("kafka") \
    .option("subscribe", "clicks") \
    .load()

# Parse and process
parsed = clicks.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Sessionize and compute recommendations
recommendations = parsed \
    .withWatermark("event_time", "30 minutes") \
    .groupBy(
        window(col("event_time"), "30 minutes"),
        col("user_id")
    ) \
    .agg(
        collect_set(col("product_id")).alias("viewed_products"),
        count("*").alias("view_count")
    )

# Write to serving layer
recommendations.writeStream \
    .format("delta") \
    .outputMode("update") \
    .option("checkpointLocation", "/checkpoints/recommendations") \
    .start("/delta/recommendations")
```

### Example 2: Real-Time Price Monitoring

```sql
-- Detect price changes and trigger alerts
WITH price_changes AS (
    SELECT 
        product_id,
        current_price,
        LAG(current_price) OVER (PARTITION BY product_id ORDER BY event_time) as prev_price,
        event_time
    FROM price_updates
)
SELECT 
    product_id,
    prev_price,
    current_price,
    (current_price - prev_price) * 100.0 / prev_price as pct_change
FROM price_changes
WHERE ABS((current_price - prev_price) * 100.0 / prev_price) > 10;
```

---

## 8. Hands-On Exercises

### Exercise 1: Kafka Producer-Consumer (Python)
```python
from kafka import KafkaProducer, KafkaConsumer
import json
from datetime import datetime

# Task: Build a streaming pipeline with Kafka

# Producer
def create_producer():
    return KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all'  # Ensure durability
    )

def send_transaction(producer, topic, transaction):
    """Send transaction event to Kafka."""
    transaction['event_time'] = datetime.now().isoformat()
    future = producer.send(topic, value=transaction)
    record_metadata = future.get(timeout=10)
    print(f"Sent to partition {record_metadata.partition}, offset {record_metadata.offset}")

# Consumer with manual offset management
def create_consumer(group_id):
    return KafkaConsumer(
        'transactions',
        bootstrap_servers='localhost:9092',
        group_id=group_id,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=False  # Manual commit
    )

def process_transactions(consumer):
    """Process transactions with exactly-once semantics."""
    for message in consumer:
        transaction = message.value
        
        # Process transaction
        print(f"Processing: {transaction}")
        
        # Business logic
        if transaction.get('amount', 0) > 10000:
            print(f"  ALERT: Large transaction from {transaction.get('customer_id')}")
        
        # Manual commit after successful processing
        consumer.commit()

# Test
def test_kafka_streaming():
    producer = create_producer()
    
    # Send test transactions
    for i in range(5):
        send_transaction(producer, 'transactions', {
            'txn_id': f'TXN-{i:04d}',
            'customer_id': f'CUST-{i % 3:03d}',
            'amount': (i + 1) * 1000,
            'type': 'DEBIT' if i % 2 == 0 else 'CREDIT'
        })
    
    producer.flush()
    producer.close()
    print("\nAll transactions sent!")

test_kafka_streaming()
```

### Exercise 2: Windowed Aggregation (Flink SQL)
```sql
-- Task: Implement different window types for sensor data

-- Create sensor readings table
CREATE TABLE sensor_readings (
    sensor_id STRING,
    temperature DOUBLE,
    humidity DOUBLE,
    reading_time TIMESTAMP(3),
    WATERMARK FOR reading_time AS reading_time - INTERVAL '10' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'sensor-readings',
    'properties.bootstrap.servers' = 'localhost:9092',
    'format' = 'json'
);

-- Solution 1: Tumbling window (5-minute averages)
SELECT 
    TUMBLE_START(reading_time, INTERVAL '5' MINUTE) as window_start,
    sensor_id,
    AVG(temperature) as avg_temp,
    MAX(humidity) as max_humidity,
    COUNT(*) as reading_count
FROM sensor_readings
GROUP BY TUMBLE(reading_time, INTERVAL '5' MINUTE), sensor_id;

-- Solution 2: Sliding window (10-min window, 1-min slide)
SELECT 
    HOP_START(reading_time, INTERVAL '1' MINUTE, INTERVAL '10' MINUTE) as window_start,
    sensor_id,
    AVG(temperature) as moving_avg_temp
FROM sensor_readings
GROUP BY HOP(reading_time, INTERVAL '1' MINUTE, INTERVAL '10' MINUTE), sensor_id;

-- Solution 3: Session window (1-hour inactivity timeout)
SELECT 
    SESSION_START(reading_time, INTERVAL '1' HOUR) as session_start,
    SESSION_END(reading_time, INTERVAL '1' HOUR) as session_end,
    sensor_id,
    AVG(temperature) as avg_temp,
    COUNT(*) as readings
FROM sensor_readings
GROUP BY SESSION(reading_time, INTERVAL '1' HOUR), sensor_id;
```

### Exercise 3: State Management (Spark Structured Streaming)
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Task: Track customer session state

spark = SparkSession.builder \
    .appName("SessionTracking") \
    .master("local[*]") \
    .getOrCreate()

# Define schema
schema = StructType([
    StructField("user_id", StringType()),
    StructField("event_type", StringType()),
    StructField("event_time", TimestampType()),
    StructField("page", StringType())
])

# Read from Kafka
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "user-events") \
    .load()

# Parse events
parsed_stream = raw_stream.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Add watermarks for late data
watermarked_stream = parsed_stream \
    .withWatermark("event_time", "30 minutes")

# Session window aggregation
session_stream = watermarked_stream \
    .groupBy(
        col("user_id"),
        window(col("event_time"), "30 minutes")
    ) \
    .agg(
        collect_list(col("page")).alias("pages_visited"),
        count("*").alias("event_count"),
        min("event_time").alias("session_start"),
        max("event_time").alias("session_end")
    )

# Write output
query = session_stream.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("checkpointLocation", "/tmp/checkpoints/sessions") \
    .start()

query.awaitTermination(60)  # Run for 60 seconds
```

### Exercise 4: Late Data Handling
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Task: Handle late-arriving data with watermarks

spark = SparkSession.builder \
    .appName("LateDataHandling") \
    .master("local[*]") \
    .getOrCreate()

# Simulate streaming data with late events
schema = "user_id STRING, action STRING, event_time TIMESTAMP"

# Create sample data with late events
from datetime import datetime, timedelta

sample_data = [
    ("user1", "click", datetime(2024, 1, 1, 10, 0)),
    ("user1", "click", datetime(2024, 1, 1, 10, 5)),
    ("user2", "purchase", datetime(2024, 1, 1, 10, 10)),
    # Late event (5 minutes late)
    ("user1", "click", datetime(2024, 1, 1, 9, 55)),
]

df = spark.createDataFrame(sample_data, schema)

# Solution: Use watermark to handle late data
result = df \
    .withWatermark("event_time", "10 minutes") \
    .groupBy(
        window(col("event_time"), "15 minutes"),
        col("user_id")
    ) \
    .agg(
        count("*").alias("action_count"),
        collect_set(col("action")).alias("actions")
    )

result.show(truncate=False)

# Explanation: Watermark of 10 minutes means:
# - Events up to 10 minutes late are included
# - Events older than 10 minutes are dropped
# - Window state is cleaned up after watermark passes
```

### Exercise 5: Exactly-Once Processing
```python
from kafka import KafkaProducer, KafkaConsumer
import json
from datetime import datetime

# Task: Implement idempotent processing for exactly-once

class IdempotentProcessor:
    def __init__(self):
        self.processed_ids = set()  # Track processed event IDs
    
    def process_event(self, event):
        """Process event with idempotency check."""
        event_id = event.get('event_id')
        
        # Check if already processed
        if event_id in self.processed_ids:
            print(f"Skipping duplicate event: {event_id}")
            return None
        
        # Process the event
        result = self._transform(event)
        
        # Mark as processed
        self.processed_ids.add(event_id)
        
        return result
    
    def _transform(self, event):
        """Transform event."""
        return {
            'processed_id': event['event_id'],
            'user_id': event['user_id'],
            'action': event['action'],
            'processed_at': datetime.now().isoformat()
        }

# Test idempotency
def test_idempotent_processing():
    processor = IdempotentProcessor()
    
    events = [
        {'event_id': 'E001', 'user_id': 'U1', 'action': 'click'},
        {'event_id': 'E002', 'user_id': 'U1', 'action': 'purchase'},
        {'event_id': 'E001', 'user_id': 'U1', 'action': 'click'},  # Duplicate
        {'event_id': 'E003', 'user_id': 'U2', 'action': 'click'},
        {'event_id': 'E002', 'user_id': 'U1', 'action': 'purchase'},  # Duplicate
    ]
    
    for event in events:
        result = processor.process_event(event)
        if result:
            print(f"Processed: {result}")
    
    print(f"\nTotal events processed: {len(processor.processed_ids)}")
    print(f"Unique events: {processor.processed_ids}")

test_idempotent_processing()
```

---

## 9. Interview Questions

### Q1: Explain event time vs processing time.

**Answer:** 

**Event time** is when the event actually occurred (e.g., when a transaction happened). 

**Processing time** is when the event is processed by the stream processor. They differ due to network delays, system load, and reprocessing. Event-time processing is more accurate but complex (requires watermarks for late data). Processing-time is simpler but may produce incorrect results for out-of-order events. Always prefer event-time processing for accuracy; use processing-time only for latency-critical approximations.

### Q2: What are watermarks in stream processing?

**Answer:** Watermarks track progress in event time, indicating "no events older than this timestamp should arrive." 
They enable: 

1) **Window completion:** Know when to emit window results. 

2) **Late data handling:** Define acceptable lateness. 

3) **State cleanup:** Remove state for old windows. Without watermarks, you'd wait indefinitely for late events. Watermarks trade latency for completeness - tighter watermarks = lower latency but may miss late events; looser watermarks = higher latency but more complete results.

### Q3: Compare Kafka vs Kinesis vs Pub/Sub.

**Answer:** 
**Kafka:** Best for high throughput, exactly-once semantics, stream processing (Kafka Streams), and ecosystem (Connect, Schema Registry). Self-managed or Confluent Cloud. 

**Kinesis:** Best for AWS-native workloads, simpler operations, built-in integration with Lambda and Firehose. Auto-scaling shards. 

**Pub/Sub:** Best for GCP workloads, push-based delivery, global distribution, at-least-once delivery. All are durable message brokers; choice depends on cloud ecosystem, throughput needs, and operational preference.

### Q4: How do you handle late-arriving data?

**Answer:** Strategies: 
1) **Watermarks with allowed lateness:** Wait N minutes before closing windows. 
2) **Side outputs:** Route late events to separate stream for special handling. 
3) **Triggering:** Emit early results, update when late data arrives. 
4) **Grace period:** Keep state for window after it should have closed. 
5) **Reprocessing:** For very late data, batch reprocess affected time periods. Flink handles this elegantly with llowedLateness() and side outputs.

### Q5: What is backpressure and how do you handle it?

**Answer:** **Backpressure** occurs when downstream consumers can't keep up with upstream producers, causing buffers to fill and potential data loss or latency. Solutions: 
1) **Buffering:** Kafka buffers between stages. 
2) **Dynamic scaling:** Auto-scale consumers based on lag. 
3) **Rate limiting:** Throttle producers. 
4) **Load shedding:** Drop low-priority events. 
5) **Backpressure-aware operators:** Flink automatically propagates backpressure. 
6) **Monitor consumer lag:** Set alerts when lag exceeds thresholds.

### Q6: What is event sourcing and when should you use it?

**Answer:** Event sourcing stores all state changes as a sequence of immutable events, rather than just the current state. Use it when:
- Complete audit trail is required (banking, healthcare)
- You need to replay/correct historical data
- Debugging complex state transitions
- Building event-driven microservices

Benefits: Full history, temporal queries, decoupled read/write models
Drawbacks: Increased storage, complexity, eventual consistency challenges

### Q7: Compare Kafka Streams vs Flink vs Spark Streaming.

**Answer:**
**Kafka Streams:**
- Library (no separate cluster needed)
- Best for simple Kafka-to-Kafka transformations
- Exactly-once with Kafka transactions
- Lightweight, easy to deploy

**Apache Flink:**
- True stream processing (event-by-event)
- Best for complex event processing
- Advanced state management
- Exactly-once with checkpointing

**Spark Structured Streaming:**
- Micro-batch processing
- Best for batch+stream unification
- Familiar Spark API
- Good for ML integration

### Q8: Explain Lambda vs Kappa architecture and when to use each.

**Answer:**
**Lambda Architecture:**
- Combines batch and stream processing
- Batch layer for comprehensive, accurate views
- Speed layer for low-latency, real-time views
- Serving layer merges both views
- Best for: Mixed workloads, existing batch systems, high accuracy needs

**Kappa Architecture:**
- Single stream processing layer only
- All data treated as streams
- Reprocessing done by replaying from Kafka
- Simpler, one codebase
- Best for: Stream-native workloads, new projects, low complexity

**Key Difference:** Lambda has two paths (batch + stream), Kappa has one (stream only). Lambda is better when you need batch for complex ML workloads; Kappa is better for simpler transformations and when you want operational simplicity.

---

## Summary Checklist

### Core Concepts
- [ ] Understand batch vs streaming vs micro-batch
- [ ] Know event-driven architecture patterns
- [ ] Explain event sourcing and CQRS
- [ ] Understand delivery semantics (at-most-once, at-least-once, exactly-once)

### Windowing Operations
- [ ] Implement tumbling windows
- [ ] Use sliding windows for moving averages
- [ ] Apply session windows for user activity tracking

### State Management
- [ ] Know stateful vs stateless processing
- [ ] Understand checkpointing for fault tolerance
- [ ] Configure exactly-once semantics

### Architecture Patterns
- [ ] Understand Lambda Architecture (batch + speed layers)
- [ ] Understand Kappa Architecture (single stream processing)
- [ ] Compare pros/cons of each approach
- [ ] Choose appropriate architecture for use case
- [ ] Know migration strategies (Lambda to Kappa)

### Modern Tools
- [ ] Know Kafka, Kinesis, Pub/Sub differences
- [ ] Understand Flink vs Spark Streaming vs Kafka Streams
- [ ] Familiar with monitoring tools (Burrow, Confluent Control Center)

### Practical Skills
- [ ] Build Kafka producer-consumer pipelines
- [ ] Implement windowed aggregations
- [ ] Handle late-arriving data with watermarks
- [ ] Design idempotent processing for exactly-once

---

*Next Section: [10 - Data Governance](../10-Data-Governance/README.md)*
