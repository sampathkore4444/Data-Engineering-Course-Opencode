# 09 - Data Streaming

## Table of Contents
1. [Stream Processing Concepts](#1-stream-processing-concepts)
2. [Windowing Operations](#2-windowing-operations)
3. [State Management](#3-state-management)
4. [Lambda vs Kappa Architecture](#4-lambda-vs-kappa-architecture)
5. [Interview Questions](#5-interview-questions)

---

## 1. Stream Processing Concepts

### Batch vs Streaming vs Micro-Batch

`
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
`

### Event-Driven Architecture

`
+--------+     +--------+     +--------+
| Event  |     | Event  |     | Event  |
| Source |     | Source |     | Source |
+---+----+     +---+----+     +---+----+
    |              |              |
    v              v              v
+--------------------------------------------------+
|              EVENT BROKER                        |
|              (Kafka / Kinesis)                    |
+--------------------------------------------------+
    |              |              |
    v              v              v
+--------+   +--------+   +--------+
|Consumer|   |Consumer|   |Consumer|
|   A    |   |   B    |   |   C    |
|(Real-  |   |(Batch  |   |(Alerts)|
| Time)  |   | ETL)   |   |        |
+--------+   +--------+   +--------+
`

### Event Sourcing

Store all state changes as a sequence of events.

`
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
`

### CQRS (Command Query Responsibility Segregation)

Separate read and write models.

`
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
`

### Delivery Semantics

`
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
`

---

## 2. Windowing Operations

### Tumbling Window
Fixed-size, non-overlapping windows.

`
Events:  |--e1--e2--e3--e4--e5--e6--e7--e8--|
Windows: [---Window 1---][---Window 2---][---Window 3---]
`

`sql
-- Flink SQL
SELECT 
    TUMBLE_START(event_time, INTERVAL '5' MINUTE) as window_start,
    COUNT(*) as event_count
FROM events
GROUP BY TUMBLE(event_time, INTERVAL '5' MINUTE);
`

### Sliding Window
Fixed-size, overlapping windows.

`
Events:  |--e1--e2--e3--e4--e5--e6--e7--e8--|
Windows: [---Window 1---]
            [---Window 2---]
              [---Window 3---]
                [---Window 4---]
`

`sql
SELECT 
    HOP_START(event_time, INTERVAL '1' MINUTE, INTERVAL '5' MINUTE) as window_start,
    AVG(metric) as moving_avg
FROM events
GROUP BY HOP(event_time, INTERVAL '1' MINUTE, INTERVAL '5' MINUTE);
`

### Session Window
Groups events by activity periods with gaps.

`
Events:  |--e1--e2----e3--e4--e5------e6--e7--|
Sessions: [Session 1][---Session 2---][Session 3]
           (active)  (gap > timeout) (active)
`

`sql
SELECT 
    SESSION_START(event_time, INTERVAL '30' MINUTE) as session_start,
    COUNT(*) as events_in_session
FROM events
GROUP BY SESSION(event_time, INTERVAL '30' MINUTE);
`

### Global Window
All events in a single window (use with triggers).

---

## 3. State Management

### Stateful vs Stateless Processing

`
STATELESS:
  Each event processed independently
  No memory of previous events
  Example: Filter, Map, FlatMap

STATEFUL:
  Processing depends on previous events
  Maintains state across events
  Example: Count, Sum, Join, Window aggregation
`

### State Backends

`
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
`

### Checkpointing

Periodic snapshot of state for fault tolerance.

`
Events:  |--e1--e2--e3--e4--e5--e6--e7--e8--|
                     |                       |
Checkpoints:        [CP1]                  [CP2]
                     |                       |
Recovery:  Start from CP1 if failure occurs between CP1 and CP2
`

### Exactly-Once with Checkpointing

`java
// Flink checkpointing configuration
env.enableCheckpointing(60000); // 1 minute
env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
env.getCheckpointConfig().setCheckpointTimeout(120000);
env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
env.setStateBackend(new RocksDBStateBackend("hdfs://checkpoints", true));
`

---

## 4. Lambda vs Kappa Architecture

### Lambda Architecture

`
+--------------------------------------------------+
|                 BATCH LAYER                       |
|  (Historical processing, batch views)            |
|  Source --> Hadoop/Spark --> Batch Views          |
+--------------------------------------------------+
                     |
                     v
+--------------------------------------------------+
|              SERVING LAYER                        |
|  (Merge batch and real-time views)               |
+--------------------------------------------------+
                     ^
                     |
+--------------------------------------------------+
|              SPEED LAYER                          |
|  (Real-time processing, incremental views)       |
|  Source --> Kafka/Flink --> Real-time Views       |
+--------------------------------------------------+
`

**Pros:** Handles both batch and real-time, fault-tolerant
**Cons:** Complex, two codebases, duplicate logic

### Kappa Architecture

`
+--------------------------------------------------+
|           SINGLE STREAM PROCESSING               |
|  All data as stream, reprocess by replaying      |
|                                                  |
|  Source --> Kafka --> Stream Processor --> Views  |
|                     (Flink/Spark)                |
+--------------------------------------------------+
`

**Pros:** Simpler, single codebase, less infrastructure
**Cons:** Stream processor must handle all use cases, replay can be slow

### When to Use Which?

| Scenario | Choose |
|----------|--------|
| Existing batch + need real-time | Lambda |
| New greenfield project | Kappa |
| Complex batch logic (ML training) | Lambda |
| Simple transformations only | Kappa |
| Need historical reprocessing | Both (Kappa easier) |

---

## 5. Real-World Streaming Scenarios

### Scenario 1: Real-Time Fraud Detection

`
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
`

### Scenario 2: E-Commerce Real-Time Inventory

`
Web Clicks --> Kafka --> Flink --> Real-time Inventory
                                    |
                                    +--> Website Display
                                    |
                                    +--> Alert System (Low Stock)
                                    |
                                    +--> Data Lake (Analytics)
`

---

## 6. Banking Examples

### Example 1: Real-Time Transaction Monitoring

`java
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
`

### Example 2: Real-Time P&L Calculation

`sql
-- Kafka Streams SQL
SELECT 
    account_id,
    SUM(CASE WHEN transaction_type = 'CREDIT' THEN amount ELSE 0 END) as credits,
    SUM(CASE WHEN transaction_type = 'DEBIT' THEN amount ELSE 0 END) as debits,
    SUM(CASE WHEN transaction_type = 'CREDIT' THEN amount ELSE -amount END) as net_pnl
FROM transactions
GROUP BY account_id;
`

---

## 7. E-Commerce Examples

### Example 1: Real-Time Recommendations

`python
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
`

### Example 2: Real-Time Price Monitoring

`sql
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
`

---

## 8. Interview Questions

### Q1: Explain event time vs processing time.

**Answer:** **Event time** is when the event actually occurred (e.g., when a transaction happened). **Processing time** is when the event is processed by the stream processor. They differ due to network delays, system load, and reprocessing. Event-time processing is more accurate but complex (requires watermarks for late data). Processing-time is simpler but may produce incorrect results for out-of-order events. Always prefer event-time processing for accuracy; use processing-time only for latency-critical approximations.

### Q2: What are watermarks in stream processing?

**Answer:** Watermarks track progress in event time, indicating "no events older than this timestamp should arrive." They enable: 1) **Window completion:** Know when to emit window results. 2) **Late data handling:** Define acceptable lateness. 3) **State cleanup:** Remove state for old windows. Without watermarks, you'd wait indefinitely for late events. Watermarks trade latency for completeness - tighter watermarks = lower latency but may miss late events; looser watermarks = higher latency but more complete results.

### Q3: Compare Kafka vs Kinesis vs Pub/Sub.

**Answer:** **Kafka:** Best for high throughput, exactly-once semantics, stream processing (Kafka Streams), and ecosystem (Connect, Schema Registry). Self-managed or Confluent Cloud. **Kinesis:** Best for AWS-native workloads, simpler operations, built-in integration with Lambda and Firehose. Auto-scaling shards. **Pub/Sub:** Best for GCP workloads, push-based delivery, global distribution, at-least-once delivery. All are durable message brokers; choice depends on cloud ecosystem, throughput needs, and operational preference.

### Q4: How do you handle late-arriving data?

**Answer:** Strategies: 1) **Watermarks with allowed lateness:** Wait N minutes before closing windows. 2) **Side outputs:** Route late events to separate stream for special handling. 3) **Triggering:** Emit early results, update when late data arrives. 4) **Grace period:** Keep state for window after it should have closed. 5) **Reprocessing:** For very late data, batch reprocess affected time periods. Flink handles this elegantly with llowedLateness() and side outputs.

### Q5: What is backpressure and how do you handle it?

**Answer:** **Backpressure** occurs when downstream consumers can't keep up with upstream producers, causing buffers to fill and potential data loss or latency. Solutions: 1) **Buffering:** Kafka buffers between stages. 2) **Dynamic scaling:** Auto-scale consumers based on lag. 3) **Rate limiting:** Throttle producers. 4) **Load shedding:** Drop low-priority events. 5) **Backpressure-aware operators:** Flink automatically propagates backpressure. 6) **Monitor consumer lag:** Set alerts when lag exceeds thresholds.

---

*Next Section: [10 - Data Governance](../10-Data-Governance/README.md)*
