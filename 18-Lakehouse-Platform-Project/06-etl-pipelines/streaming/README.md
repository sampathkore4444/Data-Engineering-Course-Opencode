# 🔄 Real-time Streaming Pipelines

> **Process data in real-time for fraud detection and live dashboards**

---

## 📋 Overview

Real-time streaming pipelines process data as it arrives, enabling:
- **Fraud Detection** - Detect suspicious transactions in milliseconds
- **Live Dashboards** - Real-time metrics for operations team
- **Instant Alerts** - Immediate notifications for critical events

---

## 📁 Files

| File | Purpose |
|------|---------|
| `fraud_detection_stream.py` | Detect fraudulent transactions in real-time |
| `realtime_metrics_stream.py` | Aggregate metrics for live dashboards |

---

## 🔄 How Streaming Works

### Batch vs Streaming

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    BATCH vs STREAMING                                    │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  BATCH PROCESSING (Airflow/dbt/Spark):                                 │
│  ┌─────────┐    ┌─────────┐    ┌─────────┐    ┌─────────┐            │
│  │ Collect │───►│ Wait    │───►│ Process │───►│ Output  │            │
│  │ 1 hour  │    │ 1 hour  │    │ 30 min  │    │         │            │
│  └─────────┘    └─────────┘    └─────────┘    └─────────┘            │
│  Latency: Minutes to Hours                                             │
│                                                                         │
│  STREAMING PROCESSING (Flink/Kafka Streams):                           │
│  ┌─────────────────────────────────────────────────────────────┐       │
│  │  Data ──► Process ──► Output (continuous)                   │       │
│  └─────────────────────────────────────────────────────────────┘       │
│  Latency: Milliseconds to Seconds                                      │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Streaming Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    REAL-TIME STREAMING ARCHITECTURE                      │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐         │
│  │ Source   │───►│  Kafka   │───►│  Flink   │───►│  Sink    │         │
│  │ Systems  │    │  Topics  │    │  Stream  │    │  (Output)│         │
│  └──────────┘    └──────────┘    └──────────┘    └──────────┘         │
│                                                                         │
│  Source Systems:                                                        │
│  • Core Banking (CDC)                                                   │
│  • Credit Cards (CDC)                                                   │
│  • Payment Gateway (API)                                                │
│                                                                         │
│  Kafka Topics:                                                          │
│  • card_transactions (raw transactions)                                 │
│  • fraud-alerts (detected fraud)                                        │
│  • realtime-metrics (aggregated metrics)                                │
│                                                                         │
│  Flink Processing:                                                      │
│  • Windowed aggregations (1-min, 5-min windows)                        │
│  • Fraud scoring (real-time rules)                                      │
│  • Pattern detection (velocity, geographic)                             │
│                                                                         │
│  Sinks:                                                                 │
│  • Kafka (for downstream consumers)                                     │
│  • Dremio (for dashboards)                                              │
│  • Alert System (for notifications)                                     │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 🚨 Pipeline 1: Fraud Detection

### Purpose
Detect fraudulent credit card transactions in real-time using rule-based scoring.

### Fraud Detection Rules

| Rule | Condition | Points |
|------|-----------|--------|
| **High Amount** | Transaction > VND 50M | 30 |
| **Velocity** | > 5 transactions in 1 hour | 40 |
| **Unusual Time** | Transaction between 12 AM - 5 AM | 20 |
| **Weekend** | Transaction on Saturday/Sunday | 10 |

### Alert Levels

| Score | Level | Action |
|-------|-------|--------|
| ≥ 80 | **CRITICAL** | Block transaction, alert customer + fraud team |
| ≥ 50 | **HIGH** | Flag for review, alert customer |
| ≥ 30 | **MEDIUM** | Log for pattern analysis |
| < 30 | **LOW** | No action |

### How It Works

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    FRAUD DETECTION FLOW                                  │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  Card Transaction ──► Kafka ──► Flink ──► Fraud Score ──► Alert        │
│                                                                         │
│  1. Transaction arrives at Kafka topic                                  │
│  2. Flink consumes transaction                                          │
│  3. Apply fraud rules and calculate score                              │
│  4. If score ≥ 30, generate alert                                       │
│  5. Send alert to fraud-alerts topic                                   │
│  6. Fraud team receives notification                                    │
│                                                                         │
│  Latency: < 1 second (from transaction to alert)                       │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Code Example

```python
# Fraud detection rule example
fraud_score = (
    # High amount rule (30 points)
    30 if amount > 50000000 else 0 +
    
    # Velocity rule (40 points)
    40 if txn_count_last_hour > 5 else 0 +
    
    # Unusual time rule (20 points)
    20 if hour in [0,1,2,3,4,5] else 0 +
    
    # Weekend rule (10 points)
    10 if day_of_week in [1,7] else 0
)

# Determine alert level
if fraud_score >= 80:
    alert_level = "CRITICAL"
elif fraud_score >= 50:
    alert_level = "HIGH"
elif fraud_score >= 30:
    alert_level = "MEDIUM"
else:
    alert_level = "LOW"
```

---

## 📊 Pipeline 2: Real-time Metrics

### Purpose
Aggregate transaction metrics in real-time for live dashboards.

### Metrics Calculated

| Metric | Window | Description |
|--------|--------|-------------|
| **Transaction Count** | 1 minute | Number of transactions per minute |
| **Total Amount** | 1 minute | Sum of transaction amounts |
| **Average Amount** | 1 minute | Average transaction amount |
| **Unique Accounts** | 1 minute | Number of unique accounts |
| **Success Rate** | 1 minute | Percentage of successful transactions |
| **High Value Count** | 5 minutes | Transactions > VND 10M |

### How It Works

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    REAL-TIME METRICS FLOW                                │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  Transaction ──► Kafka ──► Flink ──► Window Aggregation ──► Dashboard  │
│                                                                         │
│  1. Transaction arrives at Kafka topic                                  │
│  2. Flink consumes transaction                                          │
│  3. Add to 1-minute tumbling window                                     │
│  4. Aggregate metrics (count, sum, avg)                                 │
│  5. Output aggregated metrics                                           │
│  6. Dashboard displays live metrics                                     │
│                                                                         │
│  Latency: < 5 seconds (from transaction to dashboard)                  │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Window Types

| Window Type | Description | Use Case |
|-------------|-------------|----------|
| **Tumbling** | Fixed-size, non-overlapping | Transaction count per minute |
| **Sliding** | Fixed-size, overlapping | Average over last 5 minutes |
| **Session** | Activity-based | User session tracking |

---

## 🛠️ Running the Pipelines

### Prerequisites

```bash
# Install dependencies
pip install apache-flink
pip install kafka-python
```

### Running Fraud Detection

```bash
# Submit to Flink cluster
flink run \
  --parallelism 4 \
  fraud_detection_stream.py
```

### Running Real-time Metrics

```bash
# Submit to Flink cluster
flink run \
  --parallelism 2 \
  realtime_metrics_stream.py
```

### Running with Airflow

```bash
# Airflow can orchestrate streaming jobs
# See: airflow/dags/cdc_kafka_ingestion.py
```

---

## 📊 Monitoring

### Kafka Consumer Lag

```bash
# Check consumer lag
kafka-consumer-groups \
  --bootstrap-server kafka-1:9092 \
  --describe \
  --group fraud-detection
```

### Flink Metrics

```bash
# Access Flink UI
open http://localhost:8081

# Check:
# - Records processed
# - Processing latency
# - Backpressure
```

---

## 🎯 Best Practices

| Practice | Why |
|----------|-----|
| **Idempotent Processing** | Handle duplicate messages gracefully |
| **Window Sizing** | Balance latency vs accuracy |
| **State Management** | Use RocksDB for large state |
| **Checkpointing** | Enable fault tolerance |
| **Backpressure Handling** | Monitor and handle overload |

---

## 📚 References

- [Apache Flink Documentation](https://flink.apache.org/)
- [Kafka Streams Documentation](https://kafka.apache.org/documentation/streams/)
- [Real-time Fraud Detection Patterns](https://docs.confluent.io/)

---

*Built with ❤️ for Data Engineers learning Real-time Streaming and Banking Data Architecture*
