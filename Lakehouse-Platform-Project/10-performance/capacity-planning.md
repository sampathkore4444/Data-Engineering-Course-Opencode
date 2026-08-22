# Capacity Planning Guide for Banking Data Platform

## Overview
This guide helps plan and size the banking data platform infrastructure based on workload requirements.

## Workload Analysis

### 1. Data Volume Estimation

| Data Source | Daily Volume | Monthly Volume | Annual Volume | Growth Rate |
|-------------|-------------|----------------|---------------|-------------|
| Core Banking Transactions | 500K rows | 15M rows | 180M rows | 20% |
| Credit Card Transactions | 1M rows | 30M rows | 360M rows | 25% |
| Loan Payments | 50K rows | 1.5M rows | 18M rows | 15% |
| Customer Data | 10K updates | 300K updates | 3.6M updates | 10% |
| Account Data | 20K updates | 600K updates | 7.2M updates | 10% |

### 2. Storage Requirements

| Data Layer | Compression | Retention | Storage Calculation |
|------------|-------------|-----------|---------------------|
| Bronze (Raw) | Snappy | 90 days | Daily volume × 3 × 90 days |
| Silver (Cleansed) | Snappy | 1 year | Daily volume × 2 × 365 days |
| Gold (Business) | Snappy | 7 years | Daily volume × 1 × 2555 days |
| Archives | GZIP | 7 years | Daily volume × 0.5 × 2555 days |

**Example Calculation:**
```
Core Banking Transactions:
- Daily: 500K rows × 1KB = 500MB
- Bronze: 500MB × 3 × 90 = 135GB
- Silver: 500MB × 2 × 365 = 365GB
- Gold: 500MB × 1 × 2555 = 1.28TB
- Total: ~1.8TB for transactions alone
```

### 3. Compute Requirements

| Workload | CPU Cores | Memory | Concurrent Users | Query Time Target |
|----------|-----------|--------|------------------|-------------------|
| Dremio Master | 8 | 32GB | 100 | < 1s |
| Dremio Executors | 16 each | 64GB each | 50 each | < 5s |
| Kafka Brokers | 4 each | 16GB each | N/A | < 100ms |
| PostgreSQL | 8 | 32GB | 50 | < 1s |
| MinIO | 4 each | 16GB each | N/A | < 100ms |

## Infrastructure Sizing

### 1. Dremio Cluster Sizing

```yaml
# Production cluster configuration
dremio:
  master:
    instances: 1
    cpu: 8 cores
    memory: 32GB
    storage: 100GB SSD
    
  executors:
    instances: 3
    cpu: 16 cores each
    memory: 64GB each
    storage: 500GB SSD each
    
  storage:
    total: 1.5TB SSD
    type: NVMe
    
  network:
    bandwidth: 10Gbps
    latency: < 1ms
```

### 2. Kafka Cluster Sizing

```yaml
kafka:
  brokers:
    instances: 3
    cpu: 4 cores each
    memory: 16GB each
    storage: 1TB SSD each
    
  zookeeper:
    instances: 3
    cpu: 2 cores each
    memory: 8GB each
    storage: 100GB SSD each
    
  topics:
    partitions: 12 per topic
    replication_factor: 3
    retention: 7 days
```

### 3. MinIO Cluster Sizing

```yaml
minio:
  nodes: 4
  cpu: 4 cores each
  memory: 16GB each
  storage: 10TB each (40TB total)
  type: HDD (with SSD cache)
  
  erasure_coding:
    data_shards: 8
    parity_shards: 4
    usable_capacity: 20TB
```

### 4. PostgreSQL Sizing

```yaml
postgresql:
  primary:
    cpu: 8 cores
    memory: 32GB
    storage: 1TB SSD
    
  replicas:
    instances: 2
    cpu: 4 cores each
    memory: 16GB each
    storage: 500GB SSD each
    
  backups:
    retention: 30 days
    storage: 2TB
```

## Performance Requirements

### 1. Query Performance Targets

| Query Type | Target Time | SLA | Priority |
|------------|-------------|-----|----------|
| Customer 360° | < 2 seconds | 99.9% | Critical |
| Fraud Detection | < 500ms | 99.99% | Critical |
| Daily Reports | < 30 seconds | 99.5% | High |
| Regulatory Reports | < 5 minutes | 99.0% | High |
| Ad-hoc Analytics | < 60 seconds | 95.0% | Medium |

### 2. Throughput Requirements

| Operation | Target Throughput | Concurrent Users | Priority |
|-----------|-------------------|------------------|----------|
| Real-time Queries | 1000 QPS | 100 | Critical |
| Batch Processing | 100 jobs/hour | 10 | High |
| Data Ingestion | 10K rows/second | N/A | High |
| Report Generation | 50 reports/hour | 20 | Medium |

### 3. Availability Requirements

| Component | Availability | RTO | RPO | Priority |
|-----------|--------------|-----|-----|----------|
| Dremio | 99.99% | 5 minutes | 0 | Critical |
| Kafka | 99.99% | 5 minutes | 0 | Critical |
| MinIO | 99.99% | 15 minutes | 1 hour | High |
| PostgreSQL | 99.99% | 5 minutes | 0 | Critical |
| Airflow | 99.9% | 30 minutes | 1 hour | Medium |

## Cost Estimation

### 1. Infrastructure Costs (Monthly)

| Component | Units | Unit Cost | Monthly Cost |
|-----------|-------|-----------|--------------|
| Dremio Master | 1 | $500 | $500 |
| Dremio Executors | 3 | $1,000 | $3,000 |
| Kafka Brokers | 3 | $300 | $900 |
| ZooKeeper | 3 | $100 | $300 |
| MinIO Nodes | 4 | $400 | $1,600 |
| PostgreSQL Primary | 1 | $500 | $500 |
| PostgreSQL Replicas | 2 | $300 | $600 |
| Network | 1 | $200 | $200 |
| **Total** | | | **$7,600** |

### 2. Storage Costs (Monthly)

| Storage Type | Capacity | Unit Cost | Monthly Cost |
|--------------|----------|-----------|--------------|
| SSD (Hot) | 5TB | $0.10/GB | $500 |
| SSD (Warm) | 10TB | $0.05/GB | $500 |
| HDD (Cold) | 20TB | $0.02/GB | $400 |
| **Total** | 35TB | | **$1,400** |

### 3. Total Cost of Ownership (Monthly)

| Category | Cost |
|----------|------|
| Infrastructure | $7,600 |
| Storage | $1,400 |
| Network | $200 |
| Monitoring | $100 |
| Backup | $200 |
| **Total** | **$9,500** |

## Scaling Strategies

### 1. Horizontal Scaling

```yaml
# Scale out when:
# - Query latency increases > 20%
# - CPU utilization > 70%
# - Memory utilization > 80%
# - Storage utilization > 70%

scaling:
  triggers:
    cpu_utilization: 70%
    memory_utilization: 80%
    storage_utilization: 70%
    query_latency: 20% increase
    
  actions:
    - Add Dremio executors
    - Add Kafka brokers
    - Add MinIO nodes
    - Add PostgreSQL replicas
```

### 2. Vertical Scaling

```yaml
# Scale up when:
# - Single node bottleneck
# - Memory pressure
# - CPU saturation

vertical_scaling:
  triggers:
    single_node_cpu: 90%
    single_node_memory: 90%
    
  actions:
    - Upgrade CPU cores
    - Increase memory
    - Upgrade storage type
```

### 3. Storage Scaling

```yaml
# Storage scaling strategy
storage_scaling:
  hot_storage:
    threshold: 70%
    action: Add SSD nodes
    
  warm_storage:
    threshold: 80%
    action: Add HDD nodes
    
  cold_storage:
    threshold: 90%
    action: Archive to object storage
```

## Monitoring and Alerting

### 1. Key Metrics to Monitor

| Metric | Warning | Critical | Action |
|--------|---------|----------|--------|
| CPU Utilization | > 70% | > 90% | Scale out |
| Memory Utilization | > 80% | > 90% | Scale up |
| Storage Utilization | > 70% | > 85% | Add storage |
| Query Latency | > 2s | > 5s | Optimize queries |
| Queue Depth | > 1000 | > 5000 | Add consumers |
| Error Rate | > 1% | > 5% | Investigate |

### 2. Capacity Dashboard

```sql
-- Capacity utilization dashboard
SELECT 
    'Dremio' AS component,
    (SELECT COUNT(*) FROM sys."query" 
     WHERE start_time >= DATEADD(HOUR, -1, CURRENT_TIMESTAMP())) AS queries_last_hour,
    (SELECT AVG(DATEDIFF(MILLISECOND, start_time, end_time)) 
     FROM sys."query" 
     WHERE start_time >= DATEADD(HOUR, -1, CURRENT_TIMESTAMP())) AS avg_latency_ms,
    (SELECT SUM(bytes_scanned) / 1024 / 1024 / 1024 
     FROM sys."query" 
     WHERE start_time >= DATEADD(HOUR, -1, CURRENT_TIMESTAMP())) AS data_scanned_gb

UNION ALL

SELECT 
    'Kafka' AS component,
    (SELECT SUM(messages_in) FROM kafka_metrics 
     WHERE timestamp >= DATEADD(HOUR, -1, CURRENT_TIMESTAMP())) AS messages_last_hour,
    NULL,
    NULL

UNION ALL

SELECT 
    'MinIO' AS component,
    NULL,
    NULL,
    (SELECT SUM(size_bytes) / 1024 / 1024 / 1024 
     FROM minio_buckets) AS total_storage_gb;
```

## Capacity Planning Checklist

- [ ] **Data Volume Analysis**
  - [ ] Current data volume
  - [ ] Growth rate estimation
  - [ ] Retention requirements
  - [ ] Archival strategy

- [ ] **Workload Analysis**
  - [ ] Query patterns
  - [ ] Concurrency requirements
  - [ ] Performance targets
  - [ ] SLA requirements

- [ ] **Infrastructure Sizing**
  - [ ] Compute resources
  - [ ] Storage resources
  - [ ] Network resources
  - [ ] Backup resources

- [ ] **Cost Estimation**
  - [ ] Infrastructure costs
  - [ ] Storage costs
  - [ ] Network costs
  - [ ] Operational costs

- [ ] **Scaling Strategy**
  - [ ] Horizontal scaling triggers
  - [ ] Vertical scaling triggers
  - [ ] Storage scaling triggers
  - [ ] Cost optimization

- [ ] **Monitoring Setup**
  - [ ] Key metrics definition
  - [ ] Alerting thresholds
  - [ ] Dashboard creation
  - [ ] Capacity reviews
