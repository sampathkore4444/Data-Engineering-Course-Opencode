# Troubleshooting Guide - Banking Data Platform

## Common Issues and Solutions

### 1. Dremio Issues

#### Issue: Dremio Won't Start

**Symptoms:**
- Container exits immediately
- Port 9047 not accessible

**Diagnosis:**
```bash
# Check container logs
docker-compose logs dremio-master

# Check disk space
df -h /opt/dremio/data

# Check memory
free -h
```

**Solutions:**
```bash
# 1. Clean up Docker resources
docker system prune -a

# 2. Remove Dremio data (WARNING: This deletes all data)
rm -rf /opt/dremio/data/*

# 3. Restart Dremio
docker-compose restart dremio-master
```

#### Issue: Dremio High Memory Usage

**Symptoms:**
- Dremio becomes unresponsive
- OOM errors in logs

**Diagnosis:**
```bash
# Check memory usage
docker stats dremio-master

# Check Java heap
docker exec dremio-master jcmd 1 GC.heap_info
```

**Solutions:**
```yaml
# docker-compose.yml
services:
  dremio-master:
    environment:
      - DREMIO_MAX_MEMORY=16g
      - DREMIO_JAVA_SERVER_EXTRA_OPTS=-Xmx12g
```

#### Issue: Dremio Reflection Stale

**Symptoms:**
- Queries returning old data
- Reflection refresh failing

**Diagnosis:**
```sql
-- Check reflection status
SELECT 
    reflection_name,
    status,
    refresh_time,
    DATEDIFF(HOUR, refresh_time, CURRENT_TIMESTAMP()) AS hours_since_refresh
FROM sys."reflection"
WHERE status != 'ACTIVE';
```

**Solutions:**
```sql
-- Manual refresh
ALTER VIEW banking_gold.customer_360 
REFRESH REFLECTION customer_360_raw;

-- Check refresh logs
SELECT * FROM sys."job" 
WHERE job_type = 'REFLECTION_REFRESH'
ORDER BY start_time DESC;
```

---

### 2. Kafka Issues

#### Issue: Kafka Broker Down

**Symptoms:**
- Producer/consumer errors
- Topic leader not available

**Diagnosis:**
```bash
# Check broker status
docker exec kafka-1 kafka-broker-api-versions --bootstrap-server localhost:9092

# Check topic status
docker exec kafka-1 kafka-topics --bootstrap-server localhost:9092 --describe --under-replicated-partitions
```

**Solutions:**
```bash
# Restart broker
docker-compose restart kafka-1

# Check Zookeeper
docker exec zookeeper ruok
```

#### Issue: Kafka Consumer Lag

**Symptoms:**
- Consumer lag increasing
- Data not flowing to Silver layer

**Diagnosis:**
```bash
# Check consumer group lag
docker exec kafka-1 kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group silver-transform

# Check consumer status
docker exec kafka-1 kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --list
```

**Solutions:**
```bash
# Reset consumer group offset
docker exec kafka-1 kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group silver-transform \
  --reset-offsets --to-latest --topic transactions --execute

# Increase consumer instances
docker-compose up -d --scale airflow-worker=3
```

#### Issue: Kafka Topic Partition Imbalance

**Symptoms:**
- Some partitions much larger than others
- Skewed consumer processing

**Diagnosis:**
```bash
# Check partition sizes
docker exec kafka-1 kafka-log-dirs --bootstrap-server localhost:9092 --describe --topic transactions

# Check partition distribution
docker exec kafka-1 kafka-topics --bootstrap-server localhost:9092 --describe --topic transactions
```

**Solutions:**
```bash
# Reassign partitions
docker exec kafka-1 kafka-reassign-partitions \
  --bootstrap-server localhost:9092 \
  --reassignment-json-file reassignment.json \
  --execute
```

---

### 3. MinIO Issues

#### Issue: MinIO Disk Space Full

**Symptoms:**
- Write errors in logs
- Cannot create new objects

**Diagnosis:**
```bash
# Check disk usage
df -h /data/minio

# Check bucket sizes
docker exec minio mc du --recursive local/banking-lake/
```

**Solutions:**
```bash
# Archive old data
docker exec minio mc mv local/banking-lake/bronze/2023/ local/banking-archive/bronze/2023/

# Delete expired data
docker exec minio mc rm --recursive --force local/banking-lake/bronze/2022/

# Add more storage
# (Add new disk and expand cluster)
```

#### Issue: MinIO High Latency

**Symptoms:**
- Slow data reads/writes
- Timeouts in Dremio

**Diagnosis:**
```bash
# Check MinIO metrics
curl http://localhost:9000/minio/v2/metrics/cluster

# Check disk I/O
iostat -x 1 5
```

**Solutions:**
```bash
# Optimize MinIO configuration
# Add SSD cache
# Increase network bandwidth
# Use erasure coding
```

---

### 4. PostgreSQL Issues

#### Issue: PostgreSQL Connection Refused

**Symptoms:**
- Cannot connect to database
- Connection timeout errors

**Diagnosis:**
```bash
# Check PostgreSQL status
docker exec postgres pg_isready -U banking

# Check connections
docker exec postgres psql -U banking -d banking -c "
  SELECT state, COUNT(*) 
  FROM pg_stat_activity 
  GROUP BY state;
"
```

**Solutions:**
```bash
# Restart PostgreSQL
docker-compose restart postgres

# Kill long-running queries
docker exec postgres psql -U banking -d banking -c "
  SELECT pg_terminate_backend(pid) 
  FROM pg_stat_activity 
  WHERE state = 'active' 
    AND query_start < NOW() - INTERVAL '1 hour';
"

# Increase max_connections
docker exec postgres psql -U banking -d banking -c "
  ALTER SYSTEM SET max_connections = 200;
  SELECT pg_reload_conf();
"
```

#### Issue: PostgreSQL Slow Queries

**Symptoms:**
- Query execution time > 1 second
- High CPU usage

**Diagnosis:**
```sql
-- Check slow queries
SELECT 
  pid,
  query,
  state,
  DATEDIFF(SECOND, query_start, CURRENT_TIMESTAMP()) AS duration_seconds
FROM pg_stat_activity
WHERE state = 'active'
  AND DATEDIFF(SECOND, query_start, CURRENT_TIMESTAMP()) > 1;

-- Check table bloat
SELECT 
  schemaname,
  tablename,
  pg_size_pretty(pg_total_relation_size(schemaname || '.' || tablename)) AS size
FROM pg_tables
WHERE schemaname = 'banking_cleansed'
ORDER BY pg_total_relation_size(schemaname || '.' || tablename) DESC;
```

**Solutions:**
```sql
-- Create index
CREATE INDEX CONCURRENTLY idx_customer_id 
ON banking_cleansed.core_banking_customers (customer_id);

-- Analyze table
ANALYZE banking_cleansed.core_banking_customers;

-- Vacuum table
VACUUM ANALYZE banking_cleansed.core_banking_customers;
```

---

### 5. Airflow Issues

#### Issue: Airflow DAG Not Running

**Symptoms:**
- DAG not scheduled
- Tasks not executing

**Diagnosis:**
```bash
# Check DAG status
docker exec airflow-webserver airflow dags list | grep banking

# Check DAG runs
docker exec airflow-webserver airflow dags list-runs -d bronze_ingestion

# Check task logs
docker exec airflow-webserver airflow tasks logs bronze_ingestion extract_core_banking 2024-01-15
```

**Solutions:**
```bash
# Unpause DAG
docker exec airflow-webserver airflow dags unpause bronze_ingestion

# Trigger DAG manually
docker exec airflow-webserver airflow dags trigger bronze_ingestion

# Check scheduler
docker exec airflow-scheduler airflow scheduler -d bronze_ingestion
```

#### Issue: Airflow Task Failed

**Symptoms:**
- Task in failed state
- DAG run failed

**Diagnosis:**
```bash
# Check task logs
docker exec airflow-webserver airflow tasks logs bronze_ingestion extract_core_banking 2024-01-15 1

# Check task status
docker exec airflow-webserver airflow tasks states-for-dag-run bronze_ingestion 2024-01-15
```

**Solutions:**
```bash
# Clear task
docker exec airflow-webserver airflow tasks clear bronze_ingestion -t extract_core_banking -d 2024-01-15

# Mark task as success
docker exec airflow-webserver airflow tasks mark-success bronze_ingestion extract_core_banking 2024-01-15 1

# Retry task
docker exec airflow-webserver airflow tasks run bronze_ingestion extract_core_banking 2024-01-15
```

---

### 6. Network Issues

#### Issue: Inter-Service Communication Failed

**Symptoms:**
- Services cannot communicate
- Connection timeout errors

**Diagnosis:**
```bash
# Check network
docker network ls
docker network inspect banking-data-platform_default

# Test connectivity
docker exec dremio-master curl -s http://minio:9000/minio/health/live
docker exec kafka-1 ping kafka-2
```

**Solutions:**
```bash
# Recreate network
docker-compose down
docker network rm banking-data-platform_default
docker-compose up -d

# Check firewall
sudo ufw status
sudo iptables -L
```

---

### 7. Data Quality Issues

#### Issue: Data Not Matching Source

**Symptoms:**
- Silver/Gold layer data differs from source
- Missing or extra records

**Diagnosis:**
```sql
-- Compare record counts
SELECT 
  'Source' AS layer,
  COUNT(*) AS record_count
FROM bronze.core_banking_transactions
WHERE txn_date = '2024-01-15'

UNION ALL

SELECT 
  'Silver',
  COUNT(*)
FROM silver.core_banking_transactions
WHERE txn_date = '2024-01-15';

-- Check for duplicates
SELECT 
  txn_id,
  COUNT(*) AS duplicate_count
FROM silver.core_baking_transactions
WHERE txn_date = '2024-01-15'
GROUP BY txn_id
HAVING COUNT(*) > 1;
```

**Solutions:**
```sql
-- Remove duplicates
DELETE FROM silver.core_baking_transactions
WHERE txn_id IN (
  SELECT txn_id 
  FROM (
    SELECT txn_id, 
           ROW_NUMBER() OVER (PARTITION BY txn_id ORDER BY cleaned_at DESC) AS rn
    FROM silver.core_baking_transactions
    WHERE txn_date = '2024-01-15'
  ) t
  WHERE rn > 1
);

-- Re-run transformation
-- (Trigger Airflow DAG manually)
```

---

## Performance Tuning

### 1. Dremio Query Optimization

```sql
-- Enable reflection
ALTER VIEW banking_gold.customer_360 
CREATE RAW REFLECTION 
PARTITION BY (customer_id)
DISPLAY BY (customer_name, total_balance);

-- Optimize query
SELECT 
  customer_id,
  customer_name,
  total_balance
FROM banking_gold.customer_360
WHERE customer_id = 'CUST-12345';
-- Instead of: SELECT * FROM banking_gold.customer_360 WHERE customer_id = 'CUST-12345'
```

### 2. Kafka Optimization

```bash
# Increase partitions for high-throughput topics
docker exec kafka-1 kafka-topics --bootstrap-server localhost:9092 \
  --alter --topic transactions --partitions 48

# Increase replication factor
docker exec kafka-1 kafka-topics --bootstrap-server localhost:9092 \
  --alter --topic transactions --replication-factor 3
```

### 3. PostgreSQL Optimization

```sql
-- Create index
CREATE INDEX CONCURRENTLY idx_txn_account_date 
ON banking_cleansed.core_baking_transactions (account_id, txn_date);

-- Update statistics
ANALYZE banking_cleansed.core_baking_transactions;

-- Check query plan
EXPLAIN ANALYZE 
SELECT * FROM banking_cleansed.core_baking_transactions
WHERE account_id = 'ACC-12345' AND txn_date = '2024-01-15';
```

---

## Monitoring Commands

### Quick Health Check

```bash
#!/bin/bash
echo "=== Service Status ==="
docker-compose ps

echo -e "\n=== Dremio ==="
curl -s http://localhost:9047/apiv2/system/info | jq '.dataVersion'

echo -e "\n=== Kafka ==="
docker exec kafka-1 kafka-topics --bootstrap-server localhost:9092 --list | wc -l

echo -e "\n=== MinIO ==="
curl -s http://localhost:9000/minio/health/live

echo -e "\n=== PostgreSQL ==="
docker exec postgres pg_isready -U banking

echo -e "\n=== Airflow ==="
curl -s http://localhost:8080/health | jq '.status'

echo -e "\n=== Disk Usage ==="
df -h /data
```

### Log Analysis

```bash
# Find errors in logs
grep -r "ERROR" logs/ | tail -20

# Find warnings
grep -r "WARN" logs/ | tail -20

# Find slow queries
grep -r "slow query" logs/ | tail -20
```
