# Operations Runbook - Banking Data Platform

## Overview
This runbook provides operational procedures for managing the banking data platform.

## Daily Operations

### 1. Health Check (9:00 AM)

```bash
# Check all services
docker-compose ps

# Check Dremio
curl -s http://localhost:9047/apiv2/system/info | jq '.dataVersion'

# Check Kafka
docker exec kafka-1 kafka-topics --bootstrap-server localhost:9092 --list

# Check MinIO
curl -s http://localhost:9000/minio/health/live

# Check PostgreSQL
docker exec postgres pg_isready -U banking

# Check Airflow
curl -s http://localhost:8080/health | jq '.status'
```

### 2. Data Freshness Check (10:00 AM)

```sql
-- Check data freshness in Silver layer
SELECT 
    'customers' AS table_name,
    MAX(cleaned_at) AS last_refresh,
    DATEDIFF(HOUR, MAX(cleaned_at), CURRENT_TIMESTAMP()) AS hours_since_refresh
FROM banking_cleansed.core_banking_customers

UNION ALL

SELECT 
    'transactions',
    MAX(cleaned_at),
    DATEDIFF(HOUR, MAX(cleaned_at), CURRENT_TIMESTAMP())
FROM banking_cleansed.core_banking_transactions

UNION ALL

SELECT 
    'accounts',
    MAX(cleaned_at),
    DATEDIFF(HOUR, MAX(cleaned_at), CURRENT_TIMESTAMP())
FROM banking_cleansed.core_banking_accounts;
```

### 3. Reflection Status Check (11:00 AM)

```sql
-- Check reflection health
SELECT 
    reflection_name,
    reflection_type,
    status,
    refresh_time,
    size_bytes / 1024 / 1024 AS size_mb,
    query_count,
    hit_count,
    ROUND(hit_count * 100.0 / NULLIF(query_count, 0), 2) AS hit_rate_pct
FROM sys."reflection"
ORDER BY refresh_time DESC;
```

### 4. Data Quality Check (2:00 PM)

```sql
-- Check data quality metrics
SELECT 
    table_name,
    total_rows,
    uniqueness_pct,
    completeness_pct,
    validity_pct,
    measured_at
FROM banking_cleansed.data_quality_metrics
WHERE measured_at >= DATEADD(DAY, -1, CURRENT_DATE);
```

### 5. Performance Check (4:00 PM)

```sql
-- Check query performance
SELECT 
    DATE(start_time) AS query_date,
    COUNT(*) AS total_queries,
    AVG(DATEDIFF(MILLISECOND, start_time, end_time)) AS avg_duration_ms,
    MAX(DATEDIFF(MILLISECOND, start_time, end_time)) AS max_duration_ms,
    COUNT(CASE WHEN DATEDIFF(MILLISECOND, start_time, end_time) > 10000 THEN 1 END) AS slow_queries
FROM sys."query" 
WHERE start_time >= DATEADD(DAY, -7, CURRENT_DATE)
GROUP BY DATE(start_time)
ORDER BY query_date DESC;
```

## Weekly Operations

### 1. Backup Verification (Monday 9:00 AM)

```bash
# Check backup status
ls -la backups/daily/ | tail -5

# Verify backup integrity
tar -tzf backups/daily/backup_*.tar.gz | head -10
```

### 2. Storage Cleanup (Friday 5:00 PM)

```bash
# Clean up old logs
find logs/ -type f -mtime +30 -delete

# Clean up old backups
find backups/ -type f -mtime +30 -delete

# Check storage usage
df -h /data
```

### 3. Performance Review (Friday 3:00 PM)

```sql
-- Weekly performance summary
SELECT 
    user_id,
    COUNT(*) AS query_count,
    AVG(DATEDIFF(MILLISECOND, start_time, end_time)) AS avg_duration_ms,
    SUM(rows_scanned) AS total_rows_scanned,
    SUM(bytes_scanned) / 1024 / 1024 / 1024 AS total_gb_scanned
FROM sys."query" 
WHERE start_time >= DATEADD(DAY, -7, CURRENT_DATE)
GROUP BY user_id
ORDER BY total_gb_scanned DESC;
```

## Monthly Operations

### 1. Capacity Review (1st of month)

```sql
-- Storage usage by layer
SELECT 
    'Bronze' AS layer,
    SUM(size_bytes) / 1024 / 1024 / 1024 AS size_gb
FROM sys."table" 
WHERE table_path LIKE '%bronze%'

UNION ALL

SELECT 
    'Silver',
    SUM(size_bytes) / 1024 / 1024 / 1024
FROM sys."table" 
WHERE table_path LIKE '%silver%'

UNION ALL

SELECT 
    'Gold',
    SUM(size_bytes) / 1024 / 1024 / 1024
FROM sys."table" 
WHERE table_path LIKE '%gold%';
```

### 2. Security Audit (15th of month)

```sql
-- User access review
SELECT 
    user_id,
    user_role,
    COUNT(*) AS access_count,
    COUNT(DISTINCT object_name) AS unique_objects,
    MAX(event_timestamp) AS last_access
FROM banking_audit.access_log
WHERE event_timestamp >= DATEADD(DAY, -30, CURRENT_DATE)
GROUP BY user_id, user_role
ORDER BY access_count DESC;
```

### 3. Reflection Optimization (15th of month)

```sql
-- Find unused reflections
SELECT 
    reflection_name,
    query_count,
    hit_count,
    size_bytes / 1024 / 1024 AS size_mb,
    CASE 
        WHEN hit_count < 10 THEN 'CONSIDER_DROPPING'
        WHEN hit_rate_pct < 20 THEN 'OPTIMIZE'
        ELSE 'KEEP'
    END AS recommendation
FROM sys."reflection"
WHERE query_count > 0
ORDER BY size_bytes DESC;
```

## Troubleshooting

### Issue 1: Dremio High Query Latency

**Symptoms:**
- Queries taking > 5 seconds
- User complaints about slow dashboards

**Investigation:**
```sql
-- Check slow queries
SELECT 
    query_id,
    SUBSTRING(query_text, 1, 100) AS query_preview,
    start_time,
    DATEDIFF(MILLISECOND, start_time, end_time) AS duration_ms,
    rows_scanned,
    reflection_name
FROM sys."query" 
WHERE start_time >= DATEADD(HOUR, -1, CURRENT_TIMESTAMP())
  AND DATEDIFF(MILLISECOND, start_time, end_time) > 5000
ORDER BY duration_ms DESC;
```

**Resolution:**
1. Check if reflections are enabled for the query
2. Refresh stale reflections
3. Optimize query to use specific columns
4. Add partitioning for large tables

### Issue 2: Kafka Consumer Lag

**Symptoms:**
- Consumer lag increasing
- Data not flowing to Silver layer

**Investigation:**
```bash
# Check consumer group lag
docker exec kafka-1 kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group silver-transform

# Check topic status
docker exec kafka-1 kafka-topics \
  --bootstrap-server localhost:9092 \
  --describe --topic transactions
```

**Resolution:**
1. Increase consumer instances
2. Optimize consumer processing logic
3. Check for failed consumer tasks
4. Restart consumer if needed

### Issue 3: MinIO Disk Space

**Symptoms:**
- MinIO disk usage > 80%
- Write errors in logs

**Investigation:**
```bash
# Check disk usage
df -h /data/minio

# Check bucket sizes
docker exec minio mc du --recursive local/banking-lake/
```

**Resolution:**
1. Archive old data to cold storage
2. Delete expired data
3. Compress existing data
4. Add more storage capacity

### Issue 4: PostgreSQL Connection Issues

**Symptoms:**
- Connection refused errors
- High connection count

**Investigation:**
```sql
-- Check active connections
SELECT 
    state,
    COUNT(*) AS connection_count
FROM pg_stat_activity
GROUP BY state;

-- Check long-running queries
SELECT 
    pid,
    query,
    state,
    DATEDIFF(SECOND, query_start, CURRENT_TIMESTAMP) AS duration_seconds
FROM pg_stat_activity
WHERE state = 'active'
  AND DATEDIFF(SECOND, query_start, CURRENT_TIMESTAMP) > 60;
```

**Resolution:**
1. Kill long-running queries
2. Increase max_connections
3. Optimize query performance
4. Add connection pooling

## Emergency Procedures

### 1. Service Down

```bash
# Restart specific service
docker-compose restart dremio-master

# Restart all services
docker-compose down
docker-compose up -d

# Check logs
docker-compose logs -f dremio-master
```

### 2. Data Corruption

```bash
# Stop ingestion
docker-compose stop airflow-scheduler

# Restore from backup
./restore.sh --component postgres --date 2024-01-15

# Verify data integrity
docker exec postgres psql -U banking -d banking -c "SELECT COUNT(*) FROM banking_cleansed.core_banking_customers;"
```

### 3. Security Incident

```bash
# Block user access
docker exec postgres psql -U banking -d banking -c "
  UPDATE banking_security.user_branch_mapping 
  SET access_level = 'BLOCKED' 
  WHERE user_id = 'suspicious_user';
"

# Enable audit logging
docker exec postgres psql -U banking -d banking -c "
  ALTER SYSTEM SET log_statement = 'all';
  SELECT pg_reload_conf();
"

# Review access logs
docker exec postgres psql -U banking -d banking -c "
  SELECT * FROM banking_audit.access_log 
  WHERE user_id = 'suspicious_user' 
  ORDER BY event_timestamp DESC;
"
```

## Contact Information

| Role | Name | Phone | Email |
|------|------|-------|-------|
| Platform Admin | John Doe | +84-901-234-567 | john.doe@bank.com |
| DBA | Jane Smith | +84-912-345-678 | jane.smith@bank.com |
| Security Officer | Mike Wilson | +84-923-456-789 | mike.wilson@bank.com |
| On-Call Engineer | Sarah Jones | +84-934-567-890 | sarah.jones@bank.com |
