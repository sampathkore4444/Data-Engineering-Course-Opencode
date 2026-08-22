# Banking Data Platform: Monitoring & Governance

## Overview

This document covers production-ready monitoring, governance, and compliance practices for the Banking Data Platform using Dremio and Lakehouse architecture.

---

## 1. Monitoring Architecture

### Monitoring Stack

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    BANKING DATA PLATFORM MONITORING                      │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐         │
│  │   APPLICATION   │  │   INFRASTRUCTURE│  │   BUSINESS      │         │
│  │   MONITORING    │  │   MONITORING    │  │   MONITORING    │         │
│  ├─────────────────┤  ├─────────────────┤  ├─────────────────┤         │
│  │ • Query Latency │  │ • CPU/Memory    │  │ • Data Quality  │         │
│  │ • ETL Pipeline  │  │ • Disk Usage    │  │ • SLA Compliance│         │
│  │ • API Response  │  │ • Network I/O   │  │ • Fraud Alerts  │         │
│  │ • Error Rates   │  │ • Container Logs│  │ • NPA Tracking  │         │
│  └────────┬────────┘  └────────┬────────┘  └────────┬────────┘         │
│           │                    │                    │                   │
│           └────────────────────┼────────────────────┘                   │
│                                │                                        │
│                    ┌───────────v───────────┐                            │
│                    │   CENTRALIZED         │                            │
│                    │   MONITORING HUB      │                            │
│                    │   (Prometheus/Grafana)│                            │
│                    └───────────────────────┘                            │
└─────────────────────────────────────────────────────────────────────────┘
```

### Key Metrics to Monitor

#### Application Metrics

| Metric | Description | Threshold | Action |
|--------|-------------|-----------|--------|
| **Query Latency** | Time to execute SQL queries | < 5 seconds (P95) | Scale Dremio executor |
| **ETL Pipeline Duration** | Time to complete ETL jobs | < 30 minutes | Optimize queries |
| **Failed Queries** | Number of failed queries | < 1% | Investigate errors |
| **Concurrent Users** | Active users in Dremio | < 100 | Add executor nodes |
| **Data Freshness** | Time since last update | < 1 hour | Check CDC/ETL |

#### Infrastructure Metrics

| Metric | Description | Threshold | Action |
|--------|-------------|-----------|--------|
| **CPU Utilization** | Average CPU usage | < 80% | Scale horizontally |
| **Memory Usage** | RAM utilization | < 85% | Increase memory |
| **Disk Usage** | Storage consumption | < 70% | Add storage |
| **Network I/O** | Data transfer rate | < 1 Gbps | Check network |
| **Container Health** | Docker container status | All running | Restart failed |

#### Business Metrics

| Metric | Description | Threshold | Action |
|--------|-------------|-----------|--------|
| **Data Quality Score** | Percentage of valid records | > 99% | Fix data issues |
| **SLA Compliance** | Reports delivered on time | > 99.9% | Escalate |
| **Fraud Detection Rate** | Fraud caught vs missed | > 95% | Review models |
| **NPA Reporting** | Timely NPA reports | 100% | Ensure compliance |

---

## 2. Monitoring Setup

### Prometheus Configuration

```yaml
# prometheus.yml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  # Dremio Metrics
  - job_name: 'dremio'
    static_configs:
      - targets: ['dremio-master:9047']
    metrics_path: '/prometheus/metrics'
    
  # PostgreSQL Metrics
  - job_name: 'postgresql'
    static_configs:
      - targets: ['postgres-exporter:9187']
    
  # MySQL Metrics
  - job_name: 'mysql'
    static_configs:
      - targets: ['mysql-exporter:9104']
    
  # Node Exporter (Infrastructure)
  - job_name: 'node'
    static_configs:
      - targets: ['node-exporter:9100']
```

### Grafana Dashboards

#### Dashboard 1: Banking Platform Overview

```json
{
  "title": "Banking Data Platform Overview",
  "panels": [
    {
      "title": "Query Latency (P95)",
      "type": "graph",
      "targets": [
        {
          "expr": "histogram_quantile(0.95, dremio_query_duration_seconds_bucket)",
          "legendFormat": "{{query_type}}"
        }
      ]
    },
    {
      "title": "Active Users",
      "type": "stat",
      "targets": [
        {
          "expr": "dremio_active_users"
        }
      ]
    },
    {
      "title": "Data Freshness",
      "type": "gauge",
      "targets": [
        {
          "expr": "time() - lakehouse_last_update_timestamp"
        }
      ]
    },
    {
      "title": "Failed Queries",
      "type": "stat",
      "targets": [
        {
          "expr": "rate(dremio_queries_failed_total[5m])"
        }
      ]
    }
  ]
}
```

#### Dashboard 2: ETL Pipeline Monitoring

```json
{
  "title": "ETL Pipeline Monitoring",
  "panels": [
    {
      "title": "Pipeline Duration",
      "type": "graph",
      "targets": [
        {
          "expr": "etl_pipeline_duration_seconds",
          "legendFormat": "{{pipeline_name}}"
        }
      ]
    },
    {
      "title": "Rows Processed",
      "type": "graph",
      "targets": [
        {
          "expr": "etl_rows_processed_total",
          "legendFormat": "{{source}}/{{table}}"
        }
      ]
    },
    {
      "title": "Pipeline Status",
      "type": "stat",
      "targets": [
        {
          "expr": "etl_pipeline_status"
        }
      ]
    }
  ]
}
```

### Alerting Rules

```yaml
# alert_rules.yml
groups:
  - name: banking_platform_alerts
    rules:
      # High Query Latency
      - alert: HighQueryLatency
        expr: histogram_quantile(0.95, dremio_query_duration_seconds_bucket) > 10
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High query latency detected"
          description: "P95 query latency is {{ $value }}s (threshold: 10s)"
      
      # Failed Queries Spike
      - alert: FailedQueriesSpike
        expr: rate(dremio_queries_failed_total[5m]) > 0.1
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "High query failure rate"
          description: "Query failure rate is {{ $value }} per second"
      
      # ETL Pipeline Failure
      - alert: ETLPipelineFailure
        expr: etl_pipeline_status == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "ETL pipeline failed"
          description: "Pipeline {{ $labels.pipeline_name }} has failed"
      
      # Disk Space Low
      - alert: DiskSpaceLow
        expr: (node_filesystem_avail_bytes / node_filesystem_size_bytes) * 100 < 20
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Low disk space"
          description: "Disk usage is {{ $value }}% used"
      
      # Container Down
      - alert: ContainerDown
        expr: container_status_running == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Container is down"
          description: "Container {{ $labels.name }} is not running"
```

---

## 3. Data Governance Framework

### Data Classification

| Classification | Description | Examples | Access Level |
|---------------|-------------|----------|--------------|
| **PUBLIC** | Non-sensitive data | Product catalog, branch locations | All users |
| **INTERNAL** | Internal business data | Sales reports, employee data | Employees only |
| **CONFIDENTIAL** | Sensitive business data | Financial reports, customer analytics | Managers+ |
| **RESTRICTED** | Highly sensitive PII | PAN, Aadhaar, account numbers | Authorized only |
| **CRITICAL** | Regulatory/Compliance | NPA data, fraud alerts | Compliance team |

### Data Quality Rules

#### Rule 1: Customer Data Quality

```sql
-- Check for duplicate customers
SELECT customer_id, COUNT(*) as duplicate_count
FROM "banking-vault"."virtual.customer_master"
GROUP BY customer_id
HAVING COUNT(*) > 1;

-- Check for invalid PAN format
SELECT customer_id, pan_number
FROM "banking-vault"."virtual.customer_master"
WHERE pan_number !~ '^[A-Z]{5}[0-9]{4}[A-Z]$';

-- Check for missing KYC
SELECT customer_id, customer_name
FROM "banking-vault"."virtual.customer_master"
WHERE kyc_status != 'VERIFIED';
```

#### Rule 2: Transaction Data Quality

```sql
-- Check for negative amounts
SELECT transaction_id, amount
FROM "banking-vault"."virtual.transaction_analytics"
WHERE amount < 0 AND transaction_type != 'REFUND';

-- Check for future dates
SELECT transaction_id, transaction_date
FROM "banking-vault"."virtual.transaction_analytics"
WHERE transaction_date > CURRENT_TIMESTAMP;

-- Check for orphan transactions
SELECT t.transaction_id
FROM "banking-vault"."virtual.transaction_analytics" t
LEFT JOIN "banking-postgres".core_banking.customers c ON t.customer_id = c.customer_id
WHERE c.customer_id IS NULL;
```

#### Rule 3: Loan Data Quality

```sql
-- Check for invalid NPA classification
SELECT loan_id, loan_status, npa_classification
FROM "banking-postgres".core_banking.loan_accounts
WHERE loan_status = 'CLOSED' AND npa_classification != 'STANDARD';

-- Check for EMI calculation errors
SELECT loan_id, loan_amount, emi_amount, interest_rate, tenure_months
FROM "banking-postgres".core_banking.loan_accounts
WHERE ABS(emi_amount - (loan_amount * (interest_rate/100/12) * 
      POWER(1 + interest_rate/100/12, tenure_months)) / 
      (POWER(1 + interest_rate/100/12, tenure_months) - 1)) > 100;
```

### Data Lineage Tracking

```sql
-- View data lineage for Customer 360
SELECT 
    source_table,
    target_table,
    transformation_type,
    transformation_logic,
    last_refreshed,
    refresh_frequency
FROM "banking-vault".metadata.lineage
WHERE target_table = 'virtual.customer_360';

-- Check data flow
SELECT 
    level,
    source,
    target,
    process,
    timestamp
FROM "banking-vault".metadata.data_flow
ORDER BY timestamp DESC
LIMIT 100;
```

### Access Control Matrix

| Role | Core Banking | Credit Cards | Loans | Customer 360 | Reports |
|------|-------------|--------------|-------|--------------|---------|
| **Data Analyst** | Read | Read | Read | Read | Read |
| **Data Engineer** | Read/Write | Read/Write | Read/Write | Read/Write | Read |
| **Compliance Officer** | Read | Read | Read | Read | Read/Write |
| **Risk Manager** | Read | Read | Read | Read | Read/Write |
| **Branch Manager** | Read (Branch) | Read (Branch) | Read (Branch) | Read (Branch) | Read |
| **Call Center Agent** | Read (Limited) | Read (Limited) | Read (Limited) | Read (Limited) | No |
| **Admin** | Full Access | Full Access | Full Access | Full Access | Full Access |

---

## 4. Compliance & Regulatory Requirements

### RBI Compliance (India)

| Requirement | Description | Implementation |
|-------------|-------------|----------------|
| **Circular 39/2014** | KYC norms | Aadhaar verification, PAN validation |
| **Circular 23/2014** | Master KYC Document | Centralized customer data |
| **Decision 1168/QD-NHNN** | Data localization | Data stored in India |
| **Basel III** | Capital adequacy | Risk-weighted assets calculation |
| **NPA Reporting** | Asset classification | Automated NPA detection |

### Data Retention Policy

| Data Type | Retention Period | Storage Tier | Archive Strategy |
|-----------|-----------------|--------------|------------------|
| **Customer Master** | Active + 10 years | Hot → Warm → Cold | S3 Glacier |
| **Transactions** | 7 years | Hot → Warm → Cold | S3 Glacier |
| **Loan Data** | Loan tenure + 7 years | Hot → Warm → Cold | S3 Glacier |
| **Card Transactions** | 7 years | Hot → Warm → Cold | S3 Glacier |
| **NPA Data** | 10 years | Hot → Warm → Cold | S3 Glacier |
| **Audit Logs** | 10 years | Cold | S3 Glacier Deep Archive |

### Audit Trail Requirements

```sql
-- Query to generate audit trail
SELECT 
    'CUSTOMER_ACCESS' AS audit_type,
    user_id,
    action_type,
    table_accessed,
    customer_id_accessed,
    access_timestamp,
    ip_address,
    user_agent
FROM "banking-vault".audit.customer_access_log
WHERE access_timestamp >= CURRENT_DATE - INTERVAL '30' DAY
ORDER BY access_timestamp DESC;

-- Query to track data changes
SELECT 
    'DATA_CHANGE' AS audit_type,
    table_name,
    record_id,
    change_type,  -- INSERT, UPDATE, DELETE
    old_values,
    new_values,
    changed_by,
    changed_at
FROM "banking-vault".audit.data_change_log
WHERE changed_at >= CURRENT_DATE - INTERVAL '7' DAY
ORDER BY changed_at DESC;
```

---

## 5. Incident Response Procedures

### Incident Severity Levels

| Level | Description | Response Time | Escalation |
|-------|-------------|---------------|------------|
| **SEV-1** | Complete platform outage | 15 minutes | CTO, VP Engineering |
| **SEV-2** | Major feature unavailable | 1 hour | Engineering Manager |
| **SEV-3** | Minor feature degradation | 4 hours | Team Lead |
| **SEV-4** | Cosmetic issue | 24 hours | On-call engineer |

### Runbooks

#### Runbook 1: Dremio Performance Degradation

```bash
# Step 1: Check Dremio health
docker compose exec dremio-master /opt/dremio/bin/dremio-admin ping

# Step 2: Check executor status
docker compose exec dremio-master /opt/dremio/bin/dremio-admin executor list

# Step 3: Check query queue
docker compose exec dremio-master /opt/dremio/bin/dremio-admin query list

# Step 4: Restart if needed
docker compose restart dremio-master dremio-executor

# Step 5: Monitor recovery
docker compose logs -f dremio-master
```

#### Runbook 2: ETL Pipeline Failure

```bash
# Step 1: Check ETL logs
docker compose logs etl-pipeline

# Step 2: Verify source connectivity
docker compose exec postgres-core-banking pg_isready
docker compose exec mysql-credit-cards mysqladmin ping

# Step 3: Check disk space
df -h /lake

# Step 4: Restart pipeline
docker compose restart etl-pipeline

# Step 5: Verify data freshness
docker compose exec postgres-core-banking psql -U postgres -d core_banking -c "
SELECT MAX(transaction_date) FROM transactions;
"
```

#### Runbook 3: Data Quality Alert

```bash
# Step 1: Identify affected data
SELECT COUNT(*) FROM "banking-vault"."virtual.customer_360" WHERE kyc_status IS NULL;

# Step 2: Check source data
docker compose exec postgres-core-banking psql -U postgres -d core_banking -c "
SELECT COUNT(*) FROM customers WHERE kyc_status IS NULL;
"

# Step 3: Fix data quality issue
docker compose exec postgres-core-banking psql -U postgres -d core_banking -c "
UPDATE customers SET kyc_status = 'PENDING' WHERE kyc_status IS NULL;
"

# Step 4: Refresh Dremio cache
# Run in Dremio UI: Refresh virtual dataset
```

---

## 6. Best Practices

### Monitoring Best Practices

1. **Set Up Alerts Early**: Configure alerts before going to production
2. **Use Dashboards**: Create role-specific dashboards (Operations, Management, Compliance)
3. **Monitor Trends**: Don't just alert on thresholds, track trends
4. **Automate Responses**: Use runbooks for common issues
5. **Regular Reviews**: Weekly monitoring review meetings

### Governance Best Practices

1. **Data Stewardship**: Assign data owners for each domain
2. **Quality Gates**: Block bad data at ingestion
3. **Documentation**: Keep data dictionaries updated
4. **Access Reviews**: Quarterly access reviews
5. **Training**: Regular training on data policies

### Security Best Practices

1. **Encryption**: Encrypt data at rest and in transit
2. **Masking**: Mask PII in non-production environments
3. **Audit Logs**: Log all data access
4. **Least Privilege**: Grant minimum required access
5. **Regular Audits**: Monthly security audits

---

## 7. Tools & Technologies

| Category | Tool | Purpose |
|----------|------|---------|
| **Monitoring** | Prometheus | Metrics collection |
| **Visualization** | Grafana | Dashboards |
| **Logging** | ELK Stack | Log aggregation |
| **Alerting** | Alertmanager | Alert routing |
| **Data Quality** | Great Expectations | Data validation |
| **Lineage** | DataHub | Data lineage |
| **Catalog** | OpenMetadata | Data discovery |
| **Security** | HashiCorp Vault | Secrets management |

---

## Quick Reference

### Common Commands

```bash
# Check platform health
docker compose ps

# View logs
docker compose logs -f [service]

# Restart service
docker compose restart [service]

# Run ETL
docker compose exec etl-pipeline python extract_load.py --all

# Check data freshness
docker compose exec postgres-core-banking psql -U postgres -d core_banking -c "
SELECT 'customers' as tbl, MAX(created_at) FROM customers
UNION ALL
SELECT 'transactions', MAX(transaction_date) FROM transactions;
"
```

### Contact Information

| Role | Contact | Escalation |
|------|---------|------------|
| **Platform Team** | platform@bank.com | First contact |
| **Data Team** | data@bank.com | Data issues |
| **Security Team** | security@bank.com | Security incidents |
| **Compliance** | compliance@bank.com | Regulatory issues |

---

*Last Updated: 2025-01-15*
*Review Schedule: Monthly*