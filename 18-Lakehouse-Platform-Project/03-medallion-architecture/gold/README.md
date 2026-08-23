# Gold Layer - Business-Ready Data

## Purpose
The Gold layer contains pre-aggregated, business logic applied data ready for BI tools, ML models, and regulatory reports.

## Design Principles

| Principle | Description |
|-----------|-------------|
| **Star Schema** | Dimensional model for BI tools |
| **Aggregated** | Pre-computed aggregations for performance |
| **Business Logic** | Domain-specific rules applied |
| **Materialized** | Reflections enabled for fast queries |
| **Governed** | Access control at view level |

## Gold Views

| View | Purpose | Primary Use |
|------|---------|-------------|
| `gold.customer_360` | Complete customer profile | BI dashboards, CRM |
| `gold.daily_transaction_summary` | Daily transaction aggregations | Operations dashboards |
| `gold.credit_risk_dashboard` | Loan risk analysis | Risk management |
| `gold.fraud_alert_summary` | Real-time fraud alerts | Fraud team |
| `gold.sbv_large_exposure` | SBV regulatory report | Compliance |
| `gold.reflection_catalog` | Dremio reflection strategy | Admin |

## Data Flow

```
Silver (Cleansed)  →  Joins  →  Aggregations  →  Business Rules  →  Gold (Ready)
```

## Performance Optimization

### Reflections Strategy

| View | Reflection Type | Partitioning | Refresh Schedule |
|------|----------------|--------------|------------------|
| `customer_360` | RAW | customer_id | Every 2 hours |
| `daily_transaction_summary` | AGGREGATE | txn_date | Every hour |
| `credit_risk_dashboard` | RAW | customer_id | Every 4 hours |
| `fraud_alert_summary` | RAW | txn_date | Real-time (streaming) |
| `sbv_large_exposure` | AGGREGATE | customer_id | Daily |

### Query Performance Targets

| View | Target Query Time | Current (est.) |
|------|-------------------|----------------|
| `customer_360` | < 2 seconds | ~1.5s |
| `daily_transaction_summary` | < 1 second | ~0.8s |
| `credit_risk_dashboard` | < 3 seconds | ~2.5s |
| `fraud_alert_summary` | < 500ms | ~400ms |
| `sbv_large_exposure` | < 5 seconds | ~4s |

## Access Control

| Role | customer_360 | daily_txn_summary | credit_risk | fraud_alert | sbv_report |
|------|-------------|-------------------|-------------|-------------|------------|
| **Analyst** | ✅ Read | ✅ Read | ❌ | ❌ | ❌ |
| **Risk Manager** | ✅ Read | ✅ Read | ✅ Read | ✅ Read | ❌ |
| **Fraud Team** | ✅ Read | ✅ Read | ❌ | ✅ Read/Write | ❌ |
| **Compliance** | ✅ Read | ✅ Read | ✅ Read | ❌ | ✅ Read |
| **Admin** | ✅ All | ✅ All | ✅ All | ✅ All | ✅ All |
