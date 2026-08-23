# ✅ Data Quality - Banking Data Warehouse

> **Tests, alerts, and monitoring to ensure data accuracy and freshness**

---

## 📋 Table of Contents

1. [Overview](#1-overview)
2. [Why Data Quality Matters](#2-why-data-quality-matters)
3. [Data Quality Framework](#3-data-quality-framework)
4. [Tests](#4-tests)
5. [Alerts](#5-alerts)
6. [Running Quality Checks](#6-running-quality-checks)
7. [Best Practices](#7-best-practices)

---

## 1. Overview

This folder contains **data quality tests and alerts** to ensure the Data Warehouse data is accurate, complete, and fresh.

### What is Data Quality?

| Term | Meaning | Example |
|------|---------|---------|
| **Accuracy** | Data reflects reality | Customer name matches source |
| **Completeness** | No missing values | All required fields populated |
| **Consistency** | Same data across systems | Account balance matches in staging and gold |
| **Freshness** | Data is up-to-date | Updated within last 24 hours |
| **Uniqueness** | No duplicate records | Each customer_id appears once |

### Why Data Quality Matters in Banking?

| Impact | Consequence |
|--------|-------------|
| **Regulatory** | Incorrect reports → fines from SBV |
| **Financial** | Wrong balances → monetary loss |
| **Customer** | Incorrect info → poor service |
| **Decision-making** | Bad data → wrong business decisions |

---

## 2. Why Data Quality Matters

### The Cost of Bad Data

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    THE COST OF BAD DATA IN BANKING                       │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ❌ ACCURACY ISSUES:                                                    │
│  • Wrong customer balance → Customer complaint                          │
│  • Incorrect interest calculation → Financial loss                      │
│  • Wrong NPA classification → Regulatory penalty                        │
│                                                                         │
│  ❌ COMPLETENESS ISSUES:                                                │
│  • Missing transaction → Incomplete P&L report                          │
│  • Missing customer info → Incomplete AML screening                     │
│  • Missing loan data → Inaccurate Basel III calculation                 │
│                                                                         │
│  ❌ FRESHNESS ISSUES:                                                   │
│  • Stale data → Late fraud detection                                    │
│  • Delayed updates → Missed regulatory deadline                         │
│  • Old customer info → Wrong risk assessment                            │
│                                                                         │
│  ❌ UNIQUENESS ISSUES:                                                  │
│  • Duplicate transactions → Inflated revenue                            │
│  • Duplicate customers → Incorrect segmentation                         │
│  • Duplicate loans → Wrong exposure calculation                         │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Data Quality Impact on Banking

| Area | Impact of Bad Data |
|------|-------------------|
| **Regulatory Reporting** | Incorrect Basel III, AML reports → Fines |
| **Financial Statements** | Wrong P&L, Balance sheet → Audit findings |
| **Customer Service** | Incorrect balances → Complaints |
| **Risk Management** | Wrong risk scores → Bad lending decisions |
| **Fraud Detection** | Missed patterns → Financial losses |

---

## 3. Data Quality Framework

### Quality Dimensions

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    DATA QUALITY DIMENSIONS                               │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                │
│  │  ACCURACY    │  │ COMPLETENESS │  │ CONSISTENCY  │                │
│  │──────────────│  │──────────────│  │──────────────│                │
│  │ Data is      │  │ No missing   │  │ Same data    │                │
│  │ correct      │  │ values       │  │ across       │                │
│  │              │  │              │  │ systems      │                │
│  └──────────────┘  └──────────────┘  └──────────────┘                │
│                                                                         │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                │
│  │  FRESHNESS   │  │  UNIQUENESS  │  │  VALIDITY    │                │
│  │──────────────│  │──────────────│  │──────────────│                │
│  │ Data is      │  │ No duplicate │  │ Data follows │                │
│  │ up-to-date   │  │ records      │  │ business     │                │
│  │              │  │              │  │ rules        │                │
│  └──────────────┘  └──────────────┘  └──────────────┘                │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Quality Check Flow

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    DATA QUALITY CHECK FLOW                               │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐             │
│  │   EXTRACT    │───►│   VALIDATE   │───►│    ALERT     │             │
│  │   (Airflow)  │    │   (Tests)    │    │   (If Fail)  │             │
│  └──────────────┘    └──────────────┘    └──────────────┘             │
│                                      │                                 │
│                                      ▼                                 │
│                              ┌──────────────┐                         │
│                              │    LOG       │                         │
│                              │  (Results)   │                         │
│                              └──────────────┘                         │
│                                                                         │
│  QUALITY CHECKS RUN:                                                    │
│  1. Uniqueness - No duplicates                                         │
│  2. Completeness - No nulls in required fields                         │
│  3. Freshness - Data updated recently                                  │
│  4. Validity - Values within expected ranges                           │
│  5. Consistency - Cross-table validation                               │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 4. Tests

### Files

| File | Purpose |
|------|---------|
| `tests/uniqueness_test.sql` | Check for duplicate records |

### Test Types

| Test Type | Description | Tables Checked |
|-----------|-------------|----------------|
| **Uniqueness** | No duplicate primary keys | dim_customer, dim_account, fact_transactions |
| **Completeness** | No NULLs in required fields | All tables |
| **Freshness** | Data updated within threshold | Staging tables |
| **Validity** | Values within expected ranges | fact_transactions, dim_account |

### Uniqueness Test

```sql
-- Check for duplicate customer_ids
SELECT 
    customer_id,
    COUNT(*) as duplicate_count
FROM gold.dim_customer
GROUP BY customer_id
HAVING COUNT(*) > 1;
```

**What it checks:**
- Each `customer_id` should appear only once in `dim_customer`
- Each `account_id` should appear only once in `dim_account`
- Each `transaction_id` should appear only once in `fact_transactions`

**Expected result:** Empty result set (no duplicates)

### Completeness Test

```sql
-- Check for NULL values in critical fields
SELECT 
    'dim_customer' AS table_name,
    COUNT(*) AS total_rows,
    SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS null_customer_id,
    SUM(CASE WHEN customer_name IS NULL THEN 1 ELSE 0 END) AS null_customer_name
FROM gold.dim_customer;
```

**What it checks:**
- `customer_id` should never be NULL
- `customer_name` should never be NULL
- `account_id` should never be NULL
- `transaction_id` should never be NULL

**Expected result:** All null counts = 0

### Validity Test

```sql
-- Check for valid values
SELECT 
    'fact_transactions' AS table_name,
    COUNT(*) AS total_rows,
    SUM(CASE WHEN transaction_amount < 0 THEN 1 ELSE 0 END) AS negative_amounts,
    SUM(CASE WHEN transaction_amount > 10000000000 THEN 1 ELSE 0 END) AS excessive_amounts
FROM gold.fact_transactions;
```

**What it checks:**
- Transaction amounts should be positive
- Transaction amounts should be within reasonable range
- Account balances should be non-negative
- Dates should be valid

---

## 5. Alerts

### Files

| File | Purpose |
|------|---------|
| `alerts/freshness_alert.sql` | Check if data is stale |

### Alert Types

| Alert Type | Trigger | Severity | Action |
|------------|---------|----------|--------|
| **Stale Data** | Not updated in 24 hours | 🔴 High | Check ETL pipeline |
| **Duplicate Records** | Duplicate primary key found | 🔴 Critical | Stop pipeline, investigate |
| **NULL Values** | Required field is NULL | 🟡 Medium | Log and investigate |
| **Out of Range** | Value outside expected range | 🟡 Medium | Validate business rule |

### Freshness Alert

```sql
-- Check staging table freshness
SELECT 
    'stg_customers' AS table_name,
    MAX(updated_at) AS last_update,
    EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - MAX(updated_at))) / 3600 AS hours_since_update,
    CASE 
        WHEN MAX(updated_at) < CURRENT_TIMESTAMP - INTERVAL '24 hours' THEN 'STALE'
        ELSE 'FRESH'
    END AS status
FROM staging.stg_customers;
```

**What it checks:**
- `stg_customers` - Updated within last 24 hours
- `stg_accounts` - Updated within last 24 hours
- `stg_transactions` - Updated within last 24 hours

**Expected result:** All tables show 'FRESH'

### Alert Dashboard

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    DATA QUALITY ALERT DASHBOARD                          │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  TABLE              LAST UPDATE      STATUS      ACTION                │
│  ──────────────────────────────────────────────────────────────────    │
│  stg_customers      2 hours ago      ✅ FRESH    None                  │
│  stg_accounts       2 hours ago      ✅ FRESH    None                  │
│  stg_transactions   2 hours ago      ✅ FRESH    None                  │
│  stg_cards          25 hours ago     🔴 STALE    Check pipeline        │
│  stg_loans          2 hours ago      ✅ FRESH    None                  │
│                                                                         │
│  OVERALL STATUS: ⚠️ WARNING (1 table stale)                            │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 6. Running Quality Checks

### Prerequisites

```bash
# Ensure Data Warehouse is running
docker-compose up -d

# Ensure data is loaded
psql -h localhost -U postgres -d banking_dw -f 11-scripts/seed-all-data.sql
```

### Running Tests

```bash
# Connect to Data Warehouse
psql -h localhost -U postgres -d banking_dw

# Run uniqueness tests
\i 06-data-quality/tests/uniqueness_test.sql

# Run freshness checks
\i 06-data-quality/alerts/freshness_alert.sql
```

### Running All Quality Checks

```sql
-- Complete quality check script
\echo '=== Running Uniqueness Tests ==='
\i 06-data-quality/tests/uniqueness_test.sql

\echo '=== Running Freshness Checks ==='
\i 06-data-quality/alerts/freshness_alert.sql

\echo '=== Quality Check Complete ==='
```

### Integrating with Airflow

```python
# Add to your Airflow DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

def run_quality_checks():
    """Run all data quality checks"""
    hook = PostgresHook(postgres_conn_id='data_warehouse')
    
    # Run uniqueness test
    result = hook.get_records("""
        SELECT COUNT(*) 
        FROM (
            SELECT customer_id, COUNT(*) as cnt
            FROM gold.dim_customer
            GROUP BY customer_id
            HAVING COUNT(*) > 1
        ) duplicates
    """)
    
    if result[0][0] > 0:
        raise ValueError(f"Found {result[0][0]} duplicate customer_ids")
    
    # Run freshness check
    result = hook.get_records("""
        SELECT CASE 
            WHEN MAX(updated_at) < CURRENT_TIMESTAMP - INTERVAL '24 hours' 
            THEN 'STALE' ELSE 'FRESH'
        END
        FROM staging.stg_customers
    """)
    
    if result[0][0] == 'STALE':
        raise ValueError("stg_customers data is stale")

# Add to DAG
quality_check = PythonOperator(
    task_id='run_quality_checks',
    python_callable=run_quality_checks,
)
```

---

## 7. Best Practices

### Data Quality Checklist

| Check | Frequency | Owner | Tool |
|-------|-----------|-------|------|
| **Uniqueness** | Every ETL run | Data Engineer | SQL test |
| **Completeness** | Every ETL run | Data Engineer | SQL test |
| **Freshness** | Hourly | Data Engineer | Alert script |
| **Validity** | Daily | Data Analyst | Business rules |
| **Consistency** | Weekly | Data Architect | Cross-system check |

### Quality Gate Rules

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    DATA QUALITY GATE RULES                               │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ✅ PASS RULES (Pipeline continues):                                   │
│  • Uniqueness: 0 duplicates                                            │
│  • Completeness: < 1% NULLs in required fields                         │
│  • Freshness: Data updated within 24 hours                             │
│  • Validity: < 0.1% out-of-range values                                │
│                                                                         │
│  ❌ FAIL RULES (Pipeline stops):                                       │
│  • Uniqueness: Any duplicates found                                    │
│  • Completeness: > 5% NULLs in required fields                         │
│  • Freshness: Data stale for > 48 hours                                │
│  • Validity: > 1% out-of-range values                                  │
│                                                                         │
│  ⚠️ WARNING RULES (Pipeline continues with alert):                     │
│  • Uniqueness: Duplicates in non-critical tables                       │
│  • Completeness: 1-5% NULLs in required fields                         │
│  • Freshness: Data stale for 24-48 hours                               │
│  • Validity: 0.1-1% out-of-range values                                │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Quality Metrics to Track

| Metric | Target | Current | Status |
|--------|--------|---------|--------|
| **Uniqueness Score** | 100% | 100% | ✅ |
| **Completeness Score** | > 99% | 99.5% | ✅ |
| **Freshness Score** | > 95% | 90% | ⚠️ |
| **Validity Score** | > 99.9% | 99.9% | ✅ |
| **Overall Quality Score** | > 95% | 97% | ✅ |

### Quality Score Calculation

```sql
-- Calculate overall quality score
SELECT 
    ROUND(
        (uniqueness_score * 0.25 +
         completeness_score * 0.25 +
         freshness_score * 0.25 +
         validity_score * 0.25), 2
    ) AS overall_quality_score
FROM (
    SELECT 
        -- Uniqueness (0-100)
        CASE WHEN duplicate_count = 0 THEN 100 ELSE 0 END AS uniqueness_score,
        
        -- Completeness (0-100)
        100 - (null_percentage * 100) AS completeness_score,
        
        -- Freshness (0-100)
        CASE 
            WHEN hours_since_update < 24 THEN 100
            WHEN hours_since_update < 48 THEN 50
            ELSE 0
        END AS freshness_score,
        
        -- Validity (0-100)
        100 - (invalid_percentage * 100) AS validity_score
    FROM quality_metrics
) scores;
```

---

## 📊 Summary

| Component | Files | Purpose |
|-----------|-------|---------|
| **Tests** | 1 | Validate data quality (uniqueness) |
| **Alerts** | 1 | Monitor data freshness |

### Quality Dimensions Covered

| Dimension | Test | Alert | Status |
|-----------|------|-------|--------|
| **Uniqueness** | ✅ Yes | ❌ No | Tests only |
| **Completeness** | ❌ No | ❌ No | Not implemented |
| **Freshness** | ❌ No | ✅ Yes | Alerts only |
| **Validity** | ❌ No | ❌ No | Not implemented |
| **Consistency** | ❌ No | ❌ No | Not implemented |

### Key Takeaways

1. **Uniqueness** - Always check for duplicate primary keys
2. **Freshness** - Monitor data staleness with alerts
3. **Quality Gates** - Stop pipeline on critical failures
4. **Quality Scores** - Track overall data quality over time

### Next Steps

- [ ] Add completeness tests (NULL checks)
- [ ] Add validity tests (range checks)
- [ ] Add consistency tests (cross-table validation)
- [ ] Create Grafana dashboard for quality metrics
- [ ] Add quality scoring to Airflow DAGs

---

*Built with ❤️ for Data Engineers learning Data Quality in Banking*
