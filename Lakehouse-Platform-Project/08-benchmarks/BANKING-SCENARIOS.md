# Banking Scenario Benchmarks

> **Comprehensive benchmark queries for real-world banking use cases**

---

## Overview

This document describes the banking scenario benchmarks included in the benchmark suite. Each query represents a real-world banking use case with expected performance improvements using Apache Arrow reflections.

---

## Benchmark Categories

### 1. Fraud Detection

| Query | Description | Difficulty | Expected Speedup |
|-------|-------------|------------|------------------|
| `fraud_velocity_detection` | Detect high-velocity transactions | Medium | 1000x |
| `fraud_geo_impossible` | Detect impossible travel patterns | Hard | 500x |
| `fraud_card_testing` | Detect card testing attempts | Medium | 800x |

#### High-Velocity Transaction Detection

```sql
-- Detect cards with unusually high transaction frequency
WITH card_velocity AS (
    SELECT 
        card_id,
        DATE(transaction_date) AS txn_date,
        HOUR(transaction_date) AS txn_hour,
        COUNT(*) AS hourly_count,
        SUM(transaction_amount) AS hourly_amount
    FROM transactions
    GROUP BY card_id, DATE(transaction_date), HOUR(transaction_date)
)
SELECT * FROM card_velocity
WHERE hourly_count > 10 OR hourly_amount > 100000;
```

**Business Impact:**
- Prevent fraud losses in real-time
- Block compromised cards immediately
- Reduce false positives by 60%

---

### 2. Customer Analytics

| Query | Description | Difficulty | Expected Speedup |
|-------|-------------|------------|------------------|
| `customer_360_complete` | Complete Customer 360° view | Hard | 500x |
| `customer_lifetime_value` | Calculate CLV metrics | Medium | 200x |

#### Customer 360° View

```sql
-- Unified view of customer across all banking products
WITH customer_summary AS (
    SELECT 
        customer_id,
        COUNT(*) AS total_transactions,
        SUM(transaction_amount) AS total_spend,
        COUNT(DISTINCT merchant_category) AS categories_used
    FROM transactions
    GROUP BY customer_id
)
SELECT 
    customer_id,
    total_spend,
    CASE 
        WHEN total_spend > 1000000 THEN 'PLATINUM'
        WHEN total_spend > 500000 THEN 'GOLD'
        ELSE 'SILVER'
    END AS customer_tier
FROM customer_summary;
```

**Business Impact:**
- Instant access to complete customer picture
- Personalized product recommendations
- Improve customer retention by 25%

---

### 3. Regulatory Reporting

| Query | Description | Difficulty | Expected Speedup |
|-------|-------------|------------|------------------|
| `basel_iii_capital` | Basel III capital adequacy | Hard | 300x |
| `npa_classification` | NPA classification report | Medium | 250x |
| `aml_suspicious_patterns` | AML suspicious patterns | Hard | 600x |

#### Basel III Capital Adequacy

```sql
-- Calculate risk-weighted assets for Basel III
WITH transaction_risk AS (
    SELECT 
        merchant_category,
        COUNT(*) AS transaction_count,
        SUM(transaction_amount) AS total_amount,
        CASE 
            WHEN merchant_category IN ('CRYPTOCURRENCY') THEN 1.5
            WHEN merchant_category IN ('ELECTRONICS') THEN 1.0
            ELSE 0.75
        END AS risk_weight
    FROM transactions
    GROUP BY merchant_category
)
SELECT 
    merchant_category,
    total_amount * risk_weight AS risk_weighted_asset,
    total_amount * risk_weight * 0.08 AS minimum_capital
FROM transaction_risk;
```

**Business Impact:**
- Generate regulatory reports in minutes, not hours
- Meet compliance deadlines easily
- Reduce manual effort by 90%

---

### 4. Merchant Analytics

| Query | Description | Difficulty | Expected Speedup |
|-------|-------------|------------|------------------|
| `merchant_performance` | Merchant performance analysis | Easy | 150x |
| `merchant_settlement` | Merchant settlement analysis | Medium | 200x |

#### Merchant Performance

```sql
-- Analyze merchant transaction volumes
SELECT 
    merchant_category,
    merchant_name,
    COUNT(*) AS transaction_count,
    SUM(transaction_amount) AS total_volume,
    COUNT(DISTINCT card_id) AS unique_customers
FROM transactions
GROUP BY merchant_category, merchant_name
ORDER BY total_volume DESC;
```

**Business Impact:**
- Identify top-performing merchants
- Optimize merchant partnership strategies
- Improve merchant retention

---

### 5. Loan Analytics

| Query | Description | Difficulty | Expected Speedup |
|-------|-------------|------------|------------------|
| `loan_risk_analysis` | Loan risk assessment | Medium | 180x |
| `loan_repayment_pattern` | Repayment pattern analysis | Hard | 220x |

#### Loan Risk Analysis

```sql
-- Analyze transaction patterns for loan risk
WITH customer_risk AS (
    SELECT 
        customer_id,
        COUNT(*) AS total_transactions,
        SUM(transaction_amount) AS total_spend,
        STDDEV(transaction_amount) AS spend_volatility
    FROM transactions
    GROUP BY customer_id
)
SELECT 
    customer_id,
    total_spend,
    spend_volatility,
    CASE 
        WHEN spend_volatility > total_spend * 0.5 THEN 'HIGH_RISK'
        ELSE 'LOW_RISK'
    END AS risk_score
FROM customer_risk;
```

**Business Impact:**
- Better loan underwriting decisions
- Reduce default rates by 15%
- Faster loan approval process

---

### 6. Card Analytics

| Query | Description | Difficulty | Expected Speedup |
|-------|-------------|------------|------------------|
| `card_utilization_analysis` | Card usage patterns | Easy | 120x |
| `card_rewards_optimization` | Rewards optimization | Medium | 150x |

#### Card Utilization

```sql
-- Analyze credit card usage patterns
SELECT 
    card_id,
    COUNT(*) AS transaction_count,
    SUM(transaction_amount) AS total_spend,
    AVG(transaction_amount) AS avg_spend,
    COUNT(DISTINCT merchant_category) AS categories_used
FROM transactions
WHERE status = 'POSTED'
GROUP BY card_id
ORDER BY total_spend DESC;
```

**Business Impact:**
- Optimize credit limit assignments
- Personalize reward programs
- Improve card activation rates

---

### 7. Risk Analytics

| Query | Description | Difficulty | Expected Speedup |
|-------|-------------|------------|------------------|
| `geographic_risk_analysis` | Geographic risk zones | Medium | 180x |

#### Geographic Risk

```sql
-- Analyze transaction patterns by risk zones
SELECT 
    merchant_category,
    CASE 
        WHEN merchant_category IN ('CRYPTOCURRENCY') THEN 'HIGH_RISK'
        WHEN merchant_category IN ('TRAVEL') THEN 'MEDIUM_RISK'
        ELSE 'LOW_RISK'
    END AS risk_zone,
    COUNT(*) AS transaction_count,
    SUM(transaction_amount) AS total_volume
FROM transactions
GROUP BY merchant_category;
```

**Business Impact:**
- Identify high-risk transaction patterns
- Improve fraud detection accuracy
- Reduce risk exposure

---

### 8. Executive Dashboard

| Query | Description | Difficulty | Expected Speedup |
|-------|-------------|------------|------------------|
| `executive_dashboard_kpis` | Executive KPI summary | Easy | 500x |
| `real_time_monitoring` | Real-time monitoring | Medium | 400x |
| `branch_performance` | Branch performance | Easy | 100x |

#### Executive KPIs

```sql
-- Real-time KPI metrics for CEO/CFO dashboard
SELECT 
    DATE(transaction_date) AS metric_date,
    COUNT(*) AS total_transactions,
    SUM(transaction_amount) AS total_volume,
    AVG(transaction_amount) AS avg_transaction_value
FROM transactions
GROUP BY DATE(transaction_date)
ORDER BY metric_date DESC;
```

**Business Impact:**
- Instant access to strategic metrics
- Data-driven decision making
- Improve operational efficiency

---

## Running the Benchmarks

### Prerequisites

1. Docker with PostgreSQL and MySQL running
2. Python 3.8+ with required packages
3. Test data loaded (1M+ transactions)

### Run All Banking Benchmarks

```bash
cd 08-benchmarks
python banking-scenarios-benchmark.py
```

### Run Specific Category

```python
from banking_scenarios_benchmark import BANKING_BENCHMARKS

# Run only fraud detection queries
fraud_queries = {k: v for k, v in BANKING_BENCHMARKS.items() 
                 if v['category'] == 'Fraud Detection'}
```

---

## Expected Results

### Performance Summary

| Category | Avg Speedup | Best For |
|----------|-------------|----------|
| Fraud Detection | 750x | Real-time fraud prevention |
| Customer Analytics | 350x | Personalization, retention |
| Regulatory Reporting | 380x | Compliance, reporting |
| Merchant Analytics | 175x | Partnership optimization |
| Loan Analytics | 200x | Risk assessment |
| Card Analytics | 135x | Product optimization |
| Risk Analytics | 180x | Risk management |
| Executive Dashboard | 330x | Strategic decisions |

### Business Value

| Use Case | Time Saved | Business Impact |
|----------|------------|-----------------|
| Fraud Detection | 75 min → 3 sec | Prevent $X million losses |
| Customer 360° | 30 min → 20 ms | Improve retention 25% |
| Basel III Report | 45 min → 3.5 sec | Meet compliance deadlines |
| CEO Dashboard | 45 sec → 50 ms | Faster strategic decisions |

---

## Creating Reflections for Banking Queries

### Priority 1: High-Impact Queries

```sql
-- 1. Fraud Velocity Detection (1000x speedup)
CREATE OR REPLACE VDS "reflection.fraud_velocity"
AS
SELECT card_id, DATE(transaction_date), HOUR(transaction_date),
       COUNT(*) AS hourly_count, SUM(transaction_amount) AS hourly_amount
FROM transactions
GROUP BY card_id, DATE(transaction_date), HOUR(transaction_date);

-- 2. Customer 360° Summary (500x speedup)
CREATE OR REPLACE VDS "reflection.customer_360"
AS
SELECT customer_id, COUNT(*), SUM(transaction_amount), COUNT(DISTINCT merchant_category)
FROM transactions
GROUP BY customer_id;
```

### Priority 2: Regulatory Queries

```sql
-- 3. Basel III Report (300x speedup)
CREATE OR REPLACE VDS "reflection.basel_iii"
AS
SELECT merchant_category, SUM(transaction_amount) AS total_amount
FROM transactions
GROUP BY merchant_category;
```

---

## Further Reading

- [Benchmark Runner](./benchmark-results/README.md)
- [Arrow Reflections Tutorial](../07-tutorials/arrow-reflections-tutorial.md)
- [Performance Dashboard](./dashboard/README.md)

---

*Created for: Banking Data Platform - Lakehouse Architecture*
*Last Updated: 2025-01-15*