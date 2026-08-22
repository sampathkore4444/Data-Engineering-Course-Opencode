# Scenario 2: Real-Time Fraud Detection

## Business Problem

A bank needs to **detect fraudulent transactions in real-time** to prevent financial losses and protect customers. Current system detects fraud **after the fact** (batch processing), leading to losses.

## Current Pain Points

```
┌─────────────────────────────────────────────────────────────┐
│              BEFORE REAL-TIME FRAUD DETECTION                │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  Fraud Detection Flow:                                      │
│                                                             │
│  Transaction → Batch Processing (Nightly) → Alert          │
│                                                             │
│  ⏱️  Detection Time: 12-24 hours                            │
│  💸 Loss: Already occurred                                  │
│  😤 Customer: Already compromised                           │
│  📊 False Positives: High (40%+)                            │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## Solution Architecture

```
┌─────────────────────────────────────────────────────────────┐
│              REAL-TIME FRAUD DETECTION                       │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  Transaction → Real-time Processing → Instant Alert         │
│                                                             │
│  ⏱️  Detection Time: < 1 second                             │
│  💸 Loss: Prevented                                         │
│  😤 Customer: Protected                                     │
│  📊 False Positives: Low (10%)                              │
│                                                             │
│  ┌─────────┐    ┌─────────┐    ┌─────────┐                 │
│  │ Card    │───►│ Kafka   │───►│ Dremio  │───► Alert       │
│  │ System  │    │ Stream  │    │ Real-   │    Dashboard    │
│  └─────────┘    └─────────┘    │ time    │    Fraud Team   │
│                                └─────────┘                 │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## Fraud Detection Rules

### Rule 1: High-Value Transaction
```sql
-- Flag transactions above threshold
SELECT 
    txn_id,
    card_number,
    amount,
    merchant_name,
    'HIGH_VALUE' AS alert_type,
    CASE 
        WHEN amount > 100000000 THEN 'CRITICAL'
        WHEN amount > 50000000 THEN 'HIGH'
        ELSE 'MEDIUM'
    END AS severity
FROM silver.card_transactions
WHERE amount > 50000000  -- VND 50 million
  AND status = 'SUCCESS';
```

### Rule 2: Velocity Check
```sql
-- Flag cards with too many transactions in short time
WITH txn_velocity AS (
    SELECT 
        card_number,
        COUNT(*) AS txn_count_1hr,
        SUM(amount) AS total_amount_1hr
    FROM silver.card_transactions
    WHERE txn_timestamp >= DATEADD(HOUR, -1, CURRENT_TIMESTAMP())
    GROUP BY card_number
    HAVING COUNT(*) > 5  -- More than 5 transactions in 1 hour
)
SELECT 
    tv.card_number,
    tv.txn_count_1hr,
    tv.total_amount_1hr,
    'VELOCITY_ALERT' AS alert_type,
    'HIGH' AS severity
FROM txn_velocity tv
JOIN silver.credit_cards cc ON tv.card_number = cc.card_number;
```

### Rule 3: Unusual Location
```sql
-- Flag transactions from unusual locations
WITH customer_locations AS (
    SELECT 
        card_number,
        merchant_category,
        COUNT(*) AS txn_count
    FROM silver.card_transactions
    WHERE txn_date >= DATEADD(DAY, -30, CURRENT_DATE)
    GROUP BY card_number, merchant_category
),
current_txn AS (
    SELECT 
        ct.txn_id,
        ct.card_number,
        ct.merchant_name,
        ct.merchant_category,
        ct.amount,
        ct.txn_timestamp
    FROM silver.card_transactions ct
    WHERE ct.txn_timestamp >= DATEADD(HOUR, -1, CURRENT_TIMESTAMP())
)
SELECT 
    ct.txn_id,
    ct.card_number,
    ct.merchant_name,
    ct.amount,
    'UNUSUAL_LOCATION' AS alert_type,
    'MEDIUM' AS severity
FROM current_txn ct
LEFT JOIN customer_locations cl 
    ON ct.card_number = cl.card_number 
    AND ct.merchant_category = cl.merchant_category
WHERE cl.txn_count IS NULL;  -- No history in this category
```

### Rule 4: Card-Not-Present Pattern
```sql
-- Flag online transactions with unusual patterns
SELECT 
    ct.txn_id,
    ct.card_number,
    ct.merchant_name,
    ct.amount,
    'CNP_PATTERN' AS alert_type,
    CASE 
        WHEN ct.amount > 20000000 THEN 'HIGH'
        ELSE 'MEDIUM'
    END AS severity
FROM silver.card_transactions ct
WHERE ct.txn_timestamp >= DATEADD(HOUR, -1, CURRENT_TIMESTAMP())
  AND ct.merchant_category = 'ONLINE'
  AND ct.amount > 10000000  -- Online txns > VND 10 million
  AND NOT EXISTS (
      SELECT 1 FROM silver.card_transactions ct2
      WHERE ct2.card_number = ct.card_number
        AND ct2.txn_date >= DATEADD(DAY, -7, CURRENT_DATE)
        AND ct2.merchant_category = 'ONLINE'
  );  -- No recent online transaction history
```

## Alert Dashboard

```
┌─────────────────────────────────────────────────────────────┐
│              FRAUD ALERT DASHBOARD                           │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  🔴 CRITICAL: 2  |  🟠 HIGH: 5  |  🟡 MEDIUM: 12          │
│                                                             │
│  [Chart: Alerts by Hour - Last 24 Hours]                    │
│  [Chart: Alerts by Type]                                    │
│  [Chart: Top Merchants by Alert Count]                      │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │ RECENT ALERTS                                        │   │
│  │ Time    | Card      | Amount     | Type    | Action  │   │
│  │ 14:32   | XXXX-1234 | 75M VND    | HIGH    | BLOCKED │   │
│  │ 14:28   | XXXX-5678 | 45M VND    | MEDIUM  | FLAGGED │   │
│  │ 14:15   | XXXX-9012 | 120M VND   | CRIT    | BLOCKED │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## Performance Metrics

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Detection Time | 12-24 hours | < 1 second | 99.99% faster |
| False Positive Rate | 40% | 10% | 75% reduction |
| Fraud Loss | 500M VND/month | 50M VND/month | 90% reduction |
| Customer Impact | High | Low | ⭐⭐⭐⭐⭐ |
