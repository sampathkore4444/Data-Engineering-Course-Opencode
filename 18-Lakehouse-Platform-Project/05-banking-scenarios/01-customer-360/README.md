# Scenario 1: Customer 360° View

## Business Problem

A bank's relationship manager needs a **complete view of a customer** across all products (savings, credit cards, loans, investments) in **real-time** to provide personalized service during a customer call.

## Current Pain Points

```
┌─────────────────────────────────────────────────────────────┐
│                  BEFORE DATA VIRTUALIZATION                  │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  Customer calls: "What is my total relationship?"           │
│                                                             │
│  Relationship Manager must:                                 │
│  1. Login to Core Banking (Oracle) → Check savings          │
│  2. Login to Cards System (Mainframe) → Check cards         │
│  3. Login to Loans System (SQL Server) → Check loans        │
│  4. Login to Mutual Funds (API) → Check investments         │
│  5. Manually calculate total                                │
│                                                             │
│  ⏱️  Time: 15-30 minutes                                    │
│  😤 Customer frustrated                                     │
│  ❌ Risk of missing data                                     │
└─────────────────────────────────────────────────────────────┘
```

## Solution Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                  AFTER DATA VIRTUALIZATION                   │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  Customer calls: "What is my total relationship?"           │
│                                                             │
│  Relationship Manager runs ONE query:                       │
│                                                             │
│  SELECT * FROM gold.customer_360                            │
│  WHERE customer_id = 'CUST-12345';                         │
│                                                             │
│  ⏱️  Time: < 2 seconds                                      │
│  😊 Customer impressed                                      │
│  ✅ Complete data from all systems                           │
│                                                             │
│            ┌──────────────────────┐                         │
│            │   DREMIO VIRTUAL     │                         │
│            │   LAYER              │                         │
│            │   (Customer 360)     │                         │
│            └──────────┬───────────┘                         │
│                       │                                     │
│         ┌─────────────┼─────────────┐                       │
│         ▼             ▼             ▼                       │
│    ┌─────────┐  ┌──────────┐  ┌──────────┐                 │
│    │ Core    │  │ Cards    │  │ Loans    │                 │
│    │ Banking │  │ System   │  │ System   │                 │
│    └─────────┘  └──────────┘  └──────────┘                 │
└─────────────────────────────────────────────────────────────┘
```

## Implementation

### Step 1: Create Customer 360 View in Dremio

```sql
-- File: customer-360-query.sql
-- Run this in Dremio SQL Editor

CREATE VIEW banking-gold.customer_360 AS
SELECT 
    c.customer_id,
    c.customer_name,
    c.email,
    c.phone,
    c.city,
    
    -- Accounts
    COUNT(DISTINCT a.account_id) AS total_accounts,
    COALESCE(SUM(a.current_balance), 0) AS total_balance,
    
    -- Credit Cards
    COUNT(DISTINCT cc.card_number) AS total_cards,
    COALESCE(SUM(cc.card_limit), 0) AS total_card_limit,
    COALESCE(SUM(cc.credit_used), 0) AS total_card_outstanding,
    
    -- Loans
    COUNT(DISTINCT l.loan_id) AS total_loans,
    COALESCE(SUM(l.principal_outstanding), 0) AS total_loan_outstanding,
    
    -- Net Relationship Value
    (COALESCE(SUM(a.current_balance), 0) 
     + COALESCE(SUM(cc.card_limit), 0) 
     - COALESCE(SUM(l.principal_outstanding), 0)) AS net_relationship_value
    
FROM banking-cleansed.core_banking_customers c
LEFT JOIN banking-cleansed.core_banking_accounts a 
    ON c.customer_id = a.customer_id
LEFT JOIN banking-cleansed.credit_cards cc 
    ON c.customer_id = cc.customer_id
LEFT JOIN banking-cleansed.loan_accounts l 
    ON c.customer_id = l.customer_id
GROUP BY 
    c.customer_id, c.customer_name, c.email, c.phone, c.city;
```

### Step 2: Enable Reflection for Fast Queries

```sql
-- Enable RAW reflection on Customer 360
ALTER VIEW banking-gold.customer_360 
CREATE RAW REFLECTION 
PARTITION BY (customer_id)
DISPLAY BY (customer_name, city)
ORDER BY (customer_id);
```

### Step 3: Query the View

```sql
-- Simple query for relationship manager
SELECT 
    customer_name,
    total_accounts,
    FORMAT_NUMBER(total_balance, 'VND') AS balance,
    total_cards,
    FORMAT_NUMBER(total_card_outstanding, 'VND') AS card_outstanding,
    total_loans,
    FORMAT_NUMBER(total_loan_outstanding, 'VND') AS loan_outstanding,
    FORMAT_NUMBER(net_relationship_value, 'VND') AS total_relationship
FROM banking-gold.customer_360
WHERE customer_id = 'CUST-12345';
```

## Expected Results

| customer_name | total_accounts | balance | total_cards | card_outstanding | total_loans | loan_outstanding | total_relationship |
|--------------|----------------|---------|-------------|------------------|-------------|------------------|-------------------|
| Nguyen Van A | 3 | 150,000,000 | 2 | 5,000,000 | 1 | 500,000,000 | -355,000,000 |
| Tran Thi B | 2 | 25,000,000 | 1 | 2,000,000 | 0 | 0 | 23,000,000 |

## Dashboard Specification

See [dashboard-spec.md](dashboard-spec.md) for detailed dashboard requirements.

## Performance Metrics

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Query Time | 15-30 min | < 2 sec | 99.9% faster |
| Data Freshness | Days | Real-time | Always current |
| User Satisfaction | Low | High | ⭐⭐⭐⭐⭐ |
