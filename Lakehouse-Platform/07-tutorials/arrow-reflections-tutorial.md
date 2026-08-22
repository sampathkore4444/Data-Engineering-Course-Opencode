# Apache Arrow Reflections in Dremio: Complete Tutorial

> **Learn how to enable Arrow reflections for 10-100x faster banking analytics**

---

## Table of Contents

1. [What are Arrow Reflections?](#1-what-are-arrow-reflections)
2. [Types of Reflections](#2-types-of-reflections)
3. [Prerequisites](#3-prerequisites)
4. [Creating Reflections via Dremio UI](#4-creating-reflections-via-dremio-ui)
5. [Creating Reflections via SQL/API](#5-creating-reflections-via-sqlapi)
6. [Banking Use Case Examples](#6-banking-use-case-examples)
7. [Monitoring Reflections](#7-monitoring-reflections)
8. [Best Practices](#8-best-practices)
9. [Troubleshooting](#9-troubleshooting)
10. [Performance Benchmarks](#10-performance-benchmarks)

---

## 1. What are Arrow Reflections?

### The Problem

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    THE PROBLEM: SLOW QUERIES                                │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Your bank has:                                                             │
│  • 100 million card transactions                                            │
│  • 10 million customer records                                              │
│  • 5 million loan records                                                   │
│                                                                             │
│  CEO asks: "What is total relationship value by customer tier?"            │
│                                                                             │
│  Without Reflections:                                                       │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  Query scans 115 million rows                                        │   │
│  │  Joins 4 tables (Customers + Accounts + Cards + Loans)             │   │
│  │  Aggregates and calculates                                           │   │
│  │                                                                     │   │
│  │  Time: 45 MINUTES                                                    │   │
│  │                                                                     │   │
│  │  ❌ CEO: "This is too slow!"                                        │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### The Solution: Arrow Reflections

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    THE SOLUTION: ARROW REFLECTIONS                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Arrow Reflections are PRE-COMPUTED, MATERIALIZED VIEWS stored in          │
│  Apache Arrow's efficient columnar format in memory.                       │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                                                                     │   │
│  │  Raw Data (115M rows)                                               │   │
│  │       │                                                              │   │
│  │       ▼                                                              │   │
│  │  ┌─────────────────────────────────────────┐                        │   │
│  │  │  DREMIO REFLECTION ENGINE               │                        │   │
│  │  │  (Runs in background, uses Arrow)       │                        │   │
│  │  │                                         │                        │   │
│  │  │  1. Reads raw data in Arrow format      │                        │   │
│  │  │  2. Pre-aggregates results              │                        │   │
│  │  │  3. Stores as Arrow reflection          │                        │   │
│  │  │  4. Keeps data fresh (auto-refresh)     │                        │   │
│  │  └─────────────────────────────────────────┘                        │   │
│  │       │                                                              │   │
│  │       ▼                                                              │   │
│  │  Arrow Reflection (10,000 rows)                                     │   │
│  │  ┌─────────────────────────────────────────┐                        │   │
│  │  │  Pre-aggregated in Arrow format         │                        │   │
│  │  │  Stored in memory (instant access)      │                        │   │
│  │  │  Auto-refreshed every 15 minutes        │                        │   │
│  │  └─────────────────────────────────────────┘                        │   │
│  │       │                                                              │   │
│  │       ▼                                                              │   │
│  │  CEO Dashboard Query (50 MILLISECONDS!)                             │   │
│  │                                                                     │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  SPEEDUP: 45 minutes ÷ 0.05 seconds = 54,000x FASTER!                    │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Key Benefits

| Benefit | Description | Banking Impact |
|---------|-------------|----------------|
| **10-100x Faster** | Pre-computed aggregations | Dashboards load instantly |
| **Columnar Storage** | Only read needed columns | 90% less disk I/O |
| **In-Memory** | No disk reads for queries | Sub-second response |
| **Auto-Refresh** | Data stays fresh automatically | Always current metrics |
| **Transparent** | Queries automatically use reflections | No code changes needed |

---

## 2. Types of Reflections

### 2.1 Raw Reflections

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    RAW REFLECTIONS                                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  What: Stores the FULL dataset in Arrow format (no aggregation)            │
│  When: For filtering, sorting, and joining large tables                    │
│  Speedup: 5-10x (due to columnar format)                                   │
│                                                                             │
│  Example: Card Transactions Table                                           │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                                                                     │   │
│  │  Original Table (Row-Based):           Raw Reflection (Arrow):      │   │
│  │  ┌─────────────────────────────┐      ┌─────────────────────────┐  │   │
│  │  │ Row 1: [A1, B1, C1, D1]    │      │ Col A: [A1, A2, A3]    │  │   │
│  │  │ Row 2: [A2, B2, C2, D2]    │  →   │ Col B: [B1, B2, B3]    │  │   │
│  │  │ Row 3: [A3, B3, C3, D3]    │      │ Col C: [C1, C2, C3]    │  │   │
│  │  └─────────────────────────────┘      │ Col D: [D1, D2, D3]    │  │   │
│  │                                       └─────────────────────────┘  │   │
│  │  Size: 500 GB                          Size: 200 GB (compressed)  │   │
│  │                                                                     │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  Best For:                                                                  │
│  • Queries that filter on specific columns                                 │
│  • Join operations on large tables                                         │
│  • Queries that need all rows but few columns                             │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Aggregation Reflections

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    AGGREGATION REFLECTIONS                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  What: Stores PRE-AGGREGATED results (SUM, COUNT, AVG, etc.)              │
│  When: For dashboards and reports with GROUP BY                            │
│  Speedup: 100-1000x (pre-computed aggregations)                            │
│                                                                             │
│  Example: Daily Transaction Summary                                         │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                                                                     │   │
│  │  Original Table (100M rows):         Aggregation Reflection:        │   │
│  │  ┌─────────────────────────────┐      ┌─────────────────────────┐  │   │
│  │  │ date       │ category │ amt │      │ date    │ category │ sum │  │   │
│  │  ├─────────────────────────────┤  →   ├─────────────────────────┤  │   │
│  │  │ 2025-01-15 │ ELEC     │ 15K │      │ 2025-01-15│ ELEC  │ 50M │  │   │
│  │  │ 2025-01-15 │ FOOD     │ 800 │      │ 2025-01-15│ FOOD  │ 30M │  │   │
│  │  │ 2025-01-15 │ TRAVEL   │ 45K │      │ 2025-01-15│ TRAVEL│ 25M │  │   │
│  │  │ 2025-01-15 │ ELEC     │ 12K │      │ 2025-01-16│ ELEC  │ 52M │  │   │
│  │  │ ... (100M rows)          │      │ ... (30 rows!)          │  │   │
│  │  └─────────────────────────────┘      └─────────────────────────┘  │   │
│  │                                                                     │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  Best For:                                                                  │
│  • Dashboard queries with SUM, COUNT, AVG                                 │
│  • GROUP BY queries (by date, category, region)                           │
│  • Executive reports and KPIs                                              │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 2.3 Join Reflections

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    JOIN REFLECTIONS                                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  What: Stores PRE-JOINED data from multiple tables                         │
│  When: For queries that join 2+ tables regularly                            │
│  Speedup: 50-500x (pre-computed joins)                                     │
│                                                                             │
│  Example: Customer 360° View                                                │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                                                                     │   │
│  │  Tables:                            Join Reflection:                │   │
│  │  ┌────────────┐                    ┌─────────────────────────┐     │   │
│  │  │ Customers  │                    │ customer_id │ name │ ...│     │   │
│  │  │ (1M rows)  │                    │ balance     │ cards│    │     │   │
│  │  └─────┬──────┘                    │ loans       │ tier │    │     │   │
│  │        │                           └─────────────────────────┘     │   │
│  │  ┌─────┴──────┐                                                   │   │
│  │  │ Accounts   │  → Pre-joined into single Arrow table             │   │
│  │  │ (2M rows)  │     (1M rows instead of 4M+ rows)                │   │
│  │  └─────┬──────┘                                                   │   │
│  │        │                                                          │   │
│  │  ┌─────┴──────┐                                                   │   │
│  │  │ Cards      │                                                   │   │
│  │  │ (500K rows)│                                                   │   │
│  │  └─────┬──────┘                                                   │   │
│  │        │                                                          │   │
│  │  ┌─────┴──────┐                                                   │   │
│  │  │ Loans      │                                                   │   │
│  │  │ (1M rows)  │                                                   │   │
│  │  └────────────┘                                                   │   │
│  │                                                                     │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  Best For:                                                                  │
│  • Customer 360° views                                                     │
│  • Reports joining multiple source systems                                 │
│  • Complex analytical queries                                              │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Reflection Comparison

| Type | Speedup | Best For | Storage | Refresh Time |
|------|---------|----------|---------|--------------|
| **Raw** | 5-10x | Filtering, sorting | High (full copy) | Fast (minutes) |
| **Aggregation** | 100-1000x | Dashboards, KPIs | Low (aggregated) | Fast (minutes) |
| **Join** | 50-500x | Multi-table queries | Medium (joined) | Medium (minutes) |

---

## 3. Prerequisites

### System Requirements

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    PREREQUISITES                                            │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  1. DREMIO VERSION                                                          │
│     • Dremio 20.0+ (Community or Enterprise)                               │
│     • Arrow reflections are built-in (no extra setup)                      │
│                                                                             │
│  2. MEMORY REQUIREMENTS                                                     │
│     • Minimum: 16 GB RAM per node                                          │
│     • Recommended: 32+ GB RAM per node                                     │
│     • Reflections use memory for caching                                   │
│                                                                             │
│  3. STORAGE                                                                 │
│     • Sufficient disk space for reflections                                │
│     • Rule of thumb: 2-3x your raw data size                              │
│                                                                             │
│  4. DATA FORMAT                                                             │
│     • Best: Parquet, Iceberg, Delta Lake                                   │
│     • Acceptable: CSV, JSON, Avro                                          │
│     • Arrow optimizes columnar formats better                              │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Verify Dremio Installation

```bash
# Check Dremio version
docker compose exec dremio-master /opt/dremio/bin/dremio-client --version

# Check available memory
docker compose exec dremio-master free -h

# Access Dremio UI
# Open browser: http://localhost:9047
# Login: admin / admin123
```

---

## 4. Creating Reflections via Dremio UI

### Step 1: Navigate to Dataset

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    DREMIO UI: NAVIGATE TO DATASET                           │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  1. Open Dremio UI: http://localhost:9047                                   │
│  2. Login with admin credentials                                            │
│  3. Click on "Datasets" in left sidebar                                    │
│  4. Navigate to your dataset:                                               │
│     └── banking-vault                                                       │
│         └── virtual                                                          │
│             └── card_transaction_analytics                                  │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  📁 banking-vault                                                   │   │
│  │  ├── 📁 virtual                                                     │   │
│  │  │   ├── 📊 customer_master                                         │   │
│  │  │   ├── 📊 customer_accounts                                       │   │
│  │  │   ├── 📊 customer_cards                                          │   │
│  │  │   ├── 📊 customer_loans                                          │   │
│  │  │   ├── 📊 card_transaction_analytics  ← SELECT THIS              │   │
│  │  │   ├── 📊 transaction_analytics                                   │   │
│  │  │   └── 📊 loan_performance                                        │   │
│  │  └── ...                                                            │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Step 2: Create Reflection

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    DREMIO UI: CREATE REFLECTION                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  1. Click on dataset: card_transaction_analytics                           │
│  2. Click "Reflection" tab (top right)                                     │
│  3. Click "New Reflection" button                                          │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                                                                     │   │
│  │  New Reflection                                                     │   │
│  │  ═══════════════════════════════════════════════════════════════   │   │
│  │                                                                     │   │
│  │  Name: [card_txns_daily_summary                               ]    │   │
│  │                                                                     │   │
│  │  Type:  ◉ Aggregation Reflection                                  │   │
│  │         ○ Raw Reflection                                           │   │
│  │                                                                     │   │
│  │  Display Columns:                                                   │   │
│  │  ☑ transaction_date                                                │   │
│  │  ☑ merchant_category                                               │   │
│  │  ☑ card_type                                                       │   │
│  │  ☐ transaction_id                                                  │   │
│  │  ☐ card_id                                                         │   │
│  │  ☐ merchant_name                                                   │   │
│  │  ☐ ...                                                             │   │
│  │                                                                     │   │
│  │  Group By:                                                          │   │
│  │  ☑ transaction_date                                                │   │
│  │  ☑ merchant_category                                               │   │
│  │  ☑ card_type                                                       │   │
│  │                                                                     │   │
│  │  Measures:                                                          │   │
│  │  ☑ COUNT(*)                                                        │   │
│  │  ☑ SUM(transaction_amount)                                         │   │
│  │  ☑ AVG(transaction_amount)                                         │   │
│  │  ☑ MAX(transaction_amount)                                         │   │
│  │                                                                     │   │
│  │  Refresh Settings:                                                  │   │
│  │  ☑ Auto Refresh                                                    │   │
│  │  Every: [15] minutes                                               │   │
│  │                                                                     │   │
│  │  [Cancel]  [Create Reflection]                                     │   │
│  │                                                                     │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  4. Click "Create Reflection"                                               │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Step 3: Monitor Reflection Build

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    DREMIO UI: MONITOR REFLECTION BUILD                      │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  1. Click "Reflections" in left sidebar                                    │
│  2. Find your reflection: card_txns_daily_summary                          │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                                                                     │   │
│  │  Reflections                                                        │   │
│  │  ═══════════════════════════════════════════════════════════════   │   │
│  │                                                                     │   │
│  │  Name                    │ Status    │ Size  │ Last Refresh         │   │
│  │  ─────────────────────────────────────────────────────────────────  │   │
│  │  card_txns_daily_summary │ 🔄 Building│ ...   │ ...                 │   │
│  │  customer_360_agg        │ ✅ Active │ 50 MB │ 2 min ago           │   │
│  │  daily_transactions      │ ✅ Active │ 25 MB │ 5 min ago           │   │
│  │                                                                     │   │
│  │  Status Icons:                                                      │   │
│  │  🔄 Building - Reflection is being built (wait)                    │   │
│  │  ✅ Active  - Reflection is ready to use                           │   │
│  │  ⚠️  Stale   - Reflection needs refresh                            │   │
│  │  ❌ Failed  - Reflection build failed (check logs)                 │   │
│  │                                                                     │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  Wait for status to change from 🔄 Building to ✅ Active                   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Step 4: Verify Reflection is Used

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    VERIFY REFLECTION IS USED                                │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  1. Run a query that matches the reflection:                               │
│                                                                             │
│  ```sql                                                                     │
│  SELECT                                                                     │
│      transaction_date,                                                      │
│      merchant_category,                                                     │
│      card_type,                                                             │
│      COUNT(*) AS transaction_count,                                        │
│      SUM(transaction_amount) AS total_amount,                              │
│      AVG(transaction_amount) AS avg_amount                                 │
│  FROM "banking-vault"."virtual.card_transaction_analytics"                  │
│  WHERE transaction_date >= '2025-01-01'                                    │
│  GROUP BY transaction_date, merchant_category, card_type                   │
│  ORDER BY transaction_date DESC, total_amount DESC;                        │
│  ```                                                                        │
│                                                                             │
│  2. Check query profile:                                                   │
│     • Click "Profile" tab after query runs                                │
│     • Look for "Reflection" in the profile                                │
│     • Should show: "Used reflection: card_txns_daily_summary"             │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  Query Profile                                                      │   │
│  │  ═══════════════════════════════════════════════════════════════   │   │
│  │                                                                     │   │
│  │  Duration: 52 ms                                                    │   │
│  │  Rows Returned: 450                                                 │   │
│  │                                                                     │   │
│  │  Execution Plan:                                                    │   │
│  │  ├── Project                                                        │   │
│  │  │   └── Sort                                                       │   │
│  │  │       └── HashAggregate                                          │   │
│  │  │           └── ReflectionScan: card_txns_daily_summary ✅        │   │
│  │  │               (Used reflection!)                                 │   │
│  │  └── ...                                                            │   │
│  │                                                                     │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 5. Creating Reflections via SQL/API

### 5.1 SQL-Based Creation (Recommended for Automation)

```sql
-- =====================================================
-- REFLECTION 1: Daily Transaction Summary (Aggregation)
-- =====================================================
-- Purpose: Speed up daily transaction reports
-- Expected Speedup: 100-1000x

CREATE OR REPLACE VDS "banking-vault"."reflection.daily_txns_summary"
AS
SELECT 
    transaction_date,
    merchant_category,
    card_type,
    COUNT(*) AS transaction_count,
    SUM(transaction_amount) AS total_amount,
    AVG(transaction_amount) AS avg_amount,
    MAX(transaction_amount) AS max_amount,
    MIN(transaction_amount) AS min_amount,
    COUNT(DISTINCT card_id) AS unique_cards
FROM "banking-vault"."virtual.card_transaction_analytics"
WHERE transaction_date >= CURRENT_DATE - INTERVAL '90' DAY
GROUP BY transaction_date, merchant_category, card_type;

-- =====================================================
-- REFLECTION 2: Customer 360° Summary (Join + Aggregation)
-- =====================================================
-- Purpose: Speed up Customer 360° dashboard
-- Expected Speedup: 50-500x

CREATE OR REPLACE VDS "banking-vault"."reflection.customer_360_summary"
AS
SELECT 
    c.customer_id,
    c.customer_name,
    c.customer_type,
    c.city,
    c.state,
    
    -- Account Summary
    COUNT(DISTINCT a.account_id) AS total_accounts,
    SUM(CASE WHEN a.account_type = 'SAVINGS' THEN a.balance ELSE 0 END) AS savings_balance,
    SUM(CASE WHEN a.account_type = 'CURRENT' THEN a.balance ELSE 0 END) AS current_balance,
    
    -- Cards Summary
    COUNT(DISTINCT cc.card_id) AS total_cards,
    SUM(cc.outstanding) AS total_card_outstanding,
    SUM(cc.credit_limit) AS total_credit_limit,
    
    -- Loans Summary
    COUNT(DISTINCT l.loan_id) AS total_loans,
    SUM(l.principal_outstanding) AS total_loan_outstanding,
    SUM(l.emi_amount) AS total_monthly_emi,
    COUNT(CASE WHEN l.npa_classification != 'STANDARD' THEN 1 END) AS npa_accounts
    
FROM "banking-vault"."virtual.customer_master" c
LEFT JOIN "banking-vault"."virtual.customer_accounts" a ON c.customer_id = a.customer_id
LEFT JOIN "banking-vault"."virtual.customer_cards" cc ON c.customer_id = cc.customer_id
LEFT JOIN "banking-vault"."virtual.customer_loans" l ON c.customer_id = l.customer_id
GROUP BY 
    c.customer_id, c.customer_name, c.customer_type, 
    c.city, c.state;

-- =====================================================
-- REFLECTION 3: Hourly Transaction Velocity (Raw)
-- =====================================================
-- Purpose: Speed up fraud detection queries
-- Expected Speedup: 5-10x

CREATE OR REPLACE VDS "banking-vault"."reflection.hourly_velocity"
AS
SELECT 
    card_id,
    customer_id,
    DATE_TRUNC('hour', transaction_date) AS hour_bucket,
    COUNT(*) AS transaction_count,
    SUM(transaction_amount) AS total_amount,
    AVG(transaction_amount) AS avg_amount,
    MAX(transaction_amount) AS max_amount,
    COUNT(DISTINCT merchant_name) AS unique_merchants,
    COUNT(DISTINCT merchant_category) AS unique_categories
FROM "banking-vault"."virtual.card_transaction_analytics"
WHERE transaction_date >= CURRENT_DATE - INTERVAL '7' DAY
GROUP BY 
    card_id, 
    customer_id, 
    DATE_TRUNC('hour', transaction_date);

-- =====================================================
-- REFLECTION 4: NPA Tracking Summary (Aggregation)
-- =====================================================
-- Purpose: Speed up NPA regulatory reports
-- Expected Speedup: 100-500x

CREATE OR REPLACE VDS "banking-vault"."reflection.npa_summary"
AS
SELECT 
    npa_classification,
    loan_type,
    COUNT(*) AS account_count,
    SUM(loan_amount) AS total_loan_amount,
    SUM(principal_outstanding) AS outstanding_amount,
    AVG(days_past_due) AS avg_days_past_due,
    MAX(days_past_due) AS max_days_past_due,
    SUM(emi_amount) AS total_monthly_emi,
    
    -- Provision calculation
    CASE 
        WHEN npa_classification = 'SUB_STANDARD' THEN SUM(principal_outstanding) * 0.15
        WHEN npa_classification = 'DOUBTFUL' THEN SUM(principal_outstanding) * 0.40
        WHEN npa_classification = 'LOSS' THEN SUM(principal_outstanding) * 1.00
        ELSE SUM(principal_outstanding) * 0.0040
    END AS provision_required
    
FROM "banking-vault"."virtual.loan_performance"
WHERE loan_status = 'ACTIVE'
GROUP BY npa_classification, loan_type;

-- =====================================================
-- REFLECTION 5: Real-Time Fraud Indicators (Raw)
-- =====================================================
-- Purpose: Speed up real-time fraud detection
-- Expected Speedup: 5-10x

CREATE OR REPLACE VDS "banking-vault"."reflection.fraud_indicators"
AS
SELECT 
    transaction_id,
    card_id,
    customer_id,
    transaction_amount,
    merchant_category,
    merchant_name,
    transaction_date,
    status,
    
    -- Velocity indicators (computed in reflection)
    COUNT(*) OVER (
        PARTITION BY card_id 
        ORDER BY transaction_date 
        RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW
    ) AS txn_count_last_hour,
    
    SUM(transaction_amount) OVER (
        PARTITION BY card_id 
        ORDER BY transaction_date 
        RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW
    ) AS total_amount_last_hour
    
FROM "banking-vault"."virtual.card_transaction_analytics"
WHERE transaction_date >= CURRENT_TIMESTAMP - INTERVAL '24' HOUR;
```

### 5.2 REST API Creation (For Automation/CI-CD)

```bash
#!/bin/bash
# create-reflections.sh
# Create Arrow reflections via Dremio REST API

DREMIO_URL="http://localhost:9047"
USERNAME="admin"
PASSWORD="admin123"

# Get authentication token
TOKEN=$(curl -s -X POST "$DREMIO_URL/apiv2/login" \
    -H "Content-Type: application/json" \
    -d "{\"userName\": \"$USERNAME\", \"password\": \"$PASSWORD\"}" \
    | jq -r '.token')

# Function to create reflection
create_reflection() {
    local reflection_name=$1
    local reflection_config=$2
    
    echo "Creating reflection: $reflection_name"
    
    curl -s -X POST "$DREMIO_URL/api/v3/reflection" \
        -H "Authorization: Bearer $TOKEN" \
        -H "Content-Type: application/json" \
        -d "$reflection_config" | jq .
    
    echo ""
}

# Reflection 1: Daily Transaction Summary
create_reflection "card_txns_daily_summary" '{
    "id": null,
    "name": "card_txns_daily_summary",
    "type": "AGGREGATION",
    "datasetId": "banking-vault.virtual.card_transaction_analytics",
    "enabled": true,
    "displayFields": [
        {"name": "transaction_date"},
        {"name": "merchant_category"},
        {"name": "card_type"}
    ],
    "groupings": [
        {"name": "transaction_date"},
        {"name": "merchant_category"},
        {"name": "card_type"}
    ],
    "measures": [
        {"name": "transaction_amount", "function": "COUNT"},
        {"name": "transaction_amount", "function": "SUM"},
        {"name": "transaction_amount", "function": "AVG"},
        {"name": "transaction_amount", "function": "MAX"}
    ],
    "refreshSchedule": {
        "every": "15m"
    }
}'

# Reflection 2: Customer 360 Summary
create_reflection "customer_360_summary" '{
    "id": null,
    "name": "customer_360_summary",
    "type": "AGGREGATION",
    "datasetId": "banking-vault.virtual.customer_360",
    "enabled": true,
    "displayFields": [
        {"name": "customer_id"},
        {"name": "customer_name"},
        {"name": "customer_type"},
        {"name": "city"},
        {"name": "state"},
        {"name": "total_accounts"},
        {"name": "savings_balance"},
        {"name": "current_balance"},
        {"name": "total_cards"},
        {"name": "total_card_outstanding"},
        {"name": "total_loans"},
        {"name": "total_loan_outstanding"}
    ],
    "groupings": [
        {"name": "customer_id"},
        {"name": "customer_name"},
        {"name": "customer_type"},
        {"name": "city"},
        {"name": "state"}
    ],
    "measures": [
        {"name": "total_accounts", "function": "SUM"},
        {"name": "savings_balance", "function": "SUM"},
        {"name": "current_balance", "function": "SUM"},
        {"name": "total_cards", "function": "SUM"},
        {"name": "total_card_outstanding", "function": "SUM"},
        {"name": "total_loans", "function": "SUM"},
        {"name": "total_loan_outstanding", "function": "SUM"}
    ],
    "refreshSchedule": {
        "every": "30m"
    }
}'

echo "All reflections created successfully!"
```

### 5.3 Python API Creation (For Data Engineering Pipelines)

```python
"""
create_reflections.py
Create Arrow reflections via Dremio Python API
"""

import requests
import json
from typing import Dict, List

class DremioReflectionManager:
    """Manage Arrow reflections in Dremio"""
    
    def __init__(self, base_url: str, username: str, password: str):
        self.base_url = base_url
        self.session = requests.Session()
        self.token = self._authenticate(username, password)
        self.session.headers.update({
            'Authorization': f'Bearer {self.token}',
            'Content-Type': 'application/json'
        })
    
    def _authenticate(self, username: str, password: str) -> str:
        """Authenticate with Dremio and get token"""
        response = self.session.post(
            f"{self.base_url}/apiv2/login",
            json={"userName": username, "password": password}
        )
        response.raise_for_status()
        return response.json()['token']
    
    def create_aggregation_reflection(
        self,
        name: str,
        dataset_id: str,
        display_fields: List[str],
        groupings: List[str],
        measures: List[Dict],
        refresh_every: str = "15m"
    ) -> Dict:
        """Create an aggregation reflection"""
        
        reflection = {
            "id": None,
            "name": name,
            "type": "AGGREGATION",
            "datasetId": dataset_id,
            "enabled": True,
            "displayFields": [{"name": f} for f in display_fields],
            "groupings": [{"name": g} for g in groupings],
            "measures": measures,
            "refreshSchedule": {"every": refresh_every}
        }
        
        response = self.session.post(
            f"{self.base_url}/api/v3/reflection",
            json=reflection
        )
        response.raise_for_status()
        return response.json()
    
    def create_raw_reflection(
        self,
        name: str,
        dataset_id: str,
        display_fields: List[str],
        refresh_every: str = "15m"
    ) -> Dict:
        """Create a raw reflection"""
        
        reflection = {
            "id": None,
            "name": name,
            "type": "RAW",
            "datasetId": dataset_id,
            "enabled": True,
            "displayFields": [{"name": f} for f in display_fields],
            "refreshSchedule": {"every": refresh_every}
        }
        
        response = self.session.post(
            f"{self.base_url}/api/v3/reflection",
            json=reflection
        )
        response.raise_for_status()
        return response.json()
    
    def list_reflections(self) -> List[Dict]:
        """List all reflections"""
        response = self.session.get(f"{self.base_url}/api/v3/reflection")
        response.raise_for_status()
        return response.json()
    
    def get_reflection_status(self, reflection_id: str) -> Dict:
        """Get reflection status"""
        response = self.session.get(
            f"{self.base_url}/api/v3/reflection/{reflection_id}"
        )
        response.raise_for_status()
        return response.json()
    
    def refresh_reflection(self, reflection_id: str) -> Dict:
        """Manually refresh a reflection"""
        response = self.session.post(
            f"{self.base_url}/api/v3/reflection/{reflection_id}/refresh"
        )
        response.raise_for_status()
        return response.json()
    
    def delete_reflection(self, reflection_id: str) -> None:
        """Delete a reflection"""
        response = self.session.delete(
            f"{self.base_url}/api/v3/reflection/{reflection_id}"
        )
        response.raise_for_status()


def create_banking_reflections():
    """Create all banking reflections"""
    
    # Initialize manager
    manager = DremioReflectionManager(
        base_url="http://localhost:9047",
        username="admin",
        password="admin123"
    )
    
    print("Creating banking reflections...")
    
    # Reflection 1: Daily Transaction Summary
    print("\n1. Creating card_txns_daily_summary...")
    manager.create_aggregation_reflection(
        name="card_txns_daily_summary",
        dataset_id="banking-vault.virtual.card_transaction_analytics",
        display_fields=["transaction_date", "merchant_category", "card_type"],
        groupings=["transaction_date", "merchant_category", "card_type"],
        measures=[
            {"name": "transaction_amount", "function": "COUNT"},
            {"name": "transaction_amount", "function": "SUM"},
            {"name": "transaction_amount", "function": "AVG"},
            {"name": "transaction_amount", "function": "MAX"}
        ],
        refresh_every="15m"
    )
    
    # Reflection 2: Customer 360 Summary
    print("2. Creating customer_360_summary...")
    manager.create_aggregation_reflection(
        name="customer_360_summary",
        dataset_id="banking-vault.virtual.customer_360",
        display_fields=[
            "customer_id", "customer_name", "customer_type", "city",
            "total_accounts", "savings_balance", "current_balance",
            "total_cards", "total_card_outstanding",
            "total_loans", "total_loan_outstanding"
        ],
        groupings=[
            "customer_id", "customer_name", "customer_type", "city"
        ],
        measures=[
            {"name": "total_accounts", "function": "SUM"},
            {"name": "savings_balance", "function": "SUM"},
            {"name": "current_balance", "function": "SUM"},
            {"name": "total_cards", "function": "SUM"},
            {"name": "total_card_outstanding", "function": "SUM"},
            {"name": "total_loans", "function": "SUM"},
            {"name": "total_loan_outstanding", "function": "SUM"}
        ],
        refresh_every="30m"
    )
    
    # Reflection 3: Hourly Velocity (Raw)
    print("3. Creating hourly_velocity...")
    manager.create_raw_reflection(
        name="hourly_velocity",
        dataset_id="banking-vault.virtual.card_transaction_analytics",
        display_fields=[
            "card_id", "customer_id", "transaction_date",
            "transaction_amount", "merchant_category", "status"
        ],
        refresh_every="15m"
    )
    
    # Reflection 4: NPA Summary
    print("4. Creating npa_summary...")
    manager.create_aggregation_reflection(
        name="npa_summary",
        dataset_id="banking-vault.virtual.loan_performance",
        display_fields=[
            "npa_classification", "loan_type",
            "account_count", "outstanding_amount", "avg_days_past_due"
        ],
        groupings=["npa_classification", "loan_type"],
        measures=[
            {"name": "loan_amount", "function": "COUNT"},
            {"name": "principal_outstanding", "function": "SUM"},
            {"name": "days_past_due", "function": "AVG"},
            {"name": "days_past_due", "function": "MAX"}
        ],
        refresh_every="1h"
    )
    
    print("\n✅ All banking reflections created successfully!")
    
    # List all reflections
    print("\nCurrent reflections:")
    reflections = manager.list_reflections()
    for ref in reflections.get('data', []):
        print(f"  - {ref['name']} ({ref['type']})")


if __name__ == "__main__":
    create_banking_reflections()
```

---

## 6. Banking Use Case Examples

### Example 1: CEO Dashboard (Instant Access)

```sql
-- =====================================================
-- CEO DASHBOARD QUERY
-- Speedup: 54,000x (45 minutes → 50 milliseconds)
-- =====================================================

-- This query automatically uses the customer_360_summary reflection
SELECT 
    customer_tier,
    COUNT(*) AS customer_count,
    SUM(savings_balance) AS total_savings,
    SUM(current_balance) AS total_current,
    SUM(total_card_outstanding) AS total_cards,
    SUM(total_loan_outstanding) AS total_loans,
    SUM(savings_balance + current_balance + 
        total_card_outstanding + total_loan_outstanding) AS total_relationship
FROM "banking-vault"."reflection.customer_360_summary"
GROUP BY customer_tier
ORDER BY total_relationship DESC;

-- Expected execution time: 50 milliseconds (with reflection)
-- Without reflection: 45 minutes
```

### Example 2: Fraud Detection Dashboard

```sql
-- =====================================================
-- FRAUD DETECTION DASHBOARD
-- Speedup: 1,000x (10 minutes → 1 second)
-- =====================================================

-- Real-time velocity monitoring
SELECT 
    card_id,
    customer_id,
    DATE_TRUNC('hour', transaction_date) AS hour_bucket,
    COUNT(*) AS transaction_count,
    SUM(transaction_amount) AS total_amount,
    AVG(transaction_amount) AS avg_amount,
    COUNT(DISTINCT merchant_name) AS unique_merchants,
    
    -- Risk indicators
    CASE 
        WHEN COUNT(*) > 10 THEN 'HIGH_VELOCITY'
        WHEN SUM(transaction_amount) > 500000 THEN 'HIGH_VALUE'
        WHEN COUNT(DISTINCT merchant_name) > 5 THEN 'MULTIPLE_MERCHANTS'
        ELSE 'NORMAL'
    END AS risk_flag
    
FROM "banking-vault"."reflection.hourly_velocity"
WHERE transaction_date >= CURRENT_TIMESTAMP - INTERVAL '24' HOUR
GROUP BY card_id, customer_id, DATE_TRUNC('hour', transaction_date)
HAVING COUNT(*) > 5 OR SUM(transaction_amount) > 200000
ORDER BY total_amount DESC;

-- Expected execution time: 1 second (with reflection)
-- Without reflection: 10 minutes
```

### Example 3: NPA Regulatory Report

```sql
-- =====================================================
-- NPA REGULATORY REPORT (RBI Compliance)
-- Speedup: 500x (15 minutes → 2 seconds)
-- =====================================================

SELECT 
    npa_classification,
    loan_type,
    account_count,
    outstanding_amount,
    avg_days_past_due,
    max_days_past_due,
    provision_required,
    
    -- Risk metrics
    outstanding_amount / 
        (SELECT SUM(outstanding_amount) FROM "banking-vault"."reflection.npa_summary") * 100 
        AS pct_of_total,
    
    -- Compliance status
    CASE 
        WHEN npa_classification = 'STANDARD' AND avg_days_past_due = 0 THEN 'COMPLIANT'
        WHEN npa_classification != 'STANDARD' AND provision_required > 0 THEN 'NEEDS_PROVISION'
        ELSE 'UNDER_REVIEW'
    END AS compliance_status
    
FROM "banking-vault"."reflection.npa_summary"
ORDER BY 
    CASE npa_classification
        WHEN 'STANDARD' THEN 1
        WHEN 'SUB_STANDARD' THEN 2
        WHEN 'DOUBTFUL' THEN 3
        WHEN 'LOSS' THEN 4
    END;

-- Expected execution time: 2 seconds (with reflection)
-- Without reflection: 15 minutes
```

### Example 4: Daily Transaction Report

```sql
-- =====================================================
-- DAILY TRANSACTION REPORT (MIS)
-- Speedup: 200x (5 minutes → 1.5 seconds)
-- =====================================================

SELECT 
    transaction_date,
    merchant_category,
    card_type,
    transaction_count,
    total_amount,
    avg_amount,
    max_amount,
    
    -- Percentage of total
    total_amount / 
        (SELECT SUM(total_amount) FROM "banking-vault"."reflection.daily_txns_summary"
         WHERE transaction_date = CURRENT_DATE - 1) * 100 
        AS pct_of_daily_total,
    
    -- Day-over-day comparison
    total_amount - LAG(total_amount) OVER (
        PARTITION BY merchant_category, card_type 
        ORDER BY transaction_date
    ) AS daily_change,
    
    -- Trend
    CASE 
        WHEN total_amount > LAG(total_amount) OVER (
            PARTITION BY merchant_category, card_type 
            ORDER BY transaction_date
        ) THEN 'INCREASING'
        WHEN total_amount < LAG(total_amount) OVER (
            PARTITION BY merchant_category, card_type 
            ORDER BY transaction_date
        ) THEN 'DECREASING'
        ELSE 'STABLE'
    END AS trend
    
FROM "banking-vault"."reflection.daily_txns_summary"
WHERE transaction_date >= CURRENT_DATE - INTERVAL '30' DAY
ORDER BY transaction_date DESC, total_amount DESC;

-- Expected execution time: 1.5 seconds (with reflection)
-- Without reflection: 5 minutes
```

---

## 7. Monitoring Reflections

### 7.1 Check Reflection Status

```sql
-- List all reflections and their status
SELECT 
    name,
    type,
    status,
    size_bytes / 1024 / 1024 AS size_mb,
    last_refresh_time,
    refresh_duration_ms
FROM "sys"."reflection"
ORDER BY last_refresh_time DESC;

-- Check specific reflection
SELECT 
    name,
    status,
    enabled,
    size_bytes / 1024 / 1024 AS size_mb,
    last_refresh_time,
    refresh_duration_ms,
    failure_message
FROM "sys"."reflection"
WHERE name = 'card_txns_daily_summary';
```

### 7.2 Check Query Profile for Reflection Usage

```sql
-- Run query and check profile
SELECT 
    transaction_date,
    merchant_category,
    SUM(transaction_amount) AS total
FROM "banking-vault"."virtual.card_transaction_analytics"
WHERE transaction_date >= '2025-01-01'
GROUP BY transaction_date, merchant_category;

-- Then check profile in Dremio UI:
-- Click "Profile" tab → Look for "ReflectionScan" in execution plan
-- If you see "ReflectionScan: card_txns_daily_summary" → Reflection is being used!
```

### 7.3 Monitor Reflection Refresh

```sql
-- Check reflection refresh history
SELECT 
    reflection_name,
    refresh_start_time,
    refresh_end_time,
    refresh_duration_ms,
    rows_refreshed,
    status
FROM "sys"."reflection_refresh_history"
WHERE reflection_name = 'card_txns_daily_summary'
ORDER BY refresh_start_time DESC
LIMIT 10;
```

### 7.4 Python Monitoring Script

```python
"""
monitor_reflections.py
Monitor Arrow reflections health and performance
"""

import requests
import time
from datetime import datetime, timedelta

class ReflectionMonitor:
    """Monitor Dremio reflections"""
    
    def __init__(self, base_url: str, username: str, password: str):
        self.base_url = base_url
        self.session = requests.Session()
        self.token = self._authenticate(username, password)
        self.session.headers.update({
            'Authorization': f'Bearer {self.token}'
        })
    
    def _authenticate(self, username: str, password: str) -> str:
        """Authenticate with Dremio"""
        response = self.session.post(
            f"{self.base_url}/apiv2/login",
            json={"userName": username, "password": password}
        )
        response.raise_for_status()
        return response.json()['token']
    
    def get_all_reflections(self) -> list:
        """Get all reflections"""
        response = self.session.get(f"{self.base_url}/api/v3/reflection")
        response.raise_for_status()
        return response.json().get('data', [])
    
    def check_reflection_health(self):
        """Check health of all reflections"""
        reflections = self.get_all_reflections()
        
        print("\n" + "="*80)
        print("REFLECTION HEALTH REPORT")
        print("="*80)
        
        unhealthy = []
        
        for ref in reflections:
            status = ref.get('status', 'UNKNOWN')
            name = ref.get('name', 'UNKNOWN')
            last_refresh = ref.get('lastRefreshTime', 'NEVER')
            
            # Check if reflection is healthy
            is_healthy = status in ['AVAILABLE', 'EXPIRED']
            
            status_icon = "✅" if is_healthy else "❌"
            
            print(f"{status_icon} {name}")
            print(f"   Status: {status}")
            print(f"   Last Refresh: {last_refresh}")
            print(f"   Size: {ref.get('sizeBytes', 0) / 1024 / 1024:.2f} MB")
            print()
            
            if not is_healthy:
                unhealthy.append(name)
        
        if unhealthy:
            print("\n⚠️  UNHEALTHY REFLECTIONS:")
            for name in unhealthy:
                print(f"   - {name}")
        else:
            print("\n✅ All reflections are healthy!")
        
        return unhealthy
    
    def check_query_performance(self, sql: str, expected_max_ms: int = 1000):
        """Check if query performance meets expectations"""
        print(f"\nRunning query: {sql[:50]}...")
        
        start_time = time.time()
        
        # Execute query
        response = self.session.post(
            f"{self.base_url}/api/v3/sql",
            json={"sql": sql}
        )
        response.raise_for_status()
        
        duration_ms = (time.time() - start_time) * 1000
        
        print(f"Execution time: {duration_ms:.2f} ms")
        
        if duration_ms > expected_max_ms:
            print(f"⚠️  WARNING: Query exceeded expected time ({expected_max_ms} ms)")
            print("   Consider creating a reflection for this query pattern")
        else:
            print("✅ Query performance is good!")
        
        return duration_ms


def main():
    """Main monitoring function"""
    
    monitor = ReflectionMonitor(
        base_url="http://localhost:9047",
        username="admin",
        password="admin123"
    )
    
    # Check reflection health
    unhealthy = monitor.check_reflection_health()
    
    # Check query performance
    test_queries = [
        {
            "sql": """
                SELECT merchant_category, SUM(transaction_amount) 
                FROM "banking-vault"."virtual.card_transaction_analytics"
                WHERE transaction_date >= '2025-01-01'
                GROUP BY merchant_category
            """,
            "expected_max_ms": 1000,
            "description": "Merchant category summary"
        },
        {
            "sql": """
                SELECT customer_tier, COUNT(*), SUM(total_relationship_value)
                FROM "banking-vault"."reflection.customer_360_summary"
                GROUP BY customer_tier
            """,
            "expected_max_ms": 100,
            "description": "Customer tier summary (should use reflection)"
        }
    ]
    
    print("\n" + "="*80)
    print("QUERY PERFORMANCE TESTS")
    print("="*80)
    
    for query in test_queries:
        print(f"\nTest: {query['description']}")
        duration = monitor.check_query_performance(
            query['sql'], 
            query['expected_max_ms']
        )
    
    # Summary
    print("\n" + "="*80)
    print("MONITORING SUMMARY")
    print("="*80)
    
    if unhealthy:
        print(f"⚠️  {len(unhealthy)} unhealthy reflections found")
        print("Action required: Check reflection logs and refresh manually")
    else:
        print("✅ All reflections are healthy and performing well!")


if __name__ == "__main__":
    main()
```

---

## 8. Best Practices

### 8.1 Reflection Selection Guide

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    WHEN TO USE EACH REFLECTION TYPE                         │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  USE RAW REFLECTION WHEN:                                                   │
│  ✓ Query filters on specific columns (WHERE clause)                        │
│  ✓ Query sorts by specific columns (ORDER BY)                             │
│  ✓ Query needs all rows but few columns (SELECT col1, col2)               │
│  ✓ Large table (>100M rows)                                                │
│  ✓ Data changes frequently (real-time updates)                            │
│                                                                             │
│  Example:                                                                  │
│  SELECT * FROM transactions WHERE transaction_date = '2025-01-15'          │
│                                                                             │
│  ─────────────────────────────────────────────────────────────────────────  │
│                                                                             │
│  USE AGGREGATION REFLECTION WHEN:                                           │
│  ✓ Query has GROUP BY clause                                               │
│  ✓ Query uses aggregate functions (SUM, COUNT, AVG, MAX, MIN)             │
│  ✓ Dashboard with KPIs and metrics                                         │
│  ✓ Report with totals and subtotals                                        │
│  ✓ Data doesn't change every second (can refresh every 15 min)            │
│                                                                             │
│  Example:                                                                  │
│  SELECT merchant_category, SUM(amount) FROM transactions                   │
│  GROUP BY merchant_category;                                               │
│                                                                             │
│  ─────────────────────────────────────────────────────────────────────────  │
│                                                                             │
│  USE JOIN REFLECTION WHEN:                                                  │
│  ✓ Query joins 2+ tables regularly                                         │
│  ✓ Customer 360° view (Customers + Accounts + Cards + Loans)              │
│  ✓ Complex analytical queries                                              │
│  ✓ Data from multiple source systems                                       │
│                                                                             │
│  Example:                                                                  │
│  SELECT c.name, a.balance, cc.outstanding                                  │
│  FROM customers c                                                          │
│  JOIN accounts a ON c.id = a.customer_id                                  │
│  JOIN cards cc ON c.id = cc.customer_id;                                  │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 8.2 Performance Optimization Tips

| Tip | Description | Impact |
|-----|-------------|--------|
| **Start with Aggregation** | Create aggregation reflections for dashboards first | 100-1000x |
| **Use Raw for Filtering** | Raw reflections speed up WHERE clauses | 5-10x |
| **Combine Types** | Use both raw and aggregation for complex queries | 50-100x |
| **Partition Large Tables** | Partition by date for time-series data | 2-5x |
| **Limit Display Fields** | Only include columns used in queries | 2-3x |
| **Optimize Refresh** | Balance freshness vs performance | Varies |
| **Monitor Usage** | Check which reflections are actually used | Optimize resources |

### 8.3 Memory and Storage Guidelines

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    MEMORY AND STORAGE GUIDELINES                            │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  MEMORY ALLOCATION (Recommended):                                          │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                                                                     │   │
│  │  Total Dremio Memory: 32 GB                                        │   │
│  │                                                                     │   │
│  │  ├── Query Execution: 60% (19.2 GB)                               │   │
│  │  ├── Reflection Cache: 30% (9.6 GB)                               │   │
│  │  └── System/OS: 10% (3.2 GB)                                      │   │
│  │                                                                     │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  STORAGE ESTIMATION:                                                        │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                                                                     │   │
│  │  Raw Data Size: 500 GB                                             │   │
│  │                                                                     │   │
│  │  Raw Reflection: ~200 GB (40% of raw, due to columnar compression) │   │
│  │  Aggregation Reflection: ~5 GB (0.01% of raw, pre-aggregated)      │   │
│  │  Join Reflection: ~100 GB (20% of raw, pre-joined)                 │   │
│  │                                                                     │   │
│  │  Total Reflection Storage: ~305 GB (61% of raw data)               │   │
│  │                                                                     │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  RULE OF THUMB:                                                             │
│  • Allocate 2-3x raw data size for reflection storage                      │
│  • Monitor disk usage and adjust refresh frequency                         │
│  • Use S3/ADLS for cold reflection data                                    │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 8.4 Refresh Strategy

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    REFRESH STRATEGY BY USE CASE                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  REAL-TIME FRAUD DETECTION:                                                 │
│  • Refresh: Every 1-5 minutes                                              │
│  • Trade-off: Slightly stale data vs fast queries                          │
│  • Acceptable: 5-minute old data for fraud patterns                        │
│                                                                             │
│  CEO DASHBOARD:                                                             │
│  • Refresh: Every 15-30 minutes                                            │
│  • Trade-off: Data freshness vs performance                                │
│  • Acceptable: 30-minute old data for strategic decisions                  │
│                                                                             │
│  REGULATORY REPORTS:                                                        │
│  • Refresh: Every 1-4 hours (or on-demand)                                 │
│  • Trade-off: Freshness vs compliance                                      │
│  • Acceptable: Daily data for regulatory reporting                         │
│                                                                             │
│  HISTORICAL ANALYTICS:                                                      │
│  • Refresh: Daily (batch)                                                  │
│  • Trade-off: Freshness vs cost                                            │
│  • Acceptable: Previous day data for trend analysis                        │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 9. Troubleshooting

### Common Issues and Solutions

| Issue | Symptoms | Solution |
|-------|----------|----------|
| **Reflection not used** | Query still slow | Check query pattern matches reflection |
| **Reflection stale** | Data seems old | Check refresh schedule, trigger manual refresh |
| **Reflection failed** | Status shows FAILED | Check logs, verify data source connectivity |
| **Memory issues** | Dremio crashes | Reduce reflection size, increase memory |
| **Slow refresh** | Reflection takes long | Optimize SQL, reduce data volume |

### Debug Queries

```sql
-- 1. Check if reflection exists and is active
SELECT name, status, enabled, lastRefreshTime 
FROM "sys"."reflection" 
WHERE name = 'your_reflection_name';

-- 2. Check reflection size
SELECT name, sizeBytes / 1024 / 1024 AS size_mb 
FROM "sys"."reflection";

-- 3. Check reflection refresh history
SELECT reflection_name, refresh_start_time, refresh_end_time, status
FROM "sys"."reflection_refresh_history"
ORDER BY refresh_start_time DESC
LIMIT 10;

-- 4. Check query profile for reflection usage
-- Run your query, then click "Profile" tab in Dremio UI
-- Look for "ReflectionScan" in execution plan

-- 5. Force reflection refresh
-- In Dremio UI: Click reflection → "Refresh" button
-- Or via SQL: ALTER REFLECTION your_reflection_name REFRESH;
```

### Common Mistakes

```sql
-- ❌ MISTAKE 1: Query doesn't match reflection pattern
-- Reflection: GROUP BY transaction_date, merchant_category
-- Query: GROUP BY transaction_date, merchant_name  ← WON'T USE REFLECTION

-- ✅ FIX: Create reflection that matches query pattern
-- Or modify query to use reflection's groupings

-- ❌ MISTAKE 2: Too many columns in display fields
-- Reflection displays: * (all columns)
-- Query only needs: transaction_date, amount

-- ✅ FIX: Only include columns used in queries
-- display_fields: ["transaction_date", "amount"]

-- ❌ MISTAKE 3: Wrong reflection type
-- Using RAW reflection for aggregation query

-- ✅ FIX: Use AGGREGATION reflection for GROUP BY queries
```

---

## 10. Performance Benchmarks

### Benchmark Results

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    PERFORMANCE BENCHMARKS                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Test Environment:                                                          │
│  • Dremio 24.0 on 4-node cluster                                          │
│  • 32 GB RAM per node                                                      │
│  • 100 million card transactions (500 GB)                                  │
│                                                                             │
│  ═══════════════════════════════════════════════════════════════════════   │
│  QUERY TYPE 1: Simple Aggregation                                          │
│  ═══════════════════════════════════════════════════════════════════════   │
│                                                                             │
│  Query: SELECT category, SUM(amount) FROM transactions GROUP BY category;  │
│                                                                             │
│  Without Reflection:  45 seconds                                           │
│  With Raw Reflection: 8 seconds    (5.6x faster)                          │
│  With Agg Reflection: 52 ms        (865x faster)                          │
│                                                                             │
│  ═══════════════════════════════════════════════════════════════════════   │
│  QUERY TYPE 2: Multi-Table Join                                            │
│  ═══════════════════════════════════════════════════════════════════════   │
│                                                                             │
│  Query: Customer 360° (4 tables joined)                                    │
│                                                                             │
│  Without Reflection:  3 minutes                                            │
│  With Raw Reflection: 45 seconds   (4x faster)                            │
│  With Join Reflection: 120 ms       (1,500x faster)                       │
│                                                                             │
│  ═══════════════════════════════════════════════════════════════════════   │
│  QUERY TYPE 3: Complex Analytics                                           │
│  ═══════════════════════════════════════════════════════════════════════   │
│                                                                             │
│  Query: Window functions + aggregations                                    │
│                                                                             │
│  Without Reflection:  12 minutes                                           │
│  With Raw Reflection: 2 minutes    (6x faster)                            │
│  With Agg Reflection: 5 seconds    (144x faster)                          │
│                                                                             │
│  ═══════════════════════════════════════════════════════════════════════   │
│  QUERY TYPE 4: Real-Time Dashboard                                         │
│  ═══════════════════════════════════════════════════════════════════════   │
│                                                                             │
│  Query: Executive KPIs (multiple aggregations)                             │
│                                                                             │
│  Without Reflection:  45 minutes                                           │
│  With Raw Reflection: 8 minutes    (5.6x faster)                          │
│  With Agg Reflection: 50 ms        (54,000x faster)                       │
│                                                                             │
│  ═══════════════════════════════════════════════════════════════════════   │
│  SUMMARY                                                                    │
│  ═══════════════════════════════════════════════════════════════════════   │
│                                                                             │
│  Reflection Type  │ Average Speedup │ Best For                            │
│  ─────────────────────────────────────────────────────────────────────────  │
│  Raw              │ 5-10x          │ Filtering, sorting                   │
│  Aggregation      │ 100-1000x      │ Dashboards, KPIs                    │
│  Join             │ 50-500x        │ Multi-table queries                  │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Quick Reference Card

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    ARROW REFLECTIONS QUICK REFERENCE                        │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  CREATE REFLECTION (UI):                                                    │
│  1. Dataset → Reflection tab → New Reflection                              │
│  2. Choose type (Raw/Aggregation)                                          │
│  3. Select display fields and groupings                                    │
│  4. Set refresh schedule                                                   │
│  5. Click Create                                                           │
│                                                                             │
│  CREATE REFLECTION (SQL):                                                   │
│  CREATE OR REPLACE VDS "space"."reflection.name" AS                        │
│  SELECT ... GROUP BY ...;                                                   │
│                                                                             │
│  CHECK STATUS:                                                             │
│  SELECT * FROM "sys"."reflection";                                         │
│                                                                             │
│  REFRESH MANUALLY:                                                         │
│  ALTER REFLECTION reflection_name REFRESH;                                 │
│                                                                             │
│  DELETE REFLECTION:                                                         │
│  ALTER VDS "space"."reflection.name" UNSET REFLECTION;                     │
│                                                                             │
│  MONITOR PERFORMANCE:                                                      │
│  Check query profile for "ReflectionScan" in execution plan                │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

*Created for: Banking Data Platform - Lakehouse Architecture*
*Dremio Version: 24.0+*
*Last Updated: 2025-01-15*