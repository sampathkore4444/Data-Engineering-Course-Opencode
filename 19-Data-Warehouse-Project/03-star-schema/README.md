# Star Schema - Dimensional Modeling Guide

## Overview

This folder contains the **dimensional model** for the banking data warehouse. It follows the **Star Schema** pattern with **dimension tables** and **fact tables**.

---

## What is Dimensional Modeling?

Dimensional modeling is a technique for designing data warehouses that:
- Optimizes for **query performance**
- Makes data **easy to understand** for business users
- Supports **aggregation** and **analysis**

---

## Star Schema Design

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    STAR SCHEMA - BANKING DATA WAREHOUSE                      │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│                        dim_date                                              │
│                       ┌──────────┐                                          │
│                       │ date_key │                                          │
│                       │ date     │                                          │
│                       │ day_name │                                          │
│                       │ month    │                                          │
│                       │ quarter  │                                          │
│                       │ year     │                                          │
│                       └────┬─────┘                                          │
│                            │                                                │
│  dim_customer ─────┐       │       ┌───── dim_product                      │
│  ┌──────────────┐  │       │       │  ┌──────────────┐                     │
│  │ customer_key │  │       │       │  │ product_key  │                     │
│  │ customer_id  │  │       │       │  │ product_code │                     │
│  │ name         │  │       │       │  │ product_name │                     │
│  │ city         │  │       │       │  │ category     │                     │
│  │ segment      │  │       │       │  │ interest_rate│                     │
│  └──────┬───────┘  │       │       │  └──────┬───────┘                     │
│         │          │       │       │         │                              │
│         ▼          ▼       ▼       ▼         ▼                              │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                      fact_transactions                               │   │
│  │                                                                     │   │
│  │  transaction_key (PK)                                               │   │
│  │  date_key (FK)          ──► dim_date                                │   │
│  │  customer_key (FK)      ──► dim_customer                            │   │
│  │  account_key (FK)       ──► dim_account                             │   │
│  │  product_key (FK)       ──► dim_product                             │   │
│  │  branch_key (FK)        ──► dim_branch                              │   │
│  │  transaction_amount     (measure)                                   │   │
│  │  transaction_count      (measure)                                   │   │
│  │  fee_amount             (measure)                                   │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│         ▲          ▲       ▲       ▲         ▲                              │
│         │          │       │       │         │                              │
│  ┌──────┴───────┐  │       │       │  ┌──────┴───────┐                     │
│  │ account_key  │  │       │       │  │ branch_key   │                     │
│  │ account_id   │  │       │       │  │ branch_code  │                     │
│  │ account_type │  │       │       │  │ branch_name  │                     │
│  │ status       │  │       │       │  │ region       │                     │
│  └──────────────┘  │       │       │  └──────────────┘                     │
│                    │       │       │                                        │
│  dim_account ──────┘       │       └───── dim_branch                       │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Dimension Tables

### 1. dim_customer (SCD Type 2)

| Column | Type | Description |
|--------|------|-------------|
| `customer_sk` | INT | Surrogate key (auto-increment) |
| `customer_id` | VARCHAR | Natural key from source |
| `customer_name` | VARCHAR | Customer full name |
| `city` | VARCHAR | Customer city |
| `customer_segment` | VARCHAR | RETAIL, CORPORATE, HNWI |
| `effective_date` | DATE | Start date of this version |
| `expiry_date` | DATE | End date (9999-12-31 = current) |
| `is_current` | BOOLEAN | TRUE if current version |

### 2. dim_account (SCD Type 1)

| Column | Type | Description |
|--------|------|-------------|
| `account_sk` | INT | Surrogate key |
| `account_id` | VARCHAR | Natural key from source |
| `account_type` | VARCHAR | SAVINGS, CURRENT, FIXED_DEPOSIT |
| `status` | VARCHAR | ACTIVE, CLOSED, DORMANT |
| `branch_code` | VARCHAR | Branch identifier |

### 3. dim_date (Static)

| Column | Type | Description |
|--------|------|-------------|
| `date_key` | INT | YYYYMMDD format |
| `full_date` | DATE | Actual date |
| `day_name` | VARCHAR | Monday, Tuesday, etc. |
| `month_name` | VARCHAR | January, February, etc. |
| `quarter` | INT | 1, 2, 3, 4 |
| `year` | INT | 2024 |
| `is_weekend` | BOOLEAN | TRUE if Saturday/Sunday |

### 4. dim_branch (Static)

| Column | Type | Description |
|--------|------|-------------|
| `branch_sk` | INT | Surrogate key |
| `branch_code` | VARCHAR | Branch identifier |
| `branch_name` | VARCHAR | Branch name |
| `region` | VARCHAR | NORTH, CENTRAL, SOUTH |
| `city` | VARCHAR | Branch city |

### 5. dim_product (Static)

| Column | Type | Description |
|--------|------|-------------|
| `product_sk` | INT | Surrogate key |
| `product_code` | VARCHAR | Product identifier |
| `product_name` | VARCHAR | Product name |
| `product_category` | VARCHAR | DEPOSIT, LOAN, CARD |

---

## Fact Tables

### 1. fact_transactions (Transaction Fact)

| Column | Type | Description |
|--------|------|-------------|
| `transaction_sk` | INT | Surrogate key |
| `date_key` | INT | FK to dim_date |
| `customer_sk` | INT | FK to dim_customer |
| `account_sk` | INT | FK to dim_account |
| `branch_sk` | INT | FK to dim_branch |
| `transaction_amount` | DECIMAL | Amount (additive) |
| `is_high_value` | BOOLEAN | Amount > 100M VND |

### 2. fact_account_balance (Snapshot Fact)

| Column | Type | Description |
|--------|------|-------------|
| `balance_sk` | INT | Surrogate key |
| `date_key` | INT | FK to dim_date |
| `account_sk` | INT | FK to dim_account |
| `opening_balance` | DECIMAL | Balance at start of day |
| `closing_balance` | DECIMAL | Balance at end of day |
| `net_flow` | DECIMAL | Credits - Debits |

### 3. fact_loan_payment (Transaction Fact)

| Column | Type | Description |
|--------|------|-------------|
| `payment_sk` | INT | Surrogate key |
| `date_key` | INT | FK to dim_date |
| `customer_sk` | INT | FK to dim_customer |
| `loan_id` | VARCHAR | Loan identifier |
| `payment_amount` | DECIMAL | Payment amount |
| `days_past_due` | INT | Days overdue |
| `is_npa` | BOOLEAN | Non-Performing Asset |

---

## Key Concepts

### Surrogate Key vs Natural Key

| Key Type | Example | Purpose |
|----------|---------|---------|
| **Surrogate Key** | `customer_sk = 123` | Unique identifier in DW |
| **Natural Key** | `customer_id = "CUST-001"` | Identifier from source system |

### SCD Type 1 vs Type 2

| Type | Behavior | Use Case |
|------|----------|----------|
| **SCD Type 1** | Overwrite old data | Account status, balances |
| **SCD Type 2** | Keep history with effective dates | Customer name, address |

### Additive vs Semi-Additive vs Non-Additive

| Type | Example | Can Sum? |
|------|---------|----------|
| **Additive** | `transaction_amount` | ✅ Yes, across all dimensions |
| **Semi-Additive** | `account_balance` | ⚠️ Across accounts, NOT across dates |
| **Non-Additive** | `interest_rate` | ❌ No, must use averages |

---

## Sample Queries

```sql
-- Total transactions by region and month
SELECT
    b.region,
    d.month_name,
    COUNT(*) AS transaction_count,
    SUM(t.transaction_amount) AS total_amount
FROM dw.fact_transactions t
JOIN dw.dim_date d ON t.date_key = d.date_key
JOIN dw.dim_branch b ON t.branch_sk = b.branch_sk
GROUP BY b.region, d.month_name, d.month_number
ORDER BY d.month_number, b.region;

-- Customer 360 view
SELECT * FROM vw_customer_360
WHERE customer_segment = 'CORPORATE'
ORDER BY total_deposit_balance DESC;

-- Monthly P&L
SELECT * FROM vw_monthly_pnl
WHERE year = 2024;
```

---

## Files

| File | Purpose |
|------|---------|
| `dimensions/dim_customer.sql` | Customer dimension (SCD Type 2) |
| `dimensions/dim_account.sql` | Account dimension (SCD Type 1) |
| `dimensions/dim_date.sql` | Calendar dimension (2020-2025) |
| `dimensions/dim_branch.sql` | Branch dimension |
| `dimensions/dim_product.sql` | Product dimension |
| `facts/fact_transactions.sql` | Transaction fact table |
| `facts/fact_account_balance.sql` | Daily balance snapshot |
| `facts/fact_loan_payment.sql` | Loan payment fact |

---

*Part of: [Data Warehouse Project](../README.md)*
