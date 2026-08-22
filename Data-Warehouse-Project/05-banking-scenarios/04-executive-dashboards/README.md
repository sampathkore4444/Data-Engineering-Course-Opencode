# 04 - Executive Dashboards

## Overview

This folder contains executive-level dashboards for banking leadership.

---

## Dashboard 1: CEO Dashboard

**Purpose:** High-level KPIs for Chief Executive Officer.

### Key Metrics

| Metric | Target | Current | Status |
|--------|--------|---------|--------|
| **Total Deposits** | VND 50T | VND 52.3T | ✅ |
| **Total Loans** | VND 35T | VND 33.8T | ⚠️ |
| **NPL Ratio** | < 3% | 2.8% | ✅ |
| **Net Interest Margin** | > 3.5% | 3.7% | ✅ |
| **Customer Growth** | +10% | +12.5% | ✅ |

### SQL Query

```sql
-- CEO Dashboard Summary
SELECT 
    'Total Deposits' as metric,
    SUM(balance) as value,
    'VND' as unit
FROM gold.fact_account_balance
WHERE snapshot_date = CURRENT_DATE

UNION ALL

SELECT 
    'Total Loans' as metric,
    SUM(loan_amount) as value,
    'VND' as unit
FROM gold.dim_loan
WHERE status = 'ACTIVE'

UNION ALL

SELECT 
    'NPL Ratio' as metric,
    ROUND(SUM(CASE WHEN status = 'OVERDUE' THEN loan_amount ELSE 0 END) * 100.0 / 
          SUM(loan_amount), 2) as value,
    '%' as unit
FROM gold.dim_loan;
```

---

## Dashboard 2: Regional Performance

**Purpose:** Performance comparison by region.

```sql
-- Regional Performance
SELECT 
    b.region,
    COUNT(DISTINCT c.customer_id) as customers,
    SUM(f.balance) as deposits,
    SUM(f.loan_amount) as loans,
    ROUND(SUM(f.interest_income) / SUM(f.balance) * 100, 2) as yield
FROM gold.fact_account_balance f
JOIN gold.dim_branch b ON f.branch_sk = b.branch_sk
JOIN gold.dim_customer c ON f.customer_sk = c.customer_sk
GROUP BY b.region
ORDER BY deposits DESC;
```

---

## Dashboard 3: Product Performance

**Purpose:** Product-wise revenue analysis.

```sql
-- Product Performance
SELECT 
    p.product_name,
    COUNT(DISTINCT a.account_id) as accounts,
    SUM(f.balance) as total_balance,
    SUM(f.interest_income) as interest_income,
    ROUND(SUM(f.interest_income) / SUM(f.balance) * 100, 2) as margin
FROM gold.fact_account_balance f
JOIN gold.dim_account a ON f.account_sk = a.account_sk
JOIN gold.dim_product p ON a.product_sk = p.product_sk
GROUP BY p.product_name
ORDER BY interest_income DESC;
```

---

## Dashboard Schedule

| Dashboard | Refresh | Audience |
|-----------|---------|----------|
| **CEO Dashboard** | Daily | CEO, Board |
| **Regional Performance** | Weekly | Regional Managers |
| **Product Performance** | Monthly | Product Heads |
| **Risk Dashboard** | Daily | Risk Officers |

---

*Back to: [Main README](../../README.md)*
