# 03 - Regulatory Reports (SBV Compliance)

## Overview

This folder contains regulatory reports required by the State Bank of Vietnam (SBV).

---

## Reports

### 1. Call Report (C before 03)

**Purpose:** Monthly financial position report to SBV.

```sql
-- Total deposits by branch
SELECT 
    b.branch_name,
    SUM(f.balance) as total_deposits,
    COUNT(DISTINCT f.account_id) as account_count
FROM gold.fact_account_balance f
JOIN gold.dim_branch b ON f.branch_sk = b.branch_sk
JOIN gold.dim_date d ON f.snapshot_date_sk = d.date_key
WHERE d.full_date = CURRENT_DATE - INTERVAL '1 day'
GROUP BY b.branch_name
ORDER BY total_deposits DESC;
```

### 2. AML Report

**Purpose:** Anti-Money Laundering monitoring.

```sql
-- Suspicious transactions (> VND 500M)
SELECT 
    c.customer_name,
    t.amount,
    t.transaction_type,
    t.transaction_date,
    a.account_number
FROM gold.fact_transactions t
JOIN gold.dim_customer c ON t.customer_sk = c.customer_sk
JOIN gold.dim_account a ON t.account_sk = a.account_sk
WHERE t.amount > 500000000
  AND t.transaction_date >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY t.amount DESC;
```

### 3. Credit Risk Report

**Purpose:** Non-performing asset (NPA) monitoring.

```sql
-- NPA by loan type
SELECT 
    l.loan_type,
    COUNT(*) as total_loans,
    SUM(l.loan_amount) as total_loan_amount,
    SUM(CASE WHEN l.status = 'OVERDUE' THEN l.loan_amount ELSE 0 END) as npa_amount,
    ROUND(SUM(CASE WHEN l.status = 'OVERDUE' THEN l.loan_amount ELSE 0 END) * 100.0 / 
          SUM(l.loan_amount), 2) as npa_ratio
FROM gold.dim_loan l
GROUP BY l.loan_type
ORDER BY npa_ratio DESC;
```

---

## Submission Schedule

| Report | Frequency | Deadline | Submit To |
|--------|-----------|----------|-----------|
| **Call Report** | Monthly | 15th of next month | SBV |
| **AML Report** | Monthly | 10th of next month | SBV |
| **Credit Risk** | Quarterly | 30th of next quarter | SBV |
| **Basel III** | Quarterly | 30th of next quarter | SBV |

---

## Compliance Checklist

- [ ] Data accuracy verified
- [ ] All branches included
- [ ] No missing transactions
- [ ] Balances reconciled
- [ ] Report submitted on time
- [ ] Audit trail maintained

---

*Back to: [Main README](../../README.md)*
