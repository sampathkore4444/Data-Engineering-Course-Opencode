# Data Governance Framework - Banking Data Platform

## Overview
This document covers data governance practices including classification, quality rules, and compliance requirements.

---

## 1. Data Classification

| Classification | Description | Examples | Access Level |
|---------------|-------------|----------|--------------|
| **PUBLIC** | Non-sensitive data | Product catalog, branch locations | All users |
| **INTERNAL** | Internal business data | Sales reports, employee data | Employees only |
| **CONFIDENTIAL** | Sensitive business data | Financial reports, customer analytics | Managers+ |
| **RESTRICTED** | Highly sensitive PII | PAN, account numbers | Authorized only |
| **CRITICAL** | Regulatory/Compliance | NPA data, fraud alerts | Compliance team |

---

## 2. Data Quality Rules

### Customer Data Quality

```sql
-- Check for duplicate customers
SELECT customer_id, COUNT(*) as duplicate_count
FROM banking_cleansed.core_banking_customers
GROUP BY customer_id
HAVING COUNT(*) > 1;

-- Check for invalid PAN format
SELECT customer_id, pan_number
FROM banking_cleansed.core_banking_customers
WHERE pan_number !~ '^[A-Z]{5}[0-9]{4}[A-Z]$';

-- Check for missing KYC
SELECT customer_id, customer_name
FROM banking_cleansed.core_banking_customers
WHERE kyc_status != 'VERIFIED';
```

### Transaction Data Quality

```sql
-- Check for negative amounts
SELECT txn_id, amount
FROM banking_cleansed.core_baking_transactions
WHERE amount < 0 AND txn_type != 'REFUND';

-- Check for future dates
SELECT txn_id, txn_date
FROM banking_cleansed.core_baking_transactions
WHERE txn_date > CURRENT_DATE;

-- Check for orphan transactions
SELECT t.txn_id
FROM banking_cleansed.core_baking_transactions t
LEFT JOIN banking_cleansed.core_banking_accounts a ON t.account_id = a.account_id
WHERE a.account_id IS NULL;
```

### Loan Data Quality

```sql
-- Check for invalid NPA classification
SELECT loan_id, loan_status, risk_classification
FROM banking_cleansed.loan_accounts
WHERE loan_status = 'CLOSED' AND risk_classification != 'STANDARD';

-- Check for EMI calculation errors
SELECT loan_id, principal_amount, emi_amount, interest_rate, tenure_months
FROM banking_cleansed.loan_accounts
WHERE ABS(emi_amount - (principal_amount * (interest_rate/100/12) * 
      POWER(1 + interest_rate/100/12, tenure_months)) / 
      (POWER(1 + interest_rate/100/12, tenure_months) - 1)) > 100;
```

---

## 3. Data Lineage Tracking

```sql
-- View data lineage for Customer 360
SELECT 
    source_table,
    target_table,
    transformation_type,
    transformation_logic,
    last_refreshed,
    refresh_frequency
FROM banking_audit.metadata_lineage
WHERE target_table = 'customer_360';

-- Check data flow
SELECT 
    level,
    source,
    target,
    process,
    timestamp
FROM banking_audit.data_flow
ORDER BY timestamp DESC
LIMIT 100;
```

---

## 4. Access Control Matrix

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

## 5. Compliance & Regulatory Requirements

### SBV Compliance (Vietnam)

| Requirement | Description | Implementation |
|-------------|-------------|----------------|
| **Circular 39/2014** | Data security | Encryption, access control |
| **Circular 23/2014** | Reporting requirements | Daily/monthly reports |
| **Decision 1168/QD-NHNN** | AML regulations | Suspicious transaction reporting |
| **Basel III** | Capital adequacy | Risk-weighted assets calculation |
| **NPA Reporting** | Asset classification | Automated NPA detection |

### Data Retention Policy

| Data Type | Retention Period | Storage Tier | Archive Strategy |
|-----------|-----------------|--------------|------------------|
| **Customer Master** | Active + 10 years | Hot → Warm → Cold | Object Storage |
| **Transactions** | 7 years | Hot → Warm → Cold | Object Storage |
| **Loan Data** | Loan tenure + 7 years | Hot → Warm → Cold | Object Storage |
| **Card Transactions** | 7 years | Hot → Warm → Cold | Object Storage |
| **NPA Data** | 10 years | Hot → Warm → Cold | Object Storage |
| **Audit Logs** | 10 years | Cold | Deep Archive |

---

## 6. Audit Trail Requirements

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
FROM banking_audit.access_log
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
FROM banking_audit.data_change_log
WHERE changed_at >= CURRENT_DATE - INTERVAL '7' DAY
ORDER BY changed_at DESC;
```

---

## 7. Data Quality Metrics

| Metric | Formula | Target | Alert Threshold |
|--------|---------|--------|-----------------|
| **Completeness** | Non-null values / Total values | > 99% | < 95% |
| **Uniqueness** | Distinct values / Total values | > 99.9% | < 99% |
| **Validity** | Valid values / Total values | > 99% | < 95% |
| **Consistency** | Consistent values across systems | > 99% | < 98% |
| **Timeliness** | Data age < threshold | < 1 hour | > 4 hours |

---

## 8. Best Practices

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

*Last Updated: 2024-01-15*
*Review Schedule: Monthly*
