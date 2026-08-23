# Security & Governance - Banking Data Platform

## Overview

This folder contains **5 files** covering the complete security and governance framework for the banking data platform. It includes **access control**, **row-level security**, **audit logging**, **encryption**, and **data governance** — all essential for banking compliance (SBV regulations).

---

## Security Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    BANKING SECURITY ARCHITECTURE                            │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌───────────────────────────────────────────────────────────────────────┐ │
│  │                         GOVERNANCE LAYER                              │ │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐ │ │
│  │  │ Data        │  │ Data Quality│  │ Compliance  │  │ Data        │ │ │
│  │  │ Classificat.│  │ Rules       │  │ (SBV)       │  │ Retention   │ │ │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘ │ │
│  └───────────────────────────────────────────────────────────────────────┘ │
│                                    │                                        │
│                                    ▼                                        │
│  ┌───────────────────────────────────────────────────────────────────────┐ │
│  │                         ACCESS CONTROL LAYER                          │ │
│  │  ┌─────────────────────────┐    ┌─────────────────────────────────┐  │ │
│  │  │  Role-Based Access      │    │  Row-Level Security             │  │ │
│  │  │  Control (RBAC)         │    │  (RLS)                          │  │ │
│  │  │                         │    │                                 │  │ │
│  │  │  • 13 roles defined     │    │  • Branch-based access          │  │ │
│  │  │  • Role hierarchy       │    │  • Customer-based access        │  │ │
│  │  │  • Permission grants    │    │  • Transaction-based access     │  │ │
│  │  └─────────────────────────┘    └─────────────────────────────────┘  │ │
│  └───────────────────────────────────────────────────────────────────────┘ │
│                                    │                                        │
│                                    ▼                                        │
│  ┌───────────────────────────────────────────────────────────────────────┐ │
│  │                         ENCRYPTION LAYER                              │ │
│  │  ┌─────────────────────────────────────────────────────────────────┐ │ │
│  │  │  TLS/SSL for ALL components                                     │ │ │
│  │  │  • Dremio (9047)  • Kafka (9093)  • PostgreSQL (5432)          │ │ │
│  │  │  • MinIO (9000)   • Airflow (8080) • Grafana (3000)            │ │ │
│  │  └─────────────────────────────────────────────────────────────────┘ │ │
│  └───────────────────────────────────────────────────────────────────────┘ │
│                                    │                                        │
│                                    ▼                                        │
│  ┌───────────────────────────────────────────────────────────────────────┐ │
│  │                         AUDIT LAYER                                   │ │
│  │  ┌─────────────────────────────────────────────────────────────────┐ │ │
│  │  │  Complete audit trail for ALL data access                       │ │ │
│  │  │  • Access logs    • PII tracking    • Failed attempts           │ │ │
│  │  │  • Compliance reports  • High-volume detection                  │ │ │
│  │  │  • 7-year retention (SBV requirement)                           │ │ │
│  │  └─────────────────────────────────────────────────────────────────┘ │ │
│  └───────────────────────────────────────────────────────────────────────┘ │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## File Summary

| # | File | Purpose | Type | Lines |
|---|------|---------|------|-------|
| 1 | `access-control/role-hierarchy.sql` | Define 13 roles and RBAC permissions | SQL | 200+ |
| 2 | `access-control/row-level-security.sql` | Row-level access based on branch/customer | SQL | 180+ |
| 3 | `audit/audit-config.sql` | Audit logging, triggers, compliance reports | SQL | 250+ |
| 4 | `encryption/tls-config.md` | TLS/SSL setup for all 6 components | Markdown | 200+ |
| 5 | `governance/data-governance-framework.md` | Data governance policies and quality rules | Markdown | 250+ |

---

## Folder Structure

```
09-security/
│
├── README.md                              # This file
│
├── access-control/                        # Access control configuration
│   ├── role-hierarchy.sql                 # RBAC roles and permissions
│   └── row-level-security.sql             # Row-level security policies
│
├── audit/                                 # Audit logging
│   └── audit-config.sql                   # Audit tables, triggers, reports
│
├── encryption/                            # Encryption configuration
│   └── tls-config.md                      # TLS/SSL setup guide
│
└── governance/                            # Data governance
    └── data-governance-framework.md       # Governance policies and quality
```

---

## 1. Role-Based Access Control (RBAC)

### Purpose
Define **who can access what** in the banking data platform. Uses a hierarchical role system where senior roles inherit permissions from junior roles.

### Role Hierarchy

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         ROLE HIERARCHY                                      │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  DATA ENGINEERING                                                           │
│  ─────────────────                                                          │
│  platform_admins                                                            │
│       │                                                                     │
│       ├── data_architects                                                   │
│       │       │                                                             │
│       │       └── data_engineers                                            │
│                                                                             │
│  DATA ANALYSIS                                                              │
│  ─────────────                                                              │
│  business_analysts                                                          │
│       │                                                                     │
│       ├── senior_analysts                                                   │
│       │       │                                                             │
│       │       └── data_analysts                                             │
│       │           │                                                         │
│       │           ├── risk_team                                             │
│       │           ├── fraud_team                                            │
│       │           ├── compliance_team                                       │
│       │           ├── finance_team                                          │
│       │           └── executive_team                                        │
│                                                                             │
│  CUSTOMER SERVICE                                                           │
│  ────────────────                                                           │
│  branch_managers                                                            │
│       │                                                                     │
│       └── relationship_managers                                             │
│               │                                                             │
│               └── data_analysts                                             │
│                                                                             │
│  call_center (standalone, read-only)                                        │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 13 Roles Defined

| Role | Inherits From | Purpose | Access Level |
|------|---------------|---------|--------------|
| **platform_admins** | data_architects | Full system access | ALL |
| **data_architects** | data_engineers | Design data pipelines | ALL |
| **data_engineers** | — | Build and maintain pipelines | ALL (Bronze/Silver/Gold) |
| **business_analysts** | senior_analysts | Business reporting | Read (Silver/Gold) |
| **senior_analysts** | data_analysts | Advanced analytics | Read (All layers) |
| **data_analysts** | — | Basic analytics | Read (All layers) |
| **risk_team** | data_analysts | Risk management | Read (Risk views) |
| **fraud_team** | data_analysts | Fraud detection | Read/Write (Fraud views) |
| **compliance_team** | data_analysts | Regulatory compliance | Read (Compliance views) |
| **finance_team** | data_analysts | Financial reporting | Read (Finance views) |
| **executive_team** | data_analysts | Executive dashboards | Read (Executive views) |
| **relationship_managers** | data_analysts | Customer management | Read (Customer views) |
| **branch_managers** | relationship_managers | Branch operations | Read (Branch views) |
| **call_center** | — | Customer support | Read (Limited Customer) |

### Permission Matrix

| Role | Bronze | Silver | Gold | Sources | Functions |
|------|--------|--------|------|---------|-----------|
| data_engineers | ALL | ALL | ALL | ALL | ALL |
| data_architects | ALL | ALL | ALL | ALL | ALL |
| platform_admins | ALL | ALL | ALL | ALL | ALL |
| data_analysts | READ | READ | READ | READ | — |
| risk_team | — | READ | READ | — | — |
| fraud_team | — | READ | READ/WRITE | — | — |
| compliance_team | — | — | READ | — | — |
| finance_team | — | — | READ | — | — |
| executive_team | — | — | READ | — | — |
| relationship_managers | — | READ | READ | — | — |
| branch_managers | — | READ | READ | — | — |
| call_center | — | — | READ | — | — |

---

## 2. Row-Level Security (RLS)

### Purpose
Restrict **which rows** a user can see based on their role, branch assignment, or customer assignment. Even if a user has table access, they only see rows they're authorized for.

### How RLS Works

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    ROW-LEVEL SECURITY FLOW                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  User: rm_1 (Relationship Manager at Ho Chi Minh Main Branch)             │
│                                                                             │
│  Query: SELECT * FROM customer_360                                          │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  WITHOUT RLS:                                                       │   │
│  │  Returns ALL 50,000 customers across ALL branches                  │   │
│  │  ❌ Security violation!                                             │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  WITH RLS:                                                          │   │
│  │  1. Check user's role (relationship_managers)                      │   │
│  │  2. Check user's branch (BR001 - Ho Chi Minh Main)                 │   │
│  │  3. Filter: branch_code = 'BR001'                                  │   │
│  │  Returns ONLY 5,000 customers from Ho Chi Minh Main Branch         │   │
│  │  ✅ Security enforced!                                              │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Three Types of RLS

#### 1. Branch-Based Row Access

```
┌─────────────────────────────────────────────────────────────────┐
│                 BRANCH-BASED RLS                                │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  User Branch Mapping Table:                                    │
│  ┌──────────┬────────────┬──────────────┬─────────────┐       │
│  │ User ID  │ Branch     │ Region       │ Access Level│       │
│  ├──────────┼────────────┼──────────────┼─────────────┤       │
│  │ rm_1     │ BR001      │ SOUTH        │ BRANCH      │       │
│  │ rm_2     │ BR002      │ NORTH        │ BRANCH      │       │
│  │ bm_1     │ BR001      │ SOUTH        │ BRANCH      │       │
│  │ risk_mgr │ ALL        │ ALL          │ ENTERPRISE  │       │
│  └──────────┴────────────┴──────────────┴─────────────┘       │
│                                                                 │
│  Access Rules:                                                 │
│  • ENTERPRISE → See ALL branches                               │
│  • REGION     → See branches in their region                   │
│  • BRANCH     → See only their branch                          │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

#### 2. Customer-Based Row Access

```
┌─────────────────────────────────────────────────────────────────┐
│                 CUSTOMER-BASED RLS                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Customer Assignment Table:                                    │
│  ┌──────────────┬────────────┬─────────────────────┐          │
│  │ Customer ID  │ Assigned To│ Type                │          │
│  ├──────────────┼────────────┼─────────────────────┤          │
│  │ CUST-12345   │ rm_1       │ RELATIONSHIP_MGR    │          │
│  │ CUST-12346   │ rm_1       │ RELATIONSHIP_MGR    │          │
│  │ CUST-12347   │ rm_2       │ RELATIONSHIP_MGR    │          │
│  └──────────────┴────────────┴─────────────────────┘          │
│                                                                 │
│  Access Rules:                                                 │
│  • Risk/Fraud/Compliance/Executive → See ALL customers         │
│  • Relationship Manager → See ONLY assigned customers          │
│  • Others → No access                                          │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

#### 3. Transaction-Based Row Access

```
┌─────────────────────────────────────────────────────────────────┐
│                 TRANSACTION-BASED RLS                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Combines branch + customer access:                            │
│                                                                 │
│  User sees transaction IF:                                     │
│  1. User is data_engineer/risk/fraud/compliance → ALL          │
│  2. Transaction's account is in user's branch → YES            │
│  3. Transaction's customer is assigned to user → YES           │
│  4. Otherwise → NO                                             │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### RLS Verification Queries

```sql
-- Test as Relationship Manager (should see only assigned customers)
SET ROLE rm_1;
SELECT customer_id, customer_name, total_balance 
FROM banking_gold.customer_360
WHERE has_access = TRUE;
-- Expected: Only customers assigned to rm_1

-- Test as Risk Manager (should see ALL customers)
SET ROLE risk_manager_1;
SELECT customer_id, customer_name, total_balance 
FROM banking_gold.customer_360
WHERE has_access = TRUE;
-- Expected: All customers (enterprise access)

-- Reset role
RESET ROLE;
```

---

## 3. Audit Logging

### Purpose
Track **every data access** for compliance (SBV Circular 39/2014) and security monitoring. Logs who accessed what, when, from where, and why.

### Audit Tables

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         AUDIT TABLE STRUCTURE                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  TABLE 1: access_log (Main audit table)                             │   │
│  │  ───────────────────────────────────────                            │   │
│  │  • log_id, event_timestamp, user_id, user_role                     │   │
│  │  • action (SELECT/INSERT/UPDATE/DELETE)                             │   │
│  │  • object_type, object_schema, object_name                         │   │
│  │  • query_text, rows_affected, execution_time_ms                    │   │
│  │  • ip_address, user_agent, session_id                              │   │
│  │  • status (SUCCESS/FAILURE/DENIED)                                 │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  TABLE 2: access_summary_hourly (Aggregated)                        │   │
│  │  ───────────────────────────────────────────                        │   │
│  │  • Hourly summary per user/action                                   │   │
│  │  • access_count, total_rows_accessed                                │   │
│  │  • unique_tables_accessed                                           │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  TABLE 3: pii_access_log (PII tracking for compliance)             │   │
│  │  ─────────────────────────────────────────────────────              │   │
│  │  • Tracks access to PII/PHI/Financial data                         │   │
│  │  • customer_id, data_classification                                │   │
│  │  • access_purpose (business reason)                                │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  TABLE 4: failed_access_log (Security monitoring)                   │   │
│  │  ────────────────────────────────────────────────                   │   │
│  │  • Failed login attempts                                           │   │
│  │  • Access denied events                                            │   │
│  │  • retry_count for brute-force detection                           │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Audit Triggers

```
┌─────────────────────────────────────────────────────────────────┐
│                 AUDIT TRIGGERS                                  │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  TRIGGER 1: trg_audit_customer_access                          │
│  • Fires: AFTER SELECT on core_banking_customers               │
│  • Logs: Every access to customer PII data                     │
│  • Classification: PII                                         │
│                                                                 │
│  TRIGGER 2: trg_audit_card_access                              │
│  • Fires: AFTER SELECT on credit_cards                         │
│  • Logs: Every access to card numbers                          │
│  • Classification: PII                                         │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Audit Reporting Views

| View | Purpose | Time Range |
|------|---------|------------|
| `daily_access_summary` | Who accessed what, daily | Last 7 days |
| `pii_access_report` | PII access for compliance | Last 30 days |
| `failed_access_report` | Security incidents | Last 7 days |
| `high_volume_access` | Potential abuse detection | Last 1 day |
| `sbv_audit_trail` | SBV compliance report | Last 90 days |
| `compliance_report` | Full user activity summary | Last 30 days |

### High-Volume Access Detection

```
┌─────────────────────────────────────────────────────────────────┐
│                 ABUSE DETECTION RULES                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Daily Access Count  →  Volume Level  →  Action                │
│  ─────────────────────────────────────────────────────          │
│  > 10,000 queries    →  CRITICAL      →  BLOCK AND INVESTIGATE│
│  > 5,000 queries     →  HIGH          →  REVIEW AND MONITOR   │
│  > 1,000 queries     →  MEDIUM        →  MONITOR              │
│  ≤ 1,000 queries     →  LOW           →  NORMAL               │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Data Retention

| Data Type | Retention Period | Compliance |
|-----------|-----------------|------------|
| Audit Logs | **10 years** | SBV requirement |
| PII Access Logs | **10 years** | SBV requirement |
| Access Summaries | **90 days** | Operational |
| Failed Access | **7 days** | Security |

---

## 4. TLS/SSL Encryption

### Purpose
Encrypt **all data in transit** between components. Required for banking compliance.

### Components Requiring TLS

| Component | Port | Protocol | Certificate |
|-----------|------|----------|-------------|
| Dremio | 9047 | HTTPS | Required |
| Kafka | 9093 | SSL | Required |
| MinIO | 9000 | HTTPS | Required |
| PostgreSQL | 5432 | SSL | Required |
| Airflow | 8080 | HTTPS | Required |
| Grafana | 3000 | HTTPS | Required |

### Certificate Setup

```
┌─────────────────────────────────────────────────────────────────┐
│                 CERTIFICATE GENERATION                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  DEVELOPMENT (Self-Signed):                                    │
│  ──────────────────────────                                    │
│  1. Generate CA key and certificate                            │
│  2. Generate server certificate                                │
│  3. Sign with CA                                               │
│                                                                 │
│  PRODUCTION (Trusted CA):                                      │
│  ─────────────────────────                                     │
│  1. Generate CSR (Certificate Signing Request)                 │
│  2. Submit to trusted CA (DigiCert, Let's Encrypt)             │
│  3. Receive signed certificate                                 │
│  4. Install in /etc/ssl/certs/                                 │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Security Best Practices

| Practice | Description |
|----------|-------------|
| **Use TLS 1.2+** | Disable TLS 1.0 and 1.1 |
| **Strong ciphers** | Avoid RC4, DES, 3DES |
| **Certificate rotation** | Rotate before expiry |
| **Monitor expiry** | Prometheus/Grafana alerts |
| **Secure key storage** | Use vault, not disk |
| **Mutual TLS (mTLS)** | For inter-service communication |
| **Audit TLS connections** | Log all SSL/TLS handshakes |

---

## 5. Data Governance Framework

### Purpose
Define **data policies**, **quality rules**, and **compliance requirements** for the banking data platform.

### Data Classification

```
┌─────────────────────────────────────────────────────────────────┐
│                 DATA CLASSIFICATION LEVELS                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Level 1: PUBLIC                                               │
│  ───────────────                                               │
│  • Product catalog, branch locations                           │
│  • Access: All users                                           │
│                                                                 │
│  Level 2: INTERNAL                                             │
│  ─────────────────                                             │
│  • Sales reports, employee data                                │
│  • Access: Employees only                                      │
│                                                                 │
│  Level 3: CONFIDENTIAL                                         │
│  ────────────────────                                          │
│  • Financial reports, customer analytics                       │
│  • Access: Managers+                                           │
│                                                                 │
│  Level 4: RESTRICTED                                           │
│  ───────────────────                                           │
│  • PAN, account numbers, KYC documents                         │
│  • Access: Authorized only                                     │
│                                                                 │
│  Level 5: CRITICAL                                             │
│  ───────────────────                                           │
│  • NPA data, fraud alerts, regulatory reports                  │
│  • Access: Compliance team only                                │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Data Quality Metrics

| Metric | Formula | Target | Alert Threshold |
|--------|---------|--------|-----------------|
| **Completeness** | Non-null values / Total values | > 99% | < 95% |
| **Uniqueness** | Distinct values / Total values | > 99.9% | < 99% |
| **Validity** | Valid values / Total values | > 99% | < 95% |
| **Consistency** | Consistent values across systems | > 99% | < 98% |
| **Timeliness** | Data age < threshold | < 1 hour | > 4 hours |

### Data Quality Checks

```sql
-- Customer Data Quality
SELECT customer_id, COUNT(*) as duplicate_count
FROM banking_cleansed.core_banking_customers
GROUP BY customer_id
HAVING COUNT(*) > 1;

-- Transaction Data Quality
SELECT txn_id, amount
FROM banking_cleansed.core_banking_transactions
WHERE amount < 0 AND txn_type != 'REFUND';

-- Loan Data Quality
SELECT loan_id, principal_amount, emi_amount
FROM banking_cleansed.loan_accounts
WHERE ABS(emi_amount - calculate_emi(...)) > 100;
```

### SBV Compliance Requirements

| Requirement | Description | Implementation |
|-------------|-------------|----------------|
| **Circular 39/2014** | Data security | Encryption, access control |
| **Circular 23/2014** | Reporting requirements | Daily/monthly reports |
| **Decision 1168/QD-NHNN** | AML regulations | Suspicious transaction reporting |
| **Basel III** | Capital adequacy | Risk-weighted assets calculation |
| **NPA Reporting** | Asset classification | Automated NPA detection |

### Data Retention Policy

| Data Type | Retention | Storage Tier | Archive |
|-----------|-----------|--------------|---------|
| Customer Master | Active + 10 years | Hot → Warm → Cold | Object Storage |
| Transactions | 7 years | Hot → Warm → Cold | Object Storage |
| Loan Data | Loan tenure + 7 years | Hot → Warm → Cold | Object Storage |
| Card Transactions | 7 years | Hot → Warm → Cold | Object Storage |
| NPA Data | 10 years | Hot → Warm → Cold | Object Storage |
| Audit Logs | 10 years | Cold | Deep Archive |

---

## How All Files Work Together

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    SECURITY WORKFLOW                                         │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  1. USER LOGS IN                                                            │
│     └── TLS encryption (tls-config.md)                                     │
│                                                                             │
│  2. ROLE CHECK (role-hierarchy.sql)                                        │
│     └── What role does the user have?                                      │
│         ├── data_engineers → Full access                                   │
│         ├── data_analysts → Read only                                      │
│         └── risk_team → Risk views only                                    │
│                                                                             │
│  3. ROW-LEVEL CHECK (row-level-security.sql)                               │
│     └── Which rows can the user see?                                       │
│         ├── ENTERPRISE role → ALL rows                                     │
│         ├── REGION role → Branches in region                               │
│         └── BRANCH role → Only their branch                                │
│                                                                             │
│  4. DATA ACCESS                                                             │
│     └── User queries data                                                   │
│                                                                             │
│  5. AUDIT LOG (audit-config.sql)                                           │
│     └── Log: who, what, when, where, why                                   │
│         ├── access_log (all actions)                                       │
│         ├── pii_access_log (PII tracking)                                  │
│         └── failed_access_log (security incidents)                         │
│                                                                             │
│  6. GOVERNANCE CHECK (data-governance-framework.md)                        │
│     └── Data classification and quality rules applied                      │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Quick Start

### Step 1: Enable TLS
```bash
# Follow encryption/tls-config.md
# Generate certificates for all 6 components
```

### Step 2: Create Roles
```bash
# Run role-hierarchy.sql in Dremio
# This creates 13 roles with hierarchy
```

### Step 3: Apply RLS
```bash
# Run row-level-security.sql in Dremio
# This applies branch and customer-based access
```

### Step 4: Enable Audit Logging
```bash
# Run audit-config.sql in Dremio
# This creates audit tables and triggers
```

### Step 5: Implement Governance
```bash
# Follow data-governance-framework.md
# Set up data classification and quality rules
```

---

## Verification Commands

```bash
# Test TLS connection to Dremio
curl -k https://dremio-master:9047/apiv2/system/info

# Test TLS connection to Kafka
openssl s_client -connect kafka-1:9093

# Test TLS connection to PostgreSQL
psql "host=postgres-server sslmode=require dbname=banking"

# Test TLS connection to MinIO
curl -k https://minio:9000/minio/health/live

# Verify role assignments (in Dremio SQL)
SELECT groname, ARRAY_AGG(memid) 
FROM pg_roles 
WHERE groname IN ('data_engineers', 'data_analysts', 'risk_team')
GROUP BY groname;

# Verify RLS policies (in Dremio SQL)
SELECT schemaname, tablename, policyname 
FROM pg_policies 
WHERE schemaname IN ('banking_cleansed', 'banking_gold');

# Check audit logs (in Dremio SQL)
SELECT user_id, COUNT(*) as access_count
FROM banking_audit.access_log
WHERE event_timestamp >= CURRENT_DATE - INTERVAL '7' DAY
GROUP BY user_id
ORDER BY access_count DESC;
```

---

## Related Files

| File | Purpose |
|------|---------|
| `access-control/role-hierarchy.sql` | RBAC roles and permissions |
| `access-control/row-level-security.sql` | Row-level security policies |
| `audit/audit-config.sql` | Audit logging configuration |
| `encryption/tls-config.md` | TLS/SSL setup guide |
| `governance/data-governance-framework.md` | Data governance policies |

---

## Related Documentation

| Document | Location |
|----------|----------|
| Dremio Security Setup | `../04-dremio-setup/security/` |
| Monitoring & Alerting | `../08-monitoring/` |
| Regulatory Reports | `../05-banking-scenarios/03-regulatory-reporting/` |

---

*Part of: [Lakehouse Platform Project](../README.md)*
