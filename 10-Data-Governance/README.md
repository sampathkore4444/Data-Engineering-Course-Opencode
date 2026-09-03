# 10 - Data Governance

## Table of Contents
1. [Data Governance Framework](#1-data-governance-framework)
2. [Data Quality Management](#2-data-quality-management)
3. [Data Cataloging](#3-data-cataloging)
4. [Data Lineage](#4-data-lineage)
5. [Real-World Scenarios](#5-real-world-scenarios)
   - [Scenario 1: Banking Customer Data Governance](#scenario-1-banking-customer-data-governance)
   - [Scenario 2: Transaction Data Quality Management](#scenario-2-transaction-data-quality-management)
   - [Scenario 3: Regulatory Compliance Data Governance](#scenario-3-regulatory-compliance-data-governance)
   - [Scenario 4: Data Catalog for Banking Analytics](#scenario-4-data-catalog-for-banking-analytics)
   - [Scenario 5: Data Lineage for Regulatory Reporting](#scenario-5-data-lineage-for-regulatory-reporting)
6. [Hands-On Exercises](#6-hands-on-exercises)
7. [Interview Questions](#7-interview-questions)

---

## 1. Data Governance Framework

### Core Components

```
+--------------------------------------------------+
|              DATA GOVERNANCE                      |
+--------------------------------------------------+
|                                                  |
|  +------------+  +------------+  +------------+  |
|  |  Policies  |  | Standards  |  |  Roles &   |  |
|  |            |  |            |  |  Resp.     |  |
|  +------------+  +------------+  +------------+  |
|                                                  |
|  +------------+  +------------+  +------------+  |
|  |  Data      |  |  Quality   |  |  Lineage   |  |
|  |  Catalog   |  |  Mgmt      |  |  Tracking  |  |
|  +------------+  +------------+  +------------+  |
|                                                  |
|  +------------+  +------------+  +------------+  |
|  |  Master    |  | Reference  |  | Metadata   |  |
|  |  Data Mgmt |  | Data Mgmt  |  | Management |  |
|  +------------+  +------------+  +------------+  |
+--------------------------------------------------+
```

### Roles and Responsibilities

| Role | Responsibility |
|------|---------------|
| **Data Owner** | Business leader accountable for data domain |
| **Data Steward** | Day-to-day management of data quality |
| **Data Custodian** | Technical implementation of data controls |
| **Data Protection Officer** | Privacy and compliance oversight |
| **Data Engineer** | Pipeline development and maintenance |
| **Data Analyst** | Data usage and reporting |

### Data Classification

```
PUBLIC:           No restrictions (marketing materials)
INTERNAL:         Limited to employees (org charts)
CONFIDENTIAL:     Restricted access (customer data)
RESTRICTED:       Highly regulated (PII, PHI, financial)
```

### Data Policies

```yaml
# Example Data Retention Policy
data_retention_policy:
  transaction_data:
    retention_period: 7_years
    storage_tier: hot_1yr_warm_3yr_cold_3yr
    archive_after: 1_year
    
  customer_pii:
    retention_period: until_consent_revoked
    encryption: required
    access_logging: required
    
  audit_logs:
    retention_period: 10_years
    immutable: true
    access: compliance_team_only
```

---

## 2. Data Quality Management

### Six Dimensions of Data Quality

```
+--------------------------------------------------+
|           DATA QUALITY DIMENSIONS                 |
+--------------------------------------------------+
|                                                  |
|  ACCURACY        COMPLETENESS    CONSISTENCY      |
|  (Correct)       (Complete)      (Consistent)    |
|                                                  |
|  TIMELINESS      UNIQUENESS      VALIDITY         |
|  (Timely)        (Unique)        (Valid)          |
+--------------------------------------------------+
```

### Data Quality Rules

```sql
-- Accuracy: Values match expected ranges
SELECT COUNT(*) FROM orders WHERE amount < 0 OR amount > 1000000;

-- Completeness: No null values in required fields
SELECT 
    COUNT(*) as total,
    COUNT(customer_id) as non_null,
    ROUND(COUNT(customer_id) * 100.0 / COUNT(*), 2) as completeness_pct
FROM orders;

-- Uniqueness: No duplicate primary keys
SELECT order_id, COUNT(*) FROM orders GROUP BY order_id HAVING COUNT(*) > 1;

-- Consistency: Values match across systems
SELECT o.order_id
FROM orders o
LEFT JOIN order_items oi ON o.order_id = oi.order_id
WHERE o.order_id IS NULL;

-- Validity: Values match allowed patterns
SELECT * FROM customers WHERE email NOT LIKE '%@%.%';

-- Timeliness: Data freshness
SELECT MAX(updated_at) FROM orders;
```

### Great Expectations

```python
import great_expectations as gx

context = gx.get_context()

# Define expectations
validator = context.get_validator(batch_request=batch_request)

# Accuracy
validator.expect_column_values_to_be_between("amount", min_value=0, max_value=1000000)

# Completeness
validator.expect_column_values_to_not_be_null("customer_id")
validator.expect_column_values_to_not_be_null("order_date")

# Uniqueness
validator.expect_column_values_to_be_unique("order_id")

# Validity
validator.expect_column_values_to_match_regex("email", r"^[\w\.-]+@[\w\.-]+\.\w+$")
validator.expect_column_values_to_be_in_set("status", ["pending", "completed", "cancelled"])

# Freshness
validator.expect_column_max_to_be_between("updated_at", min_value=yesterday, max_value=today)

# Run validation
results = validator.validate()
```

### dbt Tests

```yaml
# schema.yml
version: 2
models:
  - name: orders
    columns:
      - name: order_id
        tests:
          - unique
          - not_null
      - name: customer_id
        tests:
          - not_null
          - relationships:
              to: ref('customers')
              field: customer_id
      - name: amount
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: "amount > 0"
      - name: status
        tests:
          - accepted_values:
              values: ['pending', 'completed', 'cancelled']
```

---

## 3. Data Cataloging

### Data Catalog Components

```
+--------------------------------------------------+
|              DATA CATALOG                         |
+--------------------------------------------------+
|                                                  |
|  +------------+  +------------+  +------------+  |
|  |  Metadata  |  |  Search &  |  |  Data      |  |
|  |  Store     |  |  Discovery |  |  Profiles  |  |
|  +------------+  +------------+  +------------+  |
|                                                  |
|  +------------+  +------------+  +------------+  |
|  |  Lineage   |  |  Glossary  |  |  Access    |  |
|  |  Graph     |  |  & Terms   |  |  Control   |  |
|  +------------+  +------------+  +------------+  |
+--------------------------------------------------+
```

### OpenMetadata Example

```yaml
# Table definition
apiVersion: metadata.githubusercontent.com/v1alpha1
kind: Table
metadata:
  name: orders
  description: "Sales orders from e-commerce platform"
  owner:
    displayName: Data Engineering
    email: data-eng@company.com
  tags:
    - name: PII
    - name: Financial
  columns:
    - name: order_id
      description: "Unique order identifier"
      dataType: BIGINT
      tags:
        - name: Primary Key
    - name: customer_id
      description: "Foreign key to customers table"
      dataType: BIGINT
      tags:
        - name: Foreign Key
        - name: PII
    - name: amount
      description: "Order total amount in USD"
      dataType: DECIMAL(12,2)
  tier: Gold
  dataQualityTests:
    - name: unique_order_id
    - name: not_null_customer_id
    - name: amount_positive
```

---

## 4. Data Lineage

### Lineage Tracking

```
Source Systems          Transformations         Target Systems
+----------+          +---------------+       +-----------+
| CRM      |----+     | Deduplicate   |----->| Data      |
+----------+    |     | Cleanse       |      | Warehouse |
+----------+    +---->| Enrich        |      +-----------+
| ERP      |----+     | Aggregate     |           |
+----------+    |     +---------------+           v
+----------+    |                           +-----------+
| Web Logs |----+                           | BI Tools  |
+----------+                                | Reports   |
                                            +-----------+
```

### Implementing Lineage with dbt

```sql
-- dbt automatically tracks lineage
-- models/staging/stg_orders.sql
WITH source AS (
    SELECT * FROM {{ source('raw', 'orders') }}
),
renamed AS (
    SELECT
        id as order_id,
        user_id as customer_id,
        amount,
        created_at as order_date
    FROM source
)
SELECT * FROM renamed;

-- models/marts/fact_orders.sql
SELECT
    o.order_id,
    c.customer_name,
    p.product_name,
    o.amount
FROM {{ ref('stg_orders') }} o
JOIN {{ ref('stg_customers') }} c ON o.customer_id = c.customer_id
JOIN {{ ref('stg_products') }} p ON o.product_id = p.product_id;
```

### Impact Analysis

```sql
-- What downstream tables are affected if stg_orders changes?
-- dbt provides this automatically via DAG
-- Manual approach:
SELECT 
    referencing_table,
    referencing_column
FROM system.referential_constraints
WHERE referenced_table = 'stg_orders';
```

---

## 5. Data Governance Tools

### Open Source Tools

| Tool | Type | Key Features |
|------|------|--------------|
| **Apache Atlas** | Metadata, Lineage | Hadoop integration, auto-discovery |
| **DataHub** | Metadata, Lineage | LinkedIn open-source, Slack integration |
| **OpenMetadata** | Metadata, Lineage | Open-source, REST API |
| **Great Expectations** | Data Quality | Python-based, flexible rules |
| **dbt** | Quality, Lineage | SQL-based, version control |
| **Soda Core** | Data Quality | YAML-based checks, SodaCL |
| **Evidently AI** | ML Data Quality | ML monitoring, drift detection |

### Enterprise/Commercial Tools

| Tool | Type | Key Features |
|------|------|--------------|
| **Collibra** | Enterprise Governance | Commercial, workflow automation |
| **Alation** | Data Catalog | Commercial, ML-powered discovery |
| **Informatica** | Enterprise Data Mgmt | End-to-end governance |
| **Talend** | Data Quality | Integration + quality |
| **Atlan** | Modern Data Catalog | Active metadata, collaboration |
| **Castor** | Data Discovery | Automated documentation |

### Cloud-Native Governance

| Cloud | Tools | Description |
|-------|-------|-------------|
| **AWS** | Lake Formation, Glue DataBrew, Macie | Data lake governance, PII detection |
| **GCP** | Data Catalog, DLP API, Dataplex | Metadata, privacy, data mesh |
| **Azure** | Purview, Data Factory | Unified governance, data map |

---

## 5. Real-World Scenarios

### Overview

This section presents **5 complete banking data governance scenarios** that demonstrate how to implement comprehensive governance frameworks for financial institutions. Each scenario includes the business context, governance architecture, implementation code, and compliance outcomes.

---

### Scenario 1: Banking Customer Data Governance

> **Business Context:** A bank with 10M+ customers needs to implement governance for customer PII while enabling analytics and regulatory reporting.

#### Governance Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│              BANKING CUSTOMER DATA GOVERNANCE                           │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│   ┌──────────────────────────────────────────────────────────────────┐  │
│   │                    DATA SOURCES                                  │  │
│   │                                                                  │  │
│   │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐       │  │
│   │  │ Core     │  │ Digital  │  │ Branch   │  │ Third-   │       │  │
│   │  │ Banking  │  │ Channels │  │ Systems  │  │ Party    │       │  │
│   │  └──────────┘  └──────────┘  └──────────┘  └──────────┘       │  │
│   │                                                                  │  │
│   └──────────────────────────────────────────────────────────────────┘  │
│                              │                                          │
│                              ▼                                          │
│   ┌──────────────────────────────────────────────────────────────────┐  │
│   │                    GOVERNANCE LAYER                               │  │
│   │                                                                  │  │
│   │  ┌────────────────────────────────────────────────────────────┐ │  │
│   │  │              DATA CLASSIFICATION ENGINE                     │ │  │
│   │  │                                                            │ │  │
│   │  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐ │ │  │
│   │  │  │ PII      │  │ Financial│  │ Sensitive│  │ Public   │ │ │  │
│   │  │  │ Detector │  │ Classifier│  │ Data    │  │ Data     │ │ │  │
│   │  │  └──────────┘  └──────────┘  └──────────┘  └──────────┘ │ │  │
│   │  │                                                            │ │  │
│   │  └────────────────────────────────────────────────────────────┘ │  │
│   │                              │                                    │  │
│   │                              ▼                                    │  │
│   │  ┌────────────────────────────────────────────────────────────┐ │  │
│   │  │              DATA CATALOG (OpenMetadata)                   │ │  │
│   │  │                                                            │ │  │
│   │  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐ │ │  │
│   │  │  │ Metadata │  │ Lineage  │  │ Quality  │  │ Access   │ │ │  │
│   │  │  │ Store    │  │ Graph    │  │ Scores   │  │ Control  │ │ │  │
│   │  │  └──────────┘  └──────────┘  └──────────┘  └──────────┘ │ │  │
│   │  │                                                            │ │  │
│   │  └────────────────────────────────────────────────────────────┘ │  │
│   │                              │                                    │  │
│   │                              ▼                                    │  │
│   │  ┌────────────────────────────────────────────────────────────┐ │  │
│   │  │              COMPLIANCE & MONITORING                        │ │  │
│   │  │                                                            │ │  │
│   │  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐ │ │  │
│   │  │  │ GDPR     │  │ Audit    │  │ Data     │  │ Alert    │ │ │  │
│   │  │  │ Controls │  │ Logging  │  │ Masking  │  │ System   │ │ │  │
│   │  │  └──────────┘  └──────────┘  └──────────┘  └──────────┘ │ │  │
│   │  │                                                            │ │  │
│   │  └────────────────────────────────────────────────────────────┘ │  │
│   │                                                                  │  │
│   └──────────────────────────────────────────────────────────────────┘  │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

#### Implementation Code

```sql
-- ============================================================
-- 1. DATA CLASSIFICATION SYSTEM
-- ============================================================

-- Create classification metadata table
CREATE TABLE data_classification (
    classification_id SERIAL PRIMARY KEY,
    table_name VARCHAR(100),
    column_name VARCHAR(100),
    classification_level VARCHAR(20),  -- PUBLIC, INTERNAL, CONFIDENTIAL, RESTRICTED
    data_type VARCHAR(50),  -- PII, FINANCIAL, SENSITIVE
    description TEXT,
    owner VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Classify customer data
INSERT INTO data_classification (table_name, column_name, classification_level, data_type, description, owner)
VALUES
    ('customers', 'customer_id', 'INTERNAL', 'IDENTIFIER', 'Unique customer identifier', 'data-governance@bank.com'),
    ('customers', 'name', 'CONFIDENTIAL', 'PII', 'Customer full name', 'data-governance@bank.com'),
    ('customers', 'email', 'CONFIDENTIAL', 'PII', 'Customer email address', 'data-governance@bank.com'),
    ('customers', 'ssn', 'RESTRICTED', 'PII', 'Social Security Number', 'compliance@bank.com'),
    ('customers', 'phone', 'CONFIDENTIAL', 'PII', 'Customer phone number', 'data-governance@bank.com'),
    ('accounts', 'account_number', 'RESTRICTED', 'FINANCIAL', 'Bank account number', 'compliance@bank.com'),
    ('accounts', 'balance', 'CONFIDENTIAL', 'FINANCIAL', 'Account balance', 'finance@bank.com');

-- ============================================================
-- 2. ACCESS CONTROL POLICIES
-- ============================================================

-- Create role-based access control table
CREATE TABLE access_policies (
    policy_id SERIAL PRIMARY KEY,
    role_name VARCHAR(50),
    classification_level VARCHAR(20),
    access_type VARCHAR(20),  -- READ, WRITE, MASKED
    conditions JSONB,  -- Additional conditions
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Define access policies
INSERT INTO access_policies (role_name, classification_level, access_type, conditions)
VALUES
    ('data_analyst', 'PUBLIC', 'READ', '{}'),
    ('data_analyst', 'INTERNAL', 'READ', '{}'),
    ('data_analyst', 'CONFIDENTIAL', 'MASKED', '{"mask_pii": true}'),
    ('data_analyst', 'RESTRICTED', 'DENY', '{}'),
    ('data_engineer', 'PUBLIC', 'READ', '{}'),
    ('data_engineer', 'INTERNAL', 'READ', '{}'),
    ('data_engineer', 'CONFIDENTIAL', 'READ', '{"audit_log": true}'),
    ('data_engineer', 'RESTRICTED', 'MASKED', '{"mask_pii": true}'),
    ('compliance_officer', 'PUBLIC', 'READ', '{}'),
    ('compliance_officer', 'INTERNAL', 'READ', '{}'),
    ('compliance_officer', 'CONFIDENTIAL', 'READ', '{}'),
    ('compliance_officer', 'RESTRICTED', 'READ', '{}');

-- ============================================================
-- 3. DATA MASKING FUNCTION
-- ============================================================

-- Create masking function
CREATE OR REPLACE FUNCTION mask_data(
    p_value TEXT,
    p_classification VARCHAR(20),
    p_data_type VARCHAR(50)
) RETURNS TEXT AS $$
BEGIN
    -- No masking for non-sensitive data
    IF p_classification IN ('PUBLIC', 'INTERNAL') THEN
        RETURN p_value;
    END IF;
    
    -- Mask PII data
    IF p_data_type = 'PII' THEN
        IF p_classification = 'CONFIDENTIAL' THEN
            -- Partial masking for confidential PII
            RETURN LEFT(p_value, 2) || '***' || RIGHT(p_value, 2);
        ELSIF p_classification = 'RESTRICTED' THEN
            -- Full masking for restricted PII
            RETURN '***MASKED***';
        END IF;
    END IF;
    
    -- Mask financial data
    IF p_data_type = 'FINANCIAL' THEN
        IF p_classification = 'RESTRICTED' THEN
            RETURN '****' || RIGHT(p_value, 4);
        END IF;
    END IF;
    
    RETURN p_value;
END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- 4. AUDIT LOGGING
-- ============================================================

-- Create audit log table
CREATE TABLE data_access_audit (
    audit_id SERIAL PRIMARY KEY,
    table_name VARCHAR(100),
    column_name VARCHAR(100),
    access_type VARCHAR(20),
    user_name VARCHAR(100),
    user_role VARCHAR(50),
    classification_level VARCHAR(20),
    access_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    query_text TEXT,
    ip_address INET
);

-- Create audit trigger function
CREATE OR REPLACE FUNCTION audit_data_access()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO data_access_audit (
        table_name, column_name, access_type, user_name, user_role
    ) VALUES (
        TG_TABLE_NAME,
        TG_ARGV[0],
        TG_OP,
        current_user,
        current_setting('app.user_role', true)
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- 5. GDPR COMPLIANCE - RIGHT TO ERASURE
-- ============================================================

-- Create customer consent tracking
CREATE TABLE customer_consents (
    consent_id SERIAL PRIMARY KEY,
    customer_id INT,
    consent_type VARCHAR(50),  -- MARKETING, ANALYTICS, THIRD_PARTY
    consent_granted BOOLEAN,
    consent_date TIMESTAMP,
    expiry_date TIMESTAMP,
    withdrawal_date TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Function to handle right to erasure
CREATE OR REPLACE FUNCTION gdpr_erase_customer(p_customer_id INT)
RETURNS BOOLEAN AS $$
BEGIN
    -- Log the erasure request
    INSERT INTO data_access_audit (table_name, access_type, user_name, query_text)
    VALUES ('customers', 'DELETE', 'GDPR_SYSTEM', 'GDPR Erasure Request');
    
    -- Anonymize customer data (keep for audit)
    UPDATE customers SET
        name = 'DELETED_' || customer_id,
        email = 'deleted_' || customer_id || '@anonymized.com',
        ssn = NULL,
        phone = NULL,
        address = NULL,
        deleted_at = CURRENT_TIMESTAMP
    WHERE customer_id = p_customer_id;
    
    -- Delete related records
    DELETE FROM customer_consents WHERE customer_id = p_customer_id;
    
    RETURN TRUE;
END;
$$ LANGUAGE plpgsql;
```

#### Key Metrics & Outcomes

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Data classification | Manual, incomplete | Automated, 100% | 100% coverage |
| Access control | Basic RBAC | RBAC + masking + audit | Defense in depth |
| GDPR compliance | Partial | Full | 100% compliant |
| Audit trail | None | Complete | Full visibility |
| Data discovery time | Hours | Minutes | 90% faster |

---

### Scenario 2: Transaction Data Quality Management

> **Business Context:** A bank processes 2M+ daily transactions and needs to ensure data quality for accurate financial reporting.

#### Quality Management Framework

```
┌─────────────────────────────────────────────────────────────────────────┐
│              TRANSACTION DATA QUALITY FRAMEWORK                         │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│   ┌──────────────────────────────────────────────────────────────────┐  │
│   │                    DATA SOURCES                                  │  │
│   │                                                                  │  │
│   │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐       │  │
│   │  │ Card     │  │ Wire     │  │ ACH      │  │ Internal │       │  │
│   │  │ Transfers│  │ Transfers│  │ Payments │  │ Transfers│       │  │
│   │  └──────────┘  └──────────┘  └──────────┘  └──────────┘       │  │
│   │                                                                  │  │
│   └──────────────────────────────────────────────────────────────────┘  │
│                              │                                          │
│                              ▼                                          │
│   ┌──────────────────────────────────────────────────────────────────┐  │
│   │                    QUALITY CHECKS                                │  │
│   │                                                                  │  │
│   │  ┌────────────────────────────────────────────────────────────┐ │  │
│   │  │  1. COMPLETENESS CHECK                                      │ │  │
│   │  │     - All required fields present                           │ │  │
│   │  │     - No null values in critical columns                   │ │  │
│   │  │                                                            │ │  │
│   │  │  2. ACCURACY CHECK                                          │ │  │
│   │  │     - Amounts within valid ranges                          │ │  │
│   │  │     - Account numbers exist                                │ │  │
│   │  │                                                            │ │  │
│   │  │  3. CONSISTENCY CHECK                                       │ │  │
│   │  │     - Balances match across systems                        │ │  │
│   │  │     - Transaction types valid                              │ │  │
│   │  │                                                            │ │  │
│   │  │  4. UNIQUENESS CHECK                                        │ │  │
│   │  │     - No duplicate transaction IDs                         │ │  │
│   │  │                                                            │ │  │
│   │  │  5. TIMELINESS CHECK                                        │ │  │
│   │  │     - Data freshness within SLA                            │ │  │
│   │  └────────────────────────────────────────────────────────────┘ │  │
│   │                              │                                    │  │
│   │                              ▼                                    │  │
│   │  ┌────────────────────────────────────────────────────────────┐ │  │
│   │  │  QUALITY DASHBOARD & ALERTING                               │ │  │
│   │  │                                                            │ │  │
│   │  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐ │ │  │
│   │  │  │ Real-time│  │ SLA      │  │ Trend    │  │ Alert    │ │ │  │
│   │  │  │ Metrics  │  │ Tracking │  │ Analysis │  │ System   │ │ │  │
│   │  │  └──────────┘  └──────────┘  └──────────┘  └──────────┘ │ │  │
│   │  │                                                            │ │  │
│   │  └────────────────────────────────────────────────────────────┘ │  │
│   │                                                                  │  │
│   └──────────────────────────────────────────────────────────────────┘  │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

#### Implementation Code

```python
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List
import json

class TransactionDataQuality:
    """Comprehensive data quality framework for banking transactions."""
    
    def __init__(self):
        self.quality_results = []
        self.alerts = []
    
    def check_completeness(self, df: pd.DataFrame) -> Dict:
        """Check completeness of transaction data."""
        required_columns = [
            'transaction_id', 'account_id', 'amount', 
            'transaction_type', 'transaction_date', 'status'
        ]
        
        results = {}
        for col in required_columns:
            if col in df.columns:
                null_count = df[col].isnull().sum()
                completeness = (len(df) - null_count) / len(df) * 100
                results[col] = {
                    'completeness_pct': completeness,
                    'null_count': null_count,
                    'passed': null_count == 0
                }
                
                if null_count > 0:
                    self.alerts.append({
                        'type': 'COMPLETENESS_FAILURE',
                        'column': col,
                        'null_count': null_count,
                        'timestamp': datetime.now()
                    })
        
        return results
    
    def check_accuracy(self, df: pd.DataFrame) -> Dict:
        """Check accuracy of transaction amounts."""
        results = {}
        
        # Check amount ranges
        invalid_amounts = df[(df['amount'] <= 0) | (df['amount'] > 10000000)]
        results['amount_range'] = {
            'invalid_count': len(invalid_amounts),
            'passed': len(invalid_amounts) == 0
        }
        
        # Check account existence (simulated)
        # In production: Query account master table
        results['account_existence'] = {
            'invalid_count': 0,  # Simulated
            'passed': True
        }
        
        # Check transaction type validity
        valid_types = ['DEBIT', 'CREDIT', 'TRANSFER', 'WIRE', 'ACH']
        invalid_types = df[~df['transaction_type'].isin(valid_types)]
        results['transaction_type'] = {
            'invalid_count': len(invalid_types),
            'passed': len(invalid_types) == 0
        }
        
        return results
    
    def check_consistency(self, df: pd.DataFrame) -> Dict:
        """Check consistency across systems."""
        results = {}
        
        # Check for duplicate transaction IDs
        duplicates = df[df.duplicated(subset=['transaction_id'], keep=False)]
        results['duplicate_ids'] = {
            'duplicate_count': len(duplicates),
            'passed': len(duplicates) == 0
        }
        
        # Check balance consistency (simulated)
        # In production: Compare with account balances
        results['balance_consistency'] = {
            'variance_count': 0,  # Simulated
            'passed': True
        }
        
        return results
    
    def check_timeliness(self, df: pd.DataFrame) -> Dict:
        """Check data freshness."""
        results = {}
        
        # Check transaction date freshness
        max_date = pd.to_datetime(df['transaction_date']).max()
        hours_old = (datetime.now() - max_date).total_seconds() / 3600
        
        results['freshness'] = {
            'hours_old': hours_old,
            'max_allowed_hours': 24,
            'passed': hours_old <= 24
        }
        
        if hours_old > 24:
            self.alerts.append({
                'type': 'TIMELINESS_FAILURE',
                'hours_old': hours_old,
                'timestamp': datetime.now()
            })
        
        return results
    
    def run_all_checks(self, df: pd.DataFrame) -> Dict:
        """Run all quality checks."""
        results = {
            'completeness': self.check_completeness(df),
            'accuracy': self.check_accuracy(df),
            'consistency': self.check_consistency(df),
            'timeliness': self.check_timeliness(df)
        }
        
        # Calculate overall score
        total_checks = 0
        passed_checks = 0
        for dimension in results.values():
            for check in dimension.values():
                total_checks += 1
                if check.get('passed', False):
                    passed_checks += 1
        
        results['overall_score'] = (passed_checks / total_checks * 100) if total_checks > 0 else 0
        results['total_checks'] = total_checks
        results['passed_checks'] = passed_checks
        
        return results
    
    def generate_report(self) -> str:
        """Generate quality report."""
        report = f"""
=== Transaction Data Quality Report ===
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

Quality Dimensions:
- Completeness: {self.results.get('completeness', {})}
- Accuracy: {self.results.get('accuracy', {})}
- Consistency: {self.results.get('consistency', {})}
- Timeliness: {self.results.get('timeliness', {})}

Overall Score: {self.results.get('overall_score', 0):.2f}%
Alerts Generated: {len(self.alerts)}
"""
        return report

# Test with sample data
def test_quality_framework():
    # Create sample transaction data with quality issues
    df = pd.DataFrame({
        'transaction_id': ['TXN001', 'TXN002', 'TXN003', 'TXN003', 'TXN005'],  # Duplicate
        'account_id': ['ACC001', 'ACC002', None, 'ACC004', 'ACC005'],  # Null
        'amount': [1000, 2500, -500, 1500, 15000000],  # Invalid ranges
        'transaction_type': ['DEBIT', 'CREDIT', 'INVALID', 'TRANSFER', 'WIRE'],  # Invalid type
        'transaction_date': [
            datetime.now() - timedelta(hours=1),
            datetime.now() - timedelta(hours=2),
            datetime.now() - timedelta(hours=3),
            datetime.now() - timedelta(hours=4),
            datetime.now() - timedelta(hours=25)  # Too old
        ],
        'status': ['COMPLETED', 'COMPLETED', 'PENDING', 'COMPLETED', 'COMPLETED']
    })
    
    checker = TransactionDataQuality()
    results = checker.run_all_checks(df)
    print(checker.generate_report())
    
    return results
```

#### Key Metrics & Outcomes

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Data quality score | 85% | 99.5% | 99.5% accurate |
| Duplicate transactions | 500/day | 0 | 100% reduction |
| Invalid amounts | 200/day | 0 | 100% reduction |
| Data freshness | 48 hours | 1 hour | 98% faster |
| Quality incidents | 50/month | 2/month | 96% reduction |

---

### Scenario 3: Regulatory Compliance Data Governance

> **Business Context:** A bank must comply with Basel III, GDPR, SOX, and local banking regulations while managing 100TB+ of data.

#### Compliance Framework

```
┌─────────────────────────────────────────────────────────────────────────┐
│              REGULATORY COMPLIANCE FRAMEWORK                            │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│   ┌──────────────────────────────────────────────────────────────────┐  │
│   │                    REGULATORY REQUIREMENTS                       │  │
│   │                                                                  │  │
│   │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐       │  │
│   │  │ Basel III│  │ GDPR     │  │ SOX      │  │ PCI DSS  │       │  │
│   │  │          │  │          │  │          │  │          │       │  │
│   │  │ Capital  │  │ Customer │  │ Financial│  │ Payment  │       │  │
│   │  │ Adequacy │  │ Privacy  │  │ Audit    │  │ Card     │       │  │
│   │  └──────────┘  └──────────┘  └──────────┘  └──────────┘       │  │
│   │                                                                  │  │
│   └──────────────────────────────────────────────────────────────────┘  │
│                              │                                          │
│                              ▼                                          │
│   ┌──────────────────────────────────────────────────────────────────┐  │
│   │                    GOVERNANCE CONTROLS                           │  │
│   │                                                                  │  │
│   │  ┌────────────────────────────────────────────────────────────┐ │  │
│   │  │  DATA RETENTION POLICIES                                    │ │  │
│   │  │                                                            │ │  │
│   │  │  - Transaction data: 7 years                               │ │  │
│   │  │  - Customer PII: Until consent revoked + 5 years          │ │  │
│   │  │  - Audit logs: 10 years (immutable)                        │ │  │
│   │  │  - Marketing data: 2 years                                 │ │  │
│   │  └────────────────────────────────────────────────────────────┘ │  │
│   │                              │                                    │  │
│   │                              ▼                                    │  │
│   │  ┌────────────────────────────────────────────────────────────┐ │  │
│   │  │  DATA LINEAGE & IMPACT ANALYSIS                           │ │  │
│   │  │                                                            │ │  │
│   │  │  - Track all data transformations                          │ │  │
│   │  │  - Impact analysis for schema changes                      │ │  │
│   │  │  - Regulatory report lineage                               │ │  │
│   │  └────────────────────────────────────────────────────────────┘ │  │
│   │                              │                                    │  │
│   │                              ▼                                    │  │
│   │  ┌────────────────────────────────────────────────────────────┐ │  │
│   │  │  AUTOMATED COMPLIANCE REPORTING                            │ │  │
│   │  │                                                            │ │  │
│   │  │  - Daily compliance dashboards                             │ │  │
│   │  │  - Automated regulatory submissions                        │ │  │
│   │  │  - Audit trail generation                                  │ │  │
│   │  └────────────────────────────────────────────────────────────┘ │  │
│   │                                                                  │  │
│   └──────────────────────────────────────────────────────────────────┘  │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

#### Implementation Code

```python
from datetime import datetime, timedelta
from typing import Dict, List
import json

class RegulatoryComplianceGovernance:
    """Automated compliance governance for banking regulations."""
    
    def __init__(self):
        self.compliance_rules = self._load_compliance_rules()
        self.audit_trail = []
    
    def _load_compliance_rules(self) -> Dict:
        """Load regulatory compliance rules."""
        return {
            'BASEL_III': {
                'data_retention': 2555,  # 7 years in days
                'required_fields': ['capital_ratio', 'rwa', 'tier1_capital'],
                'reporting_frequency': 'daily',
                'data_classification': 'RESTRICTED'
            },
            'GDPR': {
                'data_retention': None,  # Until consent revoked
                'required_fields': ['consent_status', 'consent_date'],
                'reporting_frequency': 'on_demand',
                'data_classification': 'CONFIDENTIAL',
                'right_to_erasure': True,
                'data_portability': True
            },
            'SOX': {
                'data_retention': 3650,  # 10 years
                'required_fields': ['audit_trail', 'approval_workflow'],
                'reporting_frequency': 'quarterly',
                'data_classification': 'RESTRICTED'
            },
            'PCI_DSS': {
                'data_retention': 2555,  # 7 years
                'required_fields': ['tokenization', 'encryption'],
                'reporting_frequency': 'annual',
                'data_classification': 'RESTRICTED'
            }
        }
    
    def check_data_retention(self, table_name: str, data_age_days: int, regulation: str) -> Dict:
        """Check data retention compliance."""
        rule = self.compliance_rules.get(regulation, {})
        max_retention = rule.get('data_retention')
        
        if max_retention is None:
            return {'compliant': True, 'message': 'No retention limit'}
        
        compliant = data_age_days <= max_retention
        days_until_deletion = max_retention - data_age_days
        
        result = {
            'table_name': table_name,
            'regulation': regulation,
            'data_age_days': data_age_days,
            'max_retention_days': max_retention,
            'compliant': compliant,
            'days_until_deletion': days_until_deletion
        }
        
        self.audit_trail.append({
            'action': 'RETENTION_CHECK',
            'result': result,
            'timestamp': datetime.now()
        })
        
        return result
    
    def check_required_fields(self, data: Dict, regulation: str) -> Dict:
        """Check if required fields are present."""
        rule = self.compliance_rules.get(regulation, {})
        required = rule.get('required_fields', [])
        
        missing = [field for field in required if field not in data]
        compliant = len(missing) == 0
        
        result = {
            'regulation': regulation,
            'required_fields': required,
            'missing_fields': missing,
            'compliant': compliant
        }
        
        self.audit_trail.append({
            'action': 'REQUIRED_FIELDS_CHECK',
            'result': result,
            'timestamp': datetime.now()
        })
        
        return result
    
    def check_data_classification(self, data_classification: str, regulation: str) -> Dict:
        """Check if data classification meets regulation requirements."""
        rule = self.compliance_rules.get(regulation, {})
        required_classification = rule.get('data_classification')
        
        classification_hierarchy = ['PUBLIC', 'INTERNAL', 'CONFIDENTIAL', 'RESTRICTED']
        
        data_level = classification_hierarchy.index(data_classification)
        required_level = classification_hierarchy.index(required_classification)
        
        compliant = data_level >= required_level
        
        result = {
            'regulation': regulation,
            'data_classification': data_classification,
            'required_classification': required_classification,
            'compliant': compliant
        }
        
        return result
    
    def generate_compliance_report(self) -> Dict:
        """Generate compliance report."""
        report = {
            'generated_at': datetime.now().isoformat(),
            'total_checks': len(self.audit_trail),
            'compliance_status': {},
            'audit_trail': self.audit_trail[-100:]  # Last 100 entries
        }
        
        # Calculate compliance by regulation
        for entry in self.audit_trail:
            regulation = entry.get('result', {}).get('regulation', 'UNKNOWN')
            if regulation not in report['compliance_status']:
                report['compliance_status'][regulation] = {'total': 0, 'compliant': 0}
            
            report['compliance_status'][regulation]['total'] += 1
            if entry.get('result', {}).get('compliant', False):
                report['compliance_status'][regulation]['compliant'] += 1
        
        # Calculate compliance percentages
        for regulation, stats in report['compliance_status'].items():
            if stats['total'] > 0:
                stats['compliance_pct'] = (stats['compliant'] / stats['total']) * 100
        
        return report

# Test compliance framework
def test_compliance_framework():
    governance = RegulatoryComplianceGovernance()
    
    # Test data retention
    result1 = governance.check_data_retention('transactions', 365, 'BASEL_III')
    print(f"Retention check: {result1}")
    
    # Test required fields
    data = {'capital_ratio': 0.12, 'rwa': 1000000}
    result2 = governance.check_required_fields(data, 'BASEL_III')
    print(f"Required fields check: {result2}")
    
    # Test data classification
    result3 = governance.check_data_classification('CONFIDENTIAL', 'PCI_DSS')
    print(f"Classification check: {result3}")
    
    # Generate report
    report = governance.generate_compliance_report()
    print(f"\nCompliance Report: {json.dumps(report, indent=2, default=str)}")
```

#### Key Metrics & Outcomes

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Compliance score | 75% | 99% | 99% compliant |
| Regulatory findings | 30+ per audit | < 3 per audit | 90% reduction |
| Data retention compliance | Manual | Automated | 100% automated |
| Audit preparation time | 2 weeks | 2 hours | 99% faster |
| Regulatory penalties | $500K/year | $0 | Eliminated |

---

### Scenario 4: Data Catalog for Banking Analytics

> **Business Context:** A bank's analytics team spends 40% of time searching for data and only 60% time analyzing. Need to improve data discovery.

#### Data Catalog Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│              BANKING DATA CATALOG ARCHITECTURE                         │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│   ┌──────────────────────────────────────────────────────────────────┐  │
│   │                    DATA SOURCES                                  │  │
│   │                                                                  │  │
│   │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐       │  │
│   │  │ Data     │  │ Data     │  │ Reports  │  │ ML       │       │  │
│   │  │ Warehouse│  │ Lake     │  │          │  │ Models   │       │  │
│   │  └──────────┘  └──────────┘  └──────────┘  └──────────┘       │  │
│   │                                                                  │  │
│   └──────────────────────────────────────────────────────────────────┘  │
│                              │                                          │
│                              ▼                                          │
│   ┌──────────────────────────────────────────────────────────────────┐  │
│   │                    OPENMETADATA CATALOG                          │  │
│   │                                                                  │  │
│   │  ┌────────────────────────────────────────────────────────────┐ │  │
│   │  │  METADATA STORE                                             │ │  │
│   │  │                                                            │ │  │
│   │  │  - Technical metadata (schema, types, lineage)            │ │  │
│   │  │  - Business metadata (descriptions, owners, tags)         │ │  │
│   │  │  - Operational metadata (freshness, quality, usage)       │ │  │
│   │  └────────────────────────────────────────────────────────────┘ │  │
│   │                              │                                    │  │
│   │                              ▼                                    │  │
│   │  ┌────────────────────────────────────────────────────────────┐ │  │
│   │  │  DISCOVERY & SEARCH                                         │ │  │
│   │  │                                                            │ │  │
│   │  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐ │ │  │
│   │  │  │ Full-text│  │ Tag-based│  │ Column   │  │ AI       │ │ │  │
│   │  │  │ Search   │  │ Search   │  │ Search   │  │ Recommend│ │ │  │
│   │  │  └──────────┘  └──────────┘  └──────────┘  └──────────┘ │ │  │
│   │  │                                                            │ │  │
│   │  └────────────────────────────────────────────────────────────┘ │  │
│   │                              │                                    │  │
│   │                              ▼                                    │  │
│   │  ┌────────────────────────────────────────────────────────────┐ │  │
│   │  │  GOVERNANCE & COLLABORATION                                 │ │  │
│   │  │                                                            │ │  │
│   │  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐ │ │  │
│   │  │  │ Access   │  │ Lineage  │  │ Quality  │  │ Social   │ │ │  │
│   │  │  │ Requests │  │ Tracking │  │ Scores   │  │ Features │ │ │  │
│   │  │  └──────────┘  └──────────┘  └──────────┘  └──────────┘ │ │  │
│   │  │                                                            │ │  │
│   │  └────────────────────────────────────────────────────────────┘ │  │
│   │                                                                  │  │
│   └──────────────────────────────────────────────────────────────────┘  │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

#### Implementation Code

```python
from datetime import datetime
from typing import Dict, List
import json

class BankingDataCatalog:
    """Data catalog implementation for banking analytics."""
    
    def __init__(self):
        self.catalog = {
            'tables': {},
            'columns': {},
            'lineage': {},
            'quality_scores': {},
            'ownership': {},
            'tags': {}
        }
        self.search_index = []
    
    def register_table(self, table_info: Dict):
        """Register a table in the catalog."""
        table_name = table_info['name']
        self.catalog['tables'][table_name] = {
            'name': table_name,
            'database': table_info.get('database'),
            'schema': table_info.get('schema'),
            'description': table_info.get('description'),
            'owner': table_info.get('owner'),
            'tags': table_info.get('tags', []),
            'classification': table_info.get('classification', 'INTERNAL'),
            'created_at': datetime.now().isoformat(),
            'last_updated': datetime.now().isoformat()
        }
        
        # Add to search index
        self.search_index.append({
            'type': 'table',
            'name': table_name,
            'description': table_info.get('description', ''),
            'tags': table_info.get('tags', [])
        })
        
        return table_name
    
    def register_column(self, column_info: Dict):
        """Register a column in the catalog."""
        column_key = f"{column_info['table']}.{column_info['name']}"
        self.catalog['columns'][column_key] = {
            'table': column_info['table'],
            'name': column_info['name'],
            'data_type': column_info.get('data_type'),
            'description': column_info.get('description'),
            'is_nullable': column_info.get('is_nullable', True),
            'tags': column_info.get('tags', []),
            'classification': column_info.get('classification', 'INTERNAL')
        }
        
        # Add to search index
        self.search_index.append({
            'type': 'column',
            'name': column_key,
            'description': column_info.get('description', ''),
            'tags': column_info.get('tags', [])
        })
        
        return column_key
    
    def add_lineage(self, source: str, target: str, transformation: str):
        """Add lineage relationship."""
        lineage_key = f"{source} -> {target}"
        self.catalog['lineage'][lineage_key] = {
            'source': source,
            'target': target,
            'transformation': transformation,
            'created_at': datetime.now().isoformat()
        }
    
    def search(self, query: str) -> List[Dict]:
        """Search the catalog."""
        results = []
        query_lower = query.lower()
        
        for item in self.search_index:
            if (query_lower in item['name'].lower() or 
                query_lower in item['description'].lower() or
                any(query_lower in tag.lower() for tag in item['tags'])):
                results.append(item)
        
        return results
    
    def get_table_details(self, table_name: str) -> Dict:
        """Get complete table details."""
        table_info = self.catalog['tables'].get(table_name, {})
        columns = {k: v for k, v in self.catalog['columns'].items() 
                   if v['table'] == table_name}
        
        return {
            'table': table_info,
            'columns': columns,
            'lineage': {k: v for k, v in self.catalog['lineage'].items() 
                       if table_name in k}
        }
    
    def generate_catalog_report(self) -> Dict:
        """Generate catalog statistics."""
        return {
            'total_tables': len(self.catalog['tables']),
            'total_columns': len(self.catalog['columns']),
            'total_lineage': len(self.catalog['lineage']),
            'tables_by_classification': self._count_by_classification('tables'),
            'columns_by_classification': self._count_by_classification('columns')
        }
    
    def _count_by_classification(self, entity_type: str) -> Dict:
        """Count entities by classification."""
        counts = {}
        for entity in self.catalog[entity_type].values():
            classification = entity.get('classification', 'UNKNOWN')
            counts[classification] = counts.get(classification, 0) + 1
        return counts

# Test data catalog
def test_data_catalog():
    catalog = BankingDataCatalog()
    
    # Register tables
    catalog.register_table({
        'name': 'customers',
        'database': 'banking',
        'schema': 'public',
        'description': 'Customer master data',
        'owner': 'data-governance@bank.com',
        'tags': ['PII', 'customer', 'master-data'],
        'classification': 'CONFIDENTIAL'
    })
    
    catalog.register_table({
        'name': 'transactions',
        'database': 'banking',
        'schema': 'public',
        'description': 'Daily transaction records',
        'owner': 'data-engineering@bank.com',
        'tags': ['financial', 'transaction', 'daily'],
        'classification': 'CONFIDENTIAL'
    })
    
    # Register columns
    catalog.register_column({
        'table': 'customers',
        'name': 'ssn',
        'data_type': 'VARCHAR',
        'description': 'Social Security Number',
        'tags': ['PII', 'sensitive'],
        'classification': 'RESTRICTED'
    })
    
    # Add lineage
    catalog.add_lineage('raw.orders', 'stg_orders', 'dbt transformation')
    catalog.add_lineage('stg_orders', 'fact_orders', 'dbt transformation')
    
    # Search
    results = catalog.search('customer')
    print(f"Search results: {results}")
    
    # Get table details
    details = catalog.get_table_details('customers')
    print(f"\nTable details: {json.dumps(details, indent=2)}")
    
    # Generate report
    report = catalog.generate_catalog_report()
    print(f"\nCatalog report: {json.dumps(report, indent=2)}")
```

#### Key Metrics & Outcomes

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Data discovery time | 40% of time | 10% of time | 75% reduction |
| Data documentation | 30% | 95% | 3x improvement |
| Search success rate | 40% | 95% | 2.4x improvement |
| Data request fulfillment | 5 days | 1 day | 80% faster |
| Analytics team productivity | 60% analysis | 90% analysis | 50% improvement |

---

### Scenario 5: Data Lineage for Regulatory Reporting

> **Business Context:** A bank must prove data lineage for all regulatory reports to auditors and regulators.

#### Lineage Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│              REGULATORY REPORTING LINEAGE                              │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│   ┌──────────────────────────────────────────────────────────────────┐  │
│   │                    SOURCE SYSTEMS                                │  │
│   │                                                                  │  │
│   │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐       │  │
│   │  │ Core     │  │ Cards    │  │ Loans    │  │ Treasury │       │  │
│   │  │ Banking  │  │ System   │  │ System   │  │ System   │       │  │
│   │  └──────────┘  └──────────┘  └──────────┘  └──────────┘       │  │
│   │                                                                  │  │
│   └──────────────────────────────────────────────────────────────────┘  │
│                              │                                          │
│                              ▼                                          │
│   ┌──────────────────────────────────────────────────────────────────┐  │
│   │                    DATA LAKEHOUSE (Medallion)                    │  │
│   │                                                                  │  │
│   │  ┌────────────────────────────────────────────────────────────┐ │  │
│   │  │  BRONZE LAYER (Raw)                                         │ │  │
│   │  │  - Raw data from source systems                            │ │  │
│   │  │  - Schema-on-read                                          │ │  │
│   │  │  - Append-only                                             │ │  │
│   │  └────────────────────────────────────────────────────────────┘ │  │
│   │                              │                                    │  │
│   │                              ▼                                    │  │
│   │  ┌────────────────────────────────────────────────────────────┐ │  │
│   │  │  SILVER LAYER (Cleansed)                                    │ │  │
│   │  │  - Deduplicated, validated                                  │ │  │
│   │  │  - Schema enforced                                         │ │  │
│   │  │  - CDC applied                                             │ │  │
│   │  └────────────────────────────────────────────────────────────┘ │  │
│   │                              │                                    │  │
│   │                              ▼                                    │  │
│   │  ┌────────────────────────────────────────────────────────────┐ │  │
│   │  │  GOLD LAYER (Business-Ready)                                │ │  │
│   │  │  - Star schemas, aggregations                              │ │  │
│   │  │  - Conformed dimensions                                    │ │  │
│   │  │  - Materialized views                                     │ │  │
│   │  └────────────────────────────────────────────────────────────┘ │  │
│   │                                                                  │  │
│   └──────────────────────────────────────────────────────────────────┘  │
│                              │                                          │
│                              ▼                                          │
│   ┌──────────────────────────────────────────────────────────────────┐  │
│   │                    REGULATORY REPORTS                            │  │
│   │                                                                  │  │
│   │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐       │  │
│   │  │ Basel III│  │ AML      │  │ Financial│  │ Call     │       │  │
│   │  │ Report   │  │ Report   │  │ Statements│  │ Report   │       │  │
│   │  └──────────┘  └──────────┘  └──────────┘  └──────────┘       │  │
│   │                                                                  │  │
│   └──────────────────────────────────────────────────────────────────┘  │
│                              │                                          │
│                              ▼                                          │
│   ┌──────────────────────────────────────────────────────────────────┐  │
│   │                    LINEAGE TRACKING (OpenMetadata)               │  │
│   │                                                                  │  │
│   │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐       │  │
│   │  │ Column   │  │ Impact   │  │ Audit    │  │ Compliance│      │  │
│   │  │ Lineage  │  │ Analysis │  │ Trail    │  │ Reports  │       │  │
│   │  └──────────┘  └──────────┘  └──────────┘  └──────────┘       │  │
│   │                                                                  │  │
│   └──────────────────────────────────────────────────────────────────┘  │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

#### Implementation Code

```python
from datetime import datetime
from typing import Dict, List
import json

class RegulatoryReportingLineage:
    """Data lineage tracking for regulatory reports."""
    
    def __init__(self):
        self.lineage_graph = {}
        self.audit_trail = []
    
    def add_transformation(self, source: str, target: str, transformation_type: str, details: Dict):
        """Add a transformation to the lineage graph."""
        transformation_id = f"{source}_{target}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        
        self.lineage_graph[transformation_id] = {
            'id': transformation_id,
            'source': source,
            'target': target,
            'transformation_type': transformation_type,
            'details': details,
            'created_at': datetime.now().isoformat()
        }
        
        # Log to audit trail
        self.audit_trail.append({
            'action': 'ADD_TRANSFORMATION',
            'transformation_id': transformation_id,
            'timestamp': datetime.now().isoformat()
        })
        
        return transformation_id
    
    def get_upstream_lineage(self, target: str, max_depth: int = 10) -> List[Dict]:
        """Get upstream lineage for a target table/column."""
        upstream = []
        visited = set()
        
        def _traverse(current_target, depth):
            if depth > max_depth or current_target in visited:
                return
            visited.add(current_target)
            
            for transformation_id, transformation in self.lineage_graph.items():
                if transformation['target'] == current_target:
                    upstream.append(transformation)
                    _traverse(transformation['source'], depth + 1)
        
        _traverse(target, 0)
        return upstream
    
    def get_downstream_lineage(self, source: str, max_depth: int = 10) -> List[Dict]:
        """Get downstream lineage for a source table/column."""
        downstream = []
        visited = set()
        
        def _traverse(current_source, depth):
            if depth > max_depth or current_source in visited:
                return
            visited.add(current_source)
            
            for transformation_id, transformation in self.lineage_graph.items():
                if transformation['source'] == current_source:
                    downstream.append(transformation)
                    _traverse(transformation['target'], depth + 1)
        
        _traverse(source, 0)
        return downstream
    
    def impact_analysis(self, source_table: str) -> Dict:
        """Analyze impact of changes to a source table."""
        downstream = self.get_downstream_lineage(source_table)
        
        impacted_tables = set()
        impacted_reports = set()
        
        for transformation in downstream:
            impacted_tables.add(transformation['target'])
            # Check if target is a regulatory report
            if 'report' in transformation['target'].lower():
                impacted_reports.add(transformation['target'])
        
        return {
            'source_table': source_table,
            'impacted_tables': list(impacted_tables),
            'impacted_reports': list(impacted_reports),
            'impact_level': 'HIGH' if impacted_reports else 'MEDIUM' if impacted_tables else 'LOW'
        }
    
    def generate_lineage_report(self, report_name: str) -> Dict:
        """Generate lineage report for a regulatory report."""
        upstream = self.get_upstream_lineage(report_name)
        
        return {
            'report_name': report_name,
            'generated_at': datetime.now().isoformat(),
            'total_transformations': len(upstream),
            'lineage_depth': self._calculate_depth(upstream),
            'source_systems': self._extract_source_systems(upstream),
            'data_quality_checks': self._extract_quality_checks(upstream),
            'audit_trail': self.audit_trail[-50:]  # Last 50 entries
        }
    
    def _calculate_depth(self, lineage: List[Dict]) -> int:
        """Calculate maximum lineage depth."""
        if not lineage:
            return 0
        return max(self._calculate_depth(self.get_upstream_lineage(t['source'])) for t in lineage) + 1
    
    def _extract_source_systems(self, lineage: List[Dict]) -> List[str]:
        """Extract source systems from lineage."""
        systems = set()
        for transformation in lineage:
            if 'source' in transformation.get('details', {}):
                systems.add(transformation['details']['source'])
        return list(systems)
    
    def _extract_quality_checks(self, lineage: List[Dict]) -> List[Dict]:
        """Extract quality checks from lineage."""
        checks = []
        for transformation in lineage:
            if transformation.get('transformation_type') == 'quality_check':
                checks.append(transformation.get('details', {}))
        return checks

# Test lineage tracking
def test_lineage_tracking():
    lineage = RegulatoryReportingLineage()
    
    # Add transformations
    lineage.add_transformation(
        'core_banking.accounts',
        'bronze.accounts',
        'ingestion',
        {'method': 'CDC', 'frequency': 'daily', 'source': 'Core Banking'}
    )
    
    lineage.add_transformation(
        'bronze.accounts',
        'silver.accounts',
        'transformation',
        {'method': 'dbt', 'description': 'Deduplicate and validate'}
    )
    
    lineage.add_transformation(
        'silver.accounts',
        'gold.dim_accounts',
        'transformation',
        {'method': 'dbt', 'description': 'Create dimension table'}
    )
    
    lineage.add_transformation(
        'gold.dim_accounts',
        'basel_iii_report',
        'report_generation',
        {'method': 'dbt', 'description': 'Generate Basel III report'}
    )
    
    # Get lineage
    upstream = lineage.get_upstream_lineage('basel_iii_report')
    print(f"\nUpstream lineage for Basel III report:")
    for t in upstream:
        print(f"  {t['source']} -> {t['target']}")
    
    # Impact analysis
    impact = lineage.impact_analysis('core_banking.accounts')
    print(f"\nImpact analysis:")
    print(f"  Impacted tables: {impact['impacted_tables']}")
    print(f"  Impacted reports: {impact['impacted_reports']}")
    print(f"  Impact level: {impact['impact_level']}")
    
    # Generate report
    report = lineage.generate_lineage_report('basel_iii_report')
    print(f"\nLineage report: {json.dumps(report, indent=2, default=str)}")
```

#### Key Metrics & Outcomes

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Lineage coverage | 20% | 95% | 5x improvement |
| Audit preparation | 2 weeks | 2 hours | 99% faster |
| Impact analysis | Manual, error-prone | Automated, accurate | 100% accuracy |
| Regulatory findings | 20+ per audit | 0 | Zero findings |
| Change management | Reactive | Proactive | 90% faster |

---

### Scenario Comparison Matrix

| Aspect | Customer Data | Transaction Quality | Compliance | Data Catalog | Lineage |
|--------|---------------|---------------------|------------|--------------|----------|
| **Primary Focus** | PII protection | Data accuracy | Regulatory compliance | Data discovery | Audit trail |
| **Key Tools** | Masking, RLS | Great Expectations | Custom rules | OpenMetadata | dbt, OpenMetadata |
| **Compliance** | GDPR, SOX | Basel III, SOX | All regulations | Internal | Regulatory |
| **Implementation Time** | 2 weeks | 1 week | 1 month | 2 weeks | 2 weeks |
| **Risk Reduction** | 95% | 90% | 99% | 80% | 95% |
| **Cost Impact** | Medium | Low | High | Medium | Low |

---

## 6. Hands-On Exercises

### Exercise 1: Data Quality Framework (Python)
```python
import pandas as pd
from datetime import datetime, timedelta

# Task: Build a comprehensive data quality framework

class DataQualityChecker:
    def __init__(self):
        self.results = []
    
    def check_completeness(self, df, columns):
        """Check for null values in required columns."""
        for col in columns:
            null_count = df[col].isnull().sum()
            completeness = (len(df) - null_count) / len(df) * 100
            self.results.append({
                'dimension': 'completeness',
                'column': col,
                'passed': null_count == 0,
                'score': completeness,
                'details': f'{null_count} nulls ({100-completeness:.2f}%)'
            })
    
    def check_uniqueness(self, df, columns):
        """Check for duplicate values."""
        dup_count = df.duplicated(subset=columns).sum()
        uniqueness = (len(df) - dup_count) / len(df) * 100
        self.results.append({
            'dimension': 'uniqueness',
            'column': str(columns),
            'passed': dup_count == 0,
            'score': uniqueness,
            'details': f'{dup_count} duplicates'
        })
    
    def check_accuracy(self, df, column, min_val=None, max_val=None):
        """Check if values are within expected range."""
        if min_val is not None:
            below = (df[column] < min_val).sum()
        else:
            below = 0
        if max_val is not None:
            above = (df[column] > max_val).sum()
        else:
            above = 0
        
        invalid = below + above
        accuracy = (len(df) - invalid) / len(df) * 100
        self.results.append({
            'dimension': 'accuracy',
            'column': column,
            'passed': invalid == 0,
            'score': accuracy,
            'details': f'{invalid} out of range ({min_val}-{max_val})'
        })
    
    def check_validity(self, df, column, valid_values):
        """Check if values are in allowed set."""
        invalid = ~df[column].isin(valid_values)
        invalid_count = invalid.sum()
        validity = (len(df) - invalid_count) / len(df) * 100
        self.results.append({
            'dimension': 'validity',
            'column': column,
            'passed': invalid_count == 0,
            'score': validity,
            'details': f'{invalid_count} invalid values'
        })
    
    def check_timeliness(self, df, column, max_age_hours=24):
        """Check if data is fresh enough."""
        max_date = pd.to_datetime(df[column]).max()
        age_hours = (datetime.now() - max_date).total_seconds() / 3600
        self.results.append({
            'dimension': 'timeliness',
            'column': column,
            'passed': age_hours <= max_age_hours,
            'score': max(0, 100 - (age_hours / max_age_hours * 100)),
            'details': f'Data is {age_hours:.1f} hours old (max: {max_age_hours})'
        })
    
    def generate_report(self):
        """Generate quality report."""
        report = pd.DataFrame(self.results)
        print("\n=== Data Quality Report ===")
        print(report.to_string(index=False))
        
        overall_score = report['score'].mean()
        passed = report['passed'].all()
        print(f"\nOverall Score: {overall_score:.2f}%")
        print(f"All Checks Passed: {passed}")
        return report

# Test with sample data
def test_quality_checker():
    # Create sample data with quality issues
    df = pd.DataFrame({
        'order_id': [1, 2, 3, 3, 5],  # Duplicate
        'customer_id': [101, 102, None, 104, 105],  # Null
        'amount': [100, 200, -50, 150, 5000000],  # Out of range
        'status': ['pending', 'completed', 'invalid', 'pending', 'cancelled'],
        'updated_at': [
            datetime.now() - timedelta(hours=1),
            datetime.now() - timedelta(hours=2),
            datetime.now() - timedelta(hours=48),  # Stale
            datetime.now(),
            datetime.now()
        ]
    })
    
    checker = DataQualityChecker()
    checker.check_completeness(df, ['customer_id', 'amount'])
    checker.check_uniqueness(df, ['order_id'])
    checker.check_accuracy(df, 'amount', min_val=0, max_val=1000000)
    checker.check_validity(df, 'status', ['pending', 'completed', 'cancelled'])
    checker.check_timeliness(df, 'updated_at', max_age_hours=24)
    
    checker.generate_report()

test_quality_checker()
```

### Exercise 2: Data Lineage with dbt
```sql
-- Task: Implement data lineage tracking with dbt

-- models/staging/schema.yml
version: 2

models:
  - name: stg_orders
    description: "Staged orders from raw source"
    columns:
      - name: order_id
        tests:
          - unique
          - not_null
      - name: customer_id
        tests:
          - not_null
          - relationships:
              to: ref('stg_customers')
              field: customer_id

sources:
  - name: raw
    database: raw_db
    schema: public
    tables:
      - name: orders
        loaded_at_field: _loaded_at
        freshness:
          warn_after: {count: 6, period: hour}
          error_after: {count: 24, period: hour}

-- models/staging/stg_orders.sql
WITH source AS (
    SELECT * FROM {{ source('raw', 'orders') }}
),
renamed AS (
    SELECT
        id as order_id,
        user_id as customer_id,
        amount,
        status,
        created_at as order_date,
        _loaded_at as loaded_at
    FROM source
)
SELECT * FROM renamed;

-- models/marts/fact_orders.sql
WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),
customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),
final AS (
    SELECT
        o.order_id,
        o.customer_id,
        c.customer_name,
        o.amount,
        o.status,
        o.order_date
    FROM orders o
    LEFT JOIN customers c ON o.customer_id = c.customer_id
)
SELECT * FROM final;
```

### Exercise 3: Access Control Implementation
```python
# Task: Implement role-based access control for data

from enum import Enum
from functools import wraps

class DataClassification(Enum):
    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    RESTRICTED = "restricted"

class Role(Enum):
    VIEWER = "viewer"
    ANALYST = "analyst"
    ENGINEER = "engineer"
    ADMIN = "admin"
    COMPLIANCE = "compliance"

# Access control matrix
ACCESS_MATRIX = {
    DataClassification.PUBLIC: [Role.VIEWER, Role.ANALYST, Role.ENGINEER, Role.ADMIN, Role.COMPLIANCE],
    DataClassification.INTERNAL: [Role.ANALYST, Role.ENGINEER, Role.ADMIN, Role.COMPLIANCE],
    DataClassification.CONFIDENTIAL: [Role.ENGINEER, Role.ADMIN, Role.COMPLIANCE],
    DataClassification.RESTRICTED: [Role.COMPLIANCE],
}

def check_access(required_classification):
    """Decorator to check access permissions."""
    def decorator(func):
        @wraps(func)
        def wrapper(user_role, *args, **kwargs):
            allowed_roles = ACCESS_MATRIX.get(required_classification, [])
            if user_role not in allowed_roles:
                raise PermissionError(
                    f"Access denied: {user_role.value} cannot access {required_classification.value} data"
                )
            return func(user_role, *args, **kwargs)
        return wrapper
    return decorator

@check_access(DataClassification.CONFIDENTIAL)
def get_customer_data(role, customer_id):
    """Get customer data with access control."""
    return {
        'customer_id': customer_id,
        'name': 'John Doe',
        'email': 'john@example.com',
        'ssn': '123-45-6789'
    }

# Test access control
def test_access_control():
    # Admin can access
    try:
        data = get_customer_data(Role.ADMIN, 123)
        print(f"Admin access: {data}")
    except PermissionError as e:
        print(f"Admin error: {e}")
    
    # Viewer cannot access
    try:
        data = get_customer_data(Role.VIEWER, 123)
        print(f"Viewer access: {data}")
    except PermissionError as e:
        print(f"Viewer error: {e}")
    
    # Compliance can access
    try:
        data = get_customer_data(Role.COMPLIANCE, 123)
        print(f"Compliance access: {data}")
    except PermissionError as e:
        print(f"Compliance error: {e}")

test_access_control()
```

### Exercise 4: Data Retention Policy
```sql
-- Task: Implement data retention policies

-- Create retention policy table
CREATE TABLE data_retention_policies (
    policy_id SERIAL PRIMARY KEY,
    table_name VARCHAR(100),
    retention_days INT,
    classification VARCHAR(20),
    archive_after_days INT,
    delete_after_days INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert policies
INSERT INTO data_retention_policies 
    (table_name, retention_days, classification, archive_after_days, delete_after_days)
VALUES
    ('orders', 2555, 'financial', 365, 2555),  -- 7 years
    ('customer_pii', 99999, 'restricted', NULL, NULL),  -- Until consent
    ('audit_logs', 3650, 'confidential', 365, 3650),  -- 10 years
    ('web_logs', 90, 'internal', 30, 90),  -- 90 days
    ('marketing_analytics', 730, 'internal', 180, 730);  -- 2 years

-- Query to find tables due for archival
SELECT 
    table_name,
    retention_days,
    classification,
    CURRENT_DATE - INTERVAL '1 day' * archive_after_days as archive_date
FROM data_retention_policies
WHERE archive_after_days IS NOT NULL
  AND archive_after_days <= retention_days;

-- Query to find data due for deletion
SELECT 
    table_name,
    delete_after_days,
    classification
FROM data_retention_policies
WHERE delete_after_days IS NOT NULL;
```

---

## 7. Interview Questions

### Q1: What is data governance and why is it important?

**Answer:** 
Data governance is the framework of policies, processes, and standards ensuring data is managed as an enterprise asset. It's important because: 

1) **Compliance:** GDPR, HIPAA, SOX require data controls. 

2) **Trust:** Quality data enables better decisions. 

3) **Efficiency:** Reduces time spent finding and validating data. 

4) **Risk management:** Prevents data breaches and quality failures. 

5) **Cost reduction:** Eliminates redundant data and processes. Without governance, organizations face regulatory fines, poor decisions, and wasted resources.

### Q2: How do you implement data quality checks in a data pipeline?

**Answer:** 

Multi-layer approach: 

1) **Pre-load:** Validate source data before ingestion (schema checks, completeness). 

2) **In-transit:** Validate during transformation (business rules, referential integrity). 

3) **Post-load:** Validate target data (uniqueness, freshness). 

4) **Continuous:** Monitor quality metrics (accuracy trends, volume anomalies). Tools: Great Expectations for Python-based checks, dbt tests for SQL transformations, custom SQL queries for specific rules. Automate checks in Airflow DAGs and alert on failures.

### Q3: What is the difference between a data catalog and a data dictionary?

**Answer:** 

A **data dictionary** lists column names, types, and definitions - it's a static reference document. 

A **data catalog** is a dynamic, searchable platform with metadata, lineage, ownership, quality metrics, and usage statistics. Think of a data dictionary as one component within a data catalog. Modern catalogs also include: social features (ratings, reviews), discovery capabilities (search, recommendation), and governance workflows (approval, access requests).

### Q4: How do you track data lineage in a modern data stack?

**Answer:** 

Tools and methods: 

1) **dbt:** Automatically tracks lineage through ref() and source() functions, visualized in DAG. 

2) **OpenMetadata/DataHub:** Auto-ingest lineage from Airflow, Spark, and other tools. 

3) **Column-level lineage:** OpenMetadata supports column-level tracking. 

4) **Manual tracking:** Document transformations in wiki/Confluence. 

5) **Tagging:** Use consistent naming conventions. Best practice: combine automated lineage (dbt, Airflow) with manual documentation for business context.

### Q5: Describe a data governance implementation plan.

**Answer:** 

Phased approach: 

**Phase 1 (Foundation):** Inventory existing data assets, establish data ownership, define classification scheme. 

**Phase 2 (Quality):** Implement automated quality checks, define SLAs, create data quality dashboards. 

**Phase 3 (Catalog):** Deploy data catalog, document critical datasets, establish search/discovery. 

**Phase 4 (Lineage):** Track end-to-end lineage, implement impact analysis. 

**Phase 5 (Automation):** Integrate governance into CI/CD, automate compliance reporting. Start with high-value, high-risk data domains (customer data, financial data) before expanding.

### Q6: What are the key components of a data governance program?

**Answer:** Essential components:
1. **Data Ownership:** Clear accountability for data domains
2. **Data Quality Management:** Rules, monitoring, remediation
3. **Data Catalog:** Searchable metadata and documentation
4. **Data Lineage:** Track data flow from source to destination
5. **Access Control:** Role-based permissions and encryption
6. **Compliance:** Regulatory requirements (GDPR, HIPAA)
7. **Metadata Management:** Business and technical metadata
8. **Master Data Management:** Single source of truth for critical entities

### Q7: How do you measure data governance success?

**Answer:** Key metrics:
- **Data Quality Score:** % of records passing quality checks
- **Catalog Adoption:** % of datasets documented and tagged
- **Lineage Coverage:** % of critical pipelines with lineage
- **Incident Reduction:** Decrease in data quality incidents
- **Time to Discovery:** How quickly users find data
- **Compliance Rate:** % of data meeting regulatory requirements
- **Cost Savings:** Reduction in redundant data and manual work

---

## Summary Checklist

### Governance Framework
- [ ] Understand core components (policies, roles, classification)
- [ ] Know data ownership and stewardship responsibilities
- [ ] Implement data classification scheme

### Data Quality
- [ ] Apply six dimensions of data quality
- [ ] Implement quality rules (accuracy, completeness, uniqueness)
- [ ] Use Great Expectations or dbt for automated testing

### Data Catalog
- [ ] Deploy and configure data catalog
- [ ] Document critical datasets with metadata
- [ ] Enable search and discovery capabilities

### Data Lineage
- [ ] Track end-to-end data flow
- [ ] Implement impact analysis
- [ ] Use dbt or OpenMetadata for automated lineage

### Compliance & Security
- [ ] Implement role-based access control
- [ ] Define data retention policies
- [ ] Ensure regulatory compliance (GDPR, HIPAA)

### Practical Skills
- [ ] Build data quality frameworks
- [ ] Implement access control patterns
- [ ] Create data governance dashboards
- [ ] Measure and report on governance metrics

---

*Next Section: [11 - Data Architecture](../11-Data-Architecture/README.md)*
