# 10 - Data Governance

## Table of Contents
1. [Data Governance Framework](#1-data-governance-framework)
2. [Data Quality Management](#2-data-quality-management)
3. [Data Cataloging](#3-data-cataloging)
4. [Data Lineage](#4-data-lineage)
5. [Real-World Scenarios](#5-real-world-scenarios)
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

### Scenario 1: Healthcare Data Governance

```
Data Sources          Governance Layer        Compliance
+----------+         +---------------+      +-----------+
| EHR      |-------->| Data Catalog  |----->| HIPAA     |
| (FHIR)   |         | - PHI tags    |      | Compliance|
+----------+         | - Lineage     |      | Reports   |
| Lab       |-------->| - Quality     |      |           |
| Results   |         | - Access ctrl |      | Audit     |
+----------+         +---------------+      | Trail     |
| Imaging   |-------->|               |      +-----------+
| (DICOM)   |         | +------------+|
+----------+         | | Monitoring ||
                      | | & Alerts   ||
                      | +------------+|
```

### Scenario 2: Financial Services Governance

```
Regulatory Requirements:
- Basel III: Capital adequacy reporting
- GDPR: Customer data privacy
- SOX: Financial audit trails
- PCI DSS: Payment card data security

Governance Implementation:
1. Data Classification: PII, Financial, Public
2. Access Control: Role-based + attribute-based
3. Encryption: At rest (AES-256) + in transit (TLS)
4. Audit Logging: All data access logged
5. Retention: 7 years for financial data
```

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
