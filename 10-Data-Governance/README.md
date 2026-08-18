# 10 - Data Governance

## Table of Contents
1. [Data Governance Framework](#1-data-governance-framework)
2. [Data Quality Management](#2-data-quality-management)
3. [Data Cataloging](#3-data-cataloging)
4. [Data Lineage](#4-data-lineage)
5. [Interview Questions](#5-interview-questions)

---

## 1. Data Governance Framework

### Core Components

`
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
`

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

`
PUBLIC:           No restrictions (marketing materials)
INTERNAL:         Limited to employees (org charts)
CONFIDENTIAL:     Restricted access (customer data)
RESTRICTED:       Highly regulated (PII, PHI, financial)
`

### Data Policies

`yaml
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
`

---

## 2. Data Quality Management

### Six Dimensions of Data Quality

`
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
`

### Data Quality Rules

`sql
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
`

### Great Expectations

`python
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
`

### dbt Tests

`yaml
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
`

---

## 3. Data Cataloging

### Data Catalog Components

`
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
`

### OpenMetadata Example

`yaml
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
`

---

## 4. Data Lineage

### Lineage Tracking

`
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
`

### Implementing Lineage with dbt

`sql
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
`

### Impact Analysis

`sql
-- What downstream tables are affected if stg_orders changes?
-- dbt provides this automatically via DAG
-- Manual approach:
SELECT 
    referencing_table,
    referencing_column
FROM system.referential_constraints
WHERE referenced_table = 'stg_orders';
`

---

## 5. Data Governance Tools

| Tool | Type | Key Features |
|------|------|--------------|
| **Apache Atlas** | Metadata, Lineage | Hadoop integration, auto-discovery |
| **DataHub** | Metadata, Lineage | LinkedIn open-source, Slack integration |
| **OpenMetadata** | Metadata, Lineage | Open-source, REST API |
| **Great Expectations** | Data Quality | Python-based, flexible rules |
| **dbt** | Quality, Lineage | SQL-based, version control |
| **Collibra** | Enterprise Governance | Commercial, workflow automation |
| **Alation** | Data Catalog | Commercial, ML-powered discovery |

---

## 6. Interview Questions

### Q1: What is data governance and why is it important?

**Answer:** Data governance is the framework of policies, processes, and standards ensuring data is managed as an enterprise asset. It's important because: 1) **Compliance:** GDPR, HIPAA, SOX require data controls. 2) **Trust:** Quality data enables better decisions. 3) **Efficiency:** Reduces time spent finding and validating data. 4) **Risk management:** Prevents data breaches and quality failures. 5) **Cost reduction:** Eliminates redundant data and processes. Without governance, organizations face regulatory fines, poor decisions, and wasted resources.

### Q2: How do you implement data quality checks in a data pipeline?

**Answer:** Multi-layer approach: 1) **Pre-load:** Validate source data before ingestion (schema checks, completeness). 2) **In-transit:** Validate during transformation (business rules, referential integrity). 3) **Post-load:** Validate target data (uniqueness, freshness). 4) **Continuous:** Monitor quality metrics (accuracy trends, volume anomalies). Tools: Great Expectations for Python-based checks, dbt tests for SQL transformations, custom SQL queries for specific rules. Automate checks in Airflow DAGs and alert on failures.

### Q3: What is the difference between a data catalog and a data dictionary?

**Answer:** A **data dictionary** lists column names, types, and definitions - it's a static reference document. A **data catalog** is a dynamic, searchable platform with metadata, lineage, ownership, quality metrics, and usage statistics. Think of a data dictionary as one component within a data catalog. Modern catalogs also include: social features (ratings, reviews), discovery capabilities (search, recommendation), and governance workflows (approval, access requests).

### Q4: How do you track data lineage in a modern data stack?

**Answer:** Tools and methods: 1) **dbt:** Automatically tracks lineage through ref() and source() functions, visualized in DAG. 2) **OpenMetadata/DataHub:** Auto-ingest lineage from Airflow, Spark, and other tools. 3) **Column-level lineage:** OpenMetadata supports column-level tracking. 4) **Manual tracking:** Document transformations in wiki/Confluence. 5) **Tagging:** Use consistent naming conventions. Best practice: combine automated lineage (dbt, Airflow) with manual documentation for business context.

### Q5: Describe a data governance implementation plan.

**Answer:** Phased approach: **Phase 1 (Foundation):** Inventory existing data assets, establish data ownership, define classification scheme. **Phase 2 (Quality):** Implement automated quality checks, define SLAs, create data quality dashboards. **Phase 3 (Catalog):** Deploy data catalog, document critical datasets, establish search/discovery. **Phase 4 (Lineage):** Track end-to-end lineage, implement impact analysis. **Phase 5 (Automation):** Integrate governance into CI/CD, automate compliance reporting. Start with high-value, high-risk data domains (customer data, financial data) before expanding.

---

*Next Section: [11 - Data Architecture](../11-Data-Architecture/README.md)*
