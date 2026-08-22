# 🏦 Lakehouse Platform – Real-World Banking Project with Dremio

> **End-to-End Learning: Build a Production-Ready Data Lakehouse for Banking using Dremio**

---

## 📋 Table of Contents

1. [Project Overview](#1-project-overview)
2. [Architecture](#2-architecture)
3. [Directory Structure](#3-directory-structure)
4. [Prerequisites](#4-prerequisites)
5. [Quick Start](#5-quick-start)
6. [Banking Scenarios](#6-banking-scenarios)
7. [Production Deployment](#7-production-deployment)
8. [Monitoring & Governance](#8-monitoring--governance)
9. [Performance Optimization](#9-performance-optimization)
10. [FAQ](#10-faq)

---

## 1. Project Overview

### What You'll Learn

| Skill | Description | Real-World Application |
|-------|-------------|----------------------|
| **Dremio Fundamentals** | Architecture, reflections, datasets | Understanding data lakehouse engine |
| **Medallion Architecture** | Bronze → Silver → Gold layers | Production data organization |
| **Banking Data Models** | Core banking, cards, loans, payments | Financial data structures |
| **CDC Pipeline** | Debezium + Kafka + Dremio | Real-time data ingestion |
| **SQL Analytics** | Complex queries, CTEs, window functions | Business intelligence |
| **Security** | RBAC, column masking, row-level security | Banking compliance |
| **Performance** | Reflections, partitioning, optimization | Production-grade queries |
| **Governance** | Data lineage, catalog, quality | Regulatory compliance |

### Real-World Banking Use Cases Covered

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    BANKING USE CASES IN THIS PROJECT                    │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  1️⃣  CUSTOMER 360° VIEW                                                │
│      Unified view across Core Banking, Cards, Loans, Wealth             │
│      → Relationship managers get complete customer picture              │
│                                                                         │
│  2️⃣  FRAUD DETECTION                                                    │
│      Real-time transaction monitoring across all channels               │
│      → Detect suspicious patterns in milliseconds                       │
│                                                                         │
│  3️⃣  REGULATORY REPORTING (SBV/RBI)                                     │
│      Basel III, AML, Call Reports, Prudential Reporting                 │
│      → Automated compliance reports in minutes                          │
│                                                                         │
│  4️⃣  RISK ANALYTICS                                                     │
│      Credit risk, market risk, operational risk dashboards              │
│      → Real-time risk monitoring for management                         │
│                                                                         │
│  5️⃣  LEADERSHIP DASHBOARDS                                               │
│      CEO/CFO real-time KPIs, branch performance, NPA tracking          │
│      → Strategic decision support                                       │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Why Dremio for Banking?

| Feature | Banking Benefit |
|---------|----------------|
| **Apache Arrow-based** | 10-100x faster than traditional SQL engines |
| **Reflections** | Pre-computed aggregates for instant dashboards |
| **Zero-copy cloning** | Test environments without data duplication |
| **Iceberg/Delta/Parquet** | Open formats, no vendor lock-in |
| **SQL Query Engine** | Business users write SQL directly |
| **Data Catalog** | Discover all banking data assets |
| **Row-Level Security** | Branch-level data access control |
| **Column Masking** | Mask sensitive data (account numbers, SSN) |

---

## 2. Architecture

### Production Banking Lakehouse Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    PRESENTATION LAYER                                    │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐              │
│  │ Dremio   │  │ Power BI │  │ Tableau  │  │ Custom   │              │
│  │ BI Server│  │ Desktop  │  │ Online   │  │ Apps     │              │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘              │
└─────────────────────────────────────┬───────────────────────────────────┘
                                      │
┌─────────────────────────────────────▼───────────────────────────────────┐
│                    DREMIO QUERY ENGINE                                   │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐              │
│  │ SQL      │  │Reflections│ │ Data     │  │ Arrow    │              │
│  │ Parser   │  │ Engine   │  │ Catalog  │  │ Caching  │              │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘              │
└─────────────────────────────────────┬───────────────────────────────────┘
                                      │
┌─────────────────────────────────────▼───────────────────────────────────┐
│                    MEDALLION ARCHITECTURE                                │
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │  BRONZE (Raw) - S3/MinIO                                        │   │
│  │  ├── core_banking/         (Oracle CDC)                         │   │
│  │  ├── card_transactions/    (Mainframe CDC)                      │   │
│  │  └── loan_applications/    (SQL Server CDC)                     │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                      │                                 │
│                                      ▼                                 │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │  SILVER (Cleansed) - Parquet Tables                             │   │
│  │  ├── customers_clean      (Deduplicated, validated)             │   │
│  │  ├── accounts_clean       (Schema enforced, CDC applied)        │   │
│  │  ├── transactions_clean   (Fraud flags, enrichment)             │   │
│  │  └── loans_clean          (NPA classification)                  │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                      │                                 │
│                                      ▼                                 │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │  GOLD (Business-Ready) - Views + Reflections                    │   │
│  │  ├── customer_360_view    ⚡ REFLECTION (instant queries)       │   │
│  │  ├── daily_transaction_agg ⚡ REFLECTION                        │   │
│  │  ├── risk_dashboard       ⚡ REFLECTION                         │   │
│  │  └── regulatory_reports   (Basel III, AML)                      │   │
│  └─────────────────────────────────────────────────────────────────┘    │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
                                      │
┌─────────────────────────────────────▼───────────────────────────────────┐
│                    INGESTION LAYER                                       │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐                             │
│  │ Debezium │  │ Apache   │  │ Airflow  │                             │
│  │ (CDC)    │  │ Kafka    │  │ (Batch)  │                             │
│  └──────────┘  └──────────┘  └──────────┘                             │
└─────────────────────────────────────┬───────────────────────────────────┘
                                      │
┌─────────────────────────────────────▼───────────────────────────────────┐
│                    SOURCE SYSTEMS                                        │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐                             │
│  │ Core     │  │ Cards    │  │ Loans    │                             │
│  │ Banking  │  │ System   │  │ System   │                             │
│  │ (Postgres)│ │(Postgres)│  │(Postgres)│                             │
│  └──────────┘  └──────────┘  └──────────┘                             │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Directory Structure

```
Lakehouse-Platform-Project/
│
├── README.md                           # This file
├── quick-start.sh                      # Quick setup script
│
├── 01-docker-setup/                    # Docker infrastructure
│   ├── docker-compose.yml              # Main orchestration
│   ├── .env                            # Environment variables
│   └── README.md                       # Setup instructions
│
├── 02-source-systems/                  # Banking source databases
│   ├── core-banking/                   # Core banking source
│   │   ├── schema.sql                  # Table definitions
│   │   └── seed-data.sql               # Sample data
│   ├── credit-cards/                   # Credit cards source
│   │   ├── schema.sql
│   │   └── seed-data.sql
│   └── loans/                          # Loans source
│       ├── schema.sql
│       └── seed-data.sql
│
├── 03-medallion-architecture/          # Data lake layers
│   ├── bronze/                         # Raw ingestion
│   │   ├── README.md
│   │   └── ingestion-config.json
│   ├── silver/                         # Cleansed data
│   │   ├── README.md
│   │   └── cleaning-rules.sql
│   └── gold/                           # Business-ready
│       ├── README.md
│       └── business-views.sql
│
├── 04-dremio-setup/                    # Dremio configuration
│   ├── spaces/                         # Dremio spaces
│   │   ├── banking-raw.json
│   │   ├── banking-cleansed.json
│   │   └── banking-gold.json
│   ├── sources/                        # Source connections
│   │   └── minio-s3.json
│   ├── reflections/                    # Materialized views
│   │   ├── customer-360-reflection.json
│   │   ├── daily-transactions-reflection.json
│   │   └── risk-dashboard-reflection.json
│   └── security/                       # Access control
│       ├── roles.json
│       ├── policies.json
│       └── column-masking.sql
│
├── 05-banking-scenarios/               # Real-world use cases
│   ├── 01-customer-360/                # Customer 360° view
│   │   ├── README.md
│   │   ├── customer-360-query.sql
│   │   └── dashboard-spec.md
│   ├── 02-fraud-detection/             # Fraud monitoring
│   │   ├── README.md
│   │   ├── fraud-rules.sql
│   │   └── alert-queries.sql
│   ├── 03-regulatory-reporting/        # SBV compliance
│   │   ├── README.md
│   │   ├── basel-iii-report.sql
│   │   ├── aml-monitoring.sql
│   │   └── call-report.sql
│   ├── 04-risk-analytics/              # Risk dashboards
│   │   ├── README.md
│   │   ├── credit-risk.sql
│   │   └── npa-tracking.sql
│   └── 05-leadership-dashboards/       # Executive KPIs
│       ├── README.md
│       └── ceo-dashboard.sql
│
├── 06-etl-pipelines/                   # Data engineering pipelines
│   ├── airflow/                        # Orchestration
│   │   └── dags/
│   │       ├── bronze_ingestion.py
│   │       ├── silver_transform.py
│   │       ├── gold_aggregation.py
│   │       └── regulatory_reports.py
│   ├── dbt/                            # SQL transformations
│   │   ├── dbt_project.yml
│   │   └── models/
│   │       ├── sources.yml             # Source definitions
│   │       ├── staging/                # Staging models (Bronze → Silver)
│   │       │   ├── schema.yml
│   │       │   ├── stg_customers.sql
│   │       │   ├── stg_accounts.sql
│   │       │   ├── stg_transactions.sql
│   │       │   ├── stg_cards.sql
│   │       │   └── stg_loans.sql
│   │       ├── intermediate/           # Intermediate models (Silver)
│   │       │   ├── int_customer_accounts.sql
│   │       │   ├── int_customer_cards.sql
│   │       │   └── int_customer_loans.sql
│   │       └── marts/                  # Mart models (Silver → Gold)
│   │           ├── schema.yml
│   │           ├── mart_customer_360.sql
│   │           ├── mart_daily_transactions.sql
│   │           └── mart_credit_risk.sql
│   └── spark-jobs/                     # Spark processing
│       ├── bronze-to-silver.py
│       └── silver-to-gold.py
│
├── 07-cdc-setup/                       # Change Data Capture
│   ├── debezium/                       # Debezium connectors
│   │   ├── core-banking-connector.json
│   │   ├── cards-connector.json
│   │   └── loans-connector.json
│   └── kafka/                          # Kafka config
│       └── topics.json
│
├── 08-benchmarks/                      # Performance benchmarks
│   ├── README.md
│   ├── BANKING-SCENARIOS.md
│   ├── arrow-performance-benchmark.py
│   ├── banking-scenarios-benchmark.py
│   ├── run-benchmark.sh
│   └── dashboard/
│       ├── README.md
│       ├── benchmark-dashboard.html
│       └── generate-dashboard.py
│
├── 08-monitoring/                      # Production monitoring
│   ├── README.md
│   ├── prometheus/
│   │   └── prometheus.yml
│   ├── grafana/
│   │   └── datasources/
│   │       └── prometheus.yml
│   └── alerting/
│       └── alert-rules.yml
│
├── 09-security/                        # Security & governance
│   ├── encryption/
│   │   └── tls-config.md
│   ├── access-control/
│   │   ├── role-hierarchy.sql
│   │   └── row-level-security.sql
│   ├── audit/
│   │   └── audit-config.sql
│   └── governance/
│       └── data-governance-framework.md
│
├── 10-performance/                     # Optimization guides
│   ├── reflections-guide.md
│   ├── query-optimization.md
│   ├── partitioning-guide.md
│   ├── capacity-planning.md
│   └── arrow-vs-row-storage.md
│
├── 11-scripts/                         # Utility scripts
│   ├── setup.sh                        # One-click setup
│   ├── teardown.sh                     # Cleanup
│   ├── backup.sh                       # Data backup
│   └── seed-all-data.sh               # Load sample data
│
└── 12-docs/                            # Documentation
    ├── architecture-decision-records/
    │   ├── ADR-001-dremio-selection.md
    │   └── ADR-002-medallion-arch.md
    ├── tutorials/
    │   └── arrow-reflections-tutorial.md
    ├── runbook.md
    ├── troubleshooting.md
    └── glossary.md
```

---

## 4. Prerequisites

### System Requirements

| Component | Minimum | Recommended |
|-----------|---------|-------------|
| **CPU** | 4 cores | 8+ cores |
| **RAM** | 16 GB | 32+ GB |
| **Disk** | 100 GB SSD | 500 GB SSD |
| **Docker** | 20.10+ | Latest |
| **Docker Compose** | 2.0+ | Latest |

### Software Requirements

| Software | Version | Purpose |
|----------|---------|---------|
| Docker | 20.10+ | Container runtime |
| Docker Compose | 2.0+ | Orchestration |
| Git | 2.30+ | Version control |
| curl | Any | API calls |
| jq | 1.6+ | JSON processing |

---

## 5. Quick Start

### Step 1: Clone and Setup

```bash
# Clone the repository
git clone <repository-url>
cd Lakehouse-Platform-Project

# Make scripts executable
chmod +x 11-scripts/*.sh

# Start everything
./11-scripts/setup.sh
```

### Step 2: Access Services

| Service | URL | Credentials |
|---------|-----|-------------|
| **Dremio UI** | http://localhost:9047 | admin / Admin@123 |
| **MinIO Console** | http://localhost:9001 | minioadmin / Minio@123 |
| **Airflow** | http://localhost:8080 | admin / Admin@123 |
| **Grafana** | http://localhost:3000 | admin / admin |
| **Prometheus** | http://localhost:9090 | - |

### Step 3: Load Sample Data

```bash
# Load all banking sample data
./11-scripts/seed-all-data.sh

# Verify data loaded
curl -s http://localhost:9047/api/v3/catalog | jq '.length'
```

### Step 4: Run First Query

```sql
-- Open Dremio UI and run this query
SELECT 
    c.customer_name,
    c.total_accounts,
    c.total_balance,
    c.total_cards,
    c.total_loans,
    c.net_relationship_value
FROM banking_gold.customer_360 c
WHERE c.net_relationship_value > 0
ORDER BY c.net_relationship_value DESC
LIMIT 10;
```

---

## 6. Banking Scenarios

### Scenario 1: Customer 360° View

```sql
-- Complete customer view across all banking products
SELECT 
    c.customer_id,
    c.customer_name,
    c.city,
    
    -- Deposits
    COUNT(DISTINCT a.account_id) AS total_accounts,
    COALESCE(SUM(a.current_balance), 0) AS total_balance,
    
    -- Cards
    COUNT(DISTINCT cc.card_number) AS total_cards,
    COALESCE(SUM(cc.credit_used), 0) AS total_card_outstanding,
    
    -- Loans
    COUNT(DISTINCT l.loan_id) AS total_loans,
    COALESCE(SUM(l.principal_outstanding), 0) AS total_loan_outstanding,
    
    -- Total Relationship Value
    (COALESCE(SUM(a.current_balance), 0) + 
     COALESCE(SUM(cc.credit_used), 0) + 
     COALESCE(SUM(l.principal_outstanding), 0)) AS total_relationship_value
    
FROM banking_cleansed.core_banking_customers c
LEFT JOIN banking_cleansed.core_banking_accounts a ON c.customer_id = a.customer_id
LEFT JOIN banking_cleansed.credit_cards cc ON c.customer_id = cc.customer_id
LEFT JOIN banking_cleansed.loan_accounts l ON c.customer_id = l.customer_id
GROUP BY c.customer_id, c.customer_name, c.city
ORDER BY total_relationship_value DESC;
```

### Scenario 2: Fraud Detection

```sql
-- Detect suspicious transaction patterns
WITH txn_velocity AS (
    SELECT 
        card_number,
        COUNT(*) AS txn_count_1hr,
        SUM(amount) AS total_amount_1hr
    FROM banking_cleansed.credit_card_transactions
    WHERE txn_timestamp >= DATEADD(HOUR, -1, CURRENT_TIMESTAMP())
    GROUP BY card_number
    HAVING COUNT(*) > 5
)
SELECT 
    tv.card_number,
    tv.txn_count_1hr,
    tv.total_amount_1hr,
    cc.customer_id,
    'HIGH_VELOCITY' AS alert_type,
    'HIGH' AS severity
FROM txn_velocity tv
JOIN banking_cleansed.credit_cards cc ON tv.card_number = cc.card_number;
```

### Scenario 3: Regulatory Reporting (Basel III)

```sql
-- Basel III Capital Adequacy Report
WITH risk_weighted_assets AS (
    SELECT 
        loan_type,
        SUM(principal_outstanding) AS exposure,
        SUM(principal_outstanding) * 
            CASE 
                WHEN loan_type = 'HOME_LOAN' THEN 0.35
                WHEN loan_type = 'PERSONAL_LOAN' THEN 0.75
                WHEN loan_type = 'CAR_LOAN' THEN 0.65
                WHEN loan_type = 'BUSINESS_LOAN' THEN 1.00
                ELSE 1.00
            END AS risk_weighted_exposure
    FROM banking_cleansed.loan_accounts
    WHERE loan_status = 'ACTIVE'
    GROUP BY loan_type
)
SELECT 
    SUM(exposure) AS total_exposure,
    SUM(risk_weighted_exposure) AS total_rwa,
    5000000000000 AS tier1_capital,
    ROUND(5000000000000 / SUM(risk_weighted_exposure) * 100, 2) AS car_ratio,
    CASE 
        WHEN 5000000000000 / SUM(risk_weighted_exposure) * 100 >= 8 
        THEN 'COMPLIANT'
        ELSE 'NON-COMPLIANT'
    END AS compliance_status
FROM risk_weighted_assets;
```

---

## 7. Production Deployment

### Production Checklist

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    PRODUCTION DEPLOYMENT CHECKLIST                      │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  INFRASTRUCTURE                                                         │
│  ☐ 3+ Dremio nodes (master + 2 executors)                              │
│  ☐ Load balancer for Dremio UI                                          │
│  ☐ Persistent storage (S3/MinIO with versioning)                       │
│  ☐ Backup strategy (daily snapshots)                                    │
│                                                                         │
│  SECURITY                                                               │
│  ☐ TLS/SSL enabled for all connections                                  │
│  ☐ LDAP/AD integration for authentication                               │
│  ☐ RBAC roles configured (Admin, Analyst, Engineer, Compliance)        │
│  ☐ Column masking for PII (account numbers, SSN, phone)               │
│  ☐ Row-level security for branch-level data                            │
│  ☐ Audit logging enabled                                               │
│                                                                         │
│  PERFORMANCE                                                            │
│  ☐ Reflections configured for all Gold layer tables                     │
│  ☐ Partitioning on date columns                                         │
│  ☐ Query result caching enabled                                         │
│  ☐ Memory allocation optimized                                          │
│                                                                         │
│  MONITORING                                                             │
│  ☐ Prometheus metrics exported                                          │
│  ☐ Grafana dashboards configured                                        │
│  ☐ Alert rules for query failures, slow queries, disk space            │
│  ☐ Data quality checks in pipeline                                      │
│                                                                         │
│  GOVERNANCE                                                             │
│  ☐ Data catalog populated with descriptions                             │
│  ☐ Lineage tracking enabled                                             │
│  ☐ Data classification tags (PII, CONFIDENTIAL, PUBLIC)                │
│  ☐ Retention policies configured                                        │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 8. Monitoring & Governance

### Key Metrics to Monitor

| Metric | Threshold | Alert |
|--------|-----------|-------|
| Query execution time | > 30 seconds | Warning |
| Query failure rate | > 5% | Critical |
| Reflection refresh time | > 15 minutes | Warning |
| Disk usage | > 80% | Critical |
| Memory usage | > 85% | Warning |
| Active queries | > 50 | Warning |
| Data freshness | > 1 hour | Warning |

### Data Quality Rules

```sql
-- Check for null values in critical columns
SELECT 
    'NULL_CHECK' AS check_type,
    COUNT(*) AS total_rows,
    SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS null_customer_id,
    CASE 
        WHEN SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) > 0 
        THEN 'FAILED' ELSE 'PASSED'
    END AS status
FROM banking_cleansed.core_banking_customers;
```

---

## 9. Performance Optimization

### Reflection Strategy

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    REFLECTION OPTIMIZATION STRATEGY                      │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  RAW REFLECTIONS (for filtering)                                        │
│  ├── Filter on: transaction_date, customer_id, account_type           │
│  └── Refresh: Every 15 minutes                                         │
│                                                                         │
│  AGGREGATION REFLECTIONS (for dashboards)                               │
│  ├── Daily transaction aggregates                                       │
│  ├── Monthly customer summaries                                         │
│  └── Refresh: Every hour                                                │
│                                                                         │
│  JOIN REFLECTIONS (for complex queries)                                 │
│  ├── Customer + Accounts + Cards                                        │
│  ├── Transactions + Merchants                                           │
│  └── Refresh: Every 30 minutes                                          │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Query Optimization Tips

| Tip | Example | Impact |
|-----|---------|--------|
| Use partition pruning | `WHERE txn_date = '2024-01-15'` | 10x faster |
| Leverage reflections | Query Gold layer, not Silver | 5-100x faster |
| Avoid SELECT * | Only select needed columns | 2-5x faster |
| Use CTEs wisely | Break complex queries into steps | Better readability |
| Filter early | Add WHERE before JOIN | Reduces data scanned |

---

## 10. FAQ

### Q1: Why Dremio over Snowflake/BigQuery?

| Aspect | Dremio | Snowflake | BigQuery |
|--------|--------|-----------|----------|
| **Cost** | Free (OSS) + Enterprise | Pay-per-use | Pay-per-query |
| **Data Location** | Your cloud (S3/MinIO) | Snowflake cloud | Google cloud |
| **Vendor Lock-in** | None (open formats) | Medium | High |
| **Best For** | Data lakehouse | Data warehouse | Serverless analytics |
| **Banking Use** | Real-time on data lake | Historical analytics | Quick BI |

### Q2: How does Dremio handle banking data security?

- **Column Masking**: Automatically mask account numbers, SSN
- **Row-Level Security**: Branch managers see only their branch data
- **Encryption**: TLS in transit, AES-256 at rest
- **Audit Logging**: Track all queries and access
- **LDAP/AD Integration**: Enterprise authentication

### Q3: Can Dremio replace our existing data warehouse?

**Short answer**: Not immediately, but it can complement it.

**Strategy**:
1. Keep existing DW for historical analytics
2. Use Dremio for real-time data lake queries
3. Gradually migrate workloads to Dremio
4. Use Dremio as the query engine for your data lake

---

## 📚 Additional Resources

- [Dremio Official Documentation](https://docs.dremio.com/)
- [Apache Iceberg Documentation](https://iceberg.apache.org/docs/latest/)
- [Apache Arrow Documentation](https://arrow.apache.org/docs/)
- [Banking Data Architecture Patterns](../11-Data-Architecture/README.md)

---

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

---

## 📄 License

This project is for educational purposes. See LICENSE file for details.

---

*Built with ❤️ for Data Engineers learning Dremio and Banking Data Architecture*
