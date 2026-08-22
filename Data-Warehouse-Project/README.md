# Banking Data Warehouse - Complete Learning Project

## Overview

A comprehensive **Data Warehouse project** for the banking domain, built from scratch for learning purposes. This project teaches **dimensional modeling**, **star schema design**, **ETL pipelines**, and **BI reporting** using industry-standard tools.

---

## What You'll Learn

| Topic | Description |
|-------|-------------|
| **Dimensional Modeling** | Star schema, snowflake schema, fact/dimension tables |
| **Slowly Changing Dimensions (SCD)** | Type 1, 2, 3 changes |
| **Fact Tables** | Transaction facts, snapshot facts, accumulating facts |
| **ETL Pipelines** | Airflow orchestration, dbt transformations |
| **Data Quality** | Validation rules, testing, monitoring |
| **BI Reporting** | SQL queries, dashboards, regulatory reports |
| **Performance** | Indexing, partitioning, query optimization |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    BANKING DATA WAREHOUSE ARCHITECTURE                       │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌───────────────────────────────────────────────────────────────────────┐ │
│  │                     SOURCE SYSTEMS (OLTP)                             │ │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐               │ │
│  │  │ Core Banking │  │ Cards System │  │ Loans System │               │ │
│  │  │ (PostgreSQL) │  │ (PostgreSQL) │  │ (PostgreSQL) │               │ │
│  │  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘               │ │
│  └─────────┼─────────────────┼─────────────────┼─────────────────────────┘ │
│            │                 │                 │                            │
│            ▼                 ▼                 ▼                            │
│  ┌───────────────────────────────────────────────────────────────────────┐ │
│  │                        ETL LAYER                                      │ │
│  │  ┌─────────────────────────────────────────────────────────────────┐ │ │
│  │  │  Airflow (Orchestration)  +  dbt (Transformations)             │ │ │
│  │  └─────────────────────────────────────────────────────────────────┘ │ │
│  └───────────────────────────────────┬───────────────────────────────────┘ │
│                                      │                                      │
│                                      ▼                                      │
│  ┌───────────────────────────────────────────────────────────────────────┐ │
│  │                   DATA WAREHOUSE (OLAP)                               │ │
│  │                                                                       │ │
│  │  ┌─────────────────────────────────────────────────────────────────┐ │ │
│  │  │                    STAR SCHEMA                                   │ │ │
│  │  │                                                                 │ │ │
│  │  │         ┌──────────────┐                                        │ │ │
│  │  │         │ dim_customer │                                        │ │ │
│  │  │         └──────┬───────┘                                        │ │ │
│  │  │                │                                                │ │ │
│  │  │  ┌─────────────┼─────────────┐                                 │ │ │
│  │  │  │             │             │                                  │ │ │
│  │  │  ▼             ▼             ▼                                  │ │ │
│  │  │ ┌──────────┐ ┌──────────┐ ┌──────────┐                        │ │ │
│  │  │ │dim_account│ │dim_product│ │dim_branch│                        │ │ │
│  │  │ └─────┬────┘ └─────┬────┘ └─────┬────┘                        │ │ │
│  │  │       │             │             │                              │ │ │
│  │  │       ▼             ▼             ▼                              │ │ │
│  │  │  ┌──────────────────────────────────────┐                      │ │ │
│  │  │  │         fact_transactions             │                      │ │ │
│  │  │  └──────────────────────────────────────┘                      │ │ │
│  │  │                                                                 │ │ │
│  │  └─────────────────────────────────────────────────────────────────┘ │ │
│  └───────────────────────────────────┬───────────────────────────────────┘ │
│                                      │                                      │
│                                      ▼                                      │
│  ┌───────────────────────────────────────────────────────────────────────┐ │
│  │                     PRESENTATION LAYER                                │ │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐               │ │
│  │  │ BI Reports   │  │ Dashboards   │  │ Regulatory   │               │ │
│  │  │ (SQL)        │  │ (Power BI)   │  │ Reports      │               │ │
│  │  └──────────────┘  └──────────────┘  └──────────────┘               │ │
│  └───────────────────────────────────────────────────────────────────────┘ │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Folder Structure

```
Data-Warehouse-Project/
│
├── README.md                              # This file
│
├── 01-docker-setup/                       # Docker infrastructure
│   ├── docker-compose.yml                 # PostgreSQL + pgAdmin
│   ├── .env                               # Environment variables
│   └── init-scripts/                      # Database initialization
│
├── 02-source-systems/                     # OLTP source databases
│   ├── core-banking/                      # Accounts, customers, transactions
│   │   ├── schema.sql
│   │   └── seed-data.sql
│   ├── cards-system/                      # Credit cards, card transactions
│   │   ├── schema.sql
│   │   └── seed-data.sql
│   └── loans-system/                      # Loans, loan payments
│       ├── schema.sql
│       └── seed-data.sql
│
├── 03-star-schema/                        # Dimensional modeling
│   ├── dimensions/                        # Dimension tables
│   │   ├── dim_customer.sql
│   │   ├── dim_account.sql
│   │   ├── dim_product.sql
│   │   ├── dim_branch.sql
│   │   ├── dim_date.sql
│   │   └── dim_time.sql
│   ├── facts/                             # Fact tables
│   │   ├── fact_transactions.sql
│   │   ├── fact_account_balance.sql
│   │   ├── fact_loan_payment.sql
│   │   └── fact_daily_snapshot.sql
│   └── scripts/                           # Schema creation scripts
│       └── create_schema.sql
│
├── 04-etl-pipelines/                      # Data engineering pipelines
│   ├── airflow/dags/                      # Airflow DAGs
│   │   ├── extract_source_data.py
│   │   ├── extract_source_data_with_error_handling.py  # NEW
│   │   ├── load_dimensions.py
│   │   ├── load_facts.py
│   │   ├── cdc_pipeline.py                # NEW: CDC pipeline
│   │   └── README.md                      # NEW: DAGs documentation
│   ├── cdc/                               # NEW: CDC metadata
│   │   └── metadata/cdc_metadata.sql
│   ├── dbt/                               # dbt transformations
│   │   ├── dbt_project.yml
│   │   └── models/
│   │       ├── sources.yml
│   │       ├── staging/
│   │       ├── intermediate/
│   │       └── marts/
│   └── README.md                          # NEW: ETL pipelines guide
│
├── 05-banking-scenarios/                  # Real-world use cases
│   ├── 01-customer-analytics/             # Customer segmentation, 360
│   │   └── customer_360.sql
│   ├── 02-financial-reporting/            # P&L, balance sheet
│   │   └── pnl_report.sql
│   ├── 03-regulatory-reports/             # SBV compliance, Basel III
│   ├── 04-executive-dashboards/           # CEO dashboard, KPIs
│   └── README.md                          # NEW: Banking scenarios guide
│
├── 06-data-quality/                       # Data quality framework
│   ├── tests/                             # Validation tests
│   │   └── uniqueness_test.sql
│   ├── alerts/                            # Alert rules
│   │   └── freshness_alert.sql
│   └── README.md                          # NEW: Data quality guide
│
├── 06-dbt-models/                         # NEW: dbt transformations
│   ├── dbt_project.yml
│   └── models/
│       ├── staging/
│       │   ├── sources.yml
│       │   ├── schema.yml                 # NEW: dbt tests
│       │       ├── stg_customers.sql
│       │       ├── stg_accounts.sql
│       │       └── stg_transactions.sql
│       ├── intermediate/
│       │   ├── schema.yml                 # NEW: dbt tests
│       │   └── int_customer_accounts.sql
│       └── marts/
│           ├── schema.yml                 # NEW: dbt tests
│           ├── dim_customer.sql
│           └── fact_transactions.sql
│
├── 08-monitoring/                         # Monitoring stack
│   ├── prometheus/                        # Metrics collection
│   │   └── prometheus.yml
│   ├── grafana/                           # Dashboards
│   │   ├── dashboards/overview.json
│   │   └── datasources/prometheus.yml
│   └── README.md
│
├── 09-security/                           # Security hardening
│   ├── access-control/                    # RBAC setup
│   │   ├── role-hierarchy.sql
│   │   └── row-level-security.sql
│   ├── audit/                             # Audit logging
│   │   └── audit_setup.sql
│   ├── encryption/                        # TLS/SSL config
│   │   └── encryption_setup.sql
│   └── README.md
│
├── 10-performance/                        # Optimization guide
│   ├── indexing/                           # Index strategies
│   │   └── indexing_strategies.sql
│   ├── partitioning/                      # Table partitioning
│   │   └── partitioning_strategies.sql
│   ├── tuning/                            # Query optimization
│   │   └── query_optimization.sql
│   └── README.md
│
├── 11-scripts/                            # Utility scripts
│   ├── setup.sh                           # One-click setup
│   ├── teardown.sh                        # Cleanup
│   ├── backup.sh                          # Backup
│   ├── seed-all-data.sh                   # Load sample data
│   └── README.md
│
├── 12-docs/                               # Documentation
│   ├── architecture-decision-records/     # ADRs
│   │   └── ADR-001-postgresql-selection.md
│   ├── tutorials/                         # Step-by-step guides
│   │   └── getting_started.md
│   └── README.md
│
├── 13-data-quality-pipeline/              # NEW: DQ pipeline (Airflow)
│   ├── config/
│   │   ├── quality_rules.yml
│   │   └── alert_thresholds.yml
│   ├── metadata/
│   │   ├── dq_rule_catalog.sql
│   │   ├── dq_test_results.sql
│   │   ├── dq_quality_scores.sql
│   │   ├── dq_anomalies.sql
│   │   └── dq_quality_dashboard.sql
│   ├── checks/
│   │   └── run_all_checks.py
│   ├── pipelines/airflow/dags/
│   │   └── data_quality_dag.py
│   ├── reports/
│   │   └── daily_quality_summary.sql
│   └── README.md
│
├── 14-data-lineage-pipeline/              # NEW: Lineage tracking
│   ├── metadata/
│   │   ├── lineage_nodes.sql
│   │   ├── lineage_edges.sql
│   │   ├── lineage_column_map.sql
│   │   ├── lineage_transforms.sql
│   │   └── lineage_views.sql
│   ├── analysis/
│   │   ├── impact_analysis.sql
│   │   └── root_cause_analysis.sql
│   ├── pipelines/airflow/dags/
│   │   └── lineage_pipeline_dag.py
│   └── README.md
│
├── .github/workflows/                     # NEW: CI/CD pipelines
│   ├── test.yml                           # Testing workflow
│   ├── deploy.yml                         # Deployment workflow
│   └── quality.yml                        # Data quality checks
│
├── terraform/                             # NEW: Infrastructure as Code
│   ├── main.tf                            # Main configuration
│   ├── variables.tf                       # Variables
│   ├── outputs.tf                         # Outputs
│   └── README.md
│
└── tests/                                 # NEW: Unit tests
    ├── test_etl_pipelines.py              # ETL logic tests
    └── requirements.txt
```

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

## Tools Used

| Tool | Purpose | Version |
|------|---------|---------|
| **PostgreSQL** | Data Warehouse (OLAP) | 16 |
| **pgAdmin** | Database management UI | Latest |
| **Airflow** | Workflow orchestration | 2.8 |
| **dbt** | SQL transformations | 1.7 |
| **Grafana** | Monitoring dashboards | 10.2 |
| **Prometheus** | Metrics collection | 2.48 |

---

## Quick Start

```bash
# 1. Start the platform
cd Data-Warehouse-Project
./11-scripts/setup.sh

# 2. Load sample data
./11-scripts/seed-all-data.sh

# 3. Open pgAdmin
open http://localhost:5050

# 4. Run ETL pipelines
# Via Airflow UI: http://localhost:8080

# 5. Query the data warehouse
psql -U dw_admin -d banking_dw -c "SELECT * FROM fact_transactions LIMIT 10;"
```

---

## Default Credentials

| Service | Username | Password | URL |
|---------|----------|----------|-----|
| **PostgreSQL** | dw_admin | Dw@123 | localhost:5432 |
| **pgAdmin** | admin@bank.com | Admin@123 | localhost:5050 |
| **Airflow** | admin | Admin@123 | localhost:8080 |
| **Grafana** | admin | admin | localhost:3000 |

---

## Comparison: Lakehouse vs Data Warehouse

| Aspect | Lakehouse Project | Data Warehouse Project |
|--------|-------------------|------------------------|
| **Storage** | MinIO (S3-compatible) | PostgreSQL |
| **Query Engine** | Dremio | PostgreSQL |
| **Format** | Delta Lake / Iceberg | Native SQL tables |
| **Schema** | Schema-on-read + write | Schema-on-write |
| **BI Tools** | Dremio → Power BI | pgAdmin → Power BI |
| **ML Support** | ✅ Native | ⚠️ Separate |
| **Cost** | Low (open formats) | Medium (managed DB) |
| **Best For** | BI + ML + Streaming | Pure BI analytics |

---

## Project Structure

```
Total Folders:  18+
Total Files:    100+
Languages:      SQL, Python, Shell, YAML, HCL
Tools:          PostgreSQL, Airflow, dbt, Grafana, Terraform
```

---

## New Features Added

| Feature | Folder | Purpose |
|---------|--------|---------|
| **dbt Tests** | `06-dbt-models/models/*/schema.yml` | Data validation tests |
| **Error Handling** | `04-etl-pipelines/airflow/dags/` | Robust failure recovery |
| **CI/CD** | `.github/workflows/` | Automated testing/deployment |
| **Terraform** | `terraform/` | Infrastructure as Code |
| **Unit Tests** | `tests/` | ETL logic validation |
| **CDC Pipeline** | `04-etl-pipelines/cdc/` | Real-time data sync |

---

## Learning Path

| Step | Topic | Folder |
|------|-------|--------|
| 1 | Understand source systems | `02-source-systems/` |
| 2 | Learn star schema design | `03-star-schema/` |
| 3 | Build ETL pipelines | `04-etl-pipelines/` |
| 4 | Run banking scenarios | `05-banking-scenarios/` |
| 5 | Add data quality | `06-data-quality/` |
| 6 | Optimize performance | `10-performance/` |
| 7 | Secure the warehouse | `09-security/` |

---

*Part of: Data Engineering Course*
*See also: [Lakehouse-Platform-Project](../Lakehouse-Platform-Project/README.md)*
