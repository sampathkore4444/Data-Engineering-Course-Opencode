# 14 - Data Lineage Pipeline

## Overview

A complete **Data Lineage Pipeline** that tracks, visualizes, and analyzes how data flows through the entire data warehouse. This enables impact analysis, root cause investigation, and compliance auditing.

---

## Table of Contents

1. [What is Data Lineage?](#1-what-is-data-lineage)
2. [Why It Matters in Banking](#2-why-it-matters-in-banking)
3. [Architecture](#3-architecture)
4. [Lineage Types](#4-lineage-types)
5. [Pipeline Components](#5-pipeline-components)
6. [Tracking Data Flow](#6-tracking-data-flow)
7. [Impact Analysis](#7-impact-analysis)
8. [Visualization](#8-visualization)
9. [Running the Pipeline](#9-running-the-pipeline)

---

## 1. What is Data Lineage?

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        WHAT IS DATA LINEAGE?                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Data Lineage tracks the COMPLETE JOURNEY of data:                         │
│                                                                             │
│  WHERE it comes from → HOW it's transformed → WHERE it goes                │
│                                                                             │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐            │
│  │  Source   │───►│ Staging  │───►│   Gold   │───►│ Reports  │            │
│  │  (OLTP)  │    │          │    │   (DW)   │    │  (BI)    │            │
│  └──────────┘    └──────────┘    └──────────┘    └──────────┘            │
│       │               │               │               │                    │
│       └───────────────┴───────────────┴───────────────┘                    │
│                        LINEAGE TRACKING                                    │
│                                                                             │
│  Example:                                                                   │
│  • customer.email (source) → stg_customers.email (staging) →              │
│    dim_customer.email (gold) → Customer Report (BI)                        │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Why It Matters in Banking

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    DATA LINEAGE IN BANKING                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  🔍 IMPACT ANALYSIS:                                                       │
│  • "If we change customer.email, what reports are affected?"               │
│  • "Which tables use this column?"                                         │
│  • "What's the downstream impact of a schema change?"                      │
│                                                                             │
│  🐛 ROOT CAUSE ANALYSIS:                                                   │
│  • "Why is this report showing wrong data?"                                │
│  • "Where did this data come from?"                                        │
│  • "Which transformation introduced this error?"                           │
│                                                                             │
│  📋 COMPLIANCE & AUDIT:                                                    │
│  • SBV requires data flow documentation                                    │
│  • Track who accessed/modified data                                        │
│  • Prove data provenance for regulatory reports                            │
│                                                                             │
│  🔐 SECURITY:                                                              │
│  • Track PII data flow (GDPR, privacy)                                     │
│  • Identify sensitive data locations                                        │
│  • Monitor data access patterns                                            │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    DATA LINEAGE PIPELINE ARCHITECTURE                        │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  LAYER 1: LINEAGE EXTRACTION                                        │   │
│  │  ─────────────────────────────────────────────────────────────────  │   │
│  │  ┌────────────┐ ┌────────────┐ ┌────────────┐ ┌────────────┐      │   │
│  │  │ SQL Parser │ │ DAG Parser │ │ Manual     │ │ API        │      │   │
│  │  │ (dbt, Airfl│ │ (Airflow)  │ │ Annotations│ │ Metadata   │      │   │
│  │  └────────────┘ └────────────┘ └────────────┘ └────────────┘      │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                              │                                              │
│                              ▼                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  LAYER 2: LINEAGE STORE                                             │   │
│  │  ─────────────────────────────────────────────────────────────────  │   │
│  │  ┌─────────────────────────────────────────────────────────────┐   │   │
│  │  │  Lineage Metadata Tables                                     │   │   │
│  │  │  • lineage_sources      (data sources)                      │   │   │
│  │  │  • lineage_targets      (data targets)                      │   │   │
│  │  │  • lineage_edges        (connections between)               │   │   │
│  │  │  • lineage_column_map   (column-level lineage)              │   │   │
│  │  │  • lineage_transforms   (transformation logic)              │   │   │
│  │  └─────────────────────────────────────────────────────────────┘   │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                              │                                              │
│                              ▼                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  LAYER 3: LINEAGE ANALYSIS                                          │   │
│  │  ─────────────────────────────────────────────────────────────────  │   │
│  │  ┌────────────┐ ┌────────────┐ ┌────────────┐ ┌────────────┐      │   │
│  │  │ Upstream   │ │ Downstream │ │ Impact     │ │ Root Cause │      │   │
│  │  │ Analysis   │ │ Analysis   │ │ Analysis   │ │ Analysis   │      │   │
│  │  └────────────┘ └────────────┘ └────────────┘ └────────────┘      │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                              │                                              │
│                              ▼                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  LAYER 4: LINEAGE VISUALIZATION                                     │   │
│  │  ─────────────────────────────────────────────────────────────────  │   │
│  │  ┌────────────┐ ┌────────────┐ ┌────────────┐ ┌────────────┐      │   │
│  │  │ Lineage    │ │ Graph      │ │ Impact     │ │ Compliance │      │   │
│  │  │ Graph      │ │ Explorer   │ │ Report     │ │ Report     │      │   │
│  │  └────────────┘ └────────────┘ └────────────┘ └────────────┘      │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 4. Lineage Types

| Type | Description | Example |
|------|-------------|---------|
| **Table-Level** | Which tables feed into which | stg_customers → dim_customer |
| **Column-Level** | Which columns map to which | stg_customers.email → dim_customer.email |
| **Transformation** | What logic transforms data | TRIM(UPPER(email)) |
| **Process** | Which jobs process data | Airflow DAG → dbt model |
| **Business** | Business meaning mapping | Customer Email → Customer Contact |

---

## 5. Pipeline Components

```
14-data-lineage-pipeline/
│
├── config/                          # Configuration files
│   └── lineage_config.yml           # Lineage tracking settings
│
├── extraction/                      # Lineage extraction scripts
│   ├── extract_sql_lineage.py       # Parse SQL for lineage
│   ├── extract_dbt_lineage.py       # Extract from dbt manifests
│   └── extract_airflow_lineage.py   # Extract from Airflow DAGs
│
├── metadata/                        # Lineage metadata tables
│   ├── lineage_nodes.sql            # Source/target nodes
│   ├── lineage_edges.sql            # Connections between nodes
│   ├── lineage_column_map.sql       # Column-level mapping
│   ├── lineage_transforms.sql       # Transformation logic
│   └── lineage_views.sql            # Visualization views
│
├── analysis/                        # Lineage analysis scripts
│   ├── upstream_analysis.sql        # What feeds this table?
│   ├── downstream_analysis.sql      # What does this table feed?
│   ├── impact_analysis.sql          # What breaks if we change this?
│   └── root_cause_analysis.sql      # Where did this data come from?
│
├── pipelines/                       # ETL pipelines
│   └── airflow/
│       └── dags/
│           └── lineage_pipeline_dag.py  # Airflow DAG
│
├── visualization/                   # Lineage visualization
│   └── lineage_graph.py             # Generate lineage graph
│
├── reports/                         # Lineage reports
│   ├── full_lineage_report.sql      # Complete lineage
│   ├── impact_report.sql            # Impact analysis report
│   └── compliance_report.sql        # Compliance lineage
│
└── README.md                        # This file
```

---

## 6. Tracking Data Flow

### Table-Level Lineage Example

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    TABLE-LEVEL LINEAGE                                        │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  SOURCE SYSTEMS           STAGING              GOLD                         │
│  ───────────────          ───────              ────                         │
│                                                                             │
│  core_banking.customers ──► stg_customers ──► dim_customer                  │
│                                         ├──► fact_transactions              │
│                                                                             │
│  core_banking.accounts ───► stg_accounts ───► dim_account                   │
│                                         ├──► fact_transactions              │
│                                         ├──► fact_account_balance           │
│                                                                             │
│  core_banking.transactions ► stg_transactions ► fact_transactions           │
│                                                                             │
│  cards_system.cards ──────► stg_credit_cards ► dim_card                     │
│                                                                             │
│  loans_system.loans ──────► stg_loans ────────► dim_loan                    │
│                                         ├──► fact_loan_payment              │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Column-Level Lineage Example

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    COLUMN-LEVEL LINEAGE                                       │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  dim_customer.email lineage:                                               │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                                                                     │   │
│  │  core_banking.customers.email                                       │   │
│  │         │                                                          │   │
│  │         ▼ (TRIM, LOWER)                                            │   │
│  │  staging.stg_customers.email                                        │   │
│  │         │                                                          │   │
│  │         ▼ (DIRECT MAPPING)                                         │   │
│  │  gold.dim_customer.email                                            │   │
│  │         │                                                          │   │
│  │         ▼                                                          │   │
│  │  ┌─────────────────────────────────────────────────────────────┐   │   │
│  │  │ REPORTS USING THIS COLUMN:                                   │   │   │
│  │  │ • Customer 360 Report                                        │   │   │
│  │  │ • Marketing Campaign Report                                  │   │   │
│  │  │ • Compliance Report (masked)                                 │   │   │
│  │  └─────────────────────────────────────────────────────────────┘   │   │
│  │                                                                     │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 7. Impact Analysis

### What Happens If We Change This?

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    IMPACT ANALYSIS EXAMPLE                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  SCENARIO: Change customers.email to customers.email_address               │
│                                                                             │
│  IMPACT ANALYSIS:                                                          │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                                                                     │   │
│  │  DIRECT IMPACT (immediate):                                        │   │
│  │  • staging.stg_customers.email → BROKEN                           │   │
│  │  • All dbt models using stg_customers.email → BROKEN              │   │
│  │                                                                     │   │
│  │  DOWNSTREAM IMPACT (cascading):                                    │   │
│  │  • gold.dim_customer.email → BROKEN                               │   │
│  │  • All reports using dim_customer.email → BROKEN                   │   │
│  │  • Customer 360 Dashboard → BROKEN                                │   │
│  │  • Marketing Campaign Report → BROKEN                             │   │
│  │  • Compliance Report → BROKEN                                     │   │
│  │                                                                     │   │
│  │  AFFECTED COMPONENTS:                                              │   │
│  │  • 3 ETL pipelines                                                 │   │
│  │  • 5 dbt models                                                    │   │
│  │  • 4 reports/dashboards                                            │   │
│  │  • 2 compliance reports                                            │   │
│  │                                                                     │   │
│  │  ESTIMATED EFFORT:                                                 │   │
│  │  • 2-4 hours to fix all affected components                       │   │
│  │  • 1 hour testing                                                 │   │
│  │  • Total: 3-5 hours                                               │   │
│  │                                                                     │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 8. Visualization

### Lineage Graph (ASCII)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    LINEAGE GRAPH VISUALIZATION                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│                           ┌─────────────────┐                              │
│                           │   BI REPORTS    │                              │
│                           │  ─────────────  │                              │
│                           │  Customer 360   │                              │
│                           │  Risk Dashboard │                              │
│                           └────────┬────────┘                              │
│                                    │                                        │
│                    ┌───────────────┼───────────────┐                       │
│                    │               │               │                        │
│                    ▼               ▼               ▼                        │
│            ┌──────────┐    ┌──────────┐    ┌──────────┐                   │
│            │   dim_   │    │  fact_   │    │  fact_   │                   │
│            │ customer │    │transactns│    │ balance  │                   │
│            └────┬─────┘    └────┬─────┘    └────┬─────┘                   │
│                 │               │               │                          │
│                 └───────────────┼───────────────┘                          │
│                                 │                                          │
│                    ┌────────────┼────────────┐                             │
│                    │            │            │                              │
│                    ▼            ▼            ▼                              │
│            ┌──────────┐  ┌──────────┐  ┌──────────┐                       │
│            │  stg_    │  │  stg_    │  │  stg_    │                       │
│            │ customers│  │transactns│  │ accounts │                       │
│            └────┬─────┘  └────┬─────┘  └────┬─────┘                       │
│                 │             │              │                              │
│                 └─────────────┼──────────────┘                             │
│                               │                                            │
│                    ┌──────────┼──────────┐                                 │
│                    │          │          │                                  │
│                    ▼          ▼          ▼                                  │
│            ┌──────────┐ ┌──────────┐ ┌──────────┐                         │
│            │ core_    │ │ core_    │ │ core_    │                         │
│            │ banking  │ │ banking  │ │ banking  │                         │
│            │ customers│ │transactns│ │ accounts │                         │
│            └──────────┘ └──────────┘ └──────────┘                         │
│                                                                             │
│            SOURCE SYSTEMS    STAGING    GOLD    REPORTS                    │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 9. Running the Pipeline

### Manual Run

```bash
# Setup
cd Data-Warehouse-Project
docker-compose -f 01-docker-setup/docker-compose.yml up -d

# Create lineage metadata tables
psql -h localhost -U postgres -d banking_dw -f 14-data-lineage-pipeline/metadata/lineage_nodes.sql
psql -h localhost -U postgres -d banking_dw -f 14-data-lineage-pipeline/metadata/lineage_edges.sql
psql -h localhost -U postgres -d banking_dw -f 14-data-lineage-pipeline/metadata/lineage_column_map.sql
psql -h localhost -U postgres -d banking_dw -f 14-data-lineage-pipeline/metadata/lineage_transforms.sql
psql -h localhost -U postgres -d banking_dw -f 14-data-lineage-pipeline/metadata/lineage_views.sql

# Run lineage extraction
python 14-data-lineage-pipeline/extraction/extract_sql_lineage.py
python 14-data-lineage-pipeline/extraction/extract_dbt_lineage.py

# View lineage
psql -h localhost -U postgres -d banking_dw -c "SELECT * FROM lineage.vw_full_lineage;"
```

### Automated via Airflow

```bash
# Trigger the lineage pipeline
airflow dags trigger data_lineage_pipeline

# Check status
airflow dags list-runs -d data_lineage_pipeline
```

---

## Summary

| Component | Purpose |
|-----------|---------|
| **Lineage Extraction** | Capture data flow from SQL, dbt, Airflow |
| **Lineage Store** | Store relationships in metadata tables |
| **Lineage Analysis** | Impact, upstream, downstream analysis |
| **Lineage Visualization** | Graphical representation |
| **Lineage Reports** | Compliance and audit reports |

**Data Lineage = Data Transparency + Impact Analysis + Compliance**

---

*Back to: [Main README](../README.md)*
