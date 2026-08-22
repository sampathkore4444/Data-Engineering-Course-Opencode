# 13 - Data Quality Pipeline

## Overview

A complete **Data Quality Pipeline** that validates, monitors, and reports on data quality across the entire data warehouse. This ensures data accuracy, completeness, consistency, and timeliness.

---

## Table of Contents

1. [What is Data Quality Pipeline?](#1-what-is-data-quality-pipeline)
2. [Why It Matters in Banking](#2-why-it-matters-in-banking)
3. [Architecture](#3-architecture)
4. [Quality Dimensions](#4-quality-dimensions)
5. [Pipeline Components](#5-pipeline-components)
6. [Tests & Validation](#6-tests--validation)
7. [Monitoring & Alerting](#7-monitoring--alerting)
8. [Running the Pipeline](#8-running-the-pipeline)

---

## 1. What is Data Quality Pipeline?

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    DATA QUALITY PIPELINE                                      │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Data Quality Pipeline is an automated system that:                         │
│                                                                             │
│  1. VALIDATES data against defined rules                                    │
│  2. MONITORS data quality metrics over time                                │
│  3. ALERTS when quality degrades                                           │
│  4. REPORTS quality status to stakeholders                                 │
│  5. TRACKS quality trends for continuous improvement                       │
│                                                                             │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐            │
│  │  Data    │───►│  Quality │───►│  Quality │───►│  Alert   │            │
│  │  Source  │    │  Checks  │    │  Store   │    │  System  │            │
│  └──────────┘    └──────────┘    └──────────┘    └──────────┘            │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Why It Matters in Banking

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    DATA QUALITY IN BANKING                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ❌ WITHOUT Data Quality Pipeline:                                          │
│  • Wrong balance calculations → Customer complaints, financial losses      │
│  • Missing transactions → Audit failures, SBV fines                       │
│  • Duplicate records → Incorrect analytics, bad decisions                  │
│  • Stale data → Outdated reports, compliance risks                        │
│                                                                             │
│  ✅ WITH Data Quality Pipeline:                                             │
│  • Automated validation → Catch errors before they propagate              │
│  • Real-time monitoring → Instant alerts on quality degradation           │
│  • Historical tracking → Trend analysis, root cause identification        │
│  • Compliance ready → SBV audit trail, data governance                    │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    DATA QUALITY PIPELINE ARCHITECTURE                        │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  LAYER 1: DATA INGESTION                                            │   │
│  │  ─────────────────────────────────────────────────────────────────  │   │
│  │  Source Systems → Staging → Gold Tables                            │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                              │                                              │
│                              ▼                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  LAYER 2: QUALITY CHECKS                                            │   │
│  │  ─────────────────────────────────────────────────────────────────  │   │
│  │  ┌────────────┐ ┌────────────┐ ┌────────────┐ ┌────────────┐      │   │
│  │  │ Uniqueness │ │ Not Null   │ │ Range      │ │ Freshness  │      │   │
│  │  │ Tests      │ │ Tests      │ │ Tests      │ │ Tests      │      │   │
│  │  └────────────┘ └────────────┘ └────────────┘ └────────────┘      │   │
│  │  ┌────────────┐ ┌────────────┐ ┌────────────┐ ┌────────────┐      │   │
│  │  │ Referential│ │ Schema     │ │ Statistical│ │ Custom     │      │   │
│  │  │ Integrity  │ │ Validation │ │ Checks     │ │ Business   │      │   │
│  │  └────────────┘ └────────────┘ └────────────┘ └────────────┘      │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                              │                                              │
│                              ▼                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  LAYER 3: QUALITY STORE                                             │   │
│  │  ─────────────────────────────────────────────────────────────────  │   │
│  │  ┌─────────────────────────────────────────────────────────────┐   │   │
│  │  │  Quality Metadata Tables                                     │   │   │
│  │  │  • dq_test_results    (test execution results)              │   │   │
│  │  │  • dq_quality_scores  (quality scores over time)            │   │   │
│  │  │  • dq_anomalies       (detected anomalies)                  │   │   │
│  │  │  • dq_rule_catalog    (all quality rules)                   │   │   │
│  │  └─────────────────────────────────────────────────────────────┘   │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                              │                                              │
│                              ▼                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  LAYER 4: MONITORING & ALERTING                                     │   │
│  │  ─────────────────────────────────────────────────────────────────  │   │
│  │  ┌────────────┐ ┌────────────┐ ┌────────────┐ ┌────────────┐      │   │
│  │  │ Grafana    │ │ Slack      │ │ Email      │ │ PagerDuty  │      │   │
│  │  │ Dashboard  │ │ Alerts     │ │ Reports    │ │ Escalation │      │   │
│  │  └────────────┘ └────────────┘ └────────────┘ └────────────┘      │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 4. Quality Dimensions

| Dimension | What It Measures | Example |
|-----------|------------------|---------|
| **Completeness** | % of non-null values | customer_email is 99.8% complete |
| **Uniqueness** | No duplicate records | customer_id is 100% unique |
| **Accuracy** | Data matches real-world | Balance matches source system |
| **Consistency** | Same data across systems | Customer name same in all tables |
| **Timeliness** | Data is fresh | Transaction data < 1 hour old |
| **Validity** | Data follows rules | Email format is valid |
| **Integrity** | Referential integrity | All foreign keys exist |

---

## 5. Pipeline Components

```
13-data-quality-pipeline/
│
├── config/                          # Configuration files
│   ├── quality_rules.yml            # All quality rules defined
│   └── alert_thresholds.yml         # Alert thresholds
│
├── checks/                          # Quality check scripts
│   ├── 01_completeness/             # Null checks
│   ├── 02_uniqueness/               # Duplicate checks
│   ├── 03_range/                    # Value range checks
│   ├── 04_freshness/                # Data freshness checks
│   ├── 05_referential/              # Foreign key checks
│   ├── 06_statistical/              # Statistical anomaly detection
│   └── 07_business_rules/           # Custom business logic
│
├── metadata/                        # Quality metadata tables
│   ├── dq_rule_catalog.sql          # All quality rules
│   ├── dq_test_results.sql          # Test execution history
│   ├── dq_quality_scores.sql        # Quality scores
│   ├── dq_anomalies.sql             # Detected anomalies
│   └── dq_quality_dashboard.sql     # Dashboard views
│
├── pipelines/                       # ETL pipelines
│   └── airflow/
│       └── dags/
│           └── data_quality_dag.py  # Airflow DAG
│
├── reports/                         # Quality reports
│   ├── daily_quality_summary.sql    # Daily report
│   ├── weekly_trend_analysis.sql    # Weekly trends
│   └── compliance_report.sql        # SBV compliance
│
├── dashboards/                      # Monitoring
│   └── grafana/
│       ├── quality_overview.json    # Main dashboard
│       └── anomaly_detection.json   # Anomaly dashboard
│
└── README.md                        # This file
```

---

## 6. Tests & Validation

### Test Types

| Test Type | Purpose | Example |
|-----------|---------|---------|
| **Completeness** | Check for NULLs | email is not null |
| **Uniqueness** | Check for duplicates | customer_id is unique |
| **Range** | Check value ranges | amount > 0 and amount < 1B |
| **Freshness** | Check data age | data updated within 24h |
| **Referential** | Check foreign keys | account.customer_id exists |
| **Statistical** | Detect anomalies | amount > 3 standard deviations |
| **Business Rules** | Custom logic | balance = opening + credits - debits |

### Running Tests

```bash
# Run all quality checks
python checks/run_all_checks.py

# Run specific check type
python checks/01_completeness/run_completeness_checks.py

# Run for specific table
python checks/run_all_checks.py --table gold.dim_customer
```

---

## 7. Monitoring & Alerting

### Quality Score Formula

```
Overall Quality Score = (Completeness + Uniqueness + Accuracy + Consistency + Timeliness) / 5

Example:
• Completeness: 99.5%
• Uniqueness: 100%
• Accuracy: 98.8%
• Consistency: 99.2%
• Timeliness: 100%

Overall Score = (99.5 + 100 + 98.8 + 99.2 + 100) / 5 = 99.5%
```

### Alert Thresholds

| Metric | Green | Yellow | Red |
|--------|-------|--------|-----|
| **Quality Score** | > 99% | 95-99% | < 95% |
| **Completeness** | > 99% | 95-99% | < 95% |
| **Freshness** | < 1 hour | 1-24 hours | > 24 hours |
| **Anomaly Count** | 0 | 1-5 | > 5 |

---

## 8. Running the Pipeline

### Manual Run

```bash
# Setup
cd Data-Warehouse-Project
docker-compose -f 01-docker-setup/docker-compose.yml up -d

# Create metadata tables
psql -h localhost -U postgres -d banking_dw -f 13-data-quality-pipeline/metadata/dq_rule_catalog.sql
psql -h localhost -U postgres -d banking_dw -f 13-data-quality-pipeline/metadata/dq_test_results.sql
psql -h localhost -U postgres -d banking_dw -f 13-data-quality-pipeline/metadata/dq_quality_scores.sql
psql -h localhost -U postgres -d banking_dw -f 13-data-quality-pipeline/metadata/dq_anomalies.sql

# Run all checks
python 13-data-quality-pipeline/checks/run_all_checks.py

# View results
psql -h localhost -U postgres -d banking_dw -c "SELECT * FROM dq.vw_quality_dashboard;"
```

### Automated via Airflow

```bash
# Trigger the DQ pipeline
airflow dags trigger data_quality_pipeline

# Check status
airflow dags list-runs -d data_quality_pipeline
```

---

## Summary

| Component | Purpose |
|-----------|---------|
| **Quality Rules** | Define what to check |
| **Quality Checks** | Execute the checks |
| **Quality Store** | Store results |
| **Monitoring** | Visualize quality |
| **Alerting** | Notify on issues |

**Data Quality Pipeline = Trustworthy Data + Compliance + Better Decisions**

---

*Back to: [Main README](../README.md)*
