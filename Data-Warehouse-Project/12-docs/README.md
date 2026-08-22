# 12 - Documentation

## Overview

This folder contains all documentation for the Banking Data Warehouse project.

---

## Table of Contents

1. [Documentation Structure](#1-documentation-structure)
2. [Architecture Decision Records (ADRs)](#2-architecture-decision-records)
3. [Runbook](#3-runbook)
4. [Glossary](#4-glossary)
5. [Troubleshooting](#5-troubleshooting)

---

## 1. Documentation Structure

```
12-docs/
│
├── architecture-decision-records/
│   ├── ADR-001-postgresql-selection.md
│   ├── ADR-002-star-schema-design.md
│   └── ADR-003-etl-approach.md
│
├── runbook.md              # Operations manual
├── troubleshooting.md      # Common issues and solutions
└── glossary.md             # Banking & data terms
```

---

## 2. Architecture Decision Records (ADRs)

### ADR-001: PostgreSQL Selection

**Status:** Accepted

**Context:**
- Need a database for data warehouse
- Budget: $0 (open-source)
- Team: 3 data engineers

**Decision:**
Use PostgreSQL as the data warehouse database.

**Consequences:**
- ✅ Free and open-source
- ✅ Strong SQL support
- ✅ Good ecosystem (pgAdmin, dbt)
- ❌ Not as fast as columnar databases (Redshift, BigQuery)
- ❌ Limited for very large datasets (100TB+)

**Alternatives:**
1. MySQL - Less feature-rich
2. SQLite - Not for production
3. Amazon Redshift - Paid, managed

---

### ADR-002: Star Schema Design

**Status:** Accepted

**Context:**
- Need to support BI reporting
- Query patterns: aggregations, joins
- Data volume: 10M+ rows

**Decision:**
Use Star Schema with 5 dimensions and 3 facts.

**Consequences:**
- ✅ Simple queries for analysts
- ✅ Good performance with proper indexing
- ✅ Easy to understand
- ❌ Data duplication in dimensions
- ❌ Slower updates (need full refresh for SCD Type 2)

---

### ADR-003: ETL Approach

**Status:** Accepted

**Context:**
- Data from 3 source systems
- Need daily updates
- Team knows SQL

**Decision:**
Use Airflow for orchestration + SQL for transformations.

**Consequences:**
- ✅ Simple to implement
- ✅ SQL is easy to maintain
- ✅ Good debugging with Airflow UI
- ❌ Not suitable for real-time
- ❌ Manual schema management

---

## 3. Runbook

### Daily Operations

| Time | Task | Command |
|------|------|---------|
| 06:00 | Check Airflow status | `airflow dags list` |
| 06:05 | Trigger ETL | `airflow dags trigger extract_source_data` |
| 06:30 | Check data quality | `dbt test` |
| 07:00 | Verify freshness | `dbt source freshness` |
| 08:00 | Check dashboards | Open Grafana |

### Emergency Procedures

| Issue | Action | Command |
|-------|--------|---------|
| **ETL Failed** | Check logs | `airflow tasks logs <dag_id> <task_id>` |
| **Data Stale** | Manually trigger | `airflow dags trigger <dag_id>` |
| **Disk Full** | Clean old data | `DELETE FROM staging WHERE created_at < NOW() - INTERVAL '30 days'` |
| **Slow Queries** | Check indexes | `SELECT * FROM pg_stat_user_indexes;` |

---

## 4. Glossary

### Banking Terms

| Term | Definition |
|------|------------|
| **KYC** | Know Your Customer - identity verification |
| **AML** | Anti-Money Laundering - detecting illegal money |
| **SCD** | Slowly Changing Dimension - tracking historical changes |
| **SBV** | State Bank of Vietnam - central bank |
| **NPA** | Non-Performing Asset - bad loans |
| **CDR** | Credit Deposit Ratio - lending metric |

### Data Warehouse Terms

| Term | Definition |
|------|------------|
| **Dimension** | Descriptive attribute (who, what, where) |
| **Fact** | Quantitative measurement (how much, how many) |
| **Grain** | Level of detail in a fact table |
| **Surrogate Key** | System-generated unique identifier |
| **Natural Key** | Business meaningful identifier |
| **Staging** | Temporary area for data cleaning |

---

## 5. Troubleshooting

### Common Issues

| Problem | Cause | Solution |
|---------|-------|----------|
| **"relation does not exist"** | Table not created | Run `dbt run` |
| **"column does not exist"** | Schema mismatch | Check `sources.yml` |
| **"permission denied"** | Wrong role | Check RBAC settings |
| **"connection refused"** | PostgreSQL down | `docker-compose up -d postgres` |
| **"disk full"** | No space | Clean old data or add disk |

### Debug Commands

```sql
-- Check table sizes
SELECT pg_size_pretty(pg_total_relation_size('gold.dim_customer'));

-- Check slow queries
SELECT query, calls, mean_time 
FROM pg_stat_statements 
ORDER BY mean_time DESC 
LIMIT 10;

-- Check connections
SELECT count(*) FROM pg_stat_activity;
```

---

## Summary

| Document | Purpose |
|----------|---------|
| **ADRs** | Track architectural decisions |
| **Runbook** | Daily operations guide |
| **Glossary** | Term definitions |
| **Troubleshooting** | Fix common issues |

**Good Documentation = Easy Maintenance**

---

*Back to: [Main README](../README.md)*
