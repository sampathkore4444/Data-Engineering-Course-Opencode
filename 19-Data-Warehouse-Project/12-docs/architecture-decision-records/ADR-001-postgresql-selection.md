# ADR-001: PostgreSQL Selection

## Status

Accepted

## Context

We need to select a database for our Banking Data Warehouse:
- Budget: $0 (open-source required)
- Data volume: 10M+ rows
- Team: 3 data engineers familiar with SQL
- Requirements: ACID compliance, good performance

## Decision

We will use **PostgreSQL** as the data warehouse database.

## Consequences

### Positive
- ✅ Free and open-source
- ✅ Strong SQL support (CTEs, window functions)
- ✅ Good ecosystem (pgAdmin, dbt, Airflow)
- ✅ ACID compliant
- ✅ Extensible (pgcrypto for encryption)

### Negative
- ❌ Not as fast as columnar databases (Redshift, BigQuery)
- ❌ Limited for very large datasets (100TB+)
- ❌ No built-in columnar storage

## Alternatives Considered

1. **MySQL** - Less feature-rich, weaker analytics support
2. **SQLite** - Not suitable for production multi-user
3. **Amazon Redshift** - Paid, managed service
4. **Google BigQuery** - Paid, serverless

## Review Date

2025-06-01
