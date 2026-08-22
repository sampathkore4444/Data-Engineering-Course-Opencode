# Utility Scripts - Banking Data Platform

## Overview

This folder contains **4 shell scripts** that automate the lifecycle management of the banking data platform. These scripts handle **setup**, **teardown**, **backup**, and **data seeding** — essential operations for development, testing, and production environments.

---

## Script Summary

| # | Script | Purpose | Usage |
|---|--------|---------|-------|
| 1 | `setup.sh` | One-click platform setup | `./setup.sh [environment]` |
| 2 | `teardown.sh` | Stop and clean up platform | `./teardown.sh [env] [--keep-data]` |
| 3 | `backup.sh` | Backup all platform components | `./backup.sh [component] [retention]` |
| 4 | `seed-all-data.sh` | Load all sample banking data | `./seed-all-data.sh` |

---

## How the Scripts Work Together

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    SCRIPT LIFECYCLE FLOW                                     │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌───────────────────────────────────────────────────────────────────────┐ │
│  │  STEP 1: FIRST-TIME SETUP                                            │ │
│  │  ─────────────────────────                                            │ │
│  │  ./setup.sh dev                                                       │ │
│  │       │                                                               │ │
│  │       ├── Check prerequisites (Docker, Docker Compose)               │ │
│  │       ├── Create directory structure                                 │ │
│  │       ├── Generate .env file                                         │ │
│  │       ├── Start all services (10 containers)                        │ │
│  │       ├── Initialize MinIO buckets                                   │ │
│  │       ├── Initialize Dremio source connection                        │ │
│  │       ├── Initialize Kafka topics (9 topics)                         │ │
│  │       ├── Initialize PostgreSQL schemas                              │ │
│  │       ├── Initialize Airflow admin user                              │ │
│  │       ├── Initialize Grafana datasource                              │ │
│  │       └── Load sample data                                           │ │
│  └───────────────────────────────────────────────────────────────────────┘ │
│                                    │                                        │
│                                    ▼                                        │
│  ┌───────────────────────────────────────────────────────────────────────┐ │
│  │  STEP 2: LOAD MORE DATA (Optional)                                   │ │
│  │  ────────────────────────────────                                     │ │
│  │  ./seed-all-data.sh                                                   │ │
│  │       │                                                               │ │
│  │       ├── Load 5 customers                                           │ │
│  │       ├── Load 6 accounts                                            │ │
│  │       ├── Load 7 transactions                                        │ │
│  │       ├── Load 6 credit cards                                        │ │
│  │       ├── Load 7 card transactions                                   │ │
│  │       ├── Load 5 loans                                               │ │
│  │       ├── Load 6 loan payments                                       │ │
│  │       └── Create Dremio spaces                                       │ │
│  └───────────────────────────────────────────────────────────────────────┘ │
│                                    │                                        │
│                                    ▼                                        │
│  ┌───────────────────────────────────────────────────────────────────────┐ │
│  │  STEP 3: DAILY BACKUPS (Scheduled via cron)                          │ │
│  │  ─────────────────────────────────────────                            │ │
│  │  ./backup.sh all 30                                                   │ │
│  │       │                                                               │ │
│  │       ├── Backup Dremio metadata                                     │ │
│  │       ├── Backup Kafka topics                                        │ │
│  │       ├── Backup MinIO data                                          │ │
│  │       ├── Backup PostgreSQL databases                                │ │
│  │       ├── Backup Airflow DAGs                                        │ │
│  │       ├── Cleanup old backups (>30 days)                             │ │
│  │       └── Create backup summary                                      │ │
│  └───────────────────────────────────────────────────────────────────────┘ │
│                                    │                                        │
│                                    ▼                                        │
│  ┌───────────────────────────────────────────────────────────────────────┐ │
│  │  STEP 4: TEARDOWN (When done)                                        │ │
│  │  ──────────────────────────                                           │ │
│  │  ./teardown.sh dev --keep-data                                        │ │
│  │       │                                                               │ │
│  │       ├── Confirm with user                                          │ │
│  │       ├── Stop all services (reverse order)                          │ │
│  │       ├── Cleanup containers                                         │ │
│  │       ├── Optional: Remove volumes/images/files                      │ │
│  │       └── Print summary                                              │ │
│  └───────────────────────────────────────────────────────────────────────┘ │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Script 1: Setup (`setup.sh`)

### Purpose
**One-click setup** of the entire banking data platform. Installs all 10+ Docker containers, initializes databases, creates topics, and loads sample data.

### Usage

```bash
# Development environment (default)
./setup.sh

# Specific environment
./setup.sh dev
./setup.sh staging
./setup.sh production
```

### What It Does

```
┌─────────────────────────────────────────────────────────────────┐
│                 SETUP SCRIPT FLOW                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  1. CHECK PREREQUISITES                                        │
│     ├── Docker installed? ✅                                   │
│     ├── Docker Compose installed? ✅                           │
│     ├── kubectl detected? (optional)                           │
│     └── helm detected? (optional)                              │
│                                                                 │
│  2. CREATE DIRECTORIES                                          │
│     ├── data/{bronze,silver,gold}                              │
│     ├── logs/{dremio,kafka,airflow}                            │
│     ├── backups/{daily,weekly,monthly}                         │
│     └── config/{dremio,kafka,airflow}                          │
│                                                                 │
│  3. GENERATE .ENV FILE                                         │
│     └── All credentials and configuration                      │
│                                                                 │
│  4. START SERVICES (in order)                                  │
│     ├── MinIO, PostgreSQL, Redis (base services)               │
│     ├── Zookeeper, Kafka (messaging)                           │
│     ├── Dremio master + 2 executors (query engine)             │
│     ├── Airflow webserver, scheduler, worker (orchestration)   │
│     └── Prometheus, Grafana (monitoring)                        │
│                                                                 │
│  5. INITIALIZE COMPONENTS                                      │
│     ├── MinIO: Create 5 buckets                                │
│     ├── Dremio: Create S3 source connection                    │
│     ├── Kafka: Create 9 topics                                 │
│     ├── PostgreSQL: Create 5 schemas                           │
│     ├── Airflow: Create admin user                             │
│     └── Grafana: Add Prometheus datasource                     │
│                                                                 │
│  6. LOAD SAMPLE DATA                                           │
│     └── Load initial banking data into PostgreSQL              │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Services Started

| Service | Container | Port | Purpose |
|---------|-----------|------|---------|
| **MinIO** | minio | 9000/9001 | S3-compatible object storage |
| **PostgreSQL** | postgres | 5432 | Metadata database |
| **Redis** | redis | 6379 | Caching |
| **Zookeeper** | zookeeper | 2181 | Kafka coordination |
| **Kafka** | kafka-1/2/3 | 9092 | Message streaming |
| **Dremio** | dremio-master | 9047 | Query engine |
| **Dremio** | dremio-executor-1/2 | — | Compute nodes |
| **Airflow** | airflow-webserver | 8080 | Workflow UI |
| **Airflow** | airflow-scheduler | — | Job scheduler |
| **Airflow** | airflow-worker | — | Task executor |
| **Prometheus** | prometheus | 9090 | Metrics collection |
| **Grafana** | grafana | 3000 | Dashboards |

### MinIO Buckets Created

| Bucket | Purpose |
|--------|---------|
| `banking-lake` | Main data lake |
| `banking-lake/bronze` | Raw data layer |
| `banking-lake/silver` | Cleansed data layer |
| `banking-lake/gold` | Business-ready layer |
| `banking-backups` | Backup storage |

### Kafka Topics Created

| Topic | Partitions | Replication | Purpose |
|-------|------------|-------------|---------|
| `accounts` | 12 | 3 | Account updates |
| `customers` | 6 | 3 | Customer changes |
| `transactions` | 24 | 3 | New transactions |
| `cards` | 6 | 3 | Card updates |
| `card_transactions` | 24 | 3 | Card transactions |
| `loan_accounts` | 6 | 3 | Loan updates |
| `loan_payments` | 12 | 3 | Payment events |
| `fraud-alerts` | 6 | 3 | Fraud detection |
| `aml-alerts` | 6 | 3 | AML monitoring |

### PostgreSQL Schemas Created

| Schema | Purpose |
|--------|---------|
| `banking_raw` | Raw source data |
| `banking_cleansed` | Cleansed data |
| `banking_gold` | Business-ready data |
| `banking_audit` | Audit logs |
| `banking_security` | Security policies |

### Default Credentials

| Service | Username | Password |
|---------|----------|----------|
| Dremio | admin | Admin@123 |
| Airflow | admin | Admin@123 |
| Grafana | admin | admin |
| MinIO | minioadmin | Minio@123 |
| PostgreSQL | banking | Banking@123 |

### Output

```
==================================================
BANKING DATA PLATFORM SETUP COMPLETE
==================================================

Environment: dev

Services:
  - Dremio: http://localhost:9047
  - Airflow: http://localhost:8080
  - Grafana: http://localhost:3000
  - MinIO: http://localhost:9001
  - Prometheus: http://localhost:9090

Default Credentials:
  - Dremio: admin / Admin@123
  - Airflow: admin / Admin@123
  - Grafana: admin / admin
  - MinIO: minioadmin / Minio@123

Next Steps:
  1. Open Dremio and create spaces
  2. Configure source connections
  3. Enable reflections
  4. Run sample queries
  5. Set up monitoring dashboards
==================================================
```

---

## Script 2: Teardown (`teardown.sh`)

### Purpose
**Stop and clean up** the banking data platform. Stops all containers, optionally removes data, images, and local files.

### Usage

```bash
# Stop services, keep data
./teardown.sh dev --keep-data

# Stop services AND remove all data
./teardown.sh dev --remove-data

# Interactive confirmation
./teardown.sh
```

### What It Does

```
┌─────────────────────────────────────────────────────────────────┐
│                 TEARDOWN SCRIPT FLOW                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  1. CONFIRM WITH USER                                          │
│     └── "Are you sure? (yes/no)"                               │
│                                                                 │
│  2. STOP SERVICES (reverse order)                              │
│     ├── Prometheus, Grafana (monitoring)                        │
│     ├── Airflow webserver, scheduler, worker                   │
│     ├── Dremio master + executors                              │
│     ├── Kafka, Zookeeper                                       │
│     └── MinIO, PostgreSQL, Redis                                │
│                                                                 │
│  3. CLEANUP CONTAINERS                                         │
│     ├── Remove stopped containers                              │
│     └── Remove unused networks                                 │
│                                                                 │
│  4. CLEANUP VOLUMES (if --remove-data)                         │
│     ├── Remove project Docker volumes                          │
│     └── Remove Docker Compose volumes                          │
│                                                                 │
│  5. CLEANUP IMAGES (if --remove-data)                          │
│     └── Remove project Docker images                           │
│                                                                 │
│  6. CLEANUP FILES (if --remove-data)                           │
│     ├── Remove data/ directory                                 │
│     ├── Remove logs/ directory                                 │
│     └── Remove backups/ directory                              │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Flags

| Flag | Behavior |
|------|----------|
| `--keep-data` | Stop containers but preserve all data (default) |
| `--remove-data` | Stop containers AND remove all data, images, files |

### Safety Features

- **Interactive confirmation** before destructive actions
- **Reverse order** shutdown (monitoring → app → database → storage)
- **`--keep-data` default** prevents accidental data loss

---

## Script 3: Backup (`backup.sh`)

### Purpose
**Backup all platform components** including Dremio metadata, Kafka topics, MinIO data, PostgreSQL databases, and Airflow DAGs. Supports retention policies.

### Usage

```bash
# Backup everything, keep 30 days
./backup.sh all 30

# Backup specific component
./backup.sh dremio
./backup.sh kafka
./backup.sh minio
./backup.sh postgres
./backup.sh airflow

# Custom retention
./backup.sh all 90
```

### What It Backs Up

```
┌─────────────────────────────────────────────────────────────────┐
│                 BACKUP COMPONENTS                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  DREMIO BACKUP                                          │   │
│  │  ───────────────                                        │   │
│  │  • Catalog metadata (spaces, sources, views)            │   │
│  │  • Reflection configurations                            │   │
│  │  • Job history                                          │   │
│  │  Output: dremio_catalog_YYYYMMDD_HHMMSS.json            │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  KAFKA BACKUP                                           │   │
│  │  ─────────────                                          │   │
│  │  • All topic data (from beginning)                      │   │
│  │  • Topic configurations                                 │   │
│  │  • Topic list                                           │   │
│  │  Output: kafka_{topic}_YYYYMMDD_HHMMSS.json             │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  MINIO BACKUP                                           │   │
│  │  ─────────────                                          │   │
│  │  • All data in banking-lake bucket                      │   │
│  │  • Recursive copy to backup bucket                      │   │
│  │  • File manifest                                        │   │
│  │  Output: minio_manifest_YYYYMMDD_HHMMSS.txt             │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  POSTGRESQL BACKUP                                      │   │
│  │  ──────────────────                                     │   │
│  │  • Full database dump (pg_dumpall)                      │   │
│  │  • Banking database dump (pg_dump)                      │   │
│  │  • Individual schema dumps (5 schemas)                  │   │
│  │  Output: postgres_{schema}_YYYYMMDD_HHMMSS.sql          │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  AIRFLOW BACKUP                                         │   │
│  │  ───────────────                                        │   │
│  │  • DAGs folder                                          │   │
│  │  • Configuration file                                   │   │
│  │  • Plugins folder                                       │   │
│  │  Output: airflow_dags_YYYYMMDD_HHMMSS/                  │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Backup Structure

```
backups/
├── daily/
│   ├── dremio_catalog_20240115_060000.json
│   ├── dremio_reflections_20240115_060000.json
│   ├── kafka_accounts_20240115_060000.json
│   ├── kafka_customers_20240115_060000.json
│   ├── kafka_transactions_20240115_060000.json
│   ├── minio_manifest_20240115_060000.txt
│   ├── postgres_all_20240115_060000.sql
│   ├── postgres_banking_20240115_060000.sql
│   ├── postgres_banking_raw_20240115_060000.sql
│   ├── postgres_banking_cleansed_20240115_060000.sql
│   ├── postgres_banking_gold_20240115_060000.sql
│   ├── postgres_banking_audit_20240115_060000.sql
│   ├── postgres_banking_security_20240115_060000.sql
│   ├── airflow_dags_20240115_060000/
│   ├── airflow_config_20240115_060000.cfg
│   ├── airflow_plugins_20240115_060000/
│   └── backup_summary_20240115_060000.txt
├── weekly/
└── monthly/
```

### Retention Policy

| Backup Type | Retention | Cleanup |
|-------------|-----------|---------|
| **Daily** | 30 days (default) | `find -mtime +30 -delete` |
| **Weekly** | 210 days (30 × 7) | Automatic |
| **Monthly** | 900 days (30 × 30) | Automatic |

### Backup Summary

```
BACKUP SUMMARY
=============
Timestamp: 20240115_060000
Component: all
Retention: 30 days

Files backed up:
17 files

Total size:
2.5G

Disk usage:
45%
```

---

## Script 4: Seed All Data (`seed-all-data.sh`)

### Purpose
**Load comprehensive sample banking data** into the platform. Creates customers, accounts, transactions, credit cards, loans, and loan payments with realistic Vietnamese banking data.

### Usage

```bash
./seed-all-data.sh
```

### What It Loads

```
┌─────────────────────────────────────────────────────────────────┐
│                 SAMPLE DATA LOADED                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  CUSTOMERS (5 records)                                         │
│  ─────────────────────                                         │
│  ┌──────────┬──────────────────┬────────┬──────────┐          │
│  │ ID       │ Name             │ City   │ Gender   │          │
│  ├──────────┼──────────────────┼────────┼──────────┤          │
│  │ CUST-001 │ Nguyen Van A     │ HCM    │ MALE     │          │
│  │ CUST-002 │ Tran Thi B       │ Ha Noi │ FEMALE   │          │
│  │ CUST-003 │ Le Minh C        │ Da Nang│ MALE     │          │
│  │ CUST-004 │ Pham Hong D      │ Can Tho│ FEMALE   │          │
│  │ CUST-005 │ Hoang Van E      │ Hai Phong│ MALE   │          │
│  └──────────┴──────────────────┴────────┴──────────┘          │
│                                                                 │
│  ACCOUNTS (6 records)                                          │
│  ────────────────────                                          │
│  ┌──────────┬──────────┬─────────┬──────────────┬──────────┐ │
│  │ ID       │ Customer │ Type    │ Balance (VND)│ Branch   │ │
│  ├──────────┼──────────┼─────────┼──────────────┼──────────┤ │
│  │ ACC-001  │ CUST-001 │ SAVINGS │ 150,000,000  │ BR001    │ │
│  │ ACC-002  │ CUST-001 │ CURRENT │ 25,000,000   │ BR001    │ │
│  │ ACC-003  │ CUST-002 │ SAVINGS │ 250,000,000  │ BR002    │ │
│  │ ACC-004  │ CUST-003 │ SAVINGS │ 75,000,000   │ BR003    │ │
│  │ ACC-005  │ CUST-004 │ CURRENT │ 50,000,000   │ BR004    │ │
│  │ ACC-006  │ CUST-005 │ SAVINGS │ 100,000,000  │ BR005    │ │
│  └──────────┴──────────┴─────────┴──────────────┴──────────┘ │
│                                                                 │
│  TRANSACTIONS (7 records)                                      │
│  ─────────────────────────                                     │
│  • Salary credits, bill payments, ATM withdrawals              │
│  • Mobile, online, branch channels                             │
│  • Amounts: 2.5M - 50M VND                                    │
│                                                                 │
│  CREDIT CARDS (6 records)                                      │
│  ─────────────────────────                                     │
│  • VISA and Mastercard                                         │
│  • Limits: 25M - 80M VND                                      │
│  • Utilization: 25% - 62%                                      │
│                                                                 │
│  CARD TRANSACTIONS (7 records)                                 │
│  ─────────────────────────────                                 │
│  • Coffee, electronics, restaurants, online shopping           │
│  • Amounts: 150K - 5M VND                                     │
│                                                                 │
│  LOANS (5 records)                                             │
│  ──────────────────                                            │
│  ┌──────────┬──────────┬─────────┬──────────────┬──────────┐ │
│  │ ID       │ Customer │ Type    │ Outstanding  │ Rate     │ │
│  ├──────────┼──────────┼─────────┼──────────────┼──────────┤ │
│  │ HL-001   │ CUST-001 │ HOME    │ 1,500,000,000│ 9.5%     │ │
│  │ PL-001   │ CUST-002 │ PERSONAL│ 50,000,000   │ 12.0%    │ │
│  │ CL-001   │ CUST-003 │ CAR     │ 300,000,000  │ 8.5%     │ │
│  │ BL-001   │ CUST-004 │ BUSINESS│ 800,000,000  │ 10.0%    │ │
│  │ PL-002   │ CUST-005 │ PERSONAL│ 150,000,000  │ 11.5%    │ │
│  └──────────┴──────────┴─────────┴──────────────┴──────────┘ │
│                                                                 │
│  LOAN PAYMENTS (6 records)                                     │
│  ──────────────────────────                                    │
│  • Auto-debit and bank transfer payments                       │
│  • All successful status                                       │
│                                                                 │
│  DREMIO SPACES (3 spaces)                                      │
│  ────────────────────────                                      │
│  • banking-raw                                                 │
│  • banking-cleansed                                            │
│  • banking-gold                                                │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Output

```
==================================================
ALL SAMPLE DATA LOADED SUCCESSFULLY
==================================================

Data loaded:
  - 5 customers
  - 6 accounts
  - 7 transactions
  - 6 credit cards
  - 7 card transactions
  - 5 loans
  - 6 loan payments

Next steps:
  1. Open Dremio: http://localhost:9047
  2. Create source connection to MinIO
  3. Run sample queries
==================================================
```

---

## Quick Reference

### Common Commands

```bash
# First-time setup
./setup.sh dev

# Load sample data
./seed-all-data.sh

# Daily backup
./backup.sh all 30

# Backup specific component
./backup.sh postgres 30

# Stop (keep data)
./teardown.sh dev --keep-data

# Stop (remove everything)
./teardown.sh dev --remove-data
```

### Scheduling Backups (Cron)

```bash
# Edit crontab
crontab -e

# Daily backup at 2 AM
0 2 * * * /path/to/Lakehouse-Platform-Project/11-scripts/backup.sh all 30 >> /var/log/banking-backup.log 2>&1

# Weekly backup (Sundays)
0 3 * * 0 /path/to/Lakehouse-Platform-Project/11-scripts/backup.sh all 90 >> /var/log/banking-backup.log 2>&1
```

### Troubleshooting

| Issue | Cause | Solution |
|-------|-------|----------|
| `Docker not installed` | Prerequisite missing | Install Docker Desktop |
| `Port already in use` | Another service using port | Stop conflicting service or change port |
| `Container failed to start` | Insufficient resources | Increase Docker memory (8GB+) |
| `Connection refused` | Service not ready | Wait longer or check container logs |
| `Permission denied` | Script not executable | Run `chmod +x *.sh` |

---

## Related Files

| File | Purpose |
|------|---------|
| `setup.sh` | One-click platform setup |
| `teardown.sh` | Stop and clean up platform |
| `backup.sh` | Backup all components |
| `seed-all-data.sh` | Load sample banking data |

---

## Related Documentation

| Document | Location |
|----------|----------|
| Docker Setup | `../01-docker-setup/README.md` |
| Source Systems | `../02-source-systems/` |
| Monitoring | `../08-monitoring/` |

---

*Part of: [Lakehouse Platform Project](../README.md)*
