# 🔄 CDC (Change Data Capture) - Banking Data Warehouse

> **Complete CDC setup with Debezium, Kafka, and PostgreSQL**

---

## 📋 Table of Contents

1. [Overview](#1-overview)
2. [CDC Architecture](#2-cdc-architecture)
3. [Components](#3-components)
4. [Debezium Connectors](#4-debezium-connectors)
5. [Kafka Topics](#5-kafka-topics)
6. [Setup Guide](#6-setup-guide)
7. [Monitoring](#7-monitoring)
8. [Troubleshooting](#8-troubleshooting)

---

## 1. Overview

### What is CDC?

**Change Data Capture (CDC)** captures changes made to data in source databases and streams them to downstream systems in real-time.

| Term | Meaning |
|------|---------|
| **CDC** | Capture INSERT, UPDATE, DELETE operations |
| **Debezium** | Open-source CDC platform built on Kafka Connect |
| **Kafka** | Distributed event streaming platform |
| **Kafka Connect** | Framework for streaming data between Kafka and other systems |

### Why CDC in Banking?

| Use Case | Why CDC? |
|----------|----------|
| **Real-time Dashboards** | Customer balance updates appear instantly |
| **Fraud Detection** | Suspicious transactions flagged immediately |
| **Regulatory Reporting** | Near real-time compliance data |
| **Data Synchronization** | Keep multiple systems in sync |
| **Audit Trail** | Complete history of all changes |

---

## 2. CDC Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    CDC ARCHITECTURE - BANKING DATA WAREHOUSE                 │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  SOURCE SYSTEMS                     KAFKA                        TARGET     │
│  ─────────────                     ─────                        ──────     │
│                                                                             │
│  ┌─────────────────┐              ┌─────────────────┐                      │
│  │  Core Banking   │─────WAL────►│  Debezium       │                      │
│  │  (PostgreSQL)   │              │  Connector      │                      │
│  └─────────────────┘              └────────┬────────┘                      │
│                                            │                               │
│                                            ▼                               │
│  ┌─────────────────┐              ┌─────────────────┐                      │
│  │  Cards System   │─────WAL────►│  Kafka Topics   │                      │
│  │  (PostgreSQL)   │              │  ─────────────  │                      │
│  └─────────────────┘              │  cdc.customers  │                      │
│                                   │  cdc.accounts   │                      │
│  ┌─────────────────┐              │  cdc.trans      │──────►┌───────────┐│
│  │  Loans System   │─────WAL────►│  cdc.cards      │       │ Data      ││
│  │  (PostgreSQL)   │              │  cdc.loans      │       │ Warehouse ││
│  └─────────────────┘              └─────────────────┘       └───────────┘│
│                                                                             │
│  WAL = Write-Ahead Log (PostgreSQL transaction log)                         │
│                                                                             │
│  LATENCY: Seconds to minutes (near real-time)                               │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### How It Works

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    CDC DATA FLOW                                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  1. APPLICATION writes to database                                          │
│     ┌─────────────────┐                                                     │
│     │  INSERT/UPDATE  │                                                     │
│     │  DELETE         │                                                     │
│     └────────┬────────┘                                                     │
│              │                                                              │
│              ▼                                                              │
│  2. POSTGRESQL writes to WAL (Write-Ahead Log)                             │
│     ┌─────────────────┐                                                     │
│     │  WAL File       │                                                     │
│     │  (Transaction   │                                                     │
│     │   Log)          │                                                     │
│     └────────┬────────┘                                                     │
│              │                                                              │
│              ▼                                                              │
│  3. DEBEZIUM reads WAL using replication slot                               │
│     ┌─────────────────┐                                                     │
│     │  Debezium       │                                                     │
│     │  Connector      │                                                     │
│     │  (Kafka Connect)│                                                     │
│     └────────┬────────┘                                                     │
│              │                                                              │
│              ▼                                                              │
│  4. KAFKA publishes change event to topic                                   │
│     ┌─────────────────┐                                                     │
│     │  Kafka Topic    │                                                     │
│     │  (cdc.customers)│                                                     │
│     └────────┬────────┘                                                     │
│              │                                                              │
│              ▼                                                              │
│  5. CDC PIPELINE consumes and loads to Data Warehouse                       │
│     ┌─────────────────┐                                                     │
│     │  Airflow DAG    │                                                     │
│     │  (cdc_pipeline) │                                                     │
│     └────────┬────────┘                                                     │
│              │                                                              │
│              ▼                                                              │
│  6. DATA WAREHOUSE updated                                                  │
│     ┌─────────────────┐                                                     │
│     │  Staging Tables │                                                     │
│     │  (UPSERT/DELETE)│                                                     │
│     └─────────────────┘                                                     │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Components

### Folder Structure

```
04-etl-pipelines/cdc/
│
├── README.md                              # This file
│
├── debezium/                              # Debezium connector configs
│   ├── core-banking-connector.json        # Core banking CDC
│   ├── cards-connector.json               # Cards system CDC
│   └── loans-connector.json               # Loans system CDC
│
├── kafka/                                 # Kafka configuration
│   ├── topics.json                        # Topic definitions
│   └── connect-distributed.properties     # Kafka Connect config
│
├── scripts/                               # Setup scripts
│   └── setup-debezium.sh                  # One-click setup
│
└── metadata/                              # CDC metadata tables
    └── cdc_metadata.sql                   # Tracking tables
```

### Component Summary

| Component | Purpose | Files |
|-----------|---------|-------|
| **Debezium** | Capture changes from PostgreSQL WAL | 3 connector configs |
| **Kafka** | Stream change events | Topics config, Connect config |
| **CDC Pipeline** | Consume and load to DW | Airflow DAG |
| **Metadata** | Track CDC state | SQL tables and views |

---

## 4. Debezium Connectors

### Core Banking Connector

| Setting | Value | Description |
|---------|-------|-------------|
| `connector.class` | PostgreSQLConnector | Debezium PostgreSQL connector |
| `database.hostname` | source-core-banking | Source database host |
| `database.port` | 5432 | PostgreSQL port |
| `database.dbname` | core_banking | Database name |
| `plugin.name` | pgoutput | PostgreSQL logical replication |
| `slot.name` | debezium_slot | Replication slot name |
| `table.include.list` | customers,accounts,transactions | Tables to capture |

### Cards System Connector

| Setting | Value | Description |
|---------|-------|-------------|
| `database.hostname` | source-cards-system | Cards database host |
| `database.dbname` | cards_system | Cards database name |
| `table.include.list` | cards,card_transactions | Cards tables to capture |

### Loans System Connector

| Setting | Value | Description |
|---------|-------|-------------|
| `database.hostname` | source-loans-system | Loans database host |
| `database.dbname` | loans_system | Loans database name |
| `table.include.list` | loans,loan_payments | Loans tables to capture |

### Debezium Event Structure

```json
{
  "before": {
    "customer_id": "C001",
    "customer_name": "John Doe",
    "balance": 5000000
  },
  "after": {
    "customer_id": "C001",
    "customer_name": "John Smith",
    "balance": 5500000
  },
  "source": {
    "version": "2.4.0.Final",
    "connector": "postgresql",
    "name": "core_banking",
    "ts_ms": 1705312345678,
    "snapshot": false,
    "db": "core_banking",
    "schema": "public",
    "table": "customers",
    "txId": 12345,
    "lsn": 234567890
  },
  "op": "u",
  "ts_ms": 1705312345678,
  "transaction": null
}
```

| Field | Description |
|-------|-------------|
| `before` | Record state before change |
| `after` | Record state after change |
| `source` | Source database metadata |
| `op` | Operation: c=create, u=update, d=delete, r=read |
| `ts_ms` | Timestamp in milliseconds |

---

## 5. Kafka Topics

### CDC Topics

| Topic | Partitions | Retention | Description |
|-------|------------|-----------|-------------|
| `cdc.customers` | 3 | 7 days | Customer changes |
| `cdc.accounts` | 3 | 7 days | Account changes |
| `cdc.transactions` | 6 | 3 days | Transaction changes |
| `cdc.cards` | 3 | 7 days | Card changes |
| `cdc.card_transactions` | 6 | 3 days | Card transaction changes |
| `cdc.loans` | 3 | 7 days | Loan changes |
| `cdc.loan_payments` | 3 | 7 days | Loan payment changes |

### Schema History Topics

| Topic | Partitions | Retention | Description |
|-------|------------|-----------|-------------|
| `schema-changes.core_banking` | 1 | Infinite | Schema changes for core banking |
| `schema-changes.cards_system` | 1 | Infinite | Schema changes for cards system |
| `schema-changes.loans_system` | 1 | Infinite | Schema changes for loans system |

---

## 6. Setup Guide

### Prerequisites

```bash
# Ensure Docker is running
docker ps

# Ensure Kafka is running
docker-compose ps kafka

# Ensure source databases are running
docker-compose ps source-core-banking
docker-compose ps source-cards-system
docker-compose ps source-loans-system
```

### Step 1: Start Kafka Connect

```bash
# Start Kafka Connect with Debezium
docker-compose up -d kafka-connect

# Wait for Kafka Connect to be ready
sleep 30

# Verify Kafka Connect is running
curl -s http://localhost:8083/connectors | python -m json.tool
```

### Step 2: Create PostgreSQL Replication Slots

```bash
# Connect to source databases and create replication slots
docker exec -it source-core-banking psql -U postgres -d core_banking -c \
  "SELECT pg_create_logical_replication_slot('debezium_slot', 'pgoutput');"

docker exec -it source-cards-system psql -U postgres -d cards_system -c \
  "SELECT pg_create_logical_replication_slot('debezium_cards_slot', 'pgoutput');"

docker exec -it source-loans-system psql -U postgres -d loans_system -c \
  "SELECT pg_create_logical_replication_slot('debezium_loans_slot', 'pgoutput');"
```

### Step 3: Run Debezium Setup Script

```bash
# Make script executable
chmod +x 04-etl-pipelines/cdc/scripts/setup-debezium.sh

# Run setup script
./04-etl-pipelines/cdc/scripts/setup-debezium.sh
```

### Step 4: Verify Connectors

```bash
# Check connector status
curl -s http://localhost:8083/connectors/core-banking-connector/status | python -m json.tool
curl -s http://localhost:8083/connectors/cards-system-connector/status | python -m json.tool
curl -s http://localhost:8083/connectors/loans-system-connector/status | python -m json.tool

# List all connectors
curl -s http://localhost:8083/connectors | python -m json.tool
```

### Step 5: Test CDC

```bash
# Insert test record in source database
docker exec -it source-core-banking psql -U postgres -d core_banking -c \
  "INSERT INTO customers (customer_id, customer_name, customer_type, phone, email) \
   VALUES ('TEST-001', 'Test Customer', 'individual', '0999999999', 'test@email.com');"

# Check Kafka topic for CDC event
docker exec -it kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic cdc.customers \
  --from-beginning \
  --max-messages 1
```

---

## 7. Monitoring

### Kafka Connect Monitoring

```bash
# List all connectors
curl -s http://localhost:8083/connectors | python -m json.tool

# Get connector status
curl -s http://localhost:8083/connectors/<connector-name>/status | python -m json.tool

# Get connector config
curl -s http://localhost:8083/connectors/<connector-name>/config | python -m json.tool

# Pause connector
curl -X PUT http://localhost:8083/connectors/<connector-name>/pause

# Resume connector
curl -X PUT http://localhost:8083/connectors/<connector-name>/resume

# Delete connector
curl -X DELETE http://localhost:8083/connectors/<connector-name>
```

### Kafka Topic Monitoring

```bash
# List all topics
docker exec -it kafka kafka-topics.sh --bootstrap-server localhost:9092 --list

# Get topic details
docker exec -it kafka kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic cdc.customers

# Get consumer group lag
docker exec -it kafka kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group cdc-consumer
```

### CDC Metadata Monitoring

```sql
-- Check CDC status
SELECT * FROM cdc_metadata.vw_cdc_monitoring;

-- Check CDC errors
SELECT * FROM cdc_metadata.sync_errors 
WHERE error_date = CURRENT_DATE;

-- Check CDC performance
SELECT * FROM cdc_metadata.vw_cdc_performance;

-- Get table CDC status
SELECT * FROM cdc_metadata.get_table_cdc_status('customers');
```

---

## 8. Troubleshooting

### Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| Connector not starting | Kafka Connect not ready | Wait 30 seconds, check logs |
| No CDC events | Replication slot not created | Create slot in source database |
| Events not appearing | Topic not created | Check Kafka topics |
| Connector failed | Configuration error | Check connector config |
| High latency | Large batch operations | Increase task count |

### Debug Commands

```bash
# Check Kafka Connect logs
docker logs kafka-connect -f

# Check Debezium connector logs
docker logs kafka-connect | grep -i debezium

# Check Kafka broker logs
docker logs kafka -f

# Check source database WAL
docker exec -it source-core-banking psql -U postgres -d core_banking -c \
  "SELECT * FROM pg_replication_slots;"
```

### Recovery Procedures

```bash
# Restart a failed connector
curl -X POST http://localhost:8083/connectors/<connector-name>/restart

# Restart a specific task
curl -X POST http://localhost:8083/connectors/<connector-name>/tasks/0/restart

# Delete and recreate connector
curl -X DELETE http://localhost:8083/connectors/<connector-name>
./04-etl-pipelines/cdc/scripts/setup-debezium.sh
```

---

## 📊 Summary

| Component | Purpose | Status |
|-----------|---------|--------|
| **Debezium** | Capture changes from PostgreSQL | ✅ Configured |
| **Kafka** | Stream change events | ✅ Configured |
| **Kafka Connect** | Run Debezium connectors | ✅ Configured |
| **CDC Pipeline** | Consume and load to DW | ✅ Configured |
| **Metadata** | Track CDC state | ✅ Configured |

### Quick Start

```bash
# 1. Start all services
docker-compose up -d

# 2. Run Debezium setup
./04-etl-pipelines/cdc/scripts/setup-debezium.sh

# 3. Verify connectors
curl -s http://localhost:8083/connectors | python -m json.tool

# 4. Test CDC
docker exec -it source-core-banking psql -U postgres -d core_banking -c \
  "INSERT INTO customers VALUES ('TEST-001', 'Test', 'individual', '0999999999', 'test@email.com');"
```

---

*Built with ❤️ for Data Engineers learning CDC with Debezium and Kafka*
