# 08 - Monitoring & Alerting

## Overview

Monitoring is essential for production data warehouses. This folder contains Grafana dashboards, Prometheus metrics, and alerting rules.

---

## Table of Contents

1. [Why Monitoring Matters](#1-why-monitoring-matters)
2. [Architecture](#2-architecture)
3. [Dashboards](#3-dashboards)
4. [Alerts](#4-alerts)
5. [Setup](#5-setup)

---

## 1. Why Monitoring Matters

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    MONITORING IN BANKING                                      │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ❌ WITHOUT MONITORING:                                                     │
│  • ETL job fails silently → No one knows                                    │
│  • Data freshness degrades → Reports are stale                             │
│  • Query performance degrades → Users complain                             │
│  • Disk space fills up → System crashes                                    │
│                                                                             │
│  ✅ WITH MONITORING:                                                        │
│  • Instant alerts when jobs fail                                           │
│  • Track data freshness in real-time                                       │
│  • Performance optimization                                               │
│  • Proactive capacity planning                                            │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    MONITORING ARCHITECTURE                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────────┐    ┌─────────────────────┐                        │
│  │   PostgreSQL        │    │   Airflow            │                        │
│  │   (Data Warehouse)  │    │   (ETL Jobs)         │                        │
│  └──────────┬──────────┘    └──────────┬──────────┘                        │
│             │                          │                                    │
│             ▼                          ▼                                    │
│  ┌─────────────────────┐    ┌─────────────────────┐                        │
│  │   Prometheus        │    │   Prometheus         │                        │
│  │   (Metrics)         │    │   (Metrics)          │                        │
│  └──────────┬──────────┘    └──────────┬──────────┘                        │
│             │                          │                                    │
│             └────────────┬─────────────┘                                    │
│                          ▼                                                  │
│              ┌─────────────────────┐                                        │
│              │   Grafana           │                                        │
│              │   (Dashboards)      │                                        │
│              └─────────────────────┘                                        │
│                          │                                                  │
│                          ▼                                                  │
│              ┌─────────────────────┐                                        │
│              │   Alertmanager      │                                        │
│              │   (Notifications)   │                                        │
│              └─────────────────────┘                                        │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Dashboards

### Dashboard 1: Data Warehouse Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    DATA WAREHOUSE DASHBOARD                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  Row Counts        │  Data Freshness      │  Query Performance     │   │
│  │  ────────────────  │  ──────────────────  │  ──────────────────    │   │
│  │  dim_customer: 10K │  Last Update: 2m ago │  Avg Query: 1.2s       │   │
│  │  dim_account: 25K  │  Status: ✅ Fresh     │  Slow Queries: 3       │   │
│  │  fact_txn: 500K    │  Next Update: 28m    │  P95 Latency: 2.1s     │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  ETL Job Status                                                     │   │
│  │  ─────────────────────────────────────────────────────────────────  │   │
│  │  extract_source_data: ✅ Success (15:30:05)                         │   │
│  │  load_dimensions:     ✅ Success (15:32:10)                         │   │
│  │  load_facts:          ✅ Success (15:35:22)                         │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  Disk Usage             │  Connection Pool                          │   │
│  │  ────────────────       │  ──────────────────                       │   │
│  │  Used: 45.2 GB (60%)    │  Active: 12/50                            │   │
│  │  Free: 30.1 GB          │  Idle: 38/50                              │   │
│  │  Growth: 1.2 GB/day     │  Waiting: 0                               │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Dashboard 2: Data Quality

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    DATA QUALITY DASHBOARD                                     │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  Test Results (Last 24h)                                            │   │
│  │  ─────────────────────────────────────────────────────────────────  │   │
│  │  ✅ Passed: 156    ❌ Failed: 2    ⚠️  Warned: 5                    │   │
│  │                                                                     │   │
│  │  Failed Tests:                                                      │   │
│  │  • not_null_stg_transactions.amount (3 records)                     │   │
│  │  • unique_dim_customer.customer_id (1 duplicate)                    │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  Data Freshness Trend                                               │   │
│  │  ─────────────────────────────────────────────────────────────────  │   │
│  │  24h ago: 2m │ 12h ago: 3m │ 6h ago: 2m │ Now: 2m                 │   │
│  │  Status: ✅ All sources fresh                                        │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 4. Alerts

### Alert Rules

| Alert | Condition | Severity | Action |
|-------|-----------|----------|--------|
| **ETL Job Failed** | Job status = FAILED | 🔴 Critical | Page on-call |
| **Data Stale** | Freshness > 24h | 🟡 Warning | Slack notification |
| **Disk Space Low** | Free < 10% | 🔴 Critical | Page on-call |
| **Slow Query** | Query > 30s | 🟡 Warning | Log for review |
| **Test Failed** | Test = FAIL | 🟡 Warning | Slack notification |

### Alert Examples

```yaml
# prometheus/alerting/alert-rules.yml

groups:
  - name: data_warehouse
    rules:
      - alert: ETLJobFailed
        expr: airflow_dag_run_state{dag_id=~"load_.*"} == "failed"
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "ETL Job Failed: {{ $labels.dag_id }}"
          
      - alert: DataStale
        expr: pg_stat_user_tables.n_tup_ins == 0
        for: 24h
        labels:
          severity: warning
        annotations:
          summary: "Data not updated for 24h"
```

---

## 5. Setup

### Docker Compose

```yaml
# Add to docker-compose.yml

  prometheus:
    image: prom/prometheus:latest
    ports:
      - "9090:9090"
    volumes:
      - ./08-monitoring/prometheus:/etc/prometheus

  grafana:
    image: grafana/grafana:latest
    ports:
      - "3000:3000"
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin
    volumes:
      - ./08-monitoring/grafana:/var/lib/grafana

  alertmanager:
    image: prom/alertmanager:latest
    ports:
      - "9093:9093"
    volumes:
      - ./08-monitoring/alerting:/etc/alertmanager
```

### Access

- **Grafana:** http://localhost:3000 (admin/admin)
- **Prometheus:** http://localhost:9090
- **Alertmanager:** http://localhost:9093

---

## Summary

| Component | Purpose | Tool |
|-----------|---------|------|
| **Metrics** | Collect performance data | Prometheus |
| **Dashboards** | Visualize metrics | Grafana |
| **Alerts** | Notify on issues | Alertmanager |
| **Logs** | Debug issues | ELK Stack |

**Monitoring = Proactive Operations**

---

*Back to: [Main README](../README.md)*
