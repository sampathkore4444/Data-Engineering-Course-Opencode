# Monitoring - Banking Data Platform

## Overview
This folder contains monitoring configuration for the banking data platform.

## Monitoring Stack

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    BANKING DATA PLATFORM MONITORING                      │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐         │
│  │   APPLICATION   │  │   INFRASTRUCTURE│  │   BUSINESS      │         │
│  │   MONITORING    │  │   MONITORING    │  │   MONITORING    │         │
│  ├─────────────────┤  ├─────────────────┤  ├─────────────────┤         │
│  │ • Query Latency │  │ • CPU/Memory    │  │ • Data Quality  │         │
│  │ • ETL Pipeline  │  │ • Disk Usage    │  │ • SLA Compliance│         │
│  │ • API Response  │  │ • Network I/O   │  │ • Fraud Alerts  │         │
│  │ • Error Rates   │  │ • Container Logs│  │ • NPA Tracking  │         │
│  └────────┬────────┘  └────────┬────────┘  └────────┬────────┘         │
│           │                    │                    │                   │
│           └────────────────────┼────────────────────┘                   │
│                                │                                        │
│                    ┌───────────v───────────┐                            │
│                    │   CENTRALIZED         │                            │
│                    │   MONITORING HUB      │                            │
│                    │   (Prometheus/Grafana)│                            │
│                    └───────────────────────┘                            │
└─────────────────────────────────────────────────────────────────────────┘
```

## Key Metrics to Monitor

### Application Metrics

| Metric | Description | Threshold | Action |
|--------|-------------|-----------|--------|
| **Query Latency** | Time to execute SQL queries | < 5 seconds (P95) | Scale Dremio executor |
| **ETL Pipeline Duration** | Time to complete ETL jobs | < 30 minutes | Optimize queries |
| **Failed Queries** | Number of failed queries | < 1% | Investigate errors |
| **Concurrent Users** | Active users in Dremio | < 100 | Add executor nodes |
| **Data Freshness** | Time since last update | < 1 hour | Check CDC/ETL |

### Infrastructure Metrics

| Metric | Description | Threshold | Action |
|--------|-------------|-----------|--------|
| **CPU Utilization** | Average CPU usage | < 80% | Scale horizontally |
| **Memory Usage** | RAM utilization | < 85% | Increase memory |
| **Disk Usage** | Storage consumption | < 70% | Add storage |
| **Network I/O** | Data transfer rate | < 1 Gbps | Check network |
| **Container Health** | Docker container status | All running | Restart failed |

### Business Metrics

| Metric | Description | Threshold | Action |
|--------|-------------|-----------|--------|
| **Data Quality Score** | Percentage of valid records | > 99% | Fix data issues |
| **SLA Compliance** | Reports delivered on time | > 99.9% | Escalate |
| **Fraud Detection Rate** | Fraud caught vs missed | > 95% | Review models |
| **NPA Reporting** | Timely NPA reports | 100% | Ensure compliance |

## Configuration Files

| File | Purpose |
|------|---------|
| `prometheus/prometheus.yml` | Prometheus scrape configuration |
| `grafana/datasources/prometheus.yml` | Grafana datasource setup |
| `alerting/alert-rules.yml` | Alert rules for all services |

## Alert Severity Levels

| Level | Description | Response Time | Escalation |
|-------|-------------|---------------|------------|
| **SEV-1** | Complete platform outage | 15 minutes | CTO, VP Engineering |
| **SEV-2** | Major feature unavailable | 1 hour | Engineering Manager |
| **SEV-3** | Minor feature degradation | 4 hours | Team Lead |
| **SEV-4** | Cosmetic issue | 24 hours | On-call engineer |

## Quick Commands

```bash
# Check platform health
docker-compose ps

# View Dremio logs
docker-compose logs -f dremio-master

# Check Prometheus targets
curl http://localhost:9090/api/v1/targets

# Check Grafana dashboards
curl http://localhost:3000/api/search
```

---

*See also: `../12-docs/runbook.md` for detailed incident response procedures*
