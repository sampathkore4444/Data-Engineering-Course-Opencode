# 15 - Monitoring and Observability

## Table of Contents
1. [Monitoring Fundamentals](#1-monitoring-fundamentals)
2. [Data Pipeline Monitoring](#2-data-pipeline-monitoring)
3. [Monitoring Tools](#3-monitoring-tools)
4. [Interview Questions](#4-interview-questions)

---

## 1. Monitoring Fundamentals

### Three Pillars of Observability

`
+--------------------------------------------------+
|              OBSERVABILITY                        |
+--------------------------------------------------+
|                                                  |
|  +------------+  +------------+  +------------+  |
|  |   LOGS     |  |  METRICS   |  |   TRACES   |  |
|  |            |  |            |  |            |  |
|  | What       |  | How much   |  | Where      |  |
|  | happened   |  | / how fast |  | (path)     |  |
|  +------------+  +------------+  +------------+  |
+--------------------------------------------------+
`

### Key Metrics (The Four Golden Signals)

| Signal | Description | Example |
|--------|-------------|---------|
| **Latency** | Time to serve a request | Query execution time |
| **Traffic** | Demand on the system | Queries per second |
| **Errors** | Rate of failed requests | Failed pipelines |
| **Saturation** | How full the resource is | CPU, memory, disk usage |

---

## 2. Data Pipeline Monitoring

### Pipeline Health Metrics

`
Pipeline Health Dashboard:
+--------------------------------------------+
| Metric          | Value  | Status          |
|-----------------|--------|-----------------|
| Pipelines Run   | 45/50  | Warning (90%)   |
| Success Rate    | 98%    | OK              |
| Avg Duration    | 12 min | OK              |
| Data Freshness  | 5 min  | OK              |
| Data Volume     | 1.2 TB | OK              |
| Failed Jobs     | 2      | Alert           |
+--------------------------------------------+
`

### Data Quality Monitoring

`python
# Great Expectations monitoring
import great_expectations as gx
from datetime import datetime

def monitor_data_quality(batch_df):
    results = []
    
    # Completeness
    for col in batch_df.columns:
        completeness = batch_df[col].notna().mean()
        results.append({
            'metric': 'completeness',
            'column': col,
            'value': completeness,
            'threshold': 0.95,
            'status': 'OK' if completeness >= 0.95 else 'ALERT'
        })
    
    # Volume
    row_count = len(batch_df)
    results.append({
        'metric': 'row_count',
        'value': row_count,
        'threshold_min': 1000,
        'status': 'OK' if row_count >= 1000 else 'ALERT'
    })
    
    # Freshness
    max_timestamp = batch_df['updated_at'].max()
    hours_since_update = (datetime.now() - max_timestamp).total_seconds() / 3600
    results.append({
        'metric': 'freshness_hours',
        'value': hours_since_update,
        'threshold': 24,
        'status': 'OK' if hours_since_update < 24 else 'ALERT'
    })
    
    return results
`

### Alert Configuration (YAML)

`yaml
alerts:
  pipeline_failure:
    severity: critical
    channels: [slack, pagerduty]
    message: "Pipeline failed at timestamp"
    
  data_freshness:
    severity: warning
    condition: hours_since_update > 2
    channels: [slack]
    message: "Data is stale - hours since update exceeded threshold"
    
  data_volume_anomaly:
    severity: warning
    condition: abs(current_volume - avg_volume) > 2 * stddev_volume
    channels: [slack, email]
    message: "Volume anomaly detected for table"
    
  cost_spike:
    severity: critical
    condition: daily_cost > 1.5 * avg_daily_cost
    channels: [slack, pagerduty]
    message: "Cost spike detected"
`

---

## 3. Monitoring Tools

### Prometheus + Grafana

`python
# Prometheus metrics in Python
from prometheus_client import Counter, Histogram, Gauge
import time

# Define metrics
pipeline_runs = Counter(
    'pipeline_runs_total', 
    'Total pipeline runs', 
    ['pipeline_name', 'status']
)
pipeline_duration = Histogram(
    'pipeline_duration_seconds', 
    'Pipeline duration', 
    ['pipeline_name']
)
data_volume = Gauge(
    'data_volume_rows', 
    'Data volume', 
    ['table_name']
)

# Instrument pipeline
def run_pipeline(pipeline_name):
    start_time = time.time()
    try:
        # Execute pipeline
        process_data()
        
        pipeline_runs.labels(
            pipeline_name=pipeline_name, 
            status='success'
        ).inc()
        
        pipeline_duration.labels(
            pipeline_name=pipeline_name
        ).observe(time.time() - start_time)
        
        data_volume.labels(
            table_name='fact_orders'
        ).set(row_count)
        
    except Exception as e:
        pipeline_runs.labels(
            pipeline_name=pipeline_name, 
            status='failure'
        ).inc()
        raise e
`

### ELK Stack (Elasticsearch, Logstash, Kibana)

`python
# Python logging to ELK
import logging
from pythonjsonlogger import jsonlogger

logger = logging.getLogger('data_pipeline')
handler = logging.StreamHandler()

# JSON formatter for ELK
formatter = jsonlogger.JsonFormatter(
    fmt='%(asctime) %(name) %(levelname) %(message)',
    rename_fields={'levelname': 'level', 'asctime': 'timestamp'}
)
handler.setFormatter(formatter)
logger.addHandler(handler)

# Log pipeline events
logger.info('pipeline_started', extra={
    'pipeline': 'orders_etl',
    'run_id': '12345',
    'scheduled_time': '2024-01-15T06:00:00Z'
})

logger.info('pipeline_completed', extra={
    'pipeline': 'orders_etl',
    'run_id': '12345',
    'duration_seconds': 360,
    'rows_processed': 1500000
})
`

### Monte Carlo (Data Observability)

`python
# Monte Carlo integration for data quality monitoring
# (Pseudo-code - actual API differs)

# Monitor table freshness
freshness_monitor = {
    'table': 'fact_orders',
    'connection': 'warehouse',
    'threshold_minutes': 60,
    'alert_channels': ['slack', 'pagerduty']
}

# Monitor volume anomalies
volume_monitor = {
    'table': 'fact_orders',
    'connection': 'warehouse',
    'sensitivity': 0.05,  # 5% threshold
    'baseline_days': 30
}

# Monitor schema changes
schema_monitor = {
    'table': 'fact_orders',
    'connection': 'warehouse',
    'alert_on_add': True,
    'alert_on_remove': True,
    'alert_on_type_change': True
}
`

---

## 4. Real-World Monitoring Setup

### Banking Monitoring Dashboard

`
+--------------------------------------------+
|          BANKING DATA PLATFORM             |
|          MONITORING DASHBOARD              |
+--------------------------------------------+
|                                            |
|  Pipeline Status        Data Quality       |
|  +------------------+  +----------------+  |
|  | Running: 12      |  | Accuracy: 99.5%|  |
|  | Success: 45      |  | Compl: 98%     |  |
|  | Failed: 2        |  | Fresh: OK      |  |
|  | Pending: 5       |  | Vol: Normal    |  |
|  +------------------+  +----------------+  |
|                                            |
|  Latency (ms)          Cost ($)            |
|  +------------------+  +----------------+  |
|  | Avg: 150         |  | Today: ,234  |  |
|  | P95: 450         |  | MTD: ,678   |  |
|  | P99: 890         |  | Budget: ,000|  |
|  +------------------+  +----------------+  |
|                                            |
|  Recent Alerts                            |
|  +--------------------------------------+  |
|  | [CRITICAL] Pipeline orders_etl failed |  |
|  | [WARNING] Data freshness 2.5 hours   |  |
|  | [INFO] Warehouse scaled to M5        |  |
|  +--------------------------------------+  |
+--------------------------------------------+
`

---

## 5. Banking Examples

### Example 1: Regulatory Report Monitoring

`python
# Monitor regulatory report generation
def monitor_regulatory_reports():
    reports = {
        'basel_iii': {
            'max_hours': 24,
            'check_fn': check_basel_iii_freshness
        },
        'ccar': {
            'max_hours': 24,
            'check_fn': check_ccar_freshness
        },
        'aml': {
            'max_hours': 4,
            'check_fn': check_aml_alert_freshness
        }
    }
    
    for report_name, config in reports.items():
        status = config['check_fn']()
        if status['stale']:
            send_alert(
                severity='critical',
                message=f'Regulatory report {report_name} is stale: {status["hours"]} hours',
                channels=['pagerduty', 'slack']
            )
        record_metric(f'report_freshness_{report_name}', status['hours'])
`

---

## 6. E-Commerce Examples

### Example 1: E-Commerce SLA Monitoring

`python
# Monitor SLA compliance
def check_ecommerce_slas():
    slas = {
        'inventory_update': {
            'target_minutes': 5, 
            'actual': get_inventory_latency()
        },
        'order_processing': {
            'target_minutes': 15, 
            'actual': get_order_processing_time()
        },
        'recommendation_refresh': {
            'target_minutes': 60, 
            'actual': get_recommendation_refresh_time()
        },
    }
    
    for sla_name, sla_data in slas.items():
        compliance = sla_data['actual'] <= sla_data['target_minutes']
        record_sla_metric(sla_name, compliance, sla_data['actual'])
        
        if not compliance:
            alert_engineering_team(
                f'SLA breach: {sla_name} took {sla_data["actual"]} min '
                f'(target: {sla_data["target_minutes"]})'
            )
`

---

## 7. Interview Questions

### Q1: What are the three pillars of observability and why do they matter?

**Answer:** **Logs:** Structured records of events (what happened). Essential for debugging specific issues. **Metrics:** Numerical measurements over time (how much/how fast). Essential for dashboards, alerting, capacity planning. **Traces:** End-to-end request paths (where time is spent). Essential for distributed systems debugging. Together they provide complete visibility: logs tell you what went wrong, metrics tell you when and how much, traces tell you where the bottleneck is. Without all three, you're flying blind in complex systems.

### Q2: How do you monitor data pipeline health?

**Answer:** Key metrics to monitor: 1) Pipeline success/failure rates (alert on failures). 2) Execution duration (alert on anomalies). 3) Data freshness (alert when tables are stale). 4) Row count anomalies (alert on sudden drops/spikes). 5) Data quality metrics (completeness, accuracy, uniqueness). 6) Resource utilization (CPU, memory, disk). 7) Cost metrics (daily spend, query costs). Use Prometheus/Grafana for infrastructure, Monte Carlo/Sifflet for data-specific observability. Set up tiered alerts: critical (PagerDuty), warning (Slack), info (email).

### Q3: Explain data freshness monitoring and why it is critical.

**Answer:** Data freshness measures how current the data is. Critical because: 1) Business decisions rely on current data (stale inventory = overselling). 2) Regulatory reports have deadlines (stale data = compliance violations). 3) ML models need recent data (stale = degraded predictions). Implementation: Track max timestamp per table, compare against SLA (e.g., must be < 2 hours old), alert when breached. Monitor at table level, not just pipeline level - pipeline success does not guarantee data freshness.

### Q4: Design an alerting strategy for a data platform.

**Answer:** Multi-tier approach: **Tier 1 - Critical (PagerDuty):** Pipeline failures, data loss, security incidents, regulatory report delays. Response within 15 min. **Tier 2 - Warning (Slack):** Data freshness delays, quality degradation, performance anomalies. Response within 2 hours. **Tier 3 - Info (Email/Dashboard):** Resource utilization trends, cost changes. Response daily review. Best practices: Avoid alert fatigue, use anomaly-based thresholds, include context (what, when, impact), provide runbooks for common alerts.

### Q5: How do you monitor data quality in production?

**Answer:** Multi-layer monitoring: 1) Automated checks in pipeline (validate schema, nulls, ranges, uniqueness before loading). 2) Post-load validation (verify row counts, aggregate comparisons). 3) Continuous monitoring (Monte Carlo for anomaly detection on freshness, volume, distribution). 4) Business rule validation (check data meets business constraints). 5) Cross-system reconciliation (compare metrics across systems). 6) User feedback loop (track report errors, data complaints). Dashboard showing quality trends over time; alert on degradation.

---

*Next Section: [16 - Tools and Technologies](../16-Tools-Technologies/README.md)*
