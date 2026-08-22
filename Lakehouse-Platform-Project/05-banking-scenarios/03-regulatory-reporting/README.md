# Scenario 3: Regulatory Reporting (SBV Compliance)

## Business Problem

A bank must submit **daily/monthly regulatory reports** to the State Bank of Vietnam (SBV) including Basel III capital adequacy, AML monitoring, and Call Reports. Current process takes **days** due to manual data collection.

## Current Pain Points

```
┌─────────────────────────────────────────────────────────────┐
│              BEFORE AUTOMATED REPORTING                      │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  SBV Report Required: Daily Call Report                     │
│                                                             │
│  Current Process:                                           │
│  1. Finance team downloads data from 5 systems (2 days)     │
│  2. Manually reconcile in Excel (1 day)                     │
│  3. Validate with risk team (1 day)                         │
│  4. Generate report (0.5 day)                               │
│  5. Submit to SBV                                           │
│                                                             │
│  ⏱️  Total Time: 5-7 days                                   │
│  ❌ Risk: Manual errors, late submission penalties           │
│  😤 Team: Overworked, especially month-end                  │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## Solution Architecture

```
┌─────────────────────────────────────────────────────────────┐
│              AUTOMATED REGULATORY REPORTING                  │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  Current Process:                                           │
│  1. Data automatically collected (real-time)                │
│  2. Rules applied automatically (Dremio SQL)                │
│  3. Reports generated automatically (scheduled)             │
│  4. Submitted to SBV portal (automated)                     │
│                                                             │
│  ⏱️  Total Time: 2 hours (automated)                        │
│  ✅ Accuracy: 99.9% (automated validation)                  │
│  😊 Team: Focus on analysis, not data collection            │
│                                                             │
│  ┌─────────┐    ┌─────────┐    ┌─────────┐    ┌─────────┐  │
│  │ Source  │───►│ Dremio  │───►│ Report  │───►│ SBV     │  │
│  │ Systems │    │ Virtual │    │ Engine  │    │ Portal  │  │
│  └─────────┘    └─────────┘    └─────────┘    └─────────┘  │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## Key Reports

| Report | Frequency | SBV Circular | Deadline |
|--------|-----------|--------------|----------|
| **Call Report** | Daily | Circular 23/2014 | T+1 |
| **Basel III CAR** | Monthly | Circular 06/2020 | T+10 |
| **Large Exposure** | Monthly | Circular 23/2014 | T+10 |
| **AML Monitoring** | Daily | Decision 1168/QD-NHNN | T+1 |
| **NPL Report** | Monthly | Circular 06/2020 | T+10 |
| **Liquidity Report** | Weekly | Circular 23/2014 | T+5 |

## Implementation

### Step 1: Create Report Views

```sql
-- See basel-iii-report.sql for full implementation
-- Key metric: Capital Adequacy Ratio (CAR)
SELECT 
    (Tier1_Capital + Tier2_Capital) / Risk_Weighted_Assets * 100 AS CAR
FROM gold.basel_iii_summary
WHERE report_date = CURRENT_DATE;
```

### Step 2: Schedule Reports

```bash
# Airflow DAG runs daily at 6 AM
# See 06-etl-pipelines/airflow/dags/regulatory_reports.py
```

### Step 3: Submit to SBV

```python
# Automated submission to SBV portal
# See 06-etl-pipelines/airflow/dags/sbv_submission.py
```

## Performance Metrics

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Report Generation Time | 5-7 days | 2 hours | 99% faster |
| Manual Effort | 40 hours/week | 5 hours/week | 87.5% reduction |
| Error Rate | 5% | 0.1% | 98% improvement |
| SBV Penalties | 100M VND/year | 0 VND/year | 100% eliminated |
