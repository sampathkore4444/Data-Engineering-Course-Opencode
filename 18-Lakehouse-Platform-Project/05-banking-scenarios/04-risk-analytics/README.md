# Scenario 4: Risk Analytics Dashboard

## Business Problem

A bank's risk management team needs **real-time visibility** into credit risk, portfolio quality, and concentration risk to make informed lending decisions and maintain regulatory compliance.

## Current Pain Points

```
┌─────────────────────────────────────────────────────────────┐
│              BEFORE RISK ANALYTICS                           │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  Risk Manager's Daily Routine:                              │
│  1. Request data from finance team (2 days wait)            │
│  2. Download Excel reports from 3 systems                   │
│  3. Manually consolidate in Excel                           │
│  4. Create risk dashboards in PowerPoint                    │
│  5. Present to Risk Committee (weekly)                      │
│                                                             │
│  ⏱️  Data Age: 3-7 days old                                 │
│  ❌ Decisions based on outdated information                 │
│  😤 Manual effort: 20+ hours/week                           │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## Solution Architecture

```
┌─────────────────────────────────────────────────────────────┐
│              REAL-TIME RISK ANALYTICS                        │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌─────────┐    ┌─────────┐    ┌─────────┐                 │
│  │ Loan    │───►│ Dremio  │───►│ Risk    │                 │
│  │ System  │    │ Virtual │    │ Dashboard│                 │
│  ├─────────┤    │ Layer   │    │         │                 │
│  │ Cards   │───►│         │    │ • NPA   │                 │
│  ├─────────┤    │         │    │ • CAR   │                 │
│  │ Accounts│───►│         │    │ • LCR   │                 │
│  └─────────┘    └─────────┘    └─────────┘                 │
│                                                             │
│  ⏱️  Data Age: Real-time                                    │
│  ✅ Decisions based on current data                         │
│  😊 Automated: < 1 hour/week                                │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## Key Risk Metrics

| Metric | Description | Target | Alert Threshold |
|--------|-------------|--------|-----------------|
| **NPL Ratio** | Non-Performing Loans / Total Loans | < 3% | > 5% |
| **CAR** | Capital Adequacy Ratio | > 10% | < 8% |
| **LCR** | Liquidity Coverage Ratio | > 100% | < 100% |
| **Provision Coverage** | Provisions / NPLs | > 70% | < 50% |
| **Single Borrower Limit** | Exposure / Tier 1 Capital | < 15% | > 15% |

## Implementation

### Credit Risk Dashboard

```sql
-- See credit-risk.sql for full implementation
SELECT 
    risk_classification,
    COUNT(*) AS loan_count,
    SUM(principal_outstanding) AS total_exposure,
    AVG(interest_rate) AS avg_interest_rate
FROM gold.credit_risk_dashboard
GROUP BY risk_classification;
```

### NPA Tracking

```sql
-- See npa-tracking.sql for full implementation
SELECT 
    DATE_TRUNC('MONTH', report_date) AS month,
    npa_amount,
    npa_ratio,
    LAG(npa_ratio) OVER (ORDER BY report_date) AS prev_month_npa,
    npa_ratio - LAG(npa_ratio) OVER (ORDER BY report_date) AS npa_change
FROM gold.npa_trend;
```

## Performance Metrics

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Data Age | 3-7 days | Real-time | Always current |
| Report Generation | 2 days | 5 minutes | 99.6% faster |
| Manual Effort | 20 hrs/week | 1 hr/week | 95% reduction |
| Risk Visibility | Monthly | Daily | 30x more frequent |
