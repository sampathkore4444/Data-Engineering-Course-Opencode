# Scenario 5: Executive Leadership Dashboards

## Business Problem

The **CEO and Board of Directors** need a **real-time view of key performance indicators (KPIs)** to make strategic decisions. Current reports are **monthly, outdated, and fragmented** across multiple systems.

## Current Pain Points

```
┌─────────────────────────────────────────────────────────────┐
│              BEFORE EXECUTIVE DASHBOARDS                     │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  CEO's Monthly Report:                                      │
│  1. Finance team compiles data (3 days)                     │
│  2. Risk team adds risk metrics (2 days)                    │
│  3. Operations team adds operational KPIs (1 day)           │
│  4. Board secretary creates presentation (1 day)            │
│  5. CEO reviews and presents to Board                       │
│                                                             │
│  ⏱️  Total Time: 7-10 days                                  │
│  ❌ Data Age: 1 month old                                   │
│  😤 CEO: "I need real-time visibility!"                     │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## Solution Architecture

```
┌─────────────────────────────────────────────────────────────┐
│              REAL-TIME EXECUTIVE DASHBOARDS                  │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌─────────┐    ┌─────────┐    ┌─────────┐                 │
│  │ All     │───►│ Dremio  │───►│ CEO     │                 │
│  │ Systems │    │ Virtual │    │ Dashboard│                 │
│  └─────────┘    │ Layer   │    │         │                 │
│                 │         │    │ • KPIs  │                 │
│                 │         │    │ • Trends│                 │
│                 │         │    │ • Alerts│                 │
│                 └─────────┘    └─────────┘                 │
│                                                             │
│  ⏱️  Data Age: Real-time                                    │
│  ✅ Updates: Every 15 minutes                               │
│  😊 CEO: "I can see everything anytime!"                    │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## Key CEO Dashboard KPIs

| Category | KPI | Target | Alert |
|----------|-----|--------|-------|
| **Profitability** | Net Profit Margin | > 2% | < 1.5% |
| **Growth** | Loan Book Growth | > 15% YoY | < 10% |
| **Asset Quality** | NPL Ratio | < 3% | > 5% |
| **Capital** | CAR | > 10% | < 8% |
| **Liquidity** | LCR | > 100% | < 100% |
| **Operations** | Digital Transaction % | > 70% | < 50% |
| **Customer** | Customer Satisfaction | > 4.0/5 | < 3.5 |

## Implementation

### CEO Dashboard Query

```sql
-- See ceo-dashboard.sql for full implementation
SELECT 
    report_date,
    total_assets,
    net_profit_margin,
    npl_ratio,
    car_ratio,
    customer_count
FROM gold.ceo_dashboard
WHERE report_date = CURRENT_DATE;
```

## Dashboard Layout

```
┌─────────────────────────────────────────────────────────────┐
│                    CEO DASHBOARD                             │
│                    January 2024                              │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐          │
│  │ Total   │ │ Net     │ │ NPL     │ │ CAR     │          │
│  │ Assets  │ │ Profit  │ │ Ratio   │ │ Ratio   │          │
│  │ 500T    │ │ 2.1%    │ │ 2.8%    │ │ 12.5%   │          │
│  │ VND ▲   │ │ ▲       │ │ ▼       │ │ ▲       │          │
│  └─────────┘ └─────────┘ └─────────┘ └─────────┘          │
│                                                             │
│  [Chart: Revenue Trend - Last 12 Months]                    │
│  [Chart: Loan Book Growth]                                  │
│  [Chart: Geographic Distribution]                           │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │ STRATEGIC ALERTS                                     │   │
│  │ • NPL ratio improving: 3.2% → 2.8% (▼0.4%)         │   │
│  │ • Digital adoption: 68% → 72% (▲4%)                 │   │
│  │ • New loans disbursed: 2.5T VND (+15% MoM)         │   │
│  │ • Customer complaints: -20% MoM                     │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## Performance Metrics

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Report Generation | 7-10 days | Real-time | 99% faster |
| Data Age | 1 month | Real-time | Always current |
| Board Meeting Prep | 2 days | 5 minutes | 99.6% faster |
| Strategic Decisions | Based on old data | Based on current data | Better outcomes |
